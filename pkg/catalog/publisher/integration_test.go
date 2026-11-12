package publisher

// integration_test.go covers what only shows once the publisher's rules
// (which ids may be published or retired, what "latest" is and where it
// lives) run over the store's two write paths and its write ordering: a
// process that dies between two writes, a catalog whose files or index
// entry cannot be read, and an attribute that a submission drops.

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/beckn/catalog-core/pkg/catalog"
	"github.com/beckn/catalog-core/pkg/catalog/store"
)

// --- a top-level attribute that is dropped ---------------------------------

// publishedContent is what a consumer ends up with for id: the baseline
// with every listed change applied, as the store reconstructs it.
func (w *idWorld) publishedContent(id string) json.RawMessage {
	w.t.Helper()
	states, err := w.s.LoadCatalogs(context.Background(), []string{id})
	if err != nil {
		w.t.Fatalf("LoadCatalogs: %v", err)
	}
	if states[id].Catalog == nil {
		w.t.Fatalf("no published content for %s: %+v", id, states[id])
	}
	return states[id].Catalog
}

func jsonSame(a, b []byte) bool {
	var av, bv any
	return json.Unmarshal(a, &av) == nil && json.Unmarshal(b, &bv) == nil && reflect.DeepEqual(av, bv)
}

// A change file can set a top-level catalog attribute but has no way to
// remove one. A submission that drops an attribute is therefore published
// as a baseline, so that what a consumer reconstructs is what was
// submitted. The case that matters: a catalog paused with isActive false
// and resumed by sending it without isActive, which used to leave the
// published content saying isActive false under an entry saying active.
func TestDroppedAttribute_PublishedContentMatchesSubmission(t *testing.T) {
	for _, tc := range []struct {
		name          string
		before, after string
		wantActive    bool
	}{
		{"resume by omitting isActive", `"isActive":false,`, ``, true},
		{"validity dropped", `"validity":{"endDate":"2027-01-01"},`, ``, true},
		{"one dropped, one changed", `"isActive":false,"note":"a",`, `"note":"b",`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newIDWorld(t)
			body := func(attrs string) json.RawMessage {
				return json.RawMessage(`{"id":"CAT-1",` + attrs + `"descriptor":{"name":"Test"},"provider":{},"resources":[{"id":"I-1","descriptor":{"name":"I-1"}}]}`)
			}
			w.mustCall(Submission{CatalogID: tableID, Catalog: body("")})
			w.mustCall(Submission{CatalogID: tableID, Catalog: body(tc.before)}) // a change file, version 2
			result := w.mustCall(Submission{CatalogID: tableID, Catalog: body(tc.after)})

			if got := w.publishedContent(tableID); !jsonSame(got, body(tc.after)) {
				t.Errorf("published content is not what was submitted:\n got  %s\n want %s", got, body(tc.after))
			}
			if r := result.Reports[0]; r.Mode != "baseline" || r.Version != 3 || r.EntryVersion != 3 {
				t.Errorf("outcome = mode %s version %d entryVersion %d, want a baseline at version 3, entryVersion 3", r.Mode, r.Version, r.EntryVersion)
			}
			e, _ := w.entry(tableID)
			if e.IsActive == nil || *e.IsActive != tc.wantActive {
				t.Errorf("entry isActive = %v, want %v", e.IsActive, tc.wantActive)
			}
			if doc := w.latestDoc(tableID); !jsonSame(doc.Catalog, body(tc.after)) {
				t.Errorf("latest does not carry the submitted content: %s", doc.Catalog)
			}
			// The same submission again changes nothing.
			again := w.mustCall(Submission{CatalogID: tableID, Catalog: body(tc.after)})
			if again.Reports[0].Mode != "unchanged" {
				t.Errorf("repeat: mode %s, want unchanged", again.Reports[0].Mode)
			}
		})
	}
}

// An attribute that is added or changed is still a change file.
func TestChangedAttribute_IsStillAChangeFile(t *testing.T) {
	w := priorActive(t)
	result := w.mustCall(Submission{CatalogID: tableID, Catalog: tableCatalog(false, "ITEM-1", "ITEM-2")})
	if r := result.Reports[0]; r.Mode != "change" {
		t.Errorf("mode %s, want change", r.Mode)
	}
}

// --- a catalog that cannot be read -----------------------------------------

// Retiring a catalog whose baseline cannot be read back: the tombstone
// goes into the index and the final "latest" is built from the "latest"
// file the blob store still holds. No error is reported: a retire that
// did everything must not come back REJECTED.
func TestRetire_UnreadableCatalog_BuildsFinalLatestFromStoredFile(t *testing.T) {
	w := priorActive(t)
	delete(w.blobs, store.CatalogFilePath(tableID, 1, "json", true))

	result := w.call(Submission{CatalogID: tableID, Directives: PublishDirectives{Retire: true}})
	if len(result.Errors) != 0 {
		t.Fatalf("retire reported errors: %+v", result.Errors)
	}
	e, _ := w.entry(tableID)
	if !e.IsRetired() || e.EntryVersion != 3 {
		t.Fatalf("entry = %+v, want a tombstone at entryVersion 3", e)
	}
	doc := w.latestDoc(tableID)
	if doc.RetiredAt == nil || doc.RetiredAt.Format(time.RFC3339Nano) != e.RetiredAt {
		t.Errorf("latest retiredAt = %v, want the entry's %s", doc.RetiredAt, e.RetiredAt)
	}
	if len(result.Retired) != 1 || len(result.Retired[0].Latest) != 1 || !result.Retired[0].Latest[0].Written {
		t.Errorf("retire report = %+v, want the one latest file, written", result.Retired)
	}
}

// When there is nothing to build the final "latest" from either, the
// retire is refused before anything is signed, and says what to do. After
// a forced baseline the retire goes through.
func TestRetire_UnreadableCatalogAndLatest_IsRefusedWithNothingSigned(t *testing.T) {
	w := priorActive(t)
	delete(w.blobs, store.CatalogFilePath(tableID, 1, "json", true))
	w.blobs[store.LatestFilePath(tableID, true)] = []byte("not gzip")
	entryBefore, filesBefore := w.rawEntries()[tableID], w.files()

	result := w.call(Submission{CatalogID: tableID, Directives: PublishDirectives{Retire: true}})
	if len(result.Errors) != 1 || result.Errors[0].Stage != "retire" || !strings.Contains(result.Errors[0].Reason, "forceBaseline") {
		t.Fatalf("errors = %+v, want one retire error pointing at forceBaseline", result.Errors)
	}
	if len(result.Publish.Retirements) != 0 || len(result.Retired) != 0 {
		t.Errorf("a refused retire produced a tombstone: %+v", result.Publish.Retirements)
	}
	if w.rawEntries()[tableID] != entryBefore {
		t.Errorf("the index entry changed")
	}
	if changed := changedFiles(filesBefore, w.files()); len(changed) != 0 {
		t.Errorf("files changed: %v", changed)
	}

	w.mustCall(Submission{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-1"), Directives: PublishDirectives{ForceBaseline: true}})
	w.mustCall(Submission{CatalogID: tableID, Directives: PublishDirectives{Retire: true}})
	if e, _ := w.entry(tableID); !e.IsRetired() {
		t.Errorf("after a forced baseline the retire did not go through: %+v", e)
	}
	if doc := w.latestDoc(tableID); doc.RetiredAt == nil {
		t.Errorf("final latest does not carry retiredAt")
	}
}

// A forced baseline on an unreadable catalog keeps the "latest" file where
// consumers have it, whatever Gzip is set to now.
func TestForceBaseline_UnreadableCatalog_KeepsLatestName(t *testing.T) {
	w := priorActive(t) // published with Gzip on: latest is CAT-1.latest.json.gz
	delete(w.blobs, store.CatalogFilePath(tableID, 1, "json", true))
	before, _ := w.entry(tableID)

	ctx := context.Background()
	prior, base, err := w.s.LoadForPublish(ctx, []string{tableID})
	if err != nil {
		t.Fatal(err)
	}
	result, err := Publish(ctx, Params{
		Catalogs:   []Submission{{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-9"), Directives: PublishDirectives{ForceBaseline: true}}},
		PriorState: prior, PublishLatest: true, Gzip: false, // switched off since
		SigningKey: w.priv, KeyID: w.keyID, Domain: "example.test", PublicBaseURL: "https://example.test",
	})
	if err != nil || len(result.Errors) != 0 {
		t.Fatalf("Publish: %v %+v", err, result.Errors)
	}
	result.Publish.Base = base
	if err := w.s.Publish(ctx, result.Publish); err != nil {
		t.Fatal(err)
	}
	after, _ := w.entry(tableID)
	if after.Latest == nil || after.Latest.URL != before.Latest.URL {
		t.Errorf("latest moved from %s to %+v", before.Latest.URL, after.Latest)
	}
	if _, ok := w.blobs[store.LatestFilePath(tableID, false)]; ok {
		t.Errorf("a second latest file appeared under the other name")
	}
}

// An index entry that does not parse can be neither published (not even
// with forceBaseline: its entryVersion and file versions are unknown) nor
// retired, and does not stop the other catalogs of the call.
func TestMalformedIndexEntry_IsRefusedPerCatalog(t *testing.T) {
	const bad = "example.test/BAD"
	w := priorActive(t)
	raw := string(w.blobs[store.IndexPath()])
	w.blobs[store.IndexPath()] = []byte(strings.Replace(raw, `"catalogs":[`, `"catalogs":[{"catalogId":"`+bad+`","entryVersion":"three"},`, 1))
	entryBefore := w.rawEntries()[bad]
	if entryBefore == "" {
		t.Fatal("set-up: malformed entry not in the index")
	}

	for name, sub := range map[string]Submission{
		"publish":        {CatalogID: bad, Catalog: tableCatalog(true, "X-1")},
		"force baseline": {CatalogID: bad, Catalog: tableCatalog(true, "X-1"), Directives: PublishDirectives{ForceBaseline: true}},
		"retire":         {CatalogID: bad, Directives: PublishDirectives{Retire: true}},
	} {
		t.Run(name, func(t *testing.T) {
			filesBefore := w.files()
			result := w.call(sub, Submission{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-1", "ITEM-2", name)})
			if len(result.Errors) != 1 || result.Errors[0].CatalogID != bad || !strings.Contains(result.Errors[0].Reason, "index entry") {
				t.Fatalf("errors = %+v, want one for %s naming the index entry", result.Errors, bad)
			}
			if len(result.Reports) != 1 || result.Reports[0].CatalogID != tableID || !result.Reports[0].Changed {
				t.Errorf("the other catalog was not published: %+v", result.Reports)
			}
			if w.rawEntries()[bad] != entryBefore {
				t.Errorf("the malformed entry was rewritten")
			}
			for _, p := range changedFiles(filesBefore, w.files()) {
				if strings.Contains(p, "BAD") {
					t.Errorf("a file was written for the malformed catalog: %s", p)
				}
			}
		})
	}
}

// --- a process that dies between two writes --------------------------------

var errDied = errors.New("injected: process died")

// dyingBlobs is a blob store whose Nth write, and every one after it,
// fails without storing anything: the process died there.
type dyingBlobs struct {
	memBlobs
	writes, dieAt int
}

func (d *dyingBlobs) dead() bool {
	d.writes++
	return d.dieAt > 0 && d.writes >= d.dieAt
}

func (d *dyingBlobs) Put(ctx context.Context, path string, content []byte) error {
	if d.dead() {
		return errDied
	}
	return d.memBlobs.Put(ctx, path, content)
}

type dyingCAS struct{ *dyingBlobs }

func (d dyingCAS) GetVersioned(ctx context.Context, path string) ([]byte, string, error) {
	return casBlobs{d.memBlobs}.GetVersioned(ctx, path)
}

func (d dyingCAS) PutIfVersion(ctx context.Context, path string, content []byte, ifVersion string) (string, error) {
	if d.dead() {
		return "", errDied
	}
	return casBlobs{d.memBlobs}.PutIfVersion(ctx, path, content, ifVersion)
}

// crashWorld publishes one catalog through processes that can die.
type crashWorld struct {
	t           *testing.T
	blobs       memBlobs
	conditional bool
	priv        ed25519.PrivateKey
	keyID       string
}

// process returns a Store over the shared blobs that dies at its dieAt-th
// write (0: never), and the counter of writes it made.
func (w *crashWorld) process(dieAt int) (*store.Store, *dyingBlobs) {
	d := &dyingBlobs{memBlobs: w.blobs, dieAt: dieAt}
	if w.conditional {
		return store.New(dyingCAS{d}), d
	}
	return store.New(d), d
}

// call is one publish call in one process. It returns the store error
// (errDied when the process died) and the publisher's result.
func (w *crashWorld) call(s *store.Store, sub Submission) (Result, error) {
	w.t.Helper()
	ctx := context.Background()
	prior, base, err := s.LoadForPublish(ctx, []string{sub.CatalogID})
	if err != nil {
		w.t.Fatalf("LoadForPublish: %v", err)
	}
	result, err := Publish(ctx, Params{
		Catalogs: []Submission{sub}, PriorState: prior, PublishLatest: true,
		SigningKey: w.priv, KeyID: w.keyID, Domain: "example.test", PublicBaseURL: "https://example.test",
	})
	if err != nil {
		w.t.Fatalf("Publish: %v", err)
	}
	if len(result.Errors) != 0 {
		w.t.Fatalf("Publish errors: %+v", result.Errors)
	}
	result.Publish.Base = base
	return result, s.Publish(ctx, result.Publish)
}

// readerCheck is what a crawler relies on at any moment, including right
// after a crash: every baseline and change file the index lists exists
// with the digest its entry states. It returns the entry and whether the
// "latest" file agrees with it: for a live entry, the file's digest is the
// entry's latest.digest; for a retired one, the file carries the entry's
// retiredAt.
func (w *crashWorld) readerCheck(when string) (catalog.CatalogEntry, bool) {
	w.t.Helper()
	var doc struct {
		Catalogs []catalog.CatalogEntry `json:"catalogs"`
	}
	raw, ok := w.blobs[store.IndexPath()]
	if !ok {
		return catalog.CatalogEntry{}, true
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		w.t.Fatalf("%s: index does not parse: %v", when, err)
	}
	if len(doc.Catalogs) != 1 {
		w.t.Fatalf("%s: index lists %d catalogs", when, len(doc.Catalogs))
	}
	e := doc.Catalogs[0]
	latest, hasLatest := w.blobs[store.LatestFilePath(e.CatalogID, false)]
	if e.IsRetired() {
		var file catalog.CatalogFileDoc
		return e, hasLatest && json.Unmarshal(latest, &file) == nil && file.RetiredAt != nil && file.RetiredAt.Format(time.RFC3339Nano) == e.RetiredAt
	}
	check := func(path string, fe catalog.FileEntry) {
		b, ok := w.blobs[path]
		if !ok {
			w.t.Errorf("%s: the index lists %s but it is not stored", when, path)
		} else if "sha-256:"+digestOf(b) != fe.Digest {
			w.t.Errorf("%s: %s does not have the digest its entry states", when, path)
		}
	}
	check(store.CatalogFilePath(e.CatalogID, e.Baseline.Version, "json", false), e.Baseline)
	for _, ch := range e.Changes {
		check(store.CatalogFilePath(e.CatalogID, ch.EffectiveVersion(), "changes.json", false), ch)
	}
	if e.Latest == nil {
		return e, true
	}
	return e, hasLatest && "sha-256:"+digestOf(latest) == e.Latest.Digest
}

// The store-atomicity lane fixed the order of writes (on a conditional
// store: files, index, then an update's "latest"; a retirement's "latest"
// before the index). The latest-lifecycle lane changed what an unchanged
// publish and a repeat retire write, and the identity-rules lane what may
// be published at all. This drives the real publisher over the real store
// and kills the process at every write of an update, of a retire and of a
// repeat retire's repair, on both kinds of store. At each point: a reader
// never sees a listed file that is missing or wrong, and repeating the
// call in a new process completes it, leaving index and "latest" agreeing.
func TestProcessDiesAtEachWrite_RepeatCompletes(t *testing.T) {
	const id = "example.test/CAT-1"
	update := Submission{CatalogID: id, Catalog: mustCatalogWithItems("CAT-1", "I-1", "I-2")}
	retireSub := Submission{CatalogID: id, Directives: PublishDirectives{Retire: true}}

	type scenario struct {
		name string
		// setUp brings the world to the state before the call under test.
		setUp func(w *crashWorld)
		sub   Submission
		// done checks the state after the repeated call completed.
		done func(t *testing.T, w *crashWorld, e catalog.CatalogEntry)
	}
	first := func(w *crashWorld) {
		s, _ := w.process(0)
		if _, err := w.call(s, Submission{CatalogID: id, Catalog: mustCatalogWithItems("CAT-1", "I-1")}); err != nil {
			w.t.Fatalf("set-up: %v", err)
		}
	}
	scenarios := []scenario{
		{"update", first, update, func(t *testing.T, w *crashWorld, e catalog.CatalogEntry) {
			s, _ := w.process(0)
			states, err := s.LoadCatalogs(context.Background(), []string{id})
			if err != nil || !jsonSame(states[id].Catalog, update.Catalog) {
				t.Errorf("published content is not the submission (err %v): %s", err, states[id].Catalog)
			}
			if e.IsRetired() || e.Baseline.Version != 1 || len(e.Changes) != 1 || e.Changes[0].EffectiveVersion() != 2 {
				t.Errorf("entry = baseline v%d with %d changes, want baseline v1 and one change to v2", e.Baseline.Version, len(e.Changes))
			}
		}},
		{"retire", first, retireSub, func(t *testing.T, w *crashWorld, e catalog.CatalogEntry) {
			if !e.IsRetired() {
				t.Errorf("entry is not a tombstone: %+v", e)
			}
		}},
		{"repeat retire repairing latest", func(w *crashWorld) {
			first(w)
			s, _ := w.process(0)
			if _, err := w.call(s, retireSub); err != nil {
				w.t.Fatalf("set-up: %v", err)
			}
			// What an earlier release left behind: the index retired, the
			// "latest" file still live.
			s2 := &crashWorld{t: w.t, blobs: memBlobs{}, priv: w.priv, keyID: w.keyID}
			p, _ := s2.process(0)
			if _, err := s2.call(p, Submission{CatalogID: id, Catalog: mustCatalogWithItems("CAT-1", "I-1")}); err != nil {
				w.t.Fatalf("set-up: %v", err)
			}
			w.blobs[store.LatestFilePath(id, false)] = s2.blobs[store.LatestFilePath(id, false)]
		}, retireSub, func(t *testing.T, w *crashWorld, e catalog.CatalogEntry) {
			if !e.IsRetired() || e.EntryVersion != 2 {
				t.Errorf("entry = %+v, want the original tombstone at entryVersion 2", e)
			}
		}},
	}

	for _, conditional := range []bool{false, true} {
		kind := map[bool]string{false: "plain", true: "conditional"}[conditional]
		for _, sc := range scenarios {
			t.Run(kind+"/"+sc.name, func(t *testing.T) {
				priv, keyID := testSigningKey(t)
				fresh := func() *crashWorld {
					w := &crashWorld{t: t, blobs: memBlobs{}, conditional: conditional, priv: priv, keyID: keyID}
					sc.setUp(w)
					return w
				}
				// How many writes does the call make when nothing goes wrong?
				w := fresh()
				s, counter := w.process(0)
				if _, err := w.call(s, sc.sub); err != nil {
					t.Fatalf("undisturbed call: %v", err)
				}
				total := counter.writes
				if total < 2 {
					t.Fatalf("the call made %d writes; expected at least a file and the index", total)
				}
				if _, agrees := w.readerCheck("undisturbed"); !agrees {
					t.Fatalf("undisturbed call left latest disagreeing with the index")
				}

				for dieAt := 1; dieAt <= total; dieAt++ {
					t.Run(fmt.Sprintf("dies at write %d of %d", dieAt, total), func(t *testing.T) {
						w := fresh()
						w.t = t
						s, _ := w.process(dieAt)
						if _, err := w.call(s, sc.sub); !errors.Is(err, errDied) {
							t.Fatalf("call = %v, want the injected death", err)
						}
						w.readerCheck("after the crash")

						// A new process repeats the call.
						s, _ = w.process(0)
						if _, err := w.call(s, sc.sub); err != nil {
							t.Fatalf("repeat: %v", err)
						}
						e, agrees := w.readerCheck("after the repeat")
						if !agrees {
							t.Errorf("after the repeat, latest does not agree with the index entry")
						}
						sc.done(t, w, e)

						// And once more: nothing left to do, nothing changes.
						s, _ = w.process(0)
						if _, err := w.call(s, sc.sub); err != nil {
							t.Fatalf("second repeat: %v", err)
						}
						if e2, agrees := w.readerCheck("after the second repeat"); !agrees || e2.EntryVersion != e.EntryVersion {
							t.Errorf("second repeat: entryVersion %d -> %d, latest agrees: %v", e.EntryVersion, e2.EntryVersion, agrees)
						}
					})
				}
			})
		}
	}
}

// A retire that died after writing the tombstone into "latest" but before
// the index (the conditional path's order) leaves a live catalog whose
// "latest" says retired. If the publisher then publishes instead of
// retiring again, that publish puts "latest" back in line with the index.
func TestProcessDiesMidRetire_NextPublishRestoresLatest(t *testing.T) {
	const id = "example.test/CAT-1"
	for _, conditional := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "conditional"}[conditional], func(t *testing.T) {
			priv, keyID := testSigningKey(t)
			w := &crashWorld{t: t, blobs: memBlobs{}, conditional: conditional, priv: priv, keyID: keyID}
			sub := Submission{CatalogID: id, Catalog: mustCatalogWithItems("CAT-1", "I-1")}
			s, _ := w.process(0)
			if _, err := w.call(s, sub); err != nil {
				t.Fatal(err)
			}
			// The retire's first write is the "latest" tombstone; it dies at the second (the index).
			s, _ = w.process(2)
			if _, err := w.call(s, Submission{CatalogID: id, Directives: PublishDirectives{Retire: true}}); !errors.Is(err, errDied) {
				t.Fatalf("retire = %v, want the injected death", err)
			}
			e, agrees := w.readerCheck("after the crash")
			if e.IsRetired() || agrees {
				t.Fatalf("set-up: want a live entry whose latest is a tombstone; retired=%v agrees=%v", e.IsRetired(), agrees)
			}

			s, _ = w.process(0)
			result, err := w.call(s, sub) // the same content as before
			if err != nil {
				t.Fatal(err)
			}
			e2, agrees := w.readerCheck("after the publish")
			if !agrees {
				t.Errorf("latest still disagrees with the index entry")
			}
			var file catalog.CatalogFileDoc
			if json.Unmarshal(w.blobs[store.LatestFilePath(id, false)], &file) != nil || file.RetiredAt != nil {
				t.Errorf("latest still carries retiredAt")
			}
			// The entry's latest reference changed, so entryVersion moved.
			if e2.EntryVersion != e.EntryVersion+1 || result.Reports[0].Mode != "metadata" {
				t.Errorf("entryVersion %d -> %d, mode %s; want +1 and metadata", e.EntryVersion, e2.EntryVersion, result.Reports[0].Mode)
			}
		})
	}
}

// --- change files a newer baseline has superseded --------------------------

// After a forced or automatic re-baseline the index keeps listing the
// change files that led up to it, for one next_update period (CON-TBD-32).
// They describe how to get from older versions to the new baseline; they
// are not part of what the catalog is now. The prior state a later publish
// is diffed against must be the new baseline, not the new baseline with
// those older changes applied on top -- which would put back a resource the
// baseline removed, or an older value of one it changed, and the next
// publish would then be diffed against content nobody was served.
func TestSupersededChangeFiles_AreNotAppliedOverTheNewBaseline(t *testing.T) {
	item := func(id, name string) string { return `{"id":"` + id + `","descriptor":{"name":"` + name + `"}}` }
	body := func(items ...string) json.RawMessage {
		return json.RawMessage(`{"id":"CAT-1","descriptor":{"name":"Test"},"provider":{},"resources":[` + strings.Join(items, ",") + `]}`)
	}
	for _, how := range []string{"forceBaseline", "compaction threshold"} {
		t.Run(how, func(t *testing.T) {
			w := newIDWorld(t)
			call := func(cat json.RawMessage, force bool) Result {
				t.Helper()
				ctx := context.Background()
				prior, base, err := w.s.LoadForPublish(ctx, []string{tableID})
				if err != nil {
					t.Fatal(err)
				}
				params := Params{
					Catalogs:   []Submission{{CatalogID: tableID, Catalog: cat, Directives: PublishDirectives{ForceBaseline: force}}},
					PriorState: prior, PublishLatest: true, Gzip: true, NextUpdateIn: time.Hour,
					SigningKey: w.priv, KeyID: w.keyID, Domain: "example.test", PublicBaseURL: "https://example.test",
				}
				if how == "compaction threshold" {
					params.CompactionChangeCountThreshold = 1
				}
				result, err := Publish(ctx, params)
				if err != nil || len(result.Errors) != 0 {
					t.Fatalf("Publish: %v %+v", err, result.Errors)
				}
				result.Publish.Base = base
				if err := w.s.Publish(ctx, result.Publish); err != nil {
					t.Fatal(err)
				}
				return result
			}
			call(body(item("A", "one")), false)                         // baseline v1
			call(body(item("A", "two"), item("B", "b")), false)         // change v2: A renamed, B added
			r := call(body(item("A", "three")), how == "forceBaseline") // baseline v3: A renamed again, B gone
			if r.Reports[0].Mode != "baseline" || r.Reports[0].Version != 3 {
				t.Fatalf("set-up: mode %s version %d, want a baseline at version 3", r.Reports[0].Mode, r.Reports[0].Version)
			}
			if e, _ := w.entry(tableID); len(e.Changes) != 1 {
				t.Fatalf("set-up: the superseded change file should still be listed, got %d changes", len(e.Changes))
			}
			if got := w.publishedContent(tableID); !jsonSame(got, body(item("A", "three"))) {
				t.Errorf("prior state after the re-baseline is not the baseline:\n got  %s", got)
			}

			// Going back to the older content is a real change and must be published as one.
			older := body(item("A", "two"), item("B", "b"))
			r = call(older, false)
			if r.Reports[0].Mode == "unchanged" {
				t.Errorf("publishing the pre-baseline content again was reported unchanged; consumers keep the baseline's content")
			}
			if got := w.publishedContent(tableID); !jsonSame(got, older) {
				t.Errorf("published content is not the submission:\n got  %s\n want %s", got, older)
			}
		})
	}
}
