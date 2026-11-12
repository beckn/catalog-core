package publisher

// identity_rules_test.go is the decision table for what a catalogId is
// allowed to do: every (prior state x request) cell, driven the way a
// caller drives it -- Store.LoadCatalogs, Publish, Store.Publish -- and
// checked against both what Publish reported and what ended up in the
// index and the blob store.

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/beckn/catalog-core/pkg/catalog"
	"github.com/beckn/catalog-core/pkg/catalog/store"
)

const (
	tableID    = "example.test/CAT-1"
	tableOther = "other.test/CAT-1" // same last segment as tableID
)

// idWorld is one publisher's blob store plus the fixed publish settings
// every call in a test uses.
type idWorld struct {
	t     *testing.T
	blobs memBlobs
	s     *store.Store
	priv  ed25519.PrivateKey
	keyID string
}

func newIDWorld(t *testing.T) *idWorld {
	t.Helper()
	priv, keyID := testSigningKey(t)
	blobs := memBlobs{}
	return &idWorld{t: t, blobs: blobs, s: store.New(blobs), priv: priv, keyID: keyID}
}

// call runs one publish call end to end and persists whatever it produced,
// exactly as the onix plugin does (it persists even when every submission
// was rejected).
func (w *idWorld) call(subs ...Submission) Result {
	w.t.Helper()
	ctx := context.Background()
	ids := make([]string, 0, len(subs))
	for _, sub := range subs {
		if sub.CatalogID != "" {
			ids = append(ids, sub.CatalogID)
		}
	}
	prior, err := w.s.LoadCatalogs(ctx, ids)
	if err != nil {
		w.t.Fatalf("LoadCatalogs: %v", err)
	}
	result, err := Publish(ctx, Params{
		Catalogs: subs, PriorState: prior, PublishLatest: true, Gzip: true,
		SigningKey: w.priv, KeyID: w.keyID, Domain: "example.test", PublicBaseURL: "https://example.test",
	})
	if err != nil {
		w.t.Fatalf("Publish: %v", err)
	}
	if err := w.s.Publish(ctx, result.Publish); err != nil {
		w.t.Fatalf("store.Publish: %v", err)
	}
	return result
}

// mustCall is call for set-up steps that have to succeed.
func (w *idWorld) mustCall(subs ...Submission) Result {
	w.t.Helper()
	result := w.call(subs...)
	if len(result.Errors) != 0 {
		w.t.Fatalf("set-up publish was rejected: %+v", result.Errors)
	}
	return result
}

// rawEntries returns every index entry's exact bytes, by catalogId.
func (w *idWorld) rawEntries() map[string]string {
	w.t.Helper()
	raw, err := w.blobs.Get(context.Background(), store.IndexPath())
	if err != nil {
		return map[string]string{}
	}
	var idx struct {
		Catalogs []json.RawMessage `json:"catalogs"`
	}
	if err := json.Unmarshal(raw, &idx); err != nil {
		w.t.Fatalf("parsing index: %v", err)
	}
	out := make(map[string]string, len(idx.Catalogs))
	for _, e := range idx.Catalogs {
		var probe struct {
			CatalogID string `json:"catalogId"`
		}
		if err := json.Unmarshal(e, &probe); err != nil {
			w.t.Fatalf("parsing index entry: %v", err)
		}
		if _, dup := out[probe.CatalogID]; dup {
			w.t.Fatalf("index lists %q twice", probe.CatalogID)
		}
		out[probe.CatalogID] = string(e)
	}
	return out
}

func (w *idWorld) entry(id string) (catalog.CatalogEntry, bool) {
	w.t.Helper()
	raw, ok := w.rawEntries()[id]
	if !ok {
		return catalog.CatalogEntry{}, false
	}
	var e catalog.CatalogEntry
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		w.t.Fatalf("parsing entry %s: %v", id, err)
	}
	return e, true
}

// files returns every blob except the index, by path.
func (w *idWorld) files() map[string]string {
	out := make(map[string]string, len(w.blobs))
	for p, b := range w.blobs {
		if p != store.IndexPath() {
			out[p] = string(b)
		}
	}
	return out
}

// latestDoc reads and decompresses id's "latest" file.
func (w *idWorld) latestDoc(id string) catalog.CatalogFileDoc {
	w.t.Helper()
	raw, err := w.blobs.Get(context.Background(), store.LatestFilePath(id, true))
	if err != nil {
		w.t.Fatalf("reading latest of %s: %v", id, err)
	}
	var doc catalog.CatalogFileDoc
	if err := json.Unmarshal(gunzipBytes(w.t, raw), &doc); err != nil {
		w.t.Fatalf("parsing latest of %s: %v", id, err)
	}
	return doc
}

func gunzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	gr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	out, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	return out
}

// changedFiles lists the paths whose content differs between two snapshots
// (added, removed or rewritten), sorted.
func changedFiles(before, after map[string]string) []string {
	var out []string
	for p, b := range after {
		if old, ok := before[p]; !ok || old != b {
			out = append(out, p)
		}
	}
	for p := range before {
		if _, ok := after[p]; !ok {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// tableCatalog always states isActive explicitly, so pausing and resuming
// are both a change of a field that is present before and after.
func tableCatalog(active bool, items ...string) json.RawMessage {
	cat := mustCatalogWithItems("CAT-1", items...)
	return json.RawMessage(strings.Replace(string(cat), `{"id":"CAT-1",`, fmt.Sprintf(`{"id":"CAT-1","isActive":%v,`, active), 1))
}

// Prior states. Each returns the world after set-up.
func priorNone(t *testing.T) *idWorld { return newIDWorld(t) }

// priorActive: published, then updated once -- file version 2, entryVersion 2.
func priorActive(t *testing.T) *idWorld {
	w := newIDWorld(t)
	w.mustCall(Submission{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-1"), NetworkIds: []string{"net-a"}})
	w.mustCall(Submission{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-1", "ITEM-2"), NetworkIds: []string{"net-a"}})
	return w
}

// priorPaused: as priorActive, but the second publish sets isActive false.
func priorPaused(t *testing.T) *idWorld {
	w := newIDWorld(t)
	w.mustCall(Submission{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-1"), NetworkIds: []string{"net-a"}})
	w.mustCall(Submission{CatalogID: tableID, Catalog: tableCatalog(false, "ITEM-1", "ITEM-2"), NetworkIds: []string{"net-a"}})
	return w
}

// priorRetired: as priorActive, then retired -- entryVersion 3.
func priorRetired(t *testing.T) *idWorld {
	w := priorActive(t)
	w.mustCall(Submission{CatalogID: tableID, Directives: PublishDirectives{Retire: true}})
	return w
}

// priorNameTakenLive: tableID was never published, but a different id with
// the same last path segment is live.
func priorNameTakenLive(t *testing.T) *idWorld {
	w := newIDWorld(t)
	w.mustCall(Submission{CatalogID: tableOther, Catalog: tableCatalog(true, "OTHER-1")})
	return w
}

// priorNameTakenRetired: as priorNameTakenLive, but the other id is retired.
func priorNameTakenRetired(t *testing.T) *idWorld {
	w := priorNameTakenLive(t)
	w.mustCall(Submission{CatalogID: tableOther, Directives: PublishDirectives{Retire: true}})
	return w
}

// want is one cell's expected result.
type want struct {
	// reject, when non-empty, means the request must come back as a
	// per-catalog error containing this text, with the index entries and
	// every file left exactly as they were.
	reject string

	// noop means no error and no report change is required beyond "the
	// entry and every file are exactly as they were" (a repeat retire).
	noop bool

	mode         string // Outcome.Mode for an accepted publish
	version      int64  // file-lineage version after the call
	entryVersion int64  // index entry's entryVersion after the call
	retired      bool   // index entry is a tombstone after the call
	isActive     bool   // index entry's isActive (ignored for a tombstone)
	catalogType  string // index entry's catalogType ("" = do not check)
}

func TestIdentityDecisionTable(t *testing.T) {
	publish := func(cat json.RawMessage, edit ...func(*Submission)) Submission {
		sub := Submission{CatalogID: tableID, Catalog: cat, NetworkIds: []string{"net-a"}}
		for _, e := range edit {
			e(&sub)
		}
		return sub
	}
	retire := Submission{CatalogID: tableID, Directives: PublishDirectives{Retire: true}}
	networks := func(n ...string) func(*Submission) { return func(s *Submission) { s.NetworkIds = n } }
	master := func(s *Submission) { s.CatalogType = "MASTER" }
	force := func(s *Submission) { s.Directives.ForceBaseline = true }

	same := tableCatalog(true, "ITEM-1", "ITEM-2")        // what priorActive last published
	samePaused := tableCatalog(false, "ITEM-1", "ITEM-2") // what priorPaused last published
	newer := tableCatalog(true, "ITEM-1", "ITEM-2", "ITEM-3")
	newerPaused := tableCatalog(false, "ITEM-1", "ITEM-2", "ITEM-3")

	const (
		rejectRetired  = "retired"
		rejectNotFound = "not found"
		rejectName     = tableOther // the error must name the id that owns the files
	)

	cells := []struct {
		name  string
		prior func(*testing.T) *idWorld
		req   Submission
		want  want
	}{
		// --- never published -------------------------------------------------
		{"none/publish", priorNone, publish(newer), want{mode: "baseline", version: 1, entryVersion: 1, isActive: true, catalogType: "REGULAR"}},
		{"none/publish paused", priorNone, publish(newerPaused), want{mode: "baseline", version: 1, entryVersion: 1, isActive: false}},
		{"none/publish as MASTER", priorNone, publish(newer, master), want{mode: "baseline", version: 1, entryVersion: 1, isActive: true, catalogType: "MASTER"}},
		{"none/force baseline", priorNone, publish(newer, force), want{mode: "baseline", version: 1, entryVersion: 1, isActive: true}},
		{"none/retire", priorNone, retire, want{reject: rejectNotFound}},

		// --- ACTIVE (file version 2, entryVersion 2) ---------------------------
		{"active/new content", priorActive, publish(newer), want{mode: "change", version: 3, entryVersion: 3, isActive: true}},
		{"active/same content", priorActive, publish(same), want{mode: "unchanged", version: 2, entryVersion: 2, isActive: true}},
		{"active/metadata only", priorActive, publish(same, networks("net-a", "net-b")), want{mode: "metadata", version: 2, entryVersion: 3, isActive: true}},
		{"active/pause", priorActive, publish(samePaused), want{mode: "change", version: 3, entryVersion: 3, isActive: false}},
		{"active/type to MASTER", priorActive, publish(same, master), want{mode: "metadata", version: 2, entryVersion: 3, isActive: true, catalogType: "MASTER"}},
		{"active/force baseline", priorActive, publish(same, force), want{mode: "baseline", version: 3, entryVersion: 3, isActive: true}},
		{"active/retire", priorActive, retire, want{retired: true, entryVersion: 3}},

		// --- PAUSED (file version 2, entryVersion 2) ---------------------------
		{"paused/resume", priorPaused, publish(same), want{mode: "change", version: 3, entryVersion: 3, isActive: true}},
		{"paused/same content", priorPaused, publish(samePaused), want{mode: "unchanged", version: 2, entryVersion: 2, isActive: false}},
		{"paused/new content, still paused", priorPaused, publish(newerPaused), want{mode: "change", version: 3, entryVersion: 3, isActive: false}},
		{"paused/metadata only", priorPaused, publish(samePaused, networks()), want{mode: "metadata", version: 2, entryVersion: 3, isActive: false}},
		{"paused/retire", priorPaused, retire, want{retired: true, entryVersion: 3}},

		// --- RETIRED (entryVersion 3): one-way, CON-TBD-05 / -26 ---------------
		{"retired/new content", priorRetired, publish(newer), want{reject: rejectRetired}},
		{"retired/same content", priorRetired, publish(same), want{reject: rejectRetired}},
		{"retired/metadata only", priorRetired, publish(same, networks("net-b")), want{reject: rejectRetired}},
		{"retired/paused content", priorRetired, publish(samePaused), want{reject: rejectRetired}},
		{"retired/type to MASTER", priorRetired, publish(same, master), want{reject: rejectRetired}},
		{"retired/force baseline", priorRetired, publish(newer, force), want{reject: rejectRetired}},
		{"retired/retire again", priorRetired, retire, want{noop: true}},

		// --- file names already used by another id (live) ----------------------
		{"name taken by live id/publish", priorNameTakenLive, publish(newer), want{reject: rejectName}},
		{"name taken by live id/force baseline", priorNameTakenLive, publish(newer, force), want{reject: rejectName}},
		{"name taken by live id/retire", priorNameTakenLive, retire, want{reject: rejectNotFound}},

		// --- file names already used by another id (retired) -------------------
		{"name taken by retired id/publish", priorNameTakenRetired, publish(newer), want{reject: rejectName}},
		{"name taken by retired id/force baseline", priorNameTakenRetired, publish(newer, force), want{reject: rejectName}},
		{"name taken by retired id/retire", priorNameTakenRetired, retire, want{reject: rejectNotFound}},
	}

	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			w := c.prior(t)
			entriesBefore, filesBefore := w.rawEntries(), w.files()
			before, existed := w.entry(tableID)

			result := w.call(c.req)

			entriesAfter, filesAfter := w.rawEntries(), w.files()
			after, exists := w.entry(tableID)

			if c.want.reject != "" || c.want.noop {
				if c.want.reject != "" {
					if len(result.Errors) != 1 || result.Errors[0].CatalogID != tableID {
						t.Errorf("want one per-catalog error for %s, got Errors=%+v Reports=%+v", tableID, result.Errors, result.Reports)
					} else if !strings.Contains(result.Errors[0].Reason, c.want.reject) {
						t.Errorf("error reason %q does not mention %q", result.Errors[0].Reason, c.want.reject)
					}
				} else if len(result.Errors) != 0 {
					t.Errorf("want no error, got %+v", result.Errors)
				}
				if len(result.Reports) != 0 || len(result.Publish.Updates) != 0 || len(result.Publish.Retirements) != 0 {
					t.Errorf("nothing may be produced: Reports=%d Updates=%d Retirements=%d",
						len(result.Reports), len(result.Publish.Updates), len(result.Publish.Retirements))
				}
				if !equalStringMaps(entriesBefore, entriesAfter) {
					t.Errorf("index entries changed:\n before %v\n after  %v", entriesBefore, entriesAfter)
				}
				if diff := changedFiles(filesBefore, filesAfter); len(diff) != 0 {
					t.Errorf("files changed: %v", diff)
				}
				return
			}

			if len(result.Errors) != 0 {
				t.Fatalf("want accepted, got errors %+v", result.Errors)
			}
			if !exists {
				t.Fatalf("%s is not in the index after an accepted call", tableID)
			}
			if after.EntryVersion != c.want.entryVersion {
				t.Errorf("entryVersion = %d, want %d", after.EntryVersion, c.want.entryVersion)
			}
			if existed && after.EntryVersion < before.EntryVersion {
				t.Errorf("entryVersion regressed %d -> %d (CON-TBD-03)", before.EntryVersion, after.EntryVersion)
			}
			if c.want.catalogType != "" && after.CatalogType != c.want.catalogType {
				t.Errorf("catalogType = %q, want %q", after.CatalogType, c.want.catalogType)
			}
			// CON-TBD-23: no baseline or change file that existed before may
			// have been rewritten. Only "latest" is overwritten in place.
			for _, p := range changedFiles(filesBefore, filesAfter) {
				if _, wasThere := filesBefore[p]; wasThere && !strings.Contains(p, ".latest.") {
					t.Errorf("immutable file %s was overwritten", p)
				}
			}

			if c.want.retired {
				if !after.IsRetired() || after.Baseline.URL != "" || len(after.Changes) != 0 || after.IsActive != nil {
					t.Errorf("want a tombstone, got %+v", after)
				}
				if len(result.Publish.Retirements) != 1 || len(result.Reports) != 0 {
					t.Errorf("want one retirement and no report, got Retirements=%d Reports=%d", len(result.Publish.Retirements), len(result.Reports))
				}
				if doc := w.latestDoc(tableID); doc.RetiredAt == nil || doc.CatalogID != tableID {
					t.Errorf("final latest file: catalogId=%q retiredAt=%v, want %s with retiredAt (CON-TBD-38)", doc.CatalogID, doc.RetiredAt, tableID)
				}
				return
			}

			if after.IsRetired() {
				t.Fatalf("entry is a tombstone after an accepted publish: %+v", after)
			}
			if len(result.Reports) != 1 {
				t.Fatalf("want one report, got %+v", result.Reports)
			}
			rep := result.Reports[0]
			if rep.Mode != c.want.mode || rep.Version != c.want.version || rep.EntryVersion != c.want.entryVersion {
				t.Errorf("report mode=%q version=%d entryVersion=%d, want %q %d %d",
					rep.Mode, rep.Version, rep.EntryVersion, c.want.mode, c.want.version, c.want.entryVersion)
			}
			if after.IsActive == nil || *after.IsActive != c.want.isActive {
				t.Errorf("isActive = %v, want %v", after.IsActive, c.want.isActive)
			}
			if existed && after.Baseline.Version < before.Baseline.Version {
				t.Errorf("baseline.version regressed %d -> %d (CON-TBD-04)", before.Baseline.Version, after.Baseline.Version)
			}
			if doc := w.latestDoc(tableID); doc.CatalogID != tableID || doc.Version != c.want.version || doc.RetiredAt != nil {
				t.Errorf("latest file: catalogId=%q version=%d retiredAt=%v, want %s at version %d",
					doc.CatalogID, doc.Version, doc.RetiredAt, tableID, c.want.version)
			}
			// A different id's entry is never touched by this id's publish.
			for id, raw := range entriesBefore {
				if id != tableID && entriesAfter[id] != raw {
					t.Errorf("entry of %s changed", id)
				}
			}
		})
	}
}

func equalStringMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// TestIdentity_RetiredIdStaysRetiredAcrossFurtherCalls is catalog-core#4 as
// it was observed: publish, update, retire, publish again, update again.
// The tombstone, the v1 baseline and the v2 change file must all survive.
func TestIdentity_RetiredIdStaysRetiredAcrossFurtherCalls(t *testing.T) {
	w := priorRetired(t)
	entriesBefore, filesBefore := w.rawEntries(), w.files()

	for i, cat := range []json.RawMessage{tableCatalog(true, "NEW-1"), tableCatalog(true, "NEW-1", "NEW-2")} {
		result := w.call(Submission{CatalogID: tableID, Catalog: cat})
		if len(result.Errors) != 1 || !strings.Contains(result.Errors[0].Reason, "new catalogId") {
			t.Fatalf("call %d: want a rejection telling the caller to use a new catalogId, got Errors=%+v Reports=%+v", i, result.Errors, result.Reports)
		}
	}
	if !equalStringMaps(entriesBefore, w.rawEntries()) {
		t.Errorf("tombstone changed:\n before %v\n after  %v", entriesBefore, w.rawEntries())
	}
	if diff := changedFiles(filesBefore, w.files()); len(diff) != 0 {
		t.Errorf("files of the retired catalog were rewritten: %v", diff)
	}
}

// TestIdentity_SharedLastSegment is catalog-core#6: two ids that differ only
// before the last "/" would be written to the same files.
func TestIdentity_SharedLastSegment(t *testing.T) {
	for _, tc := range []struct{ name, first, second string }{
		{"different prefix", "a.example/CAT-1", "b.example/CAT-1"},
		{"prefix versus bare id", "a.example/CAT-1", "CAT-1"},
		{"deeper path", "a.example/x/CAT-1", "a.example/y/CAT-1"},
		{"case only, last segment", "a.example/CAT-1", "a.example/cat-1"},
		{"case only, prefix", "a.example/CAT-1", "A.example/CAT-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newIDWorld(t)
			w.mustCall(Submission{CatalogID: tc.first, Catalog: tableCatalog(true, "FIRST-1")})
			entriesBefore, filesBefore := w.rawEntries(), w.files()

			result := w.call(Submission{CatalogID: tc.second, Catalog: tableCatalog(true, "SECOND-1")})
			if len(result.Errors) != 1 || result.Errors[0].CatalogID != tc.second || !strings.Contains(result.Errors[0].Reason, tc.first) {
				t.Fatalf("want %s rejected with a reason naming %s, got Errors=%+v Reports=%+v", tc.second, tc.first, result.Errors, result.Reports)
			}
			if !equalStringMaps(entriesBefore, w.rawEntries()) {
				t.Errorf("index entries changed")
			}
			if diff := changedFiles(filesBefore, w.files()); len(diff) != 0 {
				t.Errorf("files of %s were rewritten: %v", tc.first, diff)
			}

			// The first id keeps working, and its files are still its own.
			w.mustCall(Submission{CatalogID: tc.first, Catalog: tableCatalog(true, "FIRST-1", "FIRST-2")})
			if doc := w.latestDoc(tc.first); doc.CatalogID != tc.first || !bytes.Contains(doc.Catalog, []byte("FIRST-2")) {
				t.Errorf("latest of %s holds catalogId %q", tc.first, doc.CatalogID)
			}
		})
	}
}

// TestIdentity_SharedLastSegmentInOneCall: two new ids in the same call
// that would share files. The first is published, the second rejected.
func TestIdentity_SharedLastSegmentInOneCall(t *testing.T) {
	w := newIDWorld(t)
	result := w.call(
		Submission{CatalogID: "a.example/CAT-1", Catalog: tableCatalog(true, "A-1")},
		Submission{CatalogID: "b.example/CAT-1", Catalog: tableCatalog(true, "B-1")},
		Submission{CatalogID: "b.example/CAT-2", Catalog: tableCatalog(true, "B-2")},
	)
	if len(result.Errors) != 1 || result.Errors[0].CatalogID != "b.example/CAT-1" || !strings.Contains(result.Errors[0].Reason, "a.example/CAT-1") {
		t.Fatalf("want only b.example/CAT-1 rejected, naming a.example/CAT-1; got %+v", result.Errors)
	}
	entries := w.rawEntries()
	if len(entries) != 2 || entries["a.example/CAT-1"] == "" || entries["b.example/CAT-2"] == "" {
		t.Errorf("index should hold a.example/CAT-1 and b.example/CAT-2, got %v", keysOf(entries))
	}
	if doc := w.latestDoc("a.example/CAT-1"); doc.CatalogID != "a.example/CAT-1" {
		t.Errorf("latest holds catalogId %q", doc.CatalogID)
	}
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestIdentity_UnsafeNewIds: an id that is not in the index yet and cannot
// be turned into a file name and URL safely is rejected on its own; the
// valid catalog in the same call is still published.
func TestIdentity_UnsafeNewIds(t *testing.T) {
	long := "a.example/" + strings.Repeat("x", 300)
	for _, id := range []string{
		" ", "a.example/", "/CAT-1", "a.example//CAT-1", "a.example/../CAT-1", "a.example/./CAT-1",
		"a.example/..", "a.example/.", "a.example/.hidden",
		"a.example/CAT 1", " a.example/CAT-1", "a.example/CAT-1 ", "a.example/CAT-1\n", "a.example/CAT\t1",
		"a.example/%41", "a.example/CAT?x", "a.example/CAT#x", `a.example/CAT\x`, "a.example/CAT:1", "a.example/CAT*",
		"a.example/CAT\x00", "a.example/CÄT", "a example/CAT-1", long,
	} {
		t.Run(fmt.Sprintf("%.40q", id), func(t *testing.T) {
			w := newIDWorld(t)
			result := w.call(
				Submission{CatalogID: id, Catalog: tableCatalog(true, "BAD-1")},
				Submission{CatalogID: "a.example/GOOD", Catalog: tableCatalog(true, "GOOD-1")},
			)
			if len(result.Errors) != 1 || result.Errors[0].CatalogID != id || result.Errors[0].Stage != "validate" {
				t.Fatalf("want one validate error for %q, got Errors=%+v", id, result.Errors)
			}
			entries := w.rawEntries()
			if len(entries) != 1 || entries["a.example/GOOD"] == "" {
				t.Errorf("index should hold only a.example/GOOD, got %v", keysOf(entries))
			}
			for p := range w.files() {
				if !strings.Contains(p, "GOOD.") {
					t.Errorf("unexpected file %q", p)
				}
			}
		})
	}
}

// TestIdentity_SafeNewIds: shapes that must keep working.
func TestIdentity_SafeNewIds(t *testing.T) {
	for _, id := range []string{
		"CAT-DEMO-1", "open-economy.nfh.global/electronics-2026", "a.example/x/y/CAT_1", "a.example/CAT.1~b",
		"a.example/" + strings.Repeat("x", 200),
	} {
		t.Run(fmt.Sprintf("%.40q", id), func(t *testing.T) {
			w := newIDWorld(t)
			w.mustCall(Submission{CatalogID: id, Catalog: tableCatalog(true, "ITEM-1")})
			if _, ok := w.entry(id); !ok {
				t.Errorf("%q not in the index", id)
			}
		})
	}
}

// TestIdentity_ExistingIdIsNotRevalidated: the shape rules apply to ids that
// are not in the index yet. A catalog published before the rules existed
// keeps its id, its file names and its URLs: it can still be updated and
// retired.
func TestIdentity_ExistingIdIsNotRevalidated(t *testing.T) {
	const id = "a.example/Old Name:1"
	priv, keyID := testSigningKey(t)
	baseline := catalog.FileEntry{Version: 1, URL: "https://example.test/catalogs/Old Name:1.v1.json", Digest: "sha-256:00", Size: 10}
	prior := map[string]store.CatalogState{id: {
		Catalog: tableCatalog(true, "ITEM-1"), BaselineFile: &baseline, EntryVersion: 1, CatalogType: "REGULAR", IsActive: true,
	}}
	params := Params{PriorState: prior, SigningKey: priv, KeyID: keyID, Domain: "example.test", PublicBaseURL: "https://example.test"}

	params.Catalogs = []Submission{{CatalogID: id, Catalog: tableCatalog(true, "ITEM-1", "ITEM-2")}}
	result, err := Publish(context.Background(), params)
	if err != nil || len(result.Errors) != 0 || len(result.Reports) != 1 || result.Reports[0].Mode != "change" {
		t.Fatalf("update of an existing id: err=%v Errors=%+v Reports=%+v", err, result.Errors, result.Reports)
	}
	var entry catalog.CatalogEntry
	if err := json.Unmarshal(result.Publish.Updates[0].SignedEntry, &entry); err != nil {
		t.Fatal(err)
	}
	if entry.Baseline.URL != baseline.URL {
		t.Errorf("baseline URL changed to %q", entry.Baseline.URL)
	}
	if want := "https://example.test/catalogs/changes/Old Name:1.v2.changes.json"; entry.Changes[0].URL != want {
		t.Errorf("change URL = %q, want %q", entry.Changes[0].URL, want)
	}

	params.Catalogs = []Submission{{CatalogID: id, Directives: PublishDirectives{Retire: true}}}
	result, err = Publish(context.Background(), params)
	if err != nil || len(result.Errors) != 0 || len(result.Publish.Retirements) != 1 {
		t.Fatalf("retire of an existing id: err=%v Errors=%+v Retirements=%d", err, result.Errors, len(result.Publish.Retirements))
	}
}

// TestIdentity_RetireUnknown is catalog-core#5, including the ids a typo
// or an empty string produces.
func TestIdentity_RetireUnknown(t *testing.T) {
	for _, id := range []string{"a.example/NEVER", "a.example/cat-1", "CAT-1", " ", "a.example/"} {
		t.Run(fmt.Sprintf("%q", id), func(t *testing.T) {
			w := newIDWorld(t)
			w.mustCall(Submission{CatalogID: "a.example/CAT-1", Catalog: tableCatalog(true, "ITEM-1")})
			entriesBefore, filesBefore := w.rawEntries(), w.files()

			result := w.call(Submission{CatalogID: id, Directives: PublishDirectives{Retire: true}})
			if len(result.Errors) != 1 || result.Errors[0].CatalogID != id || result.Errors[0].Stage != "retire" || !strings.Contains(result.Errors[0].Reason, "not found") {
				t.Fatalf("want one retire error saying not found, got Errors=%+v Retirements=%d", result.Errors, len(result.Publish.Retirements))
			}
			if len(result.Publish.Retirements) != 0 {
				t.Errorf("a tombstone was signed for an id that was never published")
			}
			if !equalStringMaps(entriesBefore, w.rawEntries()) {
				t.Errorf("index entries changed: %v", keysOf(w.rawEntries()))
			}
			if diff := changedFiles(filesBefore, w.files()); len(diff) != 0 {
				t.Errorf("files changed: %v", diff)
			}
		})
	}
}

// TestIdentity_RetireUnknownHugeID: the id of a refused retire is whatever
// the caller sent; the error must not carry megabytes of it.
func TestIdentity_RetireUnknownHugeID(t *testing.T) {
	w := newIDWorld(t)
	id := "a.example/" + strings.Repeat("x", 1<<20)
	result := w.call(Submission{CatalogID: id, Directives: PublishDirectives{Retire: true}})
	if len(result.Errors) != 1 || len(result.Publish.Retirements) != 0 {
		t.Fatalf("want one error and no tombstone, got %d errors, %d retirements", len(result.Errors), len(result.Publish.Retirements))
	}
	if n := len(result.Errors[0].Reason); n > 400 {
		t.Errorf("reason is %d bytes long", n)
	}
	result = w.call(Submission{CatalogID: id, Catalog: tableCatalog(true, "I-1")})
	if len(result.Errors) != 1 || len(result.Errors[0].Reason) > 400 {
		t.Errorf("publish under a huge new id: %d errors, reason %d bytes", len(result.Errors), len(result.Errors[0].Reason))
	}
}

// TestIdentity_RetireEmptyID: an empty id in a retire is reported, not
// signed into a tombstone with no catalogId.
func TestIdentity_RetireEmptyID(t *testing.T) {
	w := newIDWorld(t)
	result := w.call(Submission{CatalogID: "", Directives: PublishDirectives{Retire: true}})
	if len(result.Errors) != 1 || result.Errors[0].Stage != "retire" || len(result.Publish.Retirements) != 0 {
		t.Fatalf("want one retire error and no tombstone, got Errors=%+v Retirements=%d", result.Errors, len(result.Publish.Retirements))
	}
	if n := len(w.rawEntries()); n != 0 {
		t.Errorf("index has %d entries, want 0", n)
	}
}

// TestIdentity_OneBadCatalogDoesNotPoisonTheBatch: every kind of
// per-catalog rejection next to one valid publish and one valid retire.
func TestIdentity_OneBadCatalogDoesNotPoisonTheBatch(t *testing.T) {
	w := newIDWorld(t)
	w.mustCall(
		Submission{CatalogID: "a.example/LIVE", Catalog: tableCatalog(true, "L-1")},
		Submission{CatalogID: "a.example/GONE", Catalog: tableCatalog(true, "G-1")},
		Submission{CatalogID: "a.example/TO-RETIRE", Catalog: tableCatalog(true, "R-1")},
	)
	w.mustCall(Submission{CatalogID: "a.example/GONE", Directives: PublishDirectives{Retire: true}})

	result := w.call(
		Submission{CatalogID: "a.example/GONE", Catalog: tableCatalog(true, "G-2")},              // retired
		Submission{CatalogID: "b.example/LIVE", Catalog: tableCatalog(true, "X-1")},              // file names taken
		Submission{CatalogID: "a.example/BAD ID", Catalog: tableCatalog(true, "X-2")},            // unsafe new id
		Submission{CatalogID: "a.example/NO-DESCRIPTOR", Catalog: json.RawMessage(`{"id":"x"}`)}, // invalid content
		Submission{CatalogID: "a.example/NEVER", Directives: PublishDirectives{Retire: true}},    // retire unknown
		Submission{CatalogID: "a.example/NEW", Catalog: tableCatalog(true, "N-1")},               // valid, new
		Submission{CatalogID: "a.example/LIVE", Catalog: tableCatalog(true, "L-1", "L-2")},       // valid, update
		Submission{CatalogID: "a.example/TO-RETIRE", Directives: PublishDirectives{Retire: true}},
	)
	rejected := map[string]bool{}
	for _, e := range result.Errors {
		rejected[e.CatalogID] = true
	}
	for _, id := range []string{"a.example/GONE", "b.example/LIVE", "a.example/BAD ID", "a.example/NO-DESCRIPTOR", "a.example/NEVER"} {
		if !rejected[id] {
			t.Errorf("%s was not rejected", id)
		}
	}
	if len(result.Errors) != 5 {
		t.Errorf("want 5 errors, got %+v", result.Errors)
	}
	if e, ok := w.entry("a.example/NEW"); !ok || e.EntryVersion != 1 {
		t.Errorf("a.example/NEW not published: %+v", e)
	}
	if e, ok := w.entry("a.example/LIVE"); !ok || e.EntryVersion != 2 || len(e.Changes) != 1 {
		t.Errorf("a.example/LIVE not updated: %+v", e)
	}
	if e, ok := w.entry("a.example/TO-RETIRE"); !ok || !e.IsRetired() || e.EntryVersion != 2 {
		t.Errorf("a.example/TO-RETIRE not retired: %+v", e)
	}
	if e, ok := w.entry("a.example/GONE"); !ok || !e.IsRetired() || e.EntryVersion != 2 {
		t.Errorf("a.example/GONE is no longer a tombstone at entryVersion 2: %+v", e)
	}
	if got := keysOf(w.rawEntries()); len(got) != 4 {
		t.Errorf("index should hold exactly GONE, LIVE, NEW, TO-RETIRE; got %v", got)
	}
}

// TestIdentity_SameIdTwiceInOneCall pins the existing rule: the first
// submission for an id in a call is the one that counts, whichever kind
// it is.
func TestIdentity_SameIdTwiceInOneCall(t *testing.T) {
	t.Run("two publishes", func(t *testing.T) {
		w := newIDWorld(t)
		result := w.call(
			Submission{CatalogID: tableID, Catalog: tableCatalog(true, "FIRST")},
			Submission{CatalogID: tableID, Catalog: tableCatalog(true, "SECOND")},
		)
		if len(result.Errors) != 0 || len(result.Reports) != 1 || len(result.Publish.Updates) != 1 {
			t.Fatalf("want exactly one publish, got Errors=%+v Reports=%d", result.Errors, len(result.Reports))
		}
		if doc := w.latestDoc(tableID); !bytes.Contains(doc.Catalog, []byte("FIRST")) {
			t.Errorf("the second submission was published")
		}
	})
	t.Run("publish then retire", func(t *testing.T) {
		w := priorActive(t)
		result := w.call(
			Submission{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-9")},
			Submission{CatalogID: tableID, Directives: PublishDirectives{Retire: true}},
		)
		if e, _ := w.entry(tableID); e.IsRetired() || e.EntryVersion != 3 || len(result.Publish.Retirements) != 0 {
			t.Errorf("want the publish to win at entryVersion 3, got %+v", e)
		}
	})
	t.Run("retire then publish", func(t *testing.T) {
		w := priorActive(t)
		result := w.call(
			Submission{CatalogID: tableID, Directives: PublishDirectives{Retire: true}},
			Submission{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-9")},
		)
		if e, _ := w.entry(tableID); !e.IsRetired() || e.EntryVersion != 3 || len(result.Publish.Updates) != 0 {
			t.Errorf("want the retire to win at entryVersion 3, got %+v", e)
		}
	})
}

// TestIdentity_InvalidLifecycleFields: isActive and catalogType decide the
// entry's lifecycle state and crawl order, so a value that cannot be read
// is rejected instead of being replaced by a default.
func TestIdentity_InvalidLifecycleFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		sub  Submission
	}{
		{"isActive is a string", Submission{CatalogID: tableID, Catalog: json.RawMessage(`{"id":"CAT-1","descriptor":{"name":"x"},"isActive":"false"}`)}},
		{"isActive is a number", Submission{CatalogID: tableID, Catalog: json.RawMessage(`{"id":"CAT-1","descriptor":{"name":"x"},"isActive":0}`)}},
		{"unknown catalogType", Submission{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-1"), CatalogType: "master"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newIDWorld(t)
			result := w.call(tc.sub)
			if len(result.Errors) != 1 || result.Errors[0].Stage != "validate" || len(result.Reports) != 0 {
				t.Fatalf("want one validate error, got Errors=%+v Reports=%+v", result.Errors, result.Reports)
			}
			if n := len(w.rawEntries()); n != 0 {
				t.Errorf("index has %d entries, want 0", n)
			}
		})
	}
	t.Run("isActive null is the default", func(t *testing.T) {
		w := newIDWorld(t)
		w.mustCall(Submission{CatalogID: tableID, Catalog: json.RawMessage(`{"id":"CAT-1","descriptor":{"name":"x"},"isActive":null}`)})
		if e, _ := w.entry(tableID); e.IsActive == nil || !*e.IsActive {
			t.Errorf("isActive = %v, want true", e.IsActive)
		}
	})
}

// TestIdentity_FullLifecycleKeepsVersionsMonotonic walks one id through
// every transition and checks entryVersion after each step.
func TestIdentity_FullLifecycleKeepsVersionsMonotonic(t *testing.T) {
	w := newIDWorld(t)
	steps := []struct {
		name string
		sub  Submission
		ev   int64
	}{
		{"publish", Submission{CatalogID: tableID, Catalog: tableCatalog(true, "I-1")}, 1},
		{"republish unchanged", Submission{CatalogID: tableID, Catalog: tableCatalog(true, "I-1")}, 1},
		{"pause", Submission{CatalogID: tableID, Catalog: tableCatalog(false, "I-1")}, 2},
		{"resume", Submission{CatalogID: tableID, Catalog: tableCatalog(true, "I-1")}, 3},
		{"visibility", Submission{CatalogID: tableID, Catalog: tableCatalog(true, "I-1"), NetworkIds: []string{"n"}}, 4},
		{"type", Submission{CatalogID: tableID, Catalog: tableCatalog(true, "I-1"), NetworkIds: []string{"n"}, CatalogType: "MASTER"}, 5},
		{"content", Submission{CatalogID: tableID, Catalog: tableCatalog(true, "I-1", "I-2"), NetworkIds: []string{"n"}, CatalogType: "MASTER"}, 6},
		{"compact", Submission{CatalogID: tableID, Catalog: tableCatalog(true, "I-1", "I-2"), NetworkIds: []string{"n"}, CatalogType: "MASTER", Directives: PublishDirectives{ForceBaseline: true}}, 7},
		{"retire", Submission{CatalogID: tableID, Directives: PublishDirectives{Retire: true}}, 8},
		{"retire again", Submission{CatalogID: tableID, Directives: PublishDirectives{Retire: true}}, 8},
	}
	for _, st := range steps {
		w.mustCall(st.sub)
		if e, _ := w.entry(tableID); e.EntryVersion != st.ev {
			t.Fatalf("after %q: entryVersion = %d, want %d", st.name, e.EntryVersion, st.ev)
		}
	}
	if e, _ := w.entry(tableID); !e.IsRetired() || e.CatalogType != "MASTER" || len(e.NetworkIDs) != 1 {
		t.Errorf("tombstone lost its metadata: %+v", e)
	}
}

// legacyCollision builds an index of the kind catalog-core up to v0.2.1
// could write: tableID (published, updated once) and tableOther (published
// once, later) are both live and both written under "CAT-1.*", so the v1
// baseline and the "latest" file on disk are tableOther's.
func legacyCollision(t *testing.T) *idWorld {
	t.Helper()
	w := priorActive(t)
	other := newIDWorld(t)
	other.mustCall(Submission{CatalogID: tableOther, Catalog: tableCatalog(true, "OTHER-1")})
	for p, b := range other.files() {
		w.blobs[p] = []byte(b)
	}
	var idx struct {
		NodeID   string            `json:"nodeId"`
		Catalogs []json.RawMessage `json:"catalogs"`
	}
	if err := json.Unmarshal(w.blobs[store.IndexPath()], &idx); err != nil {
		t.Fatal(err)
	}
	idx.Catalogs = append(idx.Catalogs, json.RawMessage(other.rawEntries()[tableOther]))
	merged, err := json.Marshal(idx)
	if err != nil {
		t.Fatal(err)
	}
	w.blobs[store.IndexPath()] = merged
	return w
}

// TestIdentity_IndexThatAlreadySharesFiles: the way out of a collision that
// was published before it was checked. Neither id can be published while
// both are live; either can be retired, without touching the shared
// "latest" file; the one that is left can then be published again.
func TestIdentity_IndexThatAlreadySharesFiles(t *testing.T) {
	w := legacyCollision(t)
	entriesBefore, filesBefore := w.rawEntries(), w.files()

	for _, id := range []string{tableID, tableOther} {
		result := w.call(Submission{CatalogID: id, Catalog: tableCatalog(true, "X-1"), Directives: PublishDirectives{ForceBaseline: true}})
		if len(result.Errors) != 1 || !strings.Contains(result.Errors[0].Reason, "retire one of them") {
			t.Fatalf("publish of %s while both are live: want a rejection saying to retire one, got Errors=%+v Reports=%+v", id, result.Errors, result.Reports)
		}
	}
	if !equalStringMaps(entriesBefore, w.rawEntries()) || len(changedFiles(filesBefore, w.files())) != 0 {
		t.Fatalf("a rejected publish changed the index or the files")
	}

	result := w.call(Submission{CatalogID: tableOther, Directives: PublishDirectives{Retire: true}})
	if len(result.Errors) != 0 || len(result.Publish.Retirements) != 1 {
		t.Fatalf("retire of %s: Errors=%+v Retirements=%d", tableOther, result.Errors, len(result.Publish.Retirements))
	}
	if result.Publish.Retirements[0].Latest != nil {
		t.Errorf("retiring %s wrote a tombstone into the latest file it shares with the live %s", tableOther, tableID)
	}
	if diff := changedFiles(filesBefore, w.files()); len(diff) != 0 {
		t.Errorf("retire changed files: %v", diff)
	}
	if e, _ := w.entry(tableOther); !e.IsRetired() {
		t.Errorf("%s is not a tombstone: %+v", tableOther, e)
	}
	if w.rawEntries()[tableID] != entriesBefore[tableID] {
		t.Errorf("entry of %s changed when %s was retired", tableID, tableOther)
	}

	result = w.call(Submission{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-1", "ITEM-2", "ITEM-3"), Directives: PublishDirectives{ForceBaseline: true}})
	if len(result.Errors) != 0 || len(result.Reports) != 1 || result.Reports[0].Mode != "baseline" || result.Reports[0].Version != 3 || result.Reports[0].EntryVersion != 3 {
		t.Fatalf("publish of the surviving id: Errors=%+v Reports=%+v, want a baseline at version 3, entryVersion 3", result.Errors, result.Reports)
	}
	if doc := w.latestDoc(tableID); doc.CatalogID != tableID || doc.Version != 3 {
		t.Errorf("latest holds catalogId %q version %d", doc.CatalogID, doc.Version)
	}

	// The retired id stays retired, and nothing new may take the name.
	result = w.call(Submission{CatalogID: "third.test/CAT-1", Catalog: tableCatalog(true, "T-1")})
	if len(result.Errors) != 1 {
		t.Errorf("a third id with the same last segment was published")
	}
}

// TestIdentity_UnreadableCatalogDoesNotPoisonTheBatch: a catalog whose own
// baseline or change file is missing or corrupt is one catalog's problem.
// The others in the call are published, and the broken one can be given a
// fresh baseline or retired, with its versions continuing.
func TestIdentity_UnreadableCatalogDoesNotPoisonTheBatch(t *testing.T) {
	const good = "example.test/GOOD"
	setUp := func(t *testing.T, damage func(w *idWorld)) *idWorld {
		w := priorActive(t)
		w.mustCall(Submission{CatalogID: good, Catalog: tableCatalog(true, "G-1")})
		damage(w)
		return w
	}
	damages := map[string]func(w *idWorld){
		"baseline missing":    func(w *idWorld) { delete(w.blobs, store.CatalogFilePath(tableID, 1, "json", true)) },
		"baseline not gzip":   func(w *idWorld) { w.blobs[store.CatalogFilePath(tableID, 1, "json", true)] = []byte("not gzip") },
		"change file missing": func(w *idWorld) { delete(w.blobs, store.CatalogFilePath(tableID, 2, "changes.json", true)) },
	}
	for name, damage := range damages {
		t.Run(name+"/update is refused, the other catalog is published", func(t *testing.T) {
			w := setUp(t, damage)
			before := w.rawEntries()[tableID]
			result := w.call(
				Submission{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-9"), NetworkIds: []string{"net-a"}},
				Submission{CatalogID: good, Catalog: tableCatalog(true, "G-1", "G-2")},
			)
			if len(result.Errors) != 1 || result.Errors[0].CatalogID != tableID || result.Errors[0].Stage != "load" || !strings.Contains(result.Errors[0].Reason, "forceBaseline") {
				t.Fatalf("want one load error for %s pointing at forceBaseline, got %+v", tableID, result.Errors)
			}
			if e, _ := w.entry(good); e.EntryVersion != 2 {
				t.Errorf("%s was not updated: %+v", good, e)
			}
			if w.rawEntries()[tableID] != before {
				t.Errorf("entry of the unreadable catalog changed")
			}
		})
		t.Run(name+"/force baseline recovers", func(t *testing.T) {
			w := setUp(t, damage)
			result := w.call(Submission{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-9"), NetworkIds: []string{"net-a"}, Directives: PublishDirectives{ForceBaseline: true}})
			if len(result.Errors) != 0 || len(result.Reports) != 1 || result.Reports[0].Mode != "baseline" || result.Reports[0].Version != 3 || result.Reports[0].EntryVersion != 3 {
				t.Fatalf("want a baseline at version 3, entryVersion 3; got Errors=%+v Reports=%+v", result.Errors, result.Reports)
			}
			// And from then on it diffs normally again.
			result = w.mustCall(Submission{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-9", "ITEM-10"), NetworkIds: []string{"net-a"}})
			if result.Reports[0].Mode != "change" || result.Reports[0].Version != 4 {
				t.Errorf("after recovery: %+v, want a change at version 4", result.Reports[0])
			}
		})
		t.Run(name+"/retire still works", func(t *testing.T) {
			w := setUp(t, damage)
			result := w.call(Submission{CatalogID: tableID, Directives: PublishDirectives{Retire: true}})
			if len(result.Publish.Retirements) != 1 {
				t.Fatalf("no tombstone was signed: %+v", result.Errors)
			}
			if e, _ := w.entry(tableID); !e.IsRetired() || e.EntryVersion != 3 {
				t.Errorf("want a tombstone at entryVersion 3, got %+v", e)
			}
		})
	}
}

// failingBlobs fails every read of one path with an error that is not
// "not found".
type failingBlobs struct {
	memBlobs
	failPath string
}

func (f failingBlobs) Get(ctx context.Context, path string) ([]byte, error) {
	if path == f.failPath {
		return nil, fmt.Errorf("backend unavailable")
	}
	return f.memBlobs.Get(ctx, path)
}

// TestIdentity_BlobStoreFailureIsNotACatalogProblem: a blob store that does
// not answer says nothing about the catalog, so it still fails the call --
// telling the caller to force a baseline would be wrong.
func TestIdentity_BlobStoreFailureIsNotACatalogProblem(t *testing.T) {
	w := priorActive(t)
	s := store.New(failingBlobs{memBlobs: w.blobs, failPath: store.CatalogFilePath(tableID, 1, "json", true)})
	if _, err := s.LoadCatalogs(context.Background(), []string{tableID}); err == nil {
		t.Fatal("LoadCatalogs succeeded although the blob store failed")
	}
}

// TestIdentity_LiveEntryWithoutBaseline: an entry that is listed, not
// retired, and has no baseline (a damaged or foreign index). Its
// entryVersion must continue, and it must be retirable.
func TestIdentity_LiveEntryWithoutBaseline(t *testing.T) {
	seed := func(t *testing.T) *idWorld {
		w := newIDWorld(t)
		w.blobs[store.IndexPath()] = []byte(`{"nodeId":"example.test","catalogs":[{"catalogId":"` + tableID + `","entryVersion":5,"catalogType":"MASTER","isActive":true}]}`)
		return w
	}
	t.Run("publish", func(t *testing.T) {
		w := seed(t)
		result := w.mustCall(Submission{CatalogID: tableID, Catalog: tableCatalog(true, "ITEM-1"), CatalogType: "MASTER"})
		if rep := result.Reports[0]; rep.Mode != "baseline" || rep.Version != 1 || rep.EntryVersion != 6 {
			t.Errorf("got %+v, want a baseline at version 1, entryVersion 6 (CON-TBD-03)", rep)
		}
	})
	t.Run("retire", func(t *testing.T) {
		w := seed(t)
		w.mustCall(Submission{CatalogID: tableID, Directives: PublishDirectives{Retire: true}})
		if e, _ := w.entry(tableID); !e.IsRetired() || e.EntryVersion != 6 || e.CatalogType != "MASTER" {
			t.Errorf("got %+v, want a MASTER tombstone at entryVersion 6", e)
		}
	})
}
