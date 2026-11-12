package crawlmanager_test

// reader_scenarios_test.go — the reader-side scenario table: publisher state
// (including the states deployed publishers are known to produce in breach
// of RFC NFH-014) x what a crawler must do with it. Each scenario drives the
// real PollIndexes/SyncNext against a simulated publishing node and asserts
// on what the sink ends up serving and what the cursor records.

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beckn/catalog-core/pkg/catalog"
	"github.com/beckn/catalog-core/pkg/catalog/crawler"
	"github.com/beckn/catalog-core/pkg/catalog/crawlmanager"
)

// pubSim is a publishing node whose index and files a test sets directly,
// so it can serve states a well-behaved publisher never would.
type pubSim struct {
	t    *testing.T
	srv  *httptest.Server
	pub  ed25519.PublicKey
	priv ed25519.PrivateKey

	mu       sync.Mutex
	entries  []catalog.CatalogEntry // unsigned; signed on every index GET
	rawIndex []byte                 // if set, served verbatim instead of entries
	files    map[string][]byte      // path -> bytes
	etag     string                 // if set, sent as ETag and honoured for If-None-Match
	gets     map[string]int
}

func newPubSim(t *testing.T) *pubSim {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	p := &pubSim{t: t, pub: pub, priv: priv, files: map[string][]byte{}, gets: map[string]int{}}
	p.srv = httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *pubSim) indexURL() string { return p.srv.URL + "/index" }

func (p *pubSim) serve(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gets[r.URL.Path]++
	if r.URL.Path != "/index" {
		b, ok := p.files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write(b)
		return
	}
	if p.etag != "" {
		if r.Header.Get("If-None-Match") == p.etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", p.etag)
	}
	if p.rawIndex != nil {
		w.Write(p.rawIndex)
		return
	}
	raws := make([]json.RawMessage, 0, len(p.entries))
	for _, e := range p.entries {
		raws = append(raws, signEntry(p.t, p.priv, e))
	}
	out, err := json.Marshal(struct {
		NodeID   string            `json:"nodeId"`
		Catalogs []json.RawMessage `json:"catalogs"`
	}{NodeID: "node-1", Catalogs: raws})
	if err != nil {
		p.t.Error(err)
	}
	w.Write(out)
}

// putFile stores a signed baseline for (fileCatalogID, version) carrying
// marker at path, and returns the FileEntry an index entry would list.
func (p *pubSim) putFile(path, fileCatalogID string, version int64, marker string) catalog.FileEntry {
	p.t.Helper()
	body := signBaseline(p.t, p.priv, catalog.CatalogFileDoc{
		CatalogID: fileCatalogID, Version: version, NextUpdate: time.Now(),
		Catalog: json.RawMessage(fmt.Sprintf(`{"id":%q,"descriptor":{"name":%q},"provider":{"id":"prov"},"resources":[{"id":"r1"}]}`, fileCatalogID, marker)),
	})
	p.mu.Lock()
	defer p.mu.Unlock()
	p.files[path] = body
	return catalog.FileEntry{Version: version, URL: p.srv.URL + path, Digest: sha256Prefixed(body)}
}

// setLive replaces id's index entry with a live one at (entryVersion,
// content version), writing a fresh baseline at a per-version URL.
func (p *pubSim) setLive(id string, entryVersion, version int64, marker string) {
	p.t.Helper()
	fe := p.putFile(fmt.Sprintf("/files/%s.v%d.json", strings.ReplaceAll(id, "/", "_"), version), id, version, marker)
	p.setEntry(catalog.CatalogEntry{CatalogID: id, EntryVersion: entryVersion, CatalogType: "REGULAR", Baseline: fe})
}

// setTombstone replaces id's index entry with a retired one.
func (p *pubSim) setTombstone(id string, entryVersion int64, retiredAt string) {
	p.setEntry(catalog.CatalogEntry{CatalogID: id, EntryVersion: entryVersion, RetiredAt: retiredAt})
}

func (p *pubSim) setEntry(e catalog.CatalogEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.entries {
		if p.entries[i].CatalogID == e.CatalogID {
			p.entries[i] = e
			return
		}
	}
	p.entries = append(p.entries, e)
}

func (p *pubSim) dropEntry(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	kept := p.entries[:0]
	for _, e := range p.entries {
		if e.CatalogID != id {
			kept = append(kept, e)
		}
	}
	p.entries = kept
}

func (p *pubSim) setETag(v string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.etag = v
}

// recSink records every push, so a test can tell "wiped once" from "wiped
// on every poll" and see what the destination currently serves.
type recSink struct {
	mu     sync.Mutex
	pushes map[string][]string // catalogId -> each push's descriptor.name, or "WIPE"
}

func newRecSink() *recSink { return &recSink{pushes: map[string][]string{}} }

func (s *recSink) Send(_ context.Context, entry catalog.CatalogEntry, content []byte) (crawlmanager.SinkOutcome, error) {
	var doc struct {
		Descriptor struct {
			Name string `json:"name"`
		} `json:"descriptor"`
		Resources []json.RawMessage `json:"resources"`
	}
	if err := json.Unmarshal(content, &doc); err != nil {
		return crawlmanager.SinkOutcome{}, err
	}
	what := doc.Descriptor.Name
	if len(doc.Resources) == 0 {
		what = "WIPE"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushes[entry.CatalogID] = append(s.pushes[entry.CatalogID], what)
	return crawlmanager.SinkOutcome{Accepted: true}, nil
}

// serving is what the destination holds for id: the last push ("" if none).
func (s *recSink) serving(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.pushes[id]
	if len(h) == 0 {
		return ""
	}
	return h[len(h)-1]
}

func (s *recSink) history(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.pushes[id], ",")
}

type readerRig struct {
	t     *testing.T
	pub   *pubSim
	sink  *recSink
	store *fakeStore
	p     crawlmanager.Params
	keys  crawler.KeySource
}

func newReaderRig(t *testing.T) *readerRig {
	t.Helper()
	r := &readerRig{t: t, pub: newPubSim(t), sink: newRecSink(), store: newFakeStore()}
	r.keys = crawler.StaticKeys(map[string]ed25519.PublicKey{testKeyID: r.pub.pub})
	r.rebuild()
	return r
}

func (r *readerRig) rebuild() {
	keys := func(ctx context.Context, nodeID, keyID string) (ed25519.PublicKey, error) {
		return r.keys(ctx, nodeID, keyID)
	}
	r.p = crawlmanager.Params{
		Fetcher: catalog.NewFetcher(crawler.NewClient(5*time.Second, 1<<20, true), keys, 1<<20),
		Source:  fakeSource{refs: []crawlmanager.IndexRef{{ParticipantID: "node-1", IndexURL: r.pub.indexURL()}}},
		Sink:    r.sink,
		Store:   r.store,
	}
}

func (r *readerRig) poll() {
	r.t.Helper()
	if err := r.p.PollIndexes(context.Background()); err != nil {
		r.t.Fatalf("PollIndexes: %v", err)
	}
}

// drain runs SyncNext until the queue is empty, returning the errors seen.
func (r *readerRig) drain() []error {
	r.t.Helper()
	var errs []error
	for i := 0; i < 20; i++ {
		claimed, err := r.p.SyncNext(context.Background())
		if !claimed {
			return errs
		}
		if err != nil {
			errs = append(errs, err)
			if !crawler.IsPermanent(err) {
				return errs // rescheduled: would spin until the retry delay
			}
		}
	}
	r.t.Fatal("drain: queue did not empty")
	return nil
}

// crawl is one poll followed by a full drain.
func (r *readerRig) crawl() []error {
	r.t.Helper()
	r.poll()
	return r.drain()
}

func (r *readerRig) cursor(id string) crawlmanager.CatalogCursor {
	c, _, _ := r.store.GetCatalogCursor(context.Background(), id)
	return c
}

func (r *readerRig) wantServing(id, want string) {
	r.t.Helper()
	if got := r.sink.serving(id); got != want {
		r.t.Errorf("destination serves %q for %s, want %q (push history: %s)", got, id, want, r.sink.history(id))
	}
}

func (r *readerRig) wantHistory(id, want string) {
	r.t.Helper()
	if got := r.sink.history(id); got != want {
		r.t.Errorf("push history for %s = %q, want %q", id, got, want)
	}
}

// wantFlagged asserts the refused entry was recorded for an operator, not
// only logged.
func (r *readerRig) wantFlagged(id, substr string) {
	r.t.Helper()
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	for _, f := range r.store.failures {
		if f.CatalogID == id && strings.Contains(f.Error, substr) {
			return
		}
	}
	r.t.Errorf("no recorded failure for %s containing %q (have %+v)", id, substr, r.store.failures)
}

func (r *readerRig) wantRetired(id string, want bool) {
	r.t.Helper()
	if got := r.cursor(id).Retired; got != want {
		r.t.Errorf("cursor.Retired for %s = %v, want %v (cursor %+v)", id, got, want, r.cursor(id))
	}
}

const catA = "node-1/CAT-A"

// --- retirement ------------------------------------------------------

// v1.9.2: publish (ev1), update (ev2), retire (ev3, REJECTED by its own
// verification), operator retires again (tombstone rewritten at ev1). A
// crawler that last synced ev2 sees only a tombstone below its cursor.
func TestReader_RegressedTombstone_StillRetires(t *testing.T) {
	r := newReaderRig(t)
	r.pub.setLive(catA, 2, 2, "live-v2")
	r.crawl()
	r.wantServing(catA, "live-v2")

	r.pub.setTombstone(catA, 1, "2026-11-12T10:00:05Z")
	r.crawl()
	r.wantServing(catA, "WIPE")
	r.wantRetired(catA, true)
}

// The crawler saw the first tombstone (ev3) and wiped; the publisher then
// rewrites it at ev1 with a new retiredAt, and again. Nothing more to do.
func TestReader_RepeatedTombstone_WipesOnce(t *testing.T) {
	r := newReaderRig(t)
	r.pub.setLive(catA, 2, 2, "live-v2")
	r.crawl()
	r.pub.setTombstone(catA, 3, "2026-11-12T10:00:00Z")
	r.crawl()
	r.pub.setTombstone(catA, 1, "2026-11-12T10:00:05Z")
	r.crawl()
	r.pub.setTombstone(catA, 1, "2026-11-12T10:00:09Z")
	r.crawl()
	r.wantHistory(catA, "live-v2,WIPE")
	r.wantRetired(catA, true)
}

// Two polls see the tombstone before the first retire is processed (two
// crawler replicas, or a trigger overlapping the scheduled poll), leaving
// two retire items. The second must not push a second wipe.
func TestReader_DuplicateRetireItems_WipeOnce(t *testing.T) {
	r := newReaderRig(t)
	r.pub.setLive(catA, 2, 2, "live-v2")
	r.crawl()
	r.pub.setTombstone(catA, 3, "2026-11-12T10:00:00Z")
	r.poll()
	r.poll()
	if errs := r.drain(); len(errs) != 0 {
		t.Fatalf("drain: %v", errs)
	}
	r.wantHistory(catA, "live-v2,WIPE")
	r.wantRetired(catA, true)
	if c := r.cursor(catA); c.EntryVersion != 2 || c.Version != 2 {
		t.Errorf("retired cursor = %+v, want the ev2/v2 lineage kept", c)
	}
}

// A tombstone for a catalog this crawler never carried: nothing to wipe,
// nothing recorded.
func TestReader_TombstoneNeverSeen_IsIgnored(t *testing.T) {
	r := newReaderRig(t)
	r.pub.setTombstone(catA, 1, "2026-11-12T10:00:00Z")
	r.crawl()
	r.wantHistory(catA, "")
	if _, seen, _ := r.store.GetCatalogCursor(context.Background(), catA); seen {
		t.Errorf("a cursor was recorded for a never-synced retired catalog")
	}
}

// A tombstone that does not verify is not a tombstone: an attacker who can
// serve the index must not be able to retire someone's catalog.
func TestReader_ForgedTombstone_DoesNotRetire(t *testing.T) {
	r := newReaderRig(t)
	r.pub.setLive(catA, 2, 2, "live-v2")
	r.crawl()

	_, otherPriv, _ := ed25519.GenerateKey(nil)
	forged := signEntry(t, otherPriv, catalog.CatalogEntry{CatalogID: catA, EntryVersion: 3, RetiredAt: "2026-11-12T10:00:00Z"})
	idx, _ := json.Marshal(struct {
		NodeID   string            `json:"nodeId"`
		Catalogs []json.RawMessage `json:"catalogs"`
	}{NodeID: "node-1", Catalogs: []json.RawMessage{forged}})
	r.pub.mu.Lock()
	r.pub.rawIndex = idx
	r.pub.mu.Unlock()
	r.crawl()
	r.wantServing(catA, "live-v2")
	r.wantRetired(catA, false)
}

// The entry is retired between the poll that queued a sync and the sync
// itself. The sync must retire, not park on the tombstone's missing files.
func TestReader_RetiredBetweenPollAndSync_Retires(t *testing.T) {
	r := newReaderRig(t)
	r.pub.setLive(catA, 1, 1, "live-v1")
	r.crawl()
	r.pub.setLive(catA, 2, 2, "live-v2")
	r.poll() // sync for ev2 queued
	r.pub.setTombstone(catA, 3, "2026-11-12T10:00:00Z")
	if errs := r.drain(); len(errs) != 0 {
		t.Errorf("sync of an entry retired in the meantime failed: %v", errs)
	}
	r.wantServing(catA, "WIPE")
	r.wantRetired(catA, true)
}

// --- re-listing after retirement (CON-TBD-26, catalog-core#4) --------

// v1.9.2 re-lists a retired id with versions restarting at 1 and the v1
// baseline overwritten; v0.2.1 continues entryVersion instead. Either way
// a retired catalog must not come back under the same id.
func TestReader_RelistedAfterRetirement_NotServed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		relistEV int64
	}{
		{"versions restart at 1 (v1.9.2)", 1},
		{"entryVersion continues (v0.2.1)", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newReaderRig(t)
			r.pub.setLive(catA, 2, 2, "live-v2")
			r.crawl()
			r.pub.setTombstone(catA, 3, "2026-11-12T10:00:00Z")
			r.crawl()
			r.wantServing(catA, "WIPE")

			r.pub.setLive(catA, tc.relistEV, 1, "relisted-v1")
			r.crawl()
			r.wantServing(catA, "WIPE")
			r.wantRetired(catA, true)
			r.wantFlagged(catA, "after it was retired")

			r.pub.setLive(catA, tc.relistEV+1, 2, "relisted-v2")
			r.crawl()
			r.wantHistory(catA, "live-v2,WIPE")
			r.wantRetired(catA, true)
		})
	}
}

// --- rollback (CON-TBD-11) -------------------------------------------

func TestReader_RegressedLiveEntry_NotApplied(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ev, version int64
	}{
		{"entryVersion regressed", 1, 1},
		{"content version regressed, entryVersion advanced", 4, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newReaderRig(t)
			r.pub.setLive(catA, 3, 3, "live-v3")
			r.crawl()
			r.pub.setLive(catA, tc.ev, tc.version, "old-v1")
			r.crawl()
			r.wantHistory(catA, "live-v3")
			r.wantFlagged(catA, "possible rollback")
			if c := r.cursor(catA); c.EntryVersion != 3 || c.Version != 3 {
				t.Errorf("cursor moved to %+v, want it held at ev3/v3", c)
			}
		})
	}
}

// A sync parked for an unrelated reason is revived by the park sweep after
// the publisher's index has regressed. The revived sync must not apply the
// regressed content: the poll-time check is not the only gate.
func TestReader_RegressionSeenOnlyAtSyncTime_NotApplied(t *testing.T) {
	r := newReaderRig(t)
	r.pub.setLive(catA, 3, 3, "live-v3")
	r.crawl()

	// ev4 is announced but its file is unreadable (digest mismatch): parked.
	r.pub.setLive(catA, 4, 4, "live-v4")
	r.pub.mu.Lock()
	r.pub.files["/files/node-1_CAT-A.v4.json"] = []byte(`{"tampered":true}`)
	r.pub.mu.Unlock()
	if errs := r.crawl(); len(errs) != 1 || !crawler.IsPermanent(errs[0]) {
		t.Fatalf("setup: want one permanent failure, got %v", errs)
	}
	// The publisher's index now regresses to ev1/v1, and the sweep revives
	// the parked item.
	r.pub.setLive(catA, 1, 1, "old-v1")
	r.poll()
	if _, _, err := r.store.RequeueOrAbandonParked(context.Background(), 0, 100); err != nil {
		t.Fatal(err)
	}
	r.drain()
	r.wantHistory(catA, "live-v3")
	if c := r.cursor(catA); c.EntryVersion != 3 || c.Version != 3 {
		t.Errorf("cursor moved to %+v, want it held at ev3/v3", c)
	}
}

// --- disappearance (CON-TBD-27/-35) ----------------------------------

// A concurrent publish on the publisher lost this catalog's index entry
// (beckn-onix#945). Absence is not retirement: nothing is wiped, the cursor
// stays, and the catalog syncs normally when its entry is back.
func TestReader_EntryMissingFromIndex_IsNotRemoval(t *testing.T) {
	r := newReaderRig(t)
	r.pub.setLive(catA, 2, 2, "live-v2")
	r.pub.setLive("node-1/CAT-B", 1, 1, "b-v1")
	r.crawl()

	r.pub.dropEntry(catA)
	r.crawl()
	r.wantHistory(catA, "live-v2")
	r.wantRetired(catA, false)
	if c := r.cursor(catA); c.EntryVersion != 2 {
		t.Errorf("cursor = %+v, want ev2 kept", c)
	}

	r.pub.setLive(catA, 3, 3, "live-v3")
	r.crawl()
	r.wantHistory(catA, "live-v2,live-v3")
}

// The entry disappears between the poll that queued a sync and the sync.
func TestReader_EntryMissingAtSyncTime_KeepsContent(t *testing.T) {
	r := newReaderRig(t)
	r.pub.setLive(catA, 1, 1, "live-v1")
	r.crawl()
	r.pub.setLive(catA, 2, 2, "live-v2")
	r.poll()
	r.pub.dropEntry(catA)
	errs := r.drain()
	if len(errs) != 1 {
		t.Fatalf("want one failure for the vanished entry, got %v", errs)
	}
	r.wantHistory(catA, "live-v1")
	r.wantRetired(catA, false)
}

// --- file identity and integrity (CON-TBD-09/-12) --------------------

// Two catalog ids sharing a local file name (catalog-core#6): publishing
// other/CAT-A overwrote node-1/CAT-A's baseline in place. node-1/CAT-A's
// entry still lists the old digest.
func TestReader_SharedFileOverwritten_NotIndexed(t *testing.T) {
	r := newReaderRig(t)
	r.pub.setLive(catA, 1, 1, "a-v1")
	r.crawl()

	// A's next version lands on a URL that B then overwrites.
	feA := r.pub.putFile("/files/CAT-A.v2.json", catA, 2, "a-v2")
	r.pub.setEntry(catalog.CatalogEntry{CatalogID: catA, EntryVersion: 2, CatalogType: "REGULAR", Baseline: feA})
	feB := r.pub.putFile("/files/CAT-A.v2.json", "other/CAT-A", 2, "b-v2")
	r.pub.setEntry(catalog.CatalogEntry{CatalogID: "other/CAT-A", EntryVersion: 1, CatalogType: "REGULAR", Baseline: feB})

	errs := r.crawl()
	r.wantHistory(catA, "a-v1")
	r.wantHistory("other/CAT-A", "b-v2")
	if len(errs) != 1 || crawler.PermanentClass(errs[0]) != crawler.FaultDigestMismatch {
		t.Errorf("want one digest_mismatch failure, got %v", errs)
	}
	if c := r.cursor(catA); c.EntryVersion != 1 {
		t.Errorf("cursor = %+v, want ev1 kept", c)
	}
}

// The index entry's digest matches the bytes, but the file inside says it
// is another catalog's (the entry was re-signed over a shared file).
func TestReader_FileDeclaresOtherCatalog_NotIndexed(t *testing.T) {
	r := newReaderRig(t)
	fe := r.pub.putFile("/files/CAT-A.v1.json", "other/CAT-A", 1, "b-v1")
	r.pub.setEntry(catalog.CatalogEntry{CatalogID: catA, EntryVersion: 1, CatalogType: "REGULAR", Baseline: fe})
	errs := r.crawl()
	r.wantHistory(catA, "")
	if len(errs) != 1 || crawler.PermanentClass(errs[0]) != crawler.FaultDigestMismatch {
		t.Errorf("want one digest_mismatch-class failure (CON-TBD-12), got %v", errs)
	}
}

// `latest` is not this reader's resolution path: a `latest` whose digest
// does not match (beckn-onix#946) or that still serves live content after
// retirement (beckn-onix#944) changes nothing for it.
func TestReader_BrokenLatest_DoesNotAffectBaselineReader(t *testing.T) {
	r := newReaderRig(t)
	fe := r.pub.putFile("/files/CAT-A.v1.json", catA, 1, "a-v1")
	r.pub.putFile("/files/CAT-A.latest.json", catA, 1, "stale-latest")
	latest := catalog.FileEntry{Version: 1, URL: r.pub.srv.URL + "/files/CAT-A.latest.json", Digest: "sha-256:" + strings.Repeat("0", 64)}
	r.pub.setEntry(catalog.CatalogEntry{CatalogID: catA, EntryVersion: 1, CatalogType: "REGULAR", Baseline: fe, Latest: &latest})
	if errs := r.crawl(); len(errs) != 0 {
		t.Fatalf("sync failed: %v", errs)
	}
	r.wantServing(catA, "a-v1")

	// Retired in the index; the latest file still serves live content.
	r.pub.setTombstone(catA, 2, "2026-11-12T10:00:00Z")
	r.crawl()
	r.wantServing(catA, "WIPE")
	if n := r.pub.gets["/files/CAT-A.latest.json"]; n != 0 {
		t.Errorf("reader fetched latest %d times; it is documented not to", n)
	}
}

// --- index robustness ------------------------------------------------

// An index read mid-write (truncated) is a failed poll, not an empty index.
func TestReader_TruncatedIndex_ChangesNothing(t *testing.T) {
	r := newReaderRig(t)
	r.pub.setETag(`"e1"`)
	r.pub.setLive(catA, 1, 1, "a-v1")
	r.crawl()

	r.pub.mu.Lock()
	r.pub.rawIndex = []byte(`{"nodeId":"node-1","catalogs":[{"catalogId":"node-1/CAT-A","entryVers`)
	r.pub.etag = `"e2"`
	r.pub.mu.Unlock()
	r.crawl()
	r.wantHistory(catA, "a-v1")
	if ic, _ := r.store.GetIndexCursor(context.Background(), r.pub.indexURL()); ic == nil || ic.ETag != `"e1"` {
		t.Errorf("index cursor = %+v, want the last good ETag kept so the index is re-read", ic)
	}

	// The complete index is back under the same ETag the truncated read had.
	r.pub.mu.Lock()
	r.pub.rawIndex = nil
	r.pub.mu.Unlock()
	r.pub.setLive(catA, 2, 2, "a-v2")
	r.crawl()
	r.wantServing(catA, "a-v2")
}

// The registry is unreachable while an entry's key is being resolved. The
// entry is dropped for this poll; it must be looked at again on the next
// one even though the index itself has not changed (same ETag).
func TestReader_EntryDroppedForTransientReason_IsRetriedOnNextPoll(t *testing.T) {
	r := newReaderRig(t)
	r.pub.setETag(`"e1"`)
	r.pub.setLive(catA, 1, 1, "a-v1")

	good := r.keys
	r.keys = func(context.Context, string, string) (ed25519.PublicKey, error) {
		return nil, errors.New("registry unreachable")
	}
	r.crawl()
	r.wantHistory(catA, "")

	r.keys = good
	r.crawl()
	r.wantServing(catA, "a-v1")
}

// One entry whose decision cannot be made (store error) must not stop the
// other entries of the same index from being examined.
func TestReader_OneEntryFailingToDecide_DoesNotHideTheOthers(t *testing.T) {
	r := newReaderRig(t)
	r.pub.setLive(catA, 1, 1, "a-v1")
	r.pub.setLive("node-1/CAT-B", 1, 1, "b-v1")
	r.p.Store = cursorErrStore{fakeStore: r.store, failFor: catA}
	r.poll()
	r.p.Store = r.store
	r.drain()
	r.wantServing("node-1/CAT-B", "b-v1")
}

type cursorErrStore struct {
	*fakeStore
	failFor string
}

func (s cursorErrStore) GetCatalogCursor(ctx context.Context, id string) (crawlmanager.CatalogCursor, bool, error) {
	if id == s.failFor {
		return crawlmanager.CatalogCursor{}, false, errors.New("store unavailable")
	}
	return s.fakeStore.GetCatalogCursor(ctx, id)
}
