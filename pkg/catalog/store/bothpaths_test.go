package store_test

// bothpaths_test.go -- Store.Publish has two write paths, chosen by what
// the blob store can do: the plain one, and the compare-and-swap one a
// ConditionalBlobStore gets (conditional.go). Every rule about what may be
// written has to hold on both. These tests state each such rule once and
// run it over both kinds of store.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beckn/catalog-core/pkg/catalog/store"
)

// eachStoreKind runs test once over a plain blob store and once over a
// conditional one. open returns a further, independent Store on the same
// blobs: another process sharing the blob store.
func eachStoreKind(t *testing.T, test func(t *testing.T, shared *sharedBlobs, open func() *store.Store)) {
	t.Helper()
	t.Run("plain", func(t *testing.T) {
		shared := newSharedBlobs()
		test(t, shared, func() *store.Store { return store.New(plainWriter{&writer{shared: shared}}) })
	})
	t.Run("conditional", func(t *testing.T) {
		shared := newSharedBlobs()
		test(t, shared, func() *store.Store { s, _ := newCASStore(shared); return s })
	})
}

func snapshot(shared *sharedBlobs) map[string]string {
	shared.mu.Lock()
	defer shared.mu.Unlock()
	out := make(map[string]string, len(shared.data))
	for p, b := range shared.data {
		out[p] = string(b)
	}
	return out
}

func assertUnchanged(t *testing.T, shared *sharedBlobs, before map[string]string, what string) {
	t.Helper()
	after := snapshot(shared)
	for p, b := range after {
		if old, ok := before[p]; !ok {
			t.Errorf("%s: %s was created", what, p)
		} else if old != b {
			t.Errorf("%s: %s was changed", what, p)
		}
	}
	for p := range before {
		if _, ok := after[p]; !ok {
			t.Errorf("%s: %s disappeared", what, p)
		}
	}
}

// A new catalogId whose files would land on another catalog's is refused
// before anything is written, whichever path the store takes
// (catalog-core#6).
func TestBothPaths_RefusesNewIdThatWouldShareFiles(t *testing.T) {
	eachStoreKind(t, func(t *testing.T, shared *sharedBlobs, open func() *store.Store) {
		s := open()
		if err := publishBaseline(t, s, "a.example/CAT-1"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		before := snapshot(shared)

		for _, id := range []string{"b.example/CAT-1", "a.example/cat-1", "a.example/CAT 2", "a.example/"} {
			if err := publishBaseline(t, s, id); !errors.Is(err, store.ErrCatalogIDNotAllowed) {
				t.Errorf("Publish(%q) = %v, want ErrCatalogIDNotAllowed", id, err)
			}
		}
		content := baselineFile(t, "x", json.RawMessage(`{"id":"x","descriptor":{"name":"x"}}`), time.Now().Add(time.Hour))
		err := s.Publish(context.Background(), store.PublishRequest{NodeID: "example.test", Updates: []store.CatalogUpdate{
			{CatalogID: "a.example/NEW", SignedEntry: json.RawMessage(`{"catalogId":"a.example/NEW"}`), Baseline: &store.FileWrite{Version: 1, Content: content}},
			{CatalogID: "b.example/NEW", SignedEntry: json.RawMessage(`{"catalogId":"b.example/NEW"}`), Baseline: &store.FileWrite{Version: 1, Content: content}},
		}})
		if !errors.Is(err, store.ErrCatalogIDNotAllowed) {
			t.Errorf("two new ids sharing file names: Publish = %v, want ErrCatalogIDNotAllowed", err)
		}
		assertUnchanged(t, shared, before, "a refused publish")

		if err := publishBaseline(t, s, "a.example/CAT-1"); err != nil {
			t.Errorf("update of an existing id: %v", err)
		}
	})
}

// A retirement's final tombstone reaches both "latest" names on both paths
// (beckn-onix#944: the name the retire did not reach stayed live).
func TestBothPaths_LatestAlt_WritesBothLatestNames(t *testing.T) {
	eachStoreKind(t, func(t *testing.T, shared *sharedBlobs, open func() *store.Store) {
		s := open()
		const id = "example.test/CAT-1"
		if err := publishBaseline(t, s, id); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		// Live "latest" files under both names, as a gzip switch leaves them.
		shared.put(store.LatestFilePath(id, true), []byte("live-gz"), false, "")
		shared.put(store.LatestFilePath(id, false), []byte("live-plain"), false, "")

		plain := []byte(`{"catalogId":"example.test/CAT-1","retiredAt":"2026-11-12T10:00:00Z"}`)
		zipped := gzipBytes(t, plain)
		err := s.Publish(context.Background(), store.PublishRequest{NodeID: "example.test", Retirements: []store.CatalogUpdate{{
			CatalogID: id, SignedEntry: json.RawMessage(`{"catalogId":"example.test/CAT-1","entryVersion":2,"retiredAt":"2026-11-12T10:00:00Z"}`),
			Latest:    &store.FileWrite{Content: plain, ServedContent: zipped, Compressed: true},
			LatestAlt: &store.FileWrite{Content: plain, ServedContent: plain},
		}}})
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if got := shared.mustGet(t, store.LatestFilePath(id, true)); string(got) != string(zipped) {
			t.Errorf("%s was not given the tombstone", store.LatestFilePath(id, true))
		}
		if got := shared.mustGet(t, store.LatestFilePath(id, false)); string(got) != string(plain) {
			t.Errorf("%s was not given the tombstone: %q", store.LatestFilePath(id, false), got)
		}
	})
}

// A retirement that is refused because its catalog changed underneath it
// leaves both "latest" names as they were: on the conditional path the
// tombstones are written before the commit and must both be put back.
func TestConditional_RefusedRetirementRestoresBothLatestNames(t *testing.T) {
	shared := newSharedBlobs()
	s, _ := newCASStore(shared)
	const id = "example.test/CAT-1"
	if err := publish(t, s, nil, baselineUpdate(t, id, 1, 1, "one")); err != nil {
		t.Fatal(err)
	}
	shared.put(store.LatestFilePath(id, true), []byte("live-gz"), false, "")
	_, base := loadBase(t, s, id)
	// Another writer updates the catalog after this one loaded it.
	other, _ := newCASStore(shared)
	if err := publish(t, other, nil, baselineUpdate(t, id, 2, 2, "two")); err != nil {
		t.Fatal(err)
	}
	before := snapshot(shared)

	tomb := tombstoneUpdate(t, id, 2)
	tomb.LatestAlt = &store.FileWrite{Content: json.RawMessage(`{"retiredAt":"x"}`), ServedContent: []byte("tomb-gz"), Compressed: true}
	err := s.Publish(context.Background(), store.PublishRequest{NodeID: "example.test", Retirements: []store.CatalogUpdate{tomb}, Base: base})
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("Publish = %v, want ErrConflict", err)
	}
	assertUnchanged(t, shared, before, "a refused retirement")
}

// A request with nothing in it writes nothing: no index is created and an
// existing one is not rewritten. A publish call whose every catalog was
// refused ends up here, and "refused" has to mean that nothing happened.
func TestBothPaths_EmptyRequestWritesNothing(t *testing.T) {
	eachStoreKind(t, func(t *testing.T, shared *sharedBlobs, open func() *store.Store) {
		s := open()
		next := time.Now().Add(time.Hour)
		if err := s.Publish(context.Background(), store.PublishRequest{NodeID: "example.test", NextUpdate: &next}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if got := snapshot(shared); len(got) != 0 {
			t.Fatalf("an empty request on an empty store wrote %d blobs", len(got))
		}
		if err := publishBaseline(t, s, "a.example/CAT-1"); err != nil {
			t.Fatal(err)
		}
		before := snapshot(shared)
		later := time.Now().Add(2 * time.Hour)
		if err := s.Publish(context.Background(), store.PublishRequest{NodeID: "example.test", NextUpdate: &later}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		assertUnchanged(t, shared, before, "an empty request")
	})
}

// The Base a request carries covers everything its admission depended on.
// For a new catalogId that includes who else owns its file names: if
// another writer publishes a different id with the same last segment after
// this one loaded, the request is stale (ErrConflict, recompute), not a
// request to overwrite that catalog's files.
func TestBothPaths_BaseCoversFileNameOwners(t *testing.T) {
	eachStoreKind(t, func(t *testing.T, shared *sharedBlobs, open func() *store.Store) {
		mine, theirs := open(), open()
		states, base := loadBase(t, mine, "b.example/CAT-1")
		if _, present := states["b.example/CAT-1"]; present {
			t.Fatalf("set-up: b.example/CAT-1 should be free")
		}
		if err := publish(t, theirs, nil, baselineUpdate(t, "a.example/CAT-1", 1, 1, "theirs")); err != nil {
			t.Fatal(err)
		}
		before := snapshot(shared)

		err := publish(t, mine, base, baselineUpdate(t, "b.example/CAT-1", 1, 1, "mine"))
		if !errors.Is(err, store.ErrConflict) {
			t.Errorf("Publish = %v, want ErrConflict", err)
		}
		assertUnchanged(t, shared, before, "a stale request for a new id")

		// Loading again shows why, so the caller can refuse it per catalog.
		states, _ = loadBase(t, mine, "b.example/CAT-1")
		if st := states["b.example/CAT-1"]; len(st.NameConflicts) != 1 || st.NameConflicts[0].CatalogID != "a.example/CAT-1" {
			t.Errorf("reloaded state = %+v, want a name conflict with a.example/CAT-1", st)
		}
	})
}

// Eight independent writers each publish a different new catalogId with
// the same last segment, at once, the way the adapter does it: load, then
// publish with the Base. Exactly one may end up in the index, and its
// files must be its own.
func TestConditional_ManyWriters_SameFileNames_OneWins(t *testing.T) {
	shared := newSharedBlobs()
	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, _ := newCASStore(shared)
			id := fmt.Sprintf("p%d.example/CAT-1", i)
			_, base := loadBase(t, s, id)
			errs[i] = publish(t, s, base, baselineUpdate(t, id, 1, 1, id))
		}()
	}
	wg.Wait()
	won := 0
	for i, err := range errs {
		switch {
		case err == nil:
			won++
		case errors.Is(err, store.ErrConflict), errors.Is(err, store.ErrCatalogIDNotAllowed):
		default:
			t.Errorf("writer %d: unexpected error %v", i, err)
		}
	}
	entries := readEntries(t, shared)
	if won != 1 || len(entries) != 1 {
		t.Fatalf("%d writers succeeded and the index lists %d catalogs; want exactly one of each", won, len(entries))
	}
	if stale := checkReferences(t, shared); len(stale) != 0 {
		t.Errorf("latest does not match its entry for %v", stale)
	}
}

// A catalog whose files cannot be read back still reports what "latest" it
// published and what the blob store holds, so a forced baseline keeps the
// "latest" name and a retire can reach the files.
func TestLoad_UnreadableCatalogStillCarriesLatestState(t *testing.T) {
	shared := newSharedBlobs()
	s := store.New(plainWriter{&writer{shared: shared}})
	const id = "example.test/CAT-1"
	if err := publish(t, s, nil, baselineUpdate(t, id, 1, 1, "one")); err != nil {
		t.Fatal(err)
	}
	shared.put(store.CatalogFilePath(id, 1, "json", false), []byte("not json"), false, "")

	states, err := s.LoadCatalogs(context.Background(), []string{id})
	if err != nil {
		t.Fatalf("LoadCatalogs: %v", err)
	}
	st := states[id]
	if st.Unreadable == "" {
		t.Fatalf("state = %+v, want Unreadable", st)
	}
	if st.LatestFile == nil || len(st.StoredLatest) != 1 || len(st.SignedEntry) == 0 {
		t.Errorf("unreadable state: LatestFile=%v StoredLatest=%d SignedEntry=%d bytes; want the entry's latest reference, the stored file and the entry",
			st.LatestFile, len(st.StoredLatest), len(st.SignedEntry))
	}
}

// One index entry that does not parse is that catalog's problem, not the
// whole request's: the other catalogs load, and the broken one is reported
// as such.
func TestLoad_MalformedEntryIsPerCatalog(t *testing.T) {
	shared := newSharedBlobs()
	s := store.New(plainWriter{&writer{shared: shared}})
	if err := publish(t, s, nil, baselineUpdate(t, "example.test/GOOD", 1, 1, "good")); err != nil {
		t.Fatal(err)
	}
	raw := shared.mustGet(t, store.IndexPath())
	broken := strings.Replace(string(raw), `"catalogs":[`, `"catalogs":[{"catalogId":"example.test/BAD","entryVersion":"three"},`, 1)
	if broken == string(raw) {
		t.Fatal("set-up: could not insert the malformed entry")
	}
	shared.put(store.IndexPath(), []byte(broken), false, "")

	states, err := s.LoadCatalogs(context.Background(), []string{"example.test/GOOD", "example.test/BAD"})
	if err != nil {
		t.Fatalf("LoadCatalogs: %v", err)
	}
	if st := states["example.test/GOOD"]; st.Catalog == nil || st.Unreadable != "" {
		t.Errorf("GOOD = %+v, want its content", st)
	}
	bad := states["example.test/BAD"]
	if !bad.Known() || !bad.EntryMalformed || bad.Unreadable == "" {
		t.Errorf("BAD = %+v, want a known state marked EntryMalformed with a reason", bad)
	}
}

// An index file that is empty or not JSON stops every publish, and the
// error says which file and that it has to be restored: starting a new
// index over it would restart every catalog's version lineage.
func TestBothPaths_UnreadableIndexFailsWithAUsableError(t *testing.T) {
	for _, content := range []string{"", "{not json"} {
		eachStoreKind(t, func(t *testing.T, shared *sharedBlobs, open func() *store.Store) {
			s := open()
			shared.put(store.IndexPath(), []byte(content), false, "")
			before := snapshot(shared)
			_, _, err := s.LoadForPublish(context.Background(), []string{"example.test/CAT-1"})
			if !errors.Is(err, store.ErrIndexUnreadable) || !strings.Contains(err.Error(), store.IndexPath()) {
				t.Errorf("LoadForPublish = %v, want ErrIndexUnreadable naming %s", err, store.IndexPath())
			}
			err = publish(t, s, nil, baselineUpdate(t, "example.test/CAT-1", 1, 1, "one"))
			if !errors.Is(err, store.ErrIndexUnreadable) {
				t.Errorf("Publish = %v, want ErrIndexUnreadable", err)
			}
			assertUnchanged(t, shared, before, "a publish over an unreadable index")
		})
	}
}
