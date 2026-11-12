package store_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/beckn/catalog-core/pkg/catalog/store"
)

// fakeBlobStore is an in-memory store.BlobStore, standing in for any real
// backend (local disk, S3, GCS, ...) -- store.Store is tested entirely
// against this fake, proving its assembly logic never depends on backend
// specifics.
type fakeBlobStore struct{ data map[string][]byte }

func newFakeBlobStore() *fakeBlobStore { return &fakeBlobStore{data: map[string][]byte{}} }

func (f *fakeBlobStore) Get(ctx context.Context, path string) ([]byte, error) {
	b, ok := f.data[path]
	if !ok {
		return nil, store.ErrBlobNotFound
	}
	return b, nil
}

func (f *fakeBlobStore) Put(ctx context.Context, path string, content []byte) error {
	f.data[path] = append([]byte{}, content...)
	return nil
}

func baselineFile(t *testing.T, catalogID string, catalog json.RawMessage, nextUpdate time.Time) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"catalogId":   catalogID,
		"version":     1,
		"next_update": nextUpdate,
		"catalog":     catalog,
	})
	if err != nil {
		t.Fatalf("marshaling baseline file: %v", err)
	}
	return raw
}

func TestPublishThenLoad_RoundTripsBaseline(t *testing.T) {
	blobs := newFakeBlobStore()
	s := store.New(blobs)
	ctx := context.Background()

	catalog := json.RawMessage(`{"id":"CAT-1","descriptor":{"name":"Test"},"provider":{},"resources":[{"id":"ITEM-1","descriptor":{"name":"one"}}]}`)
	content := baselineFile(t, "example.test/CAT-1", catalog, time.Now().Add(24*time.Hour))
	entry := json.RawMessage(`{"catalogId":"example.test/CAT-1","entryVersion":1,"catalogType":"REGULAR","isActive":true,"baseline":{"version":1,"url":"https://example.test/catalogs/CAT-1.v1.json","size":1,"digest":"sha-256:x"}}`)

	err := s.Publish(ctx, store.PublishRequest{
		NodeID: "example.test",
		Updates: []store.CatalogUpdate{{
			CatalogID:   "example.test/CAT-1",
			SignedEntry: entry,
			Baseline:    &store.FileWrite{Version: 1, Content: content},
		}},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	states, err := s.LoadCatalogs(ctx, []string{"example.test/CAT-1"})
	if err != nil {
		t.Fatalf("LoadCatalogs: %v", err)
	}
	state, ok := states["example.test/CAT-1"]
	if !ok {
		t.Fatal("expected state for example.test/CAT-1")
	}
	if string(state.Catalog) != string(catalog) {
		t.Errorf("reconstructed catalog mismatch:\ngot:  %s\nwant: %s", state.Catalog, catalog)
	}
	if state.BaselineFile == nil || state.BaselineFile.Version != 1 {
		t.Errorf("unexpected baseline file ref: %+v", state.BaselineFile)
	}
	if state.EntryVersion != 1 {
		t.Errorf("EntryVersion = %d, want 1", state.EntryVersion)
	}
}

func TestLoad_NoPriorIndex_ReturnsEmptyResult(t *testing.T) {
	s := store.New(newFakeBlobStore())
	states, err := s.LoadCatalogs(context.Background(), []string{"example.test/CAT-1"})
	if err != nil {
		t.Fatalf("LoadCatalogs: %v", err)
	}
	if len(states) != 0 {
		t.Errorf("expected empty result, got %+v", states)
	}
}

// TestLoad_RetiredEntry_ReturnsTombstoneState: a retired catalog has no
// publishable content, but its tombstone's own metadata is still returned
// (flagged Retired) so a caller can tell it apart from a catalog that
// never existed and keep EntryVersion's lineage going.
func TestLoad_RetiredEntry_ReturnsTombstoneState(t *testing.T) {
	blobs := newFakeBlobStore()
	s := store.New(blobs)
	ctx := context.Background()

	entry := json.RawMessage(`{"catalogId":"example.test/CAT-1","entryVersion":2,"catalogType":"REGULAR","networkIds":["ion.example"],"retiredAt":"2026-01-01T00:00:00Z"}`)
	if err := s.Publish(ctx, store.PublishRequest{
		NodeID:      "example.test",
		Retirements: []store.CatalogUpdate{{CatalogID: "example.test/CAT-1", SignedEntry: entry}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	states, err := s.LoadCatalogs(ctx, []string{"example.test/CAT-1", "example.test/NEVER-EXISTED"})
	if err != nil {
		t.Fatalf("LoadCatalogs: %v", err)
	}
	state, ok := states["example.test/CAT-1"]
	if !ok {
		t.Fatal("expected a tombstone state for a retired catalog")
	}
	if !state.Retired || state.EntryVersion != 2 || state.CatalogType != "REGULAR" || len(state.NetworkIds) != 1 {
		t.Errorf("tombstone state = %+v, want Retired with entryVersion 2 and the tombstone's metadata", state)
	}
	if state.Catalog != nil || state.BaselineFile != nil || len(state.ChangeFiles) != 0 || state.LatestPublished {
		t.Errorf("tombstone state must carry no publishable content, got %+v", state)
	}
	if _, ok := states["example.test/NEVER-EXISTED"]; ok {
		t.Error("expected no state for a catalog that is not in the index")
	}
}

func TestPublish_PreservesUntouchedEntries(t *testing.T) {
	blobs := newFakeBlobStore()
	s := store.New(blobs)
	ctx := context.Background()

	catalog1 := json.RawMessage(`{"id":"CAT-1","descriptor":{},"provider":{},"resources":[]}`)
	entry1 := json.RawMessage(`{"catalogId":"example.test/CAT-1","entryVersion":1,"catalogType":"REGULAR","isActive":true,"baseline":{"version":1,"url":"https://example.test/catalogs/CAT-1.v1.json","size":1,"digest":"sha-256:x"}}`)
	if err := s.Publish(ctx, store.PublishRequest{
		NodeID: "example.test",
		Updates: []store.CatalogUpdate{{
			CatalogID: "example.test/CAT-1", SignedEntry: entry1,
			Baseline: &store.FileWrite{Version: 1, Content: baselineFile(t, "example.test/CAT-1", catalog1, time.Now().Add(time.Hour))},
		}},
	}); err != nil {
		t.Fatalf("Publish CAT-1: %v", err)
	}

	catalog2 := json.RawMessage(`{"id":"CAT-2","descriptor":{},"provider":{},"resources":[]}`)
	entry2 := json.RawMessage(`{"catalogId":"example.test/CAT-2","entryVersion":1,"catalogType":"REGULAR","isActive":true,"baseline":{"version":1,"url":"https://example.test/catalogs/CAT-2.v1.json","size":1,"digest":"sha-256:y"}}`)
	if err := s.Publish(ctx, store.PublishRequest{
		NodeID: "example.test",
		Updates: []store.CatalogUpdate{{
			CatalogID: "example.test/CAT-2", SignedEntry: entry2,
			Baseline: &store.FileWrite{Version: 1, Content: baselineFile(t, "example.test/CAT-2", catalog2, time.Now().Add(time.Hour))},
		}},
	}); err != nil {
		t.Fatalf("Publish CAT-2: %v", err)
	}

	raw, err := blobs.Get(ctx, store.IndexPath())
	if err != nil {
		t.Fatalf("reading index: %v", err)
	}
	var doc struct {
		Catalogs []json.RawMessage `json:"catalogs"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parsing index: %v", err)
	}
	if len(doc.Catalogs) != 2 {
		t.Fatalf("expected 2 entries after the second publish, got %d", len(doc.Catalogs))
	}

	states, err := s.LoadCatalogs(ctx, []string{"example.test/CAT-1"})
	if err != nil {
		t.Fatalf("LoadCatalogs: %v", err)
	}
	if _, ok := states["example.test/CAT-1"]; !ok {
		t.Error("expected CAT-1's state to survive an unrelated publish for CAT-2")
	}
}

func TestLoad_AppliesChangeFileOntoBaseline(t *testing.T) {
	blobs := newFakeBlobStore()
	s := store.New(blobs)
	ctx := context.Background()

	catalog := json.RawMessage(`{"id":"CAT-1","descriptor":{},"provider":{},"resources":[{"id":"ITEM-1","descriptor":{"name":"one"}}]}`)
	baseline := &store.FileWrite{Version: 1, Content: baselineFile(t, "example.test/CAT-1", catalog, time.Now().Add(24*time.Hour))}
	entry1 := json.RawMessage(`{"catalogId":"example.test/CAT-1","entryVersion":1,"catalogType":"REGULAR","isActive":true,"baseline":{"version":1,"url":"https://example.test/catalogs/CAT-1.v1.json","size":1,"digest":"sha-256:x"}}`)
	if err := s.Publish(ctx, store.PublishRequest{
		NodeID:  "example.test",
		Updates: []store.CatalogUpdate{{CatalogID: "example.test/CAT-1", SignedEntry: entry1, Baseline: baseline}},
	}); err != nil {
		t.Fatalf("Publish baseline: %v", err)
	}

	change := json.RawMessage(`{"catalogId":"example.test/CAT-1","fromVersion":1,"toVersion":2,"next_update":"2099-01-01T00:00:00Z","resources":{"upserts":[{"id":"ITEM-2","descriptor":{"name":"two"}}]},"offers":{}}`)
	entry2 := json.RawMessage(`{"catalogId":"example.test/CAT-1","entryVersion":2,"catalogType":"REGULAR","isActive":true,"baseline":{"version":1,"url":"https://example.test/catalogs/CAT-1.v1.json","size":1,"digest":"sha-256:x"},"changes":[{"fromVersion":1,"toVersion":2,"url":"https://example.test/catalogs/changes/CAT-1.v2.changes.json","size":1,"digest":"sha-256:y"}]}`)
	if err := s.Publish(ctx, store.PublishRequest{
		NodeID:  "example.test",
		Updates: []store.CatalogUpdate{{CatalogID: "example.test/CAT-1", SignedEntry: entry2, Change: &store.FileWrite{Version: 2, Content: change}}},
	}); err != nil {
		t.Fatalf("Publish change: %v", err)
	}

	states, err := s.LoadCatalogs(ctx, []string{"example.test/CAT-1"})
	if err != nil {
		t.Fatalf("LoadCatalogs: %v", err)
	}
	state, ok := states["example.test/CAT-1"]
	if !ok {
		t.Fatal("expected state for example.test/CAT-1")
	}
	var effective struct {
		Resources []json.RawMessage `json:"resources"`
	}
	if err := json.Unmarshal(state.Catalog, &effective); err != nil {
		t.Fatalf("parsing reconstructed catalog: %v", err)
	}
	if len(effective.Resources) != 2 {
		t.Fatalf("expected 2 resources after applying change, got %d", len(effective.Resources))
	}
	if len(state.ChangeFiles) != 1 || state.ChangeFiles[0].FromVersion != 1 || state.ChangeFiles[0].ToVersion != 2 {
		t.Errorf("unexpected change file refs: %+v", state.ChangeFiles)
	}
}

func TestPublish_GzipSuffixAndServedContent(t *testing.T) {
	blobs := newFakeBlobStore()
	s := store.New(blobs)
	ctx := context.Background()

	served := []byte("compressed-bytes")
	err := s.Publish(ctx, store.PublishRequest{
		NodeID: "example.test",
		Updates: []store.CatalogUpdate{{
			CatalogID:   "example.test/CAT-1",
			SignedEntry: json.RawMessage(`{"catalogId":"example.test/CAT-1"}`),
			Baseline: &store.FileWrite{
				Version:       1,
				Content:       json.RawMessage(`{"catalogId":"example.test/CAT-1"}`),
				ServedContent: served,
				Compressed:    true,
			},
		}},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	path := store.CatalogFilePath("example.test/CAT-1", 1, "json", true)
	got, err := blobs.Get(ctx, path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	if string(got) != string(served) {
		t.Errorf("got %q, want %q", got, served)
	}
}

func TestPublish_ChangeMode_UsesChangesPath(t *testing.T) {
	blobs := newFakeBlobStore()
	s := store.New(blobs)
	ctx := context.Background()

	err := s.Publish(ctx, store.PublishRequest{
		NodeID: "example.test",
		Updates: []store.CatalogUpdate{{
			CatalogID:   "example.test/CAT-1",
			SignedEntry: json.RawMessage(`{"catalogId":"example.test/CAT-1"}`),
			Change:      &store.FileWrite{Version: 2, Content: json.RawMessage(`{"catalogId":"example.test/CAT-1"}`)},
		}},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	path := store.CatalogFilePath("example.test/CAT-1", 2, "changes.json", false)
	if _, err := blobs.Get(ctx, path); err != nil {
		t.Errorf("expected change file at %s: %v", path, err)
	}
}

func TestPublish_RetiredLatest_WritesFinalTombstone(t *testing.T) {
	blobs := newFakeBlobStore()
	s := store.New(blobs)
	ctx := context.Background()

	err := s.Publish(ctx, store.PublishRequest{
		NodeID: "example.test",
		Retirements: []store.CatalogUpdate{{
			CatalogID:   "example.test/CAT-1",
			SignedEntry: json.RawMessage(`{"catalogId":"example.test/CAT-1","retiredAt":"2026-01-01T00:00:00Z"}`),
			Latest:      &store.FileWrite{Content: json.RawMessage(`{"catalogId":"example.test/CAT-1","retiredAt":"2026-01-01T00:00:00Z"}`)},
		}},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	path := store.LatestFilePath("example.test/CAT-1", false)
	if _, err := blobs.Get(ctx, path); err != nil {
		t.Errorf("expected latest tombstone at %s: %v", path, err)
	}
}

func TestFileNameKey(t *testing.T) {
	for _, tc := range []struct{ a, b string }{
		{"a.example/CAT-1", "b.example/CAT-1"},
		{"a.example/CAT-1", "CAT-1"},
		{"a.example/CAT-1", "a.example/cat-1"},
		{"a.example/x/CAT-1", "a.example/y/CAT-1"},
	} {
		if store.FileNameKey(tc.a) != store.FileNameKey(tc.b) {
			t.Errorf("%q and %q are written to the same files but have different keys", tc.a, tc.b)
		}
	}
	if store.FileNameKey("a.example/CAT-1") == store.FileNameKey("a.example/CAT-2") {
		t.Error("different last segments must have different keys")
	}
	// The key is for comparison only: file names keep the id's own case.
	if got := store.CatalogFilePath("a.example/CAT-1", 1, "json", false); got != "catalogs/CAT-1.v1.json" {
		t.Errorf("CatalogFilePath = %q; published file names must not change", got)
	}
	if got := store.LatestFilePath("a.example/CAT-1", true); got != "catalogs/CAT-1.latest.json.gz" {
		t.Errorf("LatestFilePath = %q; published file names must not change", got)
	}
}

func TestValidateCatalogID(t *testing.T) {
	ok := []string{
		"CAT-1", "open-economy.nfh.global/electronics-2026", "a.example/x/y/CAT_1.v2~b",
		"a.example/" + strings.Repeat("x", store.MaxLocalNameBytes),
	}
	for _, id := range ok {
		if err := store.ValidateCatalogID(id); err != nil {
			t.Errorf("store.ValidateCatalogID(%q) = %v, want nil", id, err)
		}
	}
	bad := []string{
		"", " ", "a.example/", "/CAT-1", "a.example//CAT-1", "a.example/../CAT-1", "a.example/./CAT-1", "a.example/..", ".",
		"a.example/.hidden", "a.example/CAT 1", " CAT-1", "CAT-1 ", "CAT-1\n", "a\texample/CAT-1", "a.example/%41",
		"a.example/CAT?x", "a.example/CAT#x", `a.example/CAT\x`, "a.example/CAT:1", "a.example/CAT\x00", "a.example/CÄT",
		"a.example/CAT\xff", "a.example/" + strings.Repeat("x", store.MaxLocalNameBytes+1), strings.Repeat("a/", 600) + "CAT-1",
	}
	for _, id := range bad {
		if err := store.ValidateCatalogID(id); err == nil {
			t.Errorf("store.ValidateCatalogID(%.40q) = nil, want an error", id)
		} else if len(err.Error()) > 400 {
			t.Errorf("error for %.40q is %d bytes long", id, len(err.Error()))
		}
	}
}

// publishBaseline publishes one baseline for id through the real Store.
func publishBaseline(t *testing.T, s *store.Store, id string) error {
	t.Helper()
	content := baselineFile(t, id, json.RawMessage(`{"id":"x","descriptor":{"name":"x"}}`), time.Now().Add(time.Hour))
	entry := `{"catalogId":"` + id + `","entryVersion":1,"baseline":{"version":1,"url":"https://example.test/catalogs/` + store.LocalName(id) + `.v1.json","size":1,"digest":"sha-256:00"}}`
	return s.Publish(context.Background(), store.PublishRequest{NodeID: "example.test", Updates: []store.CatalogUpdate{{
		CatalogID: id, SignedEntry: json.RawMessage(entry), Baseline: &store.FileWrite{Version: 1, Content: content},
	}}})
}

// TestLoadCatalogs_ReportsNameConflicts: LoadCatalogs tells the caller when
// a requested id's file names belong to another index entry, whether or
// not the requested id is itself in the index.
func TestLoadCatalogs_ReportsNameConflicts(t *testing.T) {
	blobs := newFakeBlobStore()
	s := store.New(blobs)
	if err := publishBaseline(t, s, "a.example/CAT-1"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := publishBaseline(t, s, "a.example/CAT-2"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	states, err := s.LoadCatalogs(context.Background(), []string{"a.example/CAT-1", "b.example/cat-1", "b.example/CAT-3"})
	if err != nil {
		t.Fatalf("LoadCatalogs: %v", err)
	}
	if st := states["a.example/CAT-1"]; !st.Known() || len(st.NameConflicts) != 0 {
		t.Errorf("a.example/CAT-1: %+v, want known with no conflicts", st)
	}
	st, present := states["b.example/cat-1"]
	if !present || st.Known() || len(st.NameConflicts) != 1 || st.NameConflicts[0] != (store.NameConflict{CatalogID: "a.example/CAT-1"}) {
		t.Errorf("b.example/cat-1: present=%v %+v, want not known, in conflict with a.example/CAT-1", present, st)
	}
	if _, present := states["b.example/CAT-3"]; present {
		t.Error("b.example/CAT-3 has no entry and no conflict; it must be absent from the result")
	}
}

// TestPublish_RefusesNewIdThatWouldShareFiles: the store itself refuses,
// before writing anything, a request that would put a new catalogId's
// files on top of another catalog's -- whatever the caller checked.
func TestPublish_RefusesNewIdThatWouldShareFiles(t *testing.T) {
	blobs := newFakeBlobStore()
	s := store.New(blobs)
	if err := publishBaseline(t, s, "a.example/CAT-1"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	before := map[string]string{}
	for p, b := range blobs.data {
		before[p] = string(b)
	}

	for _, id := range []string{"b.example/CAT-1", "a.example/cat-1", "a.example/CAT 2", "a.example/"} {
		err := publishBaseline(t, s, id)
		if !errors.Is(err, store.ErrCatalogIDNotAllowed) {
			t.Errorf("Publish(%q) = %v, want ErrCatalogIDNotAllowed", id, err)
		}
	}
	// Two new ids in one request.
	content := baselineFile(t, "x", json.RawMessage(`{"id":"x","descriptor":{"name":"x"}}`), time.Now().Add(time.Hour))
	err := s.Publish(context.Background(), store.PublishRequest{NodeID: "example.test", Updates: []store.CatalogUpdate{
		{CatalogID: "a.example/NEW", SignedEntry: json.RawMessage(`{"catalogId":"a.example/NEW"}`), Baseline: &store.FileWrite{Version: 1, Content: content}},
		{CatalogID: "b.example/NEW", SignedEntry: json.RawMessage(`{"catalogId":"b.example/NEW"}`), Baseline: &store.FileWrite{Version: 1, Content: content}},
	}})
	if !errors.Is(err, store.ErrCatalogIDNotAllowed) {
		t.Errorf("two new ids sharing file names: Publish = %v, want ErrCatalogIDNotAllowed", err)
	}

	if len(blobs.data) != len(before) {
		t.Errorf("a refused publish wrote files: %d blobs, had %d", len(blobs.data), len(before))
	}
	for p, b := range before {
		if string(blobs.data[p]) != b {
			t.Errorf("%s changed", p)
		}
	}

	// An id already in the index is not re-checked: it can be updated.
	if err := publishBaseline(t, s, "a.example/CAT-1"); err != nil {
		t.Errorf("update of an existing id: %v", err)
	}
}

// TestLoad_CarriesPriorLatestReferenceAndStoredFiles: the prior state must
// say what "latest" was published as (the entry's own reference) and what
// the blob store holds under both "latest" names -- including a file the
// entry does not list, an unservable one, and not another catalog's.
func TestLoad_CarriesPriorLatestReferenceAndStoredFiles(t *testing.T) {
	blobs := newFakeBlobStore()
	s := store.New(blobs)
	ctx := context.Background()
	const id = "example.test/CAT-1"

	cat := json.RawMessage(`{"id":"CAT-1","descriptor":{"name":"Test"}}`)
	content := baselineFile(t, id, cat, time.Now().Add(time.Hour))
	entry := json.RawMessage(`{"catalogId":"` + id + `","entryVersion":1,"catalogType":"REGULAR","isActive":true,` +
		`"baseline":{"version":1,"url":"https://example.test/catalogs/CAT-1.v1.json","size":1,"digest":"sha-256:x"},` +
		`"latest":{"version":1,"url":"https://example.test/catalogs/CAT-1.latest.json","size":7,"digest":"sha-256:y"}}`)
	if err := s.Publish(ctx, store.PublishRequest{NodeID: "example.test", Updates: []store.CatalogUpdate{{
		CatalogID: id, SignedEntry: entry,
		Baseline: &store.FileWrite{Version: 1, Content: content},
		Latest:   &store.FileWrite{Version: 1, Content: content},
	}}}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	load := func() store.CatalogState {
		t.Helper()
		states, err := s.LoadCatalogs(ctx, []string{id})
		if err != nil {
			t.Fatalf("LoadCatalogs: %v", err)
		}
		return states[id]
	}

	st := load()
	if st.LatestFile == nil || st.LatestFile.URL != "https://example.test/catalogs/CAT-1.latest.json" || st.LatestFile.Digest != "sha-256:y" || !st.LatestPublished {
		t.Fatalf("LatestFile = %+v, want the entry's own latest reference", st.LatestFile)
	}
	if len(st.StoredLatest) != 1 || st.StoredLatest[0].Compressed || !st.StoredLatest[0].Readable ||
		string(st.StoredLatest[0].Content) != string(content) || st.StoredLatest[0].Version != 1 || st.StoredLatest[0].RetiredAt != nil {
		t.Fatalf("StoredLatest = %+v, want the one plain file, readable", st.StoredLatest)
	}

	// Plain bytes under the .gz name: present, not servable as named.
	blobs.data[store.LatestFilePath(id, true)] = content
	st = load()
	if got := st.Stored(true); got == nil || got.Readable {
		t.Errorf("Stored(true) = %+v, want present and not readable", got)
	}
	// Another catalog's file under this catalog's name is not this
	// catalog's to rewrite.
	other := baselineFile(t, "other.test/CAT-1", cat, time.Now().Add(time.Hour))
	blobs.data[store.LatestFilePath(id, true)] = gzipBytes(t, other)
	if got := load().Stored(true); got != nil {
		t.Errorf("Stored(true) = %+v, want another catalog's file left out", got)
	}
	delete(blobs.data, store.LatestFilePath(id, true))

	// Retired: the state carries the tombstone as stored, its retiredAt,
	// and still the stored latest files.
	tomb := json.RawMessage(`{"catalogId":"` + id + `","entryVersion":2,"catalogType":"REGULAR","retiredAt":"2026-11-12T10:00:00Z"}`)
	if err := s.Publish(ctx, store.PublishRequest{NodeID: "example.test", Retirements: []store.CatalogUpdate{{CatalogID: id, SignedEntry: tomb}}}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	st = load()
	if !st.Retired || st.RetiredAt != "2026-11-12T10:00:00Z" || string(st.SignedEntry) != string(tomb) {
		t.Errorf("retired state = %+v, want RetiredAt and SignedEntry from the tombstone", st)
	}
	if len(st.StoredLatest) != 1 || st.LatestFile != nil {
		t.Errorf("retired state: StoredLatest=%d LatestFile=%v, want the stored file and no reference", len(st.StoredLatest), st.LatestFile)
	}
}

// TestPublish_LatestAlt_WritesBothLatestNames: a retirement can carry the
// final tombstone for both "latest" names.
func TestPublish_LatestAlt_WritesBothLatestNames(t *testing.T) {
	blobs := newFakeBlobStore()
	s := store.New(blobs)
	const id = "example.test/CAT-1"
	plain := []byte(`{"catalogId":"example.test/CAT-1","retiredAt":"2026-11-12T10:00:00Z"}`)
	zipped := gzipBytes(t, plain)
	err := s.Publish(context.Background(), store.PublishRequest{NodeID: "example.test", Retirements: []store.CatalogUpdate{{
		CatalogID: id, SignedEntry: json.RawMessage(`{"catalogId":"example.test/CAT-1","retiredAt":"2026-11-12T10:00:00Z"}`),
		Latest:    &store.FileWrite{Content: plain, ServedContent: zipped, Compressed: true},
		LatestAlt: &store.FileWrite{Content: plain, ServedContent: plain},
	}}})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if string(blobs.data[store.LatestFilePath(id, true)]) != string(zipped) || string(blobs.data[store.LatestFilePath(id, false)]) != string(plain) {
		t.Errorf("latest files = %v, want both names written", blobs.data)
	}
}

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		t.Fatalf("gzip: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip: %v", err)
	}
	return buf.Bytes()
}
