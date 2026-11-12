package store_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beckn/catalog-core/pkg/catalog"
	"github.com/beckn/catalog-core/pkg/catalog/store"
)

// --- doubles ---------------------------------------------------------------

// sharedBlobs is the one blob store several independent writers share:
// what an object-store bucket or a shared directory is to several
// processes.
type sharedBlobs struct {
	mu   sync.Mutex
	data map[string][]byte
	gen  map[string]int
}

func newSharedBlobs() *sharedBlobs {
	return &sharedBlobs{data: map[string][]byte{}, gen: map[string]int{}}
}

func (b *sharedBlobs) get(path string) ([]byte, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.data[path]
	if !ok {
		return nil, "", store.ErrBlobNotFound
	}
	return append([]byte{}, c...), fmt.Sprintf("g%d", b.gen[path]), nil
}

func (b *sharedBlobs) put(path string, content []byte, conditional bool, ifVersion string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if conditional {
		current := ""
		if _, ok := b.data[path]; ok {
			current = fmt.Sprintf("g%d", b.gen[path])
		}
		if current != ifVersion {
			return "", store.ErrPreconditionFailed
		}
	}
	b.gen[path]++
	b.data[path] = append([]byte{}, content...)
	return fmt.Sprintf("g%d", b.gen[path]), nil
}

func (b *sharedBlobs) mustGet(t *testing.T, path string) []byte {
	t.Helper()
	c, _, err := b.get(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return c
}

var errCrashed = errors.New("injected: process died")

// writer is one process's handle on sharedBlobs. It implements
// store.ConditionalBlobStore, can be made to "die" at its Nth write
// (that write and every later one fail, nothing is stored), and can run
// a hook around its operations to stage an exact interleaving with
// another writer.
type writer struct {
	shared *sharedBlobs
	mu     sync.Mutex
	writes []string // "put <path>" / "cas <path>", in order, successful or not
	dieAt  int      // 1-based write number; 0 = never
	// before runs ahead of each operation; after runs once a write has
	// succeeded. op is "get", "put" or "cas".
	before func(op, path string)
	after  func(op, path string)
}

func (w *writer) record(op, path string) (dead bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, op+" "+path)
	return w.dieAt > 0 && len(w.writes) >= w.dieAt
}

func (w *writer) hook(f func(op, path string), op, path string) {
	if f != nil {
		f(op, path)
	}
}

func (w *writer) Get(ctx context.Context, path string) ([]byte, error) {
	w.hook(w.before, "get", path)
	c, _, err := w.shared.get(path)
	return c, err
}

func (w *writer) GetVersioned(ctx context.Context, path string) ([]byte, string, error) {
	w.hook(w.before, "get", path)
	return w.shared.get(path)
}

func (w *writer) Put(ctx context.Context, path string, content []byte) error {
	w.hook(w.before, "put", path)
	if w.record("put", path) {
		return errCrashed
	}
	_, err := w.shared.put(path, content, false, "")
	if err == nil {
		w.hook(w.after, "put", path)
	}
	return err
}

func (w *writer) PutIfVersion(ctx context.Context, path string, content []byte, ifVersion string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	w.hook(w.before, "cas", path)
	if w.record("cas", path) {
		return "", errCrashed
	}
	v, err := w.shared.put(path, content, true, ifVersion)
	if err == nil {
		w.hook(w.after, "cas", path)
	}
	return v, err
}

// plainWriter exposes only Get/Put: a blob store without the capability.
type plainWriter struct{ w *writer }

func (p plainWriter) Get(ctx context.Context, path string) ([]byte, error) { return p.w.Get(ctx, path) }
func (p plainWriter) Put(ctx context.Context, path string, content []byte) error {
	return p.w.Put(ctx, path, content)
}

var fastRetry = store.RetryPolicy{MaxAttempts: 64, BaseBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond, TakeoverGrace: 20 * time.Millisecond}

func newCASStore(shared *sharedBlobs) (*store.Store, *writer) {
	w := &writer{shared: shared}
	return store.New(w).WithRetryPolicy(fastRetry), w
}

// --- fixtures --------------------------------------------------------------

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha-256:" + hex.EncodeToString(sum[:])
}

// baselineUpdate builds a full-baseline update of catalogID at
// file version v / entry version ev whose bytes are unique to tag.
func baselineUpdate(t *testing.T, catalogID string, v, ev int64, tag string) store.CatalogUpdate {
	t.Helper()
	cat := json.RawMessage(fmt.Sprintf(`{"id":%q,"descriptor":{"name":%q},"provider":{},"resources":[]}`, store.LocalName(catalogID), tag))
	file := baselineFile(t, catalogID, cat, time.Now().Add(time.Hour))
	latest := append(append([]byte{}, file...), ' ') // same document, distinct bytes
	entry, err := json.Marshal(catalog.CatalogEntry{
		CatalogID: catalogID, EntryVersion: ev, CatalogType: "REGULAR",
		Baseline: catalog.FileEntry{Version: v, URL: "https://example.test/" + store.CatalogFilePath(catalogID, v, "json", false), Size: int64(len(file)), Digest: digest(file)},
		Latest:   &catalog.FileEntry{Version: v, URL: "https://example.test/" + store.LatestFilePath(catalogID, false), Size: int64(len(latest)), Digest: digest(latest)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return store.CatalogUpdate{
		CatalogID: catalogID, SignedEntry: entry,
		Baseline: &store.FileWrite{Version: v, Content: file},
		Latest:   &store.FileWrite{Version: v, Content: latest},
	}
}

func tombstoneUpdate(t *testing.T, catalogID string, ev int64) store.CatalogUpdate {
	t.Helper()
	entry, err := json.Marshal(catalog.CatalogEntry{CatalogID: catalogID, EntryVersion: ev, CatalogType: "REGULAR", RetiredAt: time.Now().Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatal(err)
	}
	return store.CatalogUpdate{CatalogID: catalogID, SignedEntry: entry,
		Latest: &store.FileWrite{Content: json.RawMessage(fmt.Sprintf(`{"catalogId":%q,"retiredAt":"x","ev":%d}`, catalogID, ev))}}
}

func readEntries(t *testing.T, shared *sharedBlobs) map[string]catalog.CatalogEntry {
	t.Helper()
	raw, _, err := shared.get(store.IndexPath())
	if errors.Is(err, store.ErrBlobNotFound) {
		return map[string]catalog.CatalogEntry{}
	}
	var doc struct {
		Catalogs []catalog.CatalogEntry `json:"catalogs"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("index does not parse: %v", err)
	}
	out := map[string]catalog.CatalogEntry{}
	for _, e := range doc.Catalogs {
		if _, dup := out[e.CatalogID]; dup {
			t.Fatalf("index lists %s twice", e.CatalogID)
		}
		out[e.CatalogID] = e
	}
	return out
}

// checkReferences is what a reader relies on: every baseline the index
// lists exists and has the digest its entry states. It fails the test
// if not. It returns the catalogIds whose "latest" file does not match
// its entry's latest.digest, which callers assert on separately.
func checkReferences(t *testing.T, shared *sharedBlobs) (staleLatest []string) {
	t.Helper()
	for id, e := range readEntries(t, shared) {
		if e.IsRetired() {
			continue
		}
		path := store.CatalogFilePath(id, e.Baseline.Version, "json", false)
		got, _, err := shared.get(path)
		if err != nil {
			t.Errorf("index lists %s but it cannot be read: %v", path, err)
			continue
		}
		if digest(got) != e.Baseline.Digest {
			t.Errorf("%s does not have the digest its index entry states", path)
		}
		if e.Latest != nil {
			got, _, err := shared.get(store.LatestFilePath(id, false))
			if err != nil || digest(got) != e.Latest.Digest {
				staleLatest = append(staleLatest, id)
			}
		}
	}
	return staleLatest
}

func publish(t *testing.T, s *store.Store, base *store.Base, updates ...store.CatalogUpdate) error {
	t.Helper()
	return s.Publish(context.Background(), store.PublishRequest{NodeID: "example.test", Updates: updates, Base: base})
}

func loadBase(t *testing.T, s *store.Store, ids ...string) (map[string]store.CatalogState, *store.Base) {
	t.Helper()
	states, base, err := s.LoadForPublish(context.Background(), ids)
	if err != nil {
		t.Fatalf("LoadForPublish: %v", err)
	}
	return states, base
}

// --- many writers ----------------------------------------------------------

// Sixteen independent Stores (no shared lock of any kind) publish
// sixteen different catalogs to one blob store at once. beckn-onix#952.
func TestPublish_ManyWriters_DifferentCatalogs_LoseNothing(t *testing.T) {
	shared := newSharedBlobs()
	const n = 16
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, _ := newCASStore(shared)
			id := fmt.Sprintf("example.test/CAT-%d", i)
			_, base := loadBase(t, s, id)
			errs[i] = publish(t, s, base, baselineUpdate(t, id, 1, 1, "v1"))
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("writer %d: %v", i, err)
		}
	}
	if got := len(readEntries(t, shared)); got != n {
		t.Fatalf("index has %d of %d catalogs", got, n)
	}
	if stale := checkReferences(t, shared); len(stale) != 0 {
		t.Errorf("latest out of step with the index for %v", stale)
	}
}

// Four independent Stores each publish the same catalog five times,
// recomputing from fresh state whenever Publish says the catalog moved.
// Every one of the twenty publishes must be reflected: entryVersion
// ends at 20, and the files the final entry references are intact.
func TestPublish_ManyWriters_SameCatalog_NoLostUpdate(t *testing.T) {
	shared := newSharedBlobs()
	const writers, rounds = 4, 5
	const id = "example.test/CAT-1"
	var conflicts int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, _ := newCASStore(shared)
			for r := range rounds {
				for {
					states, base := loadBase(t, s, id)
					prior := states[id]
					v := int64(1)
					if prior.BaselineFile != nil {
						v = prior.BaselineFile.Version + 1
					}
					err := publish(t, s, base, baselineUpdate(t, id, v, prior.EntryVersion+1, fmt.Sprintf("w%d-r%d", w, r)))
					if errors.Is(err, store.ErrConflict) {
						mu.Lock()
						conflicts++
						mu.Unlock()
						continue
					}
					if err != nil {
						t.Errorf("writer %d round %d: %v", w, r, err)
					}
					break
				}
			}
		}()
	}
	wg.Wait()
	e := readEntries(t, shared)[id]
	if e.EntryVersion != writers*rounds || e.Baseline.Version != writers*rounds {
		t.Fatalf("entryVersion %d, baseline.version %d after %d publishes: an update was lost", e.EntryVersion, e.Baseline.Version, writers*rounds)
	}
	if stale := checkReferences(t, shared); len(stale) != 0 {
		t.Errorf("latest out of step with the index for %v", stale)
	}
	t.Logf("%d conflicts recomputed", conflicts)
}

// --- conflict rules --------------------------------------------------------

// Two writers compute an update of one catalog from the same prior
// state. The second must be refused, by name, and must not have
// replaced the first one's file.
func TestPublish_StaleBase_IsRefusedAndWritesNothing(t *testing.T) {
	shared := newSharedBlobs()
	const id = "example.test/CAT-1"
	s1, _ := newCASStore(shared)
	s2, w2 := newCASStore(shared)
	_, base1 := loadBase(t, s1, id)
	_, base2 := loadBase(t, s2, id)

	first := baselineUpdate(t, id, 1, 1, "first")
	if err := publish(t, s1, base1, first); err != nil {
		t.Fatalf("first writer: %v", err)
	}
	err := publish(t, s2, base2, baselineUpdate(t, id, 1, 1, "second"))
	var conflict *store.ConflictError
	if !errors.As(err, &conflict) || !errors.Is(err, store.ErrConflict) {
		t.Fatalf("second writer: got %v, want a ConflictError", err)
	}
	if len(conflict.CatalogIDs) != 1 || conflict.CatalogIDs[0] != id {
		t.Errorf("conflict names %v, want [%s]", conflict.CatalogIDs, id)
	}
	if len(w2.writes) != 0 {
		t.Errorf("the refused writer wrote %v", w2.writes)
	}
	if got := shared.mustGet(t, store.CatalogFilePath(id, 1, "json", false)); string(got) != string(first.Baseline.Content) {
		t.Error("the committed baseline was replaced")
	}
	if stale := checkReferences(t, shared); len(stale) != 0 {
		t.Errorf("latest out of step for %v", stale)
	}
}

// Another writer commits a different catalog between this writer's read
// of the index and its write. Its own entry is still valid, so it is
// merged again without recomputing, and its file is not written twice.
func TestPublish_IndexMovedByOtherCatalog_MergesAgain(t *testing.T) {
	shared := newSharedBlobs()
	s1, w1 := newCASStore(shared)
	s2, _ := newCASStore(shared)
	once := sync.Once{}
	w1.before = func(op, path string) {
		if op == "cas" && path == store.IndexPath() {
			once.Do(func() {
				if err := publish(t, s2, nil, baselineUpdate(t, "example.test/CAT-B", 1, 1, "b")); err != nil {
					t.Errorf("interleaved writer: %v", err)
				}
			})
		}
	}
	_, base := loadBase(t, s1, "example.test/CAT-A")
	if err := publish(t, s1, base, baselineUpdate(t, "example.test/CAT-A", 1, 1, "a")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	entries := readEntries(t, shared)
	if len(entries) != 2 {
		t.Fatalf("index has %d entries, want both CAT-A and CAT-B", len(entries))
	}
	var baselineWrites, indexWrites int
	for _, wr := range w1.writes {
		if strings.HasSuffix(wr, "CAT-A.v1.json") {
			baselineWrites++
		}
		if strings.HasSuffix(wr, store.IndexPath()) {
			indexWrites++
		}
	}
	if baselineWrites != 1 || indexWrites != 2 {
		t.Errorf("baseline written %d times, index %d times; want 1 and 2 (%v)", baselineWrites, indexWrites, w1.writes)
	}
	if stale := checkReferences(t, shared); len(stale) != 0 {
		t.Errorf("latest out of step for %v", stale)
	}
}

// The index never stops moving: Publish gives up with ErrIndexBusy and
// commits nothing.
func TestPublish_IndexKeepsMoving_GivesUpWithoutCommitting(t *testing.T) {
	shared := newSharedBlobs()
	w := &writer{shared: shared}
	s := store.New(w).WithRetryPolicy(store.RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	n := 0
	w.before = func(op, path string) {
		if op == "cas" && path == store.IndexPath() {
			n++
			shared.put(store.IndexPath(), []byte(fmt.Sprintf(`{"nodeId":"x","catalogs":[],"n":%d}`, n)), false, "")
		}
	}
	err := publish(t, s, nil, baselineUpdate(t, "example.test/CAT-A", 1, 1, "a"))
	if !errors.Is(err, store.ErrIndexBusy) {
		t.Fatalf("got %v, want ErrIndexBusy", err)
	}
	if n != 3 {
		t.Errorf("tried the index write %d times, want 3", n)
	}
	if len(readEntries(t, shared)) != 0 {
		t.Error("an entry was committed")
	}
}

// --- files written before the commit ---------------------------------------

// A versioned file left behind by a publish that died (no entry lists
// it) does not block that version for ever: after the grace period it
// is taken over.
func TestPublish_LeftoverVersionedFile_IsTakenOver(t *testing.T) {
	shared := newSharedBlobs()
	const id = "example.test/CAT-1"
	path := store.CatalogFilePath(id, 1, "json", false)
	shared.put(path, []byte("left by a dead publish"), false, "")
	s, _ := newCASStore(shared)
	_, base := loadBase(t, s, id)
	u := baselineUpdate(t, id, 1, 1, "mine")
	if err := publish(t, s, base, u); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if string(shared.mustGet(t, path)) != string(u.Baseline.Content) {
		t.Error("the leftover file was not replaced")
	}
	checkReferences(t, shared)
}

// The same situation, but the file belongs to a writer that commits
// while this one is waiting: this one must be refused and the file left
// exactly as the committed entry references it.
func TestPublish_VersionedFileOfWriterAboutToCommit_IsNotReplaced(t *testing.T) {
	shared := newSharedBlobs()
	const id = "example.test/CAT-1"
	s1, w1 := newCASStore(shared)
	s2, w2 := newCASStore(shared)
	_, base1 := loadBase(t, s1, id)
	_, base2 := loadBase(t, s2, id)
	theirs := baselineUpdate(t, id, 1, 1, "theirs")

	// s1 stalls between writing its file and committing; s2 runs into
	// the file meanwhile.
	release := make(chan struct{})
	done := make(chan error, 1)
	w1.before = func(op, path string) {
		if op == "cas" && path == store.IndexPath() {
			<-release
		}
	}
	go func() { done <- publish(t, s1, base1, theirs) }()
	sawFile := make(chan struct{})
	var once sync.Once
	w2.before = func(op, path string) {
		if op == "get" && path == store.IndexPath() && len(w2.writes) > 0 {
			once.Do(func() { close(sawFile) }) // s2 is now in its grace wait
		}
	}
	go func() { <-sawFile; close(release) }()
	for {
		if _, _, err := shared.get(store.CatalogFilePath(id, 1, "json", false)); err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	s2.WithRetryPolicy(store.RetryPolicy{TakeoverGrace: 2 * time.Second})
	err := publish(t, s2, base2, baselineUpdate(t, id, 1, 1, "mine"))
	once.Do(func() { close(sawFile) }) // never leave s1 stalled, whatever s2 did
	if !errors.Is(err, store.ErrConflict) {
		t.Errorf("second writer: got %v, want ErrConflict", err)
	}
	if err := <-done; err != nil && !errors.Is(err, store.ErrConflict) {
		t.Fatalf("first writer: %v", err)
	}
	if string(shared.mustGet(t, store.CatalogFilePath(id, 1, "json", false))) != string(theirs.Baseline.Content) {
		t.Error("the committed writer's baseline was replaced")
	}
	if stale := checkReferences(t, shared); len(stale) != 0 {
		t.Errorf("latest out of step for %v", stale)
	}
}

// A writer that replaced a file before its commit and then loses puts
// back what was there: here a retirement's "latest" tombstone, when
// another writer changes the catalog first.
func TestPublish_LoserRestoresFileItReplaced(t *testing.T) {
	shared := newSharedBlobs()
	const id = "example.test/CAT-1"
	s1, _ := newCASStore(shared)
	if err := publish(t, s1, nil, baselineUpdate(t, id, 1, 1, "v1")); err != nil {
		t.Fatal(err)
	}
	latestPath := store.LatestFilePath(id, false)
	liveLatest := shared.mustGet(t, latestPath)

	s2, w2 := newCASStore(shared)
	_, base2 := loadBase(t, s2, id)
	var once sync.Once
	w2.before = func(op, path string) {
		if op == "cas" && path == store.IndexPath() {
			once.Do(func() {
				// Another writer makes a metadata-only change: new
				// entry, same files, "latest" untouched.
				meta := baselineUpdate(t, id, 1, 2, "v1")
				meta.Baseline, meta.Latest = nil, nil
				if err := publish(t, s1, nil, meta); err != nil {
					t.Errorf("interleaved writer: %v", err)
				}
			})
		}
	}
	err := s2.Publish(context.Background(), store.PublishRequest{NodeID: "example.test", Retirements: []store.CatalogUpdate{tombstoneUpdate(t, id, 2)}, Base: base2})
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
	if string(shared.mustGet(t, latestPath)) != string(liveLatest) {
		t.Error("the tombstone written by the refused retirement was left in the live catalog's latest file")
	}
	if readEntries(t, shared)[id].IsRetired() {
		t.Error("the refused retirement was committed")
	}
}

// A file this writer's committed entry references is replaced by a
// writer that read the index before the commit: the committed writer
// puts its bytes back.
func TestPublish_CommittedFileReplacedByLateWriter_IsRewritten(t *testing.T) {
	shared := newSharedBlobs()
	const id = "example.test/CAT-1"
	s, w := newCASStore(shared)
	path := store.CatalogFilePath(id, 1, "json", false)
	w.after = func(op, p string) {
		if op == "cas" && p == store.IndexPath() {
			shared.put(path, []byte("a late writer's bytes"), false, "")
		}
	}
	u := baselineUpdate(t, id, 1, 1, "mine")
	if err := publish(t, s, nil, u); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if string(shared.mustGet(t, path)) != string(u.Baseline.Content) {
		t.Error("the committed baseline does not hold the committed writer's bytes")
	}
	checkReferences(t, shared)
}

// "latest" is one file overwritten in place. A writer whose entry has
// been superseded must not write its older "latest" over the newer one,
// wherever the newer writer gets in.
func TestPublish_SupersededWriter_DoesNotOverwriteNewerLatest(t *testing.T) {
	for _, point := range []string{"before reading latest", "before writing latest"} {
		t.Run(point, func(t *testing.T) {
			shared := newSharedBlobs()
			const id = "example.test/CAT-1"
			latestPath := store.LatestFilePath(id, false)
			s1, w1 := newCASStore(shared)
			s2, _ := newCASStore(shared)
			newer := baselineUpdate(t, id, 2, 2, "newer")
			committed := false
			var once sync.Once
			w1.after = func(op, p string) {
				if op == "cas" && p == store.IndexPath() {
					committed = true
				}
			}
			w1.before = func(op, p string) {
				if !committed || p != latestPath {
					return
				}
				if (point == "before reading latest" && op == "get") || (point == "before writing latest" && op == "cas") {
					once.Do(func() {
						_, base := loadBase(t, s2, id)
						if err := publish(t, s2, base, newer); err != nil {
							t.Errorf("newer writer: %v", err)
						}
					})
				}
			}
			if err := publish(t, s1, nil, baselineUpdate(t, id, 1, 1, "older")); err != nil {
				t.Fatalf("older writer: %v", err)
			}
			if string(shared.mustGet(t, latestPath)) != string(newer.Latest.Content) {
				t.Error("latest holds the superseded writer's content")
			}
			if stale := checkReferences(t, shared); len(stale) != 0 {
				t.Errorf("latest out of step for %v", stale)
			}
		})
	}
}

// The caller's context ends right after the index commit (a client
// that hangs up). The publish is committed, so "latest" is still
// brought up to date.
func TestPublish_ContextEndsAfterCommit_StillWritesLatest(t *testing.T) {
	shared := newSharedBlobs()
	const id = "example.test/CAT-1"
	s, w := newCASStore(shared)
	ctx, cancel := context.WithCancel(context.Background())
	w.after = func(op, p string) {
		if op == "cas" && p == store.IndexPath() {
			cancel()
		}
	}
	err := s.Publish(ctx, store.PublishRequest{NodeID: "example.test", Updates: []store.CatalogUpdate{baselineUpdate(t, id, 1, 1, "v1")}})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if stale := checkReferences(t, shared); len(stale) != 0 {
		t.Errorf("latest left behind the committed index for %v", stale)
	}
}

// --- a process that dies part-way ------------------------------------------

// The process dies at each write of a publish and of a retirement in
// turn. Whatever the step, every baseline the index lists must exist
// with its digest. The table states what else a reader sees, and that
// repeating the operation repairs it.
func TestPublish_DiesAtEachWrite(t *testing.T) {
	const id = "example.test/CAT-1"
	type step struct {
		dieAt       int
		died        string // which write was lost
		entryAfter  int64  // entryVersion a reader then sees
		retired     bool
		latestStale bool   // latest.digest does not match the file
		latestHolds string // "v1", "v2" or "tombstone"
	}
	cases := []struct {
		name       string
		capability bool
		retire     bool
		steps      []step
	}{
		{"update, conditional store", true, false, []step{
			{1, "cas catalogs/CAT-1.v2.json", 1, false, false, "v1"},
			{2, "cas index/becknCatalogs.index.json", 1, false, false, "v1"}, // orphan CAT-1.v2.json
			{3, "cas catalogs/CAT-1.latest.json", 2, false, true, "v1"},      // committed; latest one version behind
		}},
		{"retire, conditional store", true, true, []step{
			{1, "cas catalogs/CAT-1.latest.json", 1, false, false, "v1"},
			{2, "cas index/becknCatalogs.index.json", 1, false, true, "tombstone"}, // not committed; latest already says retired
		}},
		{"update, plain store", false, false, []step{
			{1, "put catalogs/CAT-1.v2.json", 1, false, false, "v1"},
			{2, "put catalogs/CAT-1.latest.json", 1, false, false, "v1"},    // orphan CAT-1.v2.json
			{3, "put index/becknCatalogs.index.json", 1, false, true, "v2"}, // not committed; latest one version ahead
		}},
		{"retire, plain store", false, true, []step{
			{1, "put catalogs/CAT-1.latest.json", 1, false, false, "v1"},
			{2, "put index/becknCatalogs.index.json", 1, false, true, "tombstone"},
		}},
	}
	for _, tc := range cases {
		for _, st := range tc.steps {
			t.Run(fmt.Sprintf("%s/dies at write %d", tc.name, st.dieAt), func(t *testing.T) {
				shared := newSharedBlobs()
				newStore := func(dieAt int) (*store.Store, *writer) {
					w := &writer{shared: shared, dieAt: dieAt}
					if tc.capability {
						return store.New(w).WithRetryPolicy(fastRetry), w
					}
					return store.New(plainWriter{w}), w
				}
				setup, _ := newStore(0)
				v1 := baselineUpdate(t, id, 1, 1, "v1")
				if err := publish(t, setup, nil, v1); err != nil {
					t.Fatal(err)
				}
				v2 := baselineUpdate(t, id, 2, 2, "v2")
				tomb := tombstoneUpdate(t, id, 2)
				run := func(s *store.Store) error {
					_, base := loadBase(t, s, id)
					if tc.retire {
						return s.Publish(context.Background(), store.PublishRequest{NodeID: "example.test", Retirements: []store.CatalogUpdate{tomb}, Base: base})
					}
					return publish(t, s, base, v2)
				}

				dying, w := newStore(st.dieAt)
				if err := run(dying); !errors.Is(err, errCrashed) {
					t.Fatalf("got %v, want the injected failure", err)
				}
				if got := w.writes[len(w.writes)-1]; len(w.writes) != st.dieAt || got != st.died {
					t.Fatalf("died at write %d (%s), table says %d (%s); all writes: %v", len(w.writes), got, st.dieAt, st.died, w.writes)
				}

				// What a reader sees now.
				stale := checkReferences(t, shared)
				e := readEntries(t, shared)[id]
				if e.EntryVersion != st.entryAfter || e.IsRetired() != st.retired {
					t.Errorf("reader sees entryVersion %d retired=%v, want %d %v", e.EntryVersion, e.IsRetired(), st.entryAfter, st.retired)
				}
				if (len(stale) == 1) != st.latestStale {
					t.Errorf("latest stale = %v, want %v", len(stale) == 1, st.latestStale)
				}
				holds := map[string]string{string(v1.Latest.Content): "v1", string(v2.Latest.Content): "v2", string(tomb.Latest.Content): "tombstone"}[string(shared.mustGet(t, store.LatestFilePath(id, false)))]
				if holds != st.latestHolds {
					t.Errorf("latest holds %q, want %q", holds, st.latestHolds)
				}

				// A new process repeats the operation.
				again, _ := newStore(0)
				if !tc.retire && e.EntryVersion == 2 {
					// The update is committed; what a caller sends next for
					// unchanged content is the same entry and "latest" only.
					v2.Baseline = nil
				}
				if err := run(again); err != nil {
					t.Fatalf("repeating the operation: %v", err)
				}
				if stale := checkReferences(t, shared); len(stale) != 0 {
					t.Errorf("latest still out of step after the repeat")
				}
				e = readEntries(t, shared)[id]
				want := "v2"
				if tc.retire {
					want = "tombstone"
				}
				holds = map[string]string{string(v2.Latest.Content): "v2", string(tomb.Latest.Content): "tombstone"}[string(shared.mustGet(t, store.LatestFilePath(id, false)))]
				if e.EntryVersion != 2 || e.IsRetired() != tc.retire || holds != want {
					t.Errorf("after the repeat: entryVersion %d retired=%v latest=%q", e.EntryVersion, e.IsRetired(), holds)
				}
			})
		}
	}
}

// --- a blob store without the capability -----------------------------------

// A plain BlobStore sees exactly the writes it always has, in the same
// order.
func TestPublish_PlainStore_WriteOrderUnchanged(t *testing.T) {
	w := &writer{shared: newSharedBlobs()}
	s := store.New(plainWriter{w})
	a := baselineUpdate(t, "example.test/CAT-A", 1, 1, "a")
	b := baselineUpdate(t, "example.test/CAT-B", 1, 1, "b")
	err := s.Publish(context.Background(), store.PublishRequest{NodeID: "example.test", Updates: []store.CatalogUpdate{a}, Retirements: []store.CatalogUpdate{b}})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"put catalogs/CAT-A.v1.json", "put catalogs/CAT-A.latest.json",
		"put catalogs/CAT-B.v1.json", "put catalogs/CAT-B.latest.json",
		"put index/becknCatalogs.index.json",
	}
	if strings.Join(w.writes, "\n") != strings.Join(want, "\n") {
		t.Errorf("writes:\n%s\nwant:\n%s", strings.Join(w.writes, "\n"), strings.Join(want, "\n"))
	}
}

// Even without the capability, a Base that is already stale when
// Publish reads the index is refused before anything is written.
func TestPublish_PlainStore_StaleBaseIsRefused(t *testing.T) {
	shared := newSharedBlobs()
	const id = "example.test/CAT-1"
	w1, w2 := &writer{shared: shared}, &writer{shared: shared}
	s1, s2 := store.New(plainWriter{w1}), store.New(plainWriter{w2})
	_, base2 := loadBase(t, s2, id)
	if err := publish(t, s1, nil, baselineUpdate(t, id, 1, 1, "first")); err != nil {
		t.Fatal(err)
	}
	if err := publish(t, s2, base2, baselineUpdate(t, id, 1, 1, "second")); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
	if len(w2.writes) != 0 {
		t.Errorf("the refused writer wrote %v", w2.writes)
	}
}
