package crawlmanager_test

// crawlmanager_test.go — covers PollIndexes and SyncNext against fakes for
// every injected port (Source, Sink, Store) plus a real *catalog.Fetcher
// pointed at an httptest server, so the fetch/verify/decode/resolve path is
// exercised for real and only discovery/handoff/persistence are faked.

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beckn/catalog-core/pkg/catalog"
	"github.com/beckn/catalog-core/pkg/catalog/crawler"
	"github.com/beckn/catalog-core/pkg/catalog/crawlmanager"
	"github.com/beckn/catalog-core/pkg/security/artifactsigner"
)

const testKeyID = "key-1"

func sha256Prefixed(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha-256:" + hex.EncodeToString(sum[:])
}

func signEntry(t *testing.T, priv ed25519.PrivateKey, entry catalog.CatalogEntry) []byte {
	t.Helper()
	unsigned, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	sigValue, err := artifactsigner.SignJSON(unsigned, "signature", priv)
	if err != nil {
		t.Fatal(err)
	}
	entry.Signature = catalog.EntrySignature{KeyID: testKeyID, Value: sigValue}
	out, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func signBaseline(t *testing.T, priv ed25519.PrivateKey, doc catalog.CatalogFileDoc) []byte {
	t.Helper()
	unsigned, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	sigValue, err := artifactsigner.SignJSON(unsigned, "signature", priv)
	if err != nil {
		t.Fatal(err)
	}
	doc.Signature = catalog.FileSignature{KeyID: testKeyID, Canonicalization: "JCS", Value: sigValue}
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// testNetwork serves one nodeId/index/baseline triple: a single-catalog
// index whose baseline is served at the same server under /baseline.
type testNetwork struct {
	srv  *httptest.Server
	pub  ed25519.PublicKey
	priv ed25519.PrivateKey

	mu            sync.Mutex
	entryVersion  int64
	baselineVer   int64
	baselineBytes []byte // cached: signBaseline stamps NextUpdate:time.Now(), so it must be signed once, not per request
	retired       bool
	networkIDs    []string
}

func newTestNetwork(t *testing.T) *testNetwork {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	n := &testNetwork{pub: pub, priv: priv, entryVersion: 1, baselineVer: 1}
	mux := http.NewServeMux()
	mux.HandleFunc("/index", n.serveIndex(t))
	mux.HandleFunc("/baseline", n.serveBaseline(t))
	n.srv = httptest.NewServer(mux)
	t.Cleanup(n.srv.Close)
	return n
}

// baseline returns this network's baseline bytes, signing them once (lazily)
// and reusing the same bytes thereafter -- signBaseline stamps NextUpdate
// with the current time, so signing twice would produce two different,
// equally-valid-looking baselines with different digests.
func (n *testNetwork) baseline(t *testing.T) []byte {
	t.Helper()
	if n.baselineBytes == nil {
		n.baselineBytes = signBaseline(t, n.priv, catalog.CatalogFileDoc{
			CatalogID: "p/c", Version: n.baselineVer, NextUpdate: time.Now(),
			Catalog: json.RawMessage(`{"id":"p/c","resources":[]}`),
		})
	}
	return n.baselineBytes
}

func (n *testNetwork) serveBaseline(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		n.mu.Lock()
		defer n.mu.Unlock()
		w.Write(n.baseline(t))
	}
}

func (n *testNetwork) serveIndex(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		n.mu.Lock()
		defer n.mu.Unlock()
		fields := catalog.CatalogEntry{CatalogID: "p/c", EntryVersion: n.entryVersion, CatalogType: "REGULAR", NetworkIDs: n.networkIDs}
		if n.retired {
			fields.RetiredAt = "2026-01-01T00:00:00Z" // retired: no baseline/changes, per the file spec
		} else {
			baseline := n.baseline(t)
			fields.Baseline = catalog.FileEntry{Version: n.baselineVer, URL: n.srv.URL + "/baseline", Digest: sha256Prefixed(baseline)}
		}
		entry := signEntry(t, n.priv, fields)
		index, err := json.Marshal(struct {
			NodeID   string            `json:"nodeId"`
			Catalogs []json.RawMessage `json:"catalogs"`
		}{NodeID: "node-1", Catalogs: []json.RawMessage{entry}})
		if err != nil {
			t.Fatal(err)
		}
		w.Write(index)
	}
}

func (n *testNetwork) bumpEntryVersion() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.entryVersion++
}

func (n *testNetwork) retire() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.retired = true
	n.entryVersion++
}

func (n *testNetwork) setNetworks(ids []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.networkIDs = ids
}

func newFetcherFor(n *testNetwork) *catalog.Fetcher {
	client := crawler.NewClient(5*time.Second, 1<<20, true)
	keys := crawler.StaticKeys(map[string]ed25519.PublicKey{testKeyID: n.pub})
	return catalog.NewFetcher(client, keys, 1<<20)
}

// --- fakes -----------------------------------------------------------

type fakeSource struct{ refs []crawlmanager.IndexRef }

func (f fakeSource) Discover(context.Context) ([]crawlmanager.IndexRef, error) { return f.refs, nil }

type fakeSink struct {
	mu  sync.Mutex
	got map[string][]byte
}

func newFakeSink() *fakeSink { return &fakeSink{got: map[string][]byte{}} }

func (f *fakeSink) Send(_ context.Context, entry catalog.CatalogEntry, content []byte) (crawlmanager.SinkOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got[entry.CatalogID] = content
	return crawlmanager.SinkOutcome{Accepted: true}, nil
}

type parkedEntry struct {
	item      crawlmanager.ClaimedItem
	parkCount int
	parkedAt  time.Time
	lastErr   string
}

type fakeStore struct {
	mu        sync.Mutex
	cursors   map[string]crawlmanager.CatalogCursor
	indexes   map[string]crawlmanager.IndexCursor
	queue     []crawlmanager.ClaimedItem
	claimed   map[string]crawlmanager.ClaimedItem // id -> item, held between ClaimNext and settle
	failures  []crawlmanager.PassReport
	parked    map[string]*parkedEntry // id -> entry, held while Parked
	abandoned []crawlmanager.AbandonedCatalog
	now       func() time.Time
	nextID    int
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		cursors: map[string]crawlmanager.CatalogCursor{},
		indexes: map[string]crawlmanager.IndexCursor{},
		claimed: map[string]crawlmanager.ClaimedItem{},
		parked:  map[string]*parkedEntry{},
		now:     time.Now,
	}
}

func (s *fakeStore) GetCatalogCursor(_ context.Context, catalogID string) (crawlmanager.CatalogCursor, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.cursors[catalogID]
	return c, ok, nil
}

func (s *fakeStore) RecordFailure(_ context.Context, report crawlmanager.PassReport) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures = append(s.failures, report)
	return nil
}

// lastError returns catalogID's most recently recorded failure, for
// stamping onto a parkedEntry -- must be called with s.mu already held.
func (s *fakeStore) lastError(catalogID string) string {
	for i := len(s.failures) - 1; i >= 0; i-- {
		if s.failures[i].CatalogID == catalogID {
			return s.failures[i].Error
		}
	}
	return ""
}

func (s *fakeStore) GetCatalogEnvelope(_ context.Context, catalogID string) (descriptor, provider json.RawMessage, catalogType, participantID string, ok bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, seen := s.cursors[catalogID]
	if !seen || (len(c.Descriptor) == 0 && len(c.Provider) == 0) {
		return nil, nil, "", "", false, nil
	}
	return c.Descriptor, c.Provider, c.CatalogType, c.ParticipantID, true, nil
}

func (s *fakeStore) GetIndexCursor(_ context.Context, indexURL string) (*crawlmanager.IndexCursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.indexes[indexURL]
	if !ok {
		return nil, nil
	}
	return &c, nil
}

func (s *fakeStore) UpsertIndexCursor(_ context.Context, cursor crawlmanager.IndexCursor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.indexes[cursor.IndexURL] = cursor
	return nil
}

func (s *fakeStore) Enqueue(_ context.Context, item crawlmanager.QueueItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	s.queue = append(s.queue, crawlmanager.ClaimedItem{QueueItem: item, ID: itoa(s.nextID), ClaimID: "claim"})
	return nil
}

func (s *fakeStore) ClaimNext(context.Context) (*crawlmanager.ClaimedItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return nil, nil
	}
	item := s.queue[0]
	s.queue = s.queue[1:]
	s.claimed[item.ID] = item
	return &item, nil
}

func (s *fakeStore) Complete(_ context.Context, id, _ string, cursor crawlmanager.CatalogCursor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.claimed, id)
	// Like the Postgres store: a settle that carries no envelope of its own
	// (a retire) keeps the one a previous sync captured.
	if prev, ok := s.cursors[cursor.CatalogID]; ok {
		if len(cursor.Descriptor) == 0 {
			cursor.Descriptor = prev.Descriptor
		}
		if len(cursor.Provider) == 0 {
			cursor.Provider = prev.Provider
		}
		if cursor.CatalogType == "" {
			cursor.CatalogType = prev.CatalogType
		}
	}
	s.cursors[cursor.CatalogID] = cursor
	return nil
}

func (s *fakeStore) Reschedule(_ context.Context, id, claimID string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.claimed[id]
	if !ok || item.ClaimID != claimID {
		return nil
	}
	delete(s.claimed, id)
	item.Attempts++
	s.queue = append(s.queue, item)
	return nil
}

func (s *fakeStore) Park(_ context.Context, id, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.claimed[id]
	delete(s.claimed, id)
	entry, ok := s.parked[id]
	if !ok {
		entry = &parkedEntry{item: item}
	}
	entry.parkCount++
	entry.parkedAt = s.now()
	entry.lastErr = s.lastError(item.CatalogID)
	s.parked[id] = entry
	return nil
}

func (s *fakeStore) RequeueOrAbandonParked(_ context.Context, olderThan time.Duration, maxParkCount int) (revived, abandoned int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for id, entry := range s.parked {
		if now.Sub(entry.parkedAt) < olderThan {
			continue
		}
		delete(s.parked, id)
		if entry.parkCount >= maxParkCount {
			s.abandoned = append(s.abandoned, crawlmanager.AbandonedCatalog{
				CatalogID: entry.item.CatalogID, IndexURL: entry.item.IndexURL, ParticipantID: entry.item.ParticipantID,
				ParkCount: entry.parkCount, LastError: entry.lastErr, AbandonedAt: now,
			})
			abandoned++
			continue
		}
		revivedItem := entry.item
		revivedItem.Attempts = 0
		s.queue = append(s.queue, revivedItem)
		revived++
	}
	return revived, abandoned, nil
}

func (s *fakeStore) ListAbandoned(context.Context) ([]crawlmanager.AbandonedCatalog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]crawlmanager.AbandonedCatalog, len(s.abandoned))
	copy(out, s.abandoned)
	return out, nil
}

func itoa(n int) string {
	return string(rune('0' + n))
}

// --- tests -----------------------------------------------------------

func TestPollIndexes_EnqueuesNewCatalog(t *testing.T) {
	net := newTestNetwork(t)
	store := newFakeStore()
	p := crawlmanager.Params{
		Fetcher: newFetcherFor(net),
		Source:  fakeSource{refs: []crawlmanager.IndexRef{{ParticipantID: "p", IndexURL: net.srv.URL + "/index"}}},
		Sink:    newFakeSink(),
		Store:   store,
	}
	if err := p.PollIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.queue) != 1 || store.queue[0].CatalogID != "p/c" {
		t.Fatalf("queue = %+v, want one item for p/c", store.queue)
	}
}

func TestPollIndexes_UnchangedCatalogNotRequeued(t *testing.T) {
	net := newTestNetwork(t)
	store := newFakeStore()
	p := crawlmanager.Params{
		Fetcher: newFetcherFor(net),
		Source:  fakeSource{refs: []crawlmanager.IndexRef{{ParticipantID: "p", IndexURL: net.srv.URL + "/index"}}},
		Sink:    newFakeSink(),
		Store:   store,
	}
	ctx := context.Background()
	if err := p.PollIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	// Drain the queue and record the cursor a sync would have settled, so the
	// second poll sees "already at this version".
	item, _ := store.ClaimNext(ctx)
	if item == nil {
		t.Fatal("expected a queued item after first poll")
	}
	if err := store.Complete(ctx, item.ID, item.ClaimID, crawlmanager.CatalogCursor{CatalogID: "p/c", Version: 1, EntryVersion: 1}); err != nil {
		t.Fatal(err)
	}

	if err := p.PollIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	if len(store.queue) != 0 {
		t.Fatalf("queue = %+v, want empty (nothing changed since the cursor)", store.queue)
	}
}

func TestPollIndexes_EntryVersionBumpRequeuesWithoutContentChange(t *testing.T) {
	net := newTestNetwork(t)
	store := newFakeStore()
	store.cursors["p/c"] = crawlmanager.CatalogCursor{CatalogID: "p/c", Version: 1, EntryVersion: 1}
	net.bumpEntryVersion() // simulate e.g. an isActive toggle: EntryVersion advances, content doesn't

	p := crawlmanager.Params{
		Fetcher: newFetcherFor(net),
		Source:  fakeSource{refs: []crawlmanager.IndexRef{{ParticipantID: "p", IndexURL: net.srv.URL + "/index"}}},
		Sink:    newFakeSink(),
		Store:   store,
	}
	if err := p.PollIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.queue) != 1 {
		t.Fatalf("queue = %+v, want one item (EntryVersion advanced past the cursor)", store.queue)
	}
}

func TestSyncNext_ResolvesAndSendsToSink(t *testing.T) {
	net := newTestNetwork(t)
	store := newFakeStore()
	sink := newFakeSink()
	p := crawlmanager.Params{Fetcher: newFetcherFor(net), Sink: sink, Store: store}
	ctx := context.Background()

	if err := store.Enqueue(ctx, crawlmanager.QueueItem{CatalogID: "p/c", IndexURL: net.srv.URL + "/index", ParticipantID: "p"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := p.SyncNext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Fatal("expected an item to be claimed")
	}
	if _, ok := sink.got["p/c"]; !ok {
		t.Fatalf("sink received %v, want p/c", sink.got)
	}
	cursor, seen, err := store.GetCatalogCursor(ctx, "p/c")
	if err != nil {
		t.Fatal(err)
	}
	if !seen || cursor.Version != 1 {
		t.Fatalf("cursor = %+v seen=%v, want version 1", cursor, seen)
	}
}

func TestSyncNext_EmptyQueueReturnsFalse(t *testing.T) {
	p := crawlmanager.Params{Store: newFakeStore()}
	claimed, err := p.SyncNext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if claimed {
		t.Fatal("expected claimed=false on an empty queue")
	}
}

func TestSyncNext_PermanentErrorParks(t *testing.T) {
	net := newTestNetwork(t)
	store := newFakeStore()
	p := crawlmanager.Params{Fetcher: newFetcherFor(net), Sink: newFakeSink(), Store: store}
	ctx := context.Background()

	// This catalogId is never listed in the index -- FindCatalog fails,
	// which syncOne raises as a permanent error.
	if err := store.Enqueue(ctx, crawlmanager.QueueItem{CatalogID: "p/missing", IndexURL: net.srv.URL + "/index", ParticipantID: "p"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := p.SyncNext(ctx)
	if !claimed || err == nil || !crawler.IsPermanent(err) {
		t.Fatalf("claimed=%v err=%v, want claimed=true and a permanent error", claimed, err)
	}
	if len(store.parked) != 1 {
		t.Fatalf("parked = %v, want exactly one parked item", store.parked)
	}
	if len(store.failures) != 1 {
		t.Fatalf("failures = %+v, want one recorded failure", store.failures)
	}
}

func TestSyncNext_TransientErrorReschedules(t *testing.T) {
	store := newFakeStore()
	// No Fetcher configured for a network that's actually reachable: point at
	// a closed server so the fetch fails with a plain (non-permanent) error.
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closed.Close()
	client := crawler.NewClient(time.Second, 1<<20, true)
	p := crawlmanager.Params{Fetcher: catalog.NewFetcher(client, crawler.StaticKeys(nil), 1<<20), Sink: newFakeSink(), Store: store}
	ctx := context.Background()

	if err := store.Enqueue(ctx, crawlmanager.QueueItem{CatalogID: "p/c", IndexURL: closed.URL, ParticipantID: "p"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := p.SyncNext(ctx)
	if !claimed || err == nil {
		t.Fatalf("claimed=%v err=%v, want claimed=true and an error", claimed, err)
	}
	if crawler.IsPermanent(err) {
		t.Fatalf("err = %v, want transient (connection failure), not permanent", err)
	}
	if len(store.parked) != 0 {
		t.Fatalf("parked = %v, want nothing parked for a transient error", store.parked)
	}
	if len(store.queue) != 1 {
		t.Fatalf("queue = %+v, want the item rescheduled", store.queue)
	}
}

func TestSyncNext_SinkRejectionIsReported(t *testing.T) {
	net := newTestNetwork(t)
	store := newFakeStore()
	rejecting := rejectingSink{}
	p := crawlmanager.Params{Fetcher: newFetcherFor(net), Sink: rejecting, Store: store}
	ctx := context.Background()

	if err := store.Enqueue(ctx, crawlmanager.QueueItem{CatalogID: "p/c", IndexURL: net.srv.URL + "/index", ParticipantID: "p"}); err != nil {
		t.Fatal(err)
	}
	_, err := p.SyncNext(ctx)
	if err == nil || !strings.Contains(err.Error(), "rejection reason") {
		t.Fatalf("err = %v, want it to carry the sink's rejection reason", err)
	}
}

type rejectingSink struct{}

func (rejectingSink) Send(context.Context, catalog.CatalogEntry, []byte) (crawlmanager.SinkOutcome, error) {
	return crawlmanager.SinkOutcome{Accepted: false, Reason: "rejection reason"}, nil
}

func TestPollIndexes_RollbackIsLoggedNotEnqueued(t *testing.T) {
	net := newTestNetwork(t)
	store := newFakeStore()
	// The index currently declares v1/entryVersion 1, but the stored cursor
	// claims we already synced past that -- a regression (CON-TBD-11).
	store.cursors["p/c"] = crawlmanager.CatalogCursor{CatalogID: "p/c", Version: 5, EntryVersion: 5}

	p := crawlmanager.Params{
		Fetcher: newFetcherFor(net),
		Source:  fakeSource{refs: []crawlmanager.IndexRef{{ParticipantID: "p", IndexURL: net.srv.URL + "/index"}}},
		Sink:    newFakeSink(),
		Store:   store,
	}
	if err := p.PollIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.queue) != 0 {
		t.Fatalf("queue = %+v, want nothing enqueued for a rollback", store.queue)
	}
}

func TestPollIndexes_RetiredNeverSeenCatalogIsIgnored(t *testing.T) {
	net := newTestNetwork(t)
	net.retire()
	store := newFakeStore()

	p := crawlmanager.Params{
		Fetcher: newFetcherFor(net),
		Source:  fakeSource{refs: []crawlmanager.IndexRef{{ParticipantID: "p", IndexURL: net.srv.URL + "/index"}}},
		Sink:    newFakeSink(),
		Store:   store,
	}
	if err := p.PollIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.queue) != 0 {
		t.Fatalf("queue = %+v, want nothing enqueued (never synced, nothing to retire)", store.queue)
	}
}

func TestPollIndexes_RetiredPreviouslySeenCatalogEnqueuesRetire(t *testing.T) {
	net := newTestNetwork(t)
	store := newFakeStore()
	store.cursors["p/c"] = crawlmanager.CatalogCursor{CatalogID: "p/c", Version: 1, EntryVersion: 1}
	net.retire()

	p := crawlmanager.Params{
		Fetcher: newFetcherFor(net),
		Source:  fakeSource{refs: []crawlmanager.IndexRef{{ParticipantID: "p", IndexURL: net.srv.URL + "/index"}}},
		Sink:    newFakeSink(),
		Store:   store,
	}
	if err := p.PollIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.queue) != 1 || store.queue[0].Op != crawlmanager.OpRetire {
		t.Fatalf("queue = %+v, want one OpRetire item", store.queue)
	}
}

func TestSyncNext_SyncCapturesEnvelopeThenRetireWipesIt(t *testing.T) {
	net := newTestNetwork(t)
	store := newFakeStore()
	sink := newFakeSink()
	p := crawlmanager.Params{Fetcher: newFetcherFor(net), Sink: sink, Store: store}
	ctx := context.Background()

	// First, an ordinary sync -- this should capture the resolved content's
	// descriptor/provider envelope into the cursor.
	if err := store.Enqueue(ctx, crawlmanager.QueueItem{CatalogID: "p/c", IndexURL: net.srv.URL + "/index", ParticipantID: "p", Op: crawlmanager.OpSync}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.SyncNext(ctx); err != nil {
		t.Fatal(err)
	}
	cursor, seen, err := store.GetCatalogCursor(ctx, "p/c")
	if err != nil {
		t.Fatal(err)
	}
	if !seen || cursor.Retired {
		t.Fatalf("cursor = %+v, want a settled, non-retired sync", cursor)
	}

	// Now retire it -- the wipe push must come from the captured envelope,
	// not a fresh fetch (the index no longer carries any content once
	// retired), and it must settle as Retired only after Sink accepts it.
	net.retire()
	if err := store.Enqueue(ctx, crawlmanager.QueueItem{CatalogID: "p/c", IndexURL: net.srv.URL + "/index", ParticipantID: "p", Op: crawlmanager.OpRetire}); err != nil {
		t.Fatal(err)
	}
	claimed, err := p.SyncNext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Fatal("expected the retire item to be claimed")
	}
	got, ok := sink.got["p/c"]
	if !ok {
		t.Fatal("expected a wipe push to reach the sink")
	}
	var wipeDoc map[string]json.RawMessage
	if err := json.Unmarshal(got, &wipeDoc); err != nil {
		t.Fatal(err)
	}
	if _, hasResources := wipeDoc["resources"]; !hasResources || strings.TrimSpace(string(wipeDoc["resources"])) != "[]" {
		t.Fatalf("wipe doc = %s, want an explicit empty resources array", got)
	}

	cursor, seen, err = store.GetCatalogCursor(ctx, "p/c")
	if err != nil {
		t.Fatal(err)
	}
	if !seen || !cursor.Retired {
		t.Fatalf("cursor = %+v, want Retired=true after the wipe settles", cursor)
	}
}

func TestSyncNext_RetireWithNoCapturedEnvelopeSettlesWithoutPushing(t *testing.T) {
	store := newFakeStore() // never synced -- GetCatalogEnvelope reports ok=false
	sink := newFakeSink()
	p := crawlmanager.Params{Sink: sink, Store: store}
	ctx := context.Background()

	if err := store.Enqueue(ctx, crawlmanager.QueueItem{CatalogID: "p/c", Op: crawlmanager.OpRetire}); err != nil {
		t.Fatal(err)
	}
	claimed, err := p.SyncNext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Fatal("expected the retire item to be claimed")
	}
	if _, pushed := sink.got["p/c"]; pushed {
		t.Fatal("expected no push when there is nothing to wipe")
	}
	cursor, seen, err := store.GetCatalogCursor(ctx, "p/c")
	if err != nil {
		t.Fatal(err)
	}
	if !seen || !cursor.Retired {
		t.Fatalf("cursor = %+v, want Retired=true even with nothing to wipe", cursor)
	}
}

func TestPollIndexes_OutOfScopeNetworkNotEnqueued(t *testing.T) {
	net := newTestNetwork(t)
	net.setNetworks([]string{"beckn.one/other"})
	store := newFakeStore()

	p := crawlmanager.Params{
		Fetcher:  newFetcherFor(net),
		Source:   fakeSource{refs: []crawlmanager.IndexRef{{ParticipantID: "p", IndexURL: net.srv.URL + "/index"}}},
		Sink:     newFakeSink(),
		Store:    store,
		Networks: []string{"beckn.one/mine"},
	}
	if err := p.PollIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.queue) != 0 {
		t.Fatalf("queue = %+v, want nothing enqueued (no network overlap)", store.queue)
	}
}

func TestPollIndexes_IntersectingNetworkIsEnqueued(t *testing.T) {
	net := newTestNetwork(t)
	net.setNetworks([]string{"beckn.one/mine", "beckn.one/other"})
	store := newFakeStore()

	p := crawlmanager.Params{
		Fetcher:  newFetcherFor(net),
		Source:   fakeSource{refs: []crawlmanager.IndexRef{{ParticipantID: "p", IndexURL: net.srv.URL + "/index"}}},
		Sink:     newFakeSink(),
		Store:    store,
		Networks: []string{"beckn.one/mine"},
	}
	if err := p.PollIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.queue) != 1 {
		t.Fatalf("queue = %+v, want one item (networks intersect)", store.queue)
	}
}

func TestPollIndexes_PublicCatalogAlwaysEnqueuedRegardlessOfNetworks(t *testing.T) {
	net := newTestNetwork(t) // no networkIDs set -- public
	store := newFakeStore()

	p := crawlmanager.Params{
		Fetcher:  newFetcherFor(net),
		Source:   fakeSource{refs: []crawlmanager.IndexRef{{ParticipantID: "p", IndexURL: net.srv.URL + "/index"}}},
		Sink:     newFakeSink(),
		Store:    store,
		Networks: []string{"beckn.one/unrelated"},
	}
	if err := p.PollIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.queue) != 1 {
		t.Fatalf("queue = %+v, want one item (public catalog, always taken)", store.queue)
	}
}

func TestPollIndexes_RetireIgnoresNetworkScope(t *testing.T) {
	net := newTestNetwork(t)
	net.setNetworks([]string{"beckn.one/other"}) // out of scope
	store := newFakeStore()
	store.cursors["p/c"] = crawlmanager.CatalogCursor{CatalogID: "p/c", Version: 1, EntryVersion: 1}
	net.retire()

	p := crawlmanager.Params{
		Fetcher:  newFetcherFor(net),
		Source:   fakeSource{refs: []crawlmanager.IndexRef{{ParticipantID: "p", IndexURL: net.srv.URL + "/index"}}},
		Sink:     newFakeSink(),
		Store:    store,
		Networks: []string{"beckn.one/mine"},
	}
	if err := p.PollIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.queue) != 1 || store.queue[0].Op != crawlmanager.OpRetire {
		t.Fatalf("queue = %+v, want the retire enqueued despite being out of scope", store.queue)
	}
}

func TestSyncNext_ParksAfterMaxAttempts(t *testing.T) {
	store := newFakeStore()
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closed.Close()
	client := crawler.NewClient(time.Second, 1<<20, true)
	p := crawlmanager.Params{
		Fetcher: catalog.NewFetcher(client, crawler.StaticKeys(nil), 1<<20),
		Sink:    newFakeSink(), Store: store, MaxAttempts: 3,
	}
	ctx := context.Background()
	if err := store.Enqueue(ctx, crawlmanager.QueueItem{CatalogID: "p/c", IndexURL: closed.URL}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		claimed, err := p.SyncNext(ctx)
		if !claimed || err == nil || crawler.IsPermanent(err) {
			t.Fatalf("attempt %d: claimed=%v err=%v, want a transient failure", i, claimed, err)
		}
	}
	if len(store.parked) != 0 {
		t.Fatalf("parked too early: %v", store.parked)
	}
	// Third attempt reaches MaxAttempts and should park instead of reschedule.
	claimed, err := p.SyncNext(ctx)
	if !claimed || err == nil {
		t.Fatalf("claimed=%v err=%v, want a failure on the final attempt", claimed, err)
	}
	if len(store.parked) != 1 {
		t.Fatalf("parked = %v, want exactly one parked item after MaxAttempts", store.parked)
	}
	if len(store.queue) != 0 {
		t.Fatalf("queue = %+v, want empty (parked, not rescheduled)", store.queue)
	}
}

// TestRequeueOrAbandonParked_RevivesBelowThreshold proves a parked catalog
// under maxParkCount is moved back to Queued (with Attempts reset) once
// it's aged past olderThan, rather than sitting parked forever.
func TestRequeueOrAbandonParked_RevivesBelowThreshold(t *testing.T) {
	store := newFakeStore()
	fixedNow := time.Now()
	store.now = func() time.Time { return fixedNow }
	p := crawlmanager.Params{Store: store, MaxParkCount: 3}
	ctx := context.Background()

	if err := store.Park(ctx, "item-1", "claim"); err != nil {
		t.Fatal(err)
	}
	store.parked["item-1"].item = crawlmanager.ClaimedItem{QueueItem: crawlmanager.QueueItem{CatalogID: "p/c"}, ID: "item-1", Attempts: 2}

	store.now = func() time.Time { return fixedNow.Add(time.Hour) }
	if err := p.RequeueOrAbandonParked(ctx, 30*time.Minute); err != nil {
		t.Fatalf("RequeueOrAbandonParked: %v", err)
	}
	if len(store.parked) != 0 {
		t.Fatalf("parked = %v, want revived out of Parked", store.parked)
	}
	if len(store.queue) != 1 || store.queue[0].CatalogID != "p/c" || store.queue[0].Attempts != 0 {
		t.Fatalf("queue = %+v, want p/c revived with Attempts reset to 0", store.queue)
	}
	abandoned, err := p.ListAbandoned(ctx)
	if err != nil || len(abandoned) != 0 {
		t.Fatalf("ListAbandoned = %+v, %v, want none abandoned", abandoned, err)
	}
}

// TestRequeueOrAbandonParked_TooYoungIsNotSwept proves a catalog parked
// more recently than olderThan is left alone -- neither revived nor
// abandoned -- so a sweep run right after a Park doesn't immediately undo
// it.
func TestRequeueOrAbandonParked_TooYoungIsNotSwept(t *testing.T) {
	store := newFakeStore()
	fixedNow := time.Now()
	store.now = func() time.Time { return fixedNow }
	p := crawlmanager.Params{Store: store, MaxParkCount: 3}
	ctx := context.Background()

	if err := store.Park(ctx, "item-1", "claim"); err != nil {
		t.Fatal(err)
	}
	store.parked["item-1"].item = crawlmanager.ClaimedItem{QueueItem: crawlmanager.QueueItem{CatalogID: "p/c"}, ID: "item-1"}

	// Sweep runs immediately after Park, well inside olderThan -- nothing
	// should move yet.
	store.now = func() time.Time { return fixedNow.Add(time.Minute) }
	if err := p.RequeueOrAbandonParked(ctx, 30*time.Minute); err != nil {
		t.Fatalf("RequeueOrAbandonParked: %v", err)
	}
	if len(store.parked) != 1 {
		t.Fatalf("parked = %v, want the item left parked (too young to sweep)", store.parked)
	}
	if len(store.queue) != 0 {
		t.Fatalf("queue = %+v, want nothing revived yet", store.queue)
	}
	if abandoned, err := p.ListAbandoned(ctx); err != nil || len(abandoned) != 0 {
		t.Fatalf("ListAbandoned = %+v, %v, want none abandoned yet", abandoned, err)
	}
}

// TestRequeueOrAbandonParked_AbandonsAtThreshold proves a catalog that has
// already been parked maxParkCount times is moved to Abandoned instead of
// revived again, and is surfaced via ListAbandoned.
func TestRequeueOrAbandonParked_AbandonsAtThreshold(t *testing.T) {
	store := newFakeStore()
	fixedNow := time.Now()
	store.now = func() time.Time { return fixedNow }
	p := crawlmanager.Params{Store: store, MaxParkCount: 3}
	ctx := context.Background()

	item := crawlmanager.ClaimedItem{QueueItem: crawlmanager.QueueItem{CatalogID: "p/c", IndexURL: "https://idx.test"}, ID: "item-1"}
	store.claimed["item-1"] = item
	if err := store.RecordFailure(ctx, crawlmanager.PassReport{CatalogID: "p/c", Error: "still down"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		store.claimed["item-1"] = item
		if err := store.Park(ctx, "item-1", "claim"); err != nil {
			t.Fatal(err)
		}
	}

	store.now = func() time.Time { return fixedNow.Add(time.Hour) }
	if err := p.RequeueOrAbandonParked(ctx, 30*time.Minute); err != nil {
		t.Fatalf("RequeueOrAbandonParked: %v", err)
	}
	if len(store.parked) != 0 {
		t.Fatalf("parked = %v, want removed from Parked either way", store.parked)
	}
	if len(store.queue) != 0 {
		t.Fatalf("queue = %+v, want nothing revived", store.queue)
	}
	abandoned, err := p.ListAbandoned(ctx)
	if err != nil || len(abandoned) != 1 {
		t.Fatalf("ListAbandoned = %+v, %v, want exactly one abandoned catalog", abandoned, err)
	}
	if abandoned[0].CatalogID != "p/c" || abandoned[0].ParkCount != 3 || abandoned[0].LastError != "still down" {
		t.Errorf("unexpected abandoned entry: %+v", abandoned[0])
	}
}

// TestRequeueOrAbandonParked_DefaultsMaxParkCount proves an unset
// Params.MaxParkCount falls back to DefaultMaxParkCount rather than 0
// (which would abandon every parked item on its very first sweep).
func TestRequeueOrAbandonParked_DefaultsMaxParkCount(t *testing.T) {
	store := newFakeStore()
	fixedNow := time.Now()
	store.now = func() time.Time { return fixedNow }
	p := crawlmanager.Params{Store: store} // MaxParkCount left unset
	ctx := context.Background()

	if err := store.Park(ctx, "item-1", "claim"); err != nil {
		t.Fatal(err)
	}
	store.parked["item-1"].item = crawlmanager.ClaimedItem{QueueItem: crawlmanager.QueueItem{CatalogID: "p/c"}, ID: "item-1"}

	store.now = func() time.Time { return fixedNow.Add(time.Hour) }
	if err := p.RequeueOrAbandonParked(ctx, 30*time.Minute); err != nil {
		t.Fatalf("RequeueOrAbandonParked: %v", err)
	}
	if len(store.queue) != 1 {
		t.Fatalf("queue = %+v, want the single-parked-instance item revived under DefaultMaxParkCount (%d)", store.queue, crawlmanager.DefaultMaxParkCount)
	}
	if abandoned, err := p.ListAbandoned(ctx); err != nil || len(abandoned) != 0 {
		t.Fatalf("ListAbandoned = %+v, %v, want none abandoned", abandoned, err)
	}
}

func TestDeriveMaxParkCount(t *testing.T) {
	if got := crawlmanager.DeriveMaxParkCount(5*time.Minute, 4*time.Hour); got != 48 {
		t.Errorf("DeriveMaxParkCount(5m, 4h) = %d, want 48", got)
	}
	if got := crawlmanager.DeriveMaxParkCount(time.Hour, 30*time.Minute); got != 1 {
		t.Errorf("DeriveMaxParkCount(1h, 30m) = %d, want 1 (floor)", got)
	}
	if got := crawlmanager.DeriveMaxParkCount(0, time.Hour); got != 1 {
		t.Errorf("DeriveMaxParkCount(0, 1h) = %d, want 1 (guard against zero interval)", got)
	}
}

func TestSyncNext_StampsIdentityWhenMissing(t *testing.T) {
	net := newTestNetwork(t)
	store := newFakeStore()
	sink := newFakeSink()
	p := crawlmanager.Params{
		Fetcher: newFetcherFor(net), Sink: sink, Store: store,
		ResolveBppURI: func(context.Context, string, string) (string, error) { return "https://node-1.example", nil },
	}
	ctx := context.Background()

	if err := store.Enqueue(ctx, crawlmanager.QueueItem{CatalogID: "p/c", IndexURL: net.srv.URL + "/index"}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.SyncNext(ctx); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(sink.got["p/c"], &got); err != nil {
		t.Fatal(err)
	}
	if got["bppId"] != "node-1" || got["bppUri"] != "https://node-1.example" {
		t.Fatalf("got = %+v, want bppId/bppUri stamped from the resolved node identity", got)
	}
}

func TestSyncNext_DoesNotOverwritePublisherSuppliedIdentity(t *testing.T) {
	net := newTestNetwork(t)
	// Override the baseline to already carry a bppId the publisher set.
	net.baselineBytes = signBaseline(t, net.priv, catalog.CatalogFileDoc{
		CatalogID: "p/c", Version: 1, NextUpdate: time.Now(),
		Catalog: json.RawMessage(`{"id":"p/c","resources":[],"bppId":"publisher-own-id"}`),
	})
	store := newFakeStore()
	sink := newFakeSink()
	p := crawlmanager.Params{Fetcher: newFetcherFor(net), Sink: sink, Store: store}
	ctx := context.Background()

	if err := store.Enqueue(ctx, crawlmanager.QueueItem{CatalogID: "p/c", IndexURL: net.srv.URL + "/index"}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.SyncNext(ctx); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(sink.got["p/c"], &got); err != nil {
		t.Fatal(err)
	}
	if got["bppId"] != "publisher-own-id" {
		t.Fatalf("bppId = %v, want the publisher's own value preserved", got["bppId"])
	}
}
