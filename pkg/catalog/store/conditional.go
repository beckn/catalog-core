package store

// conditional.go -- the multi-writer write path. Store.Publish reads the
// one index, merges its entries and writes it back; without a
// precondition on that write two writers each drop the other's entries.
// A BlobStore that also implements ConditionalBlobStore lets Publish
// compare-and-swap the index instead, and guard the few files that are
// written before that commit.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"time"
)

// ErrPreconditionFailed is the sentinel a ConditionalBlobStore.PutIfVersion
// implementation must return (checked via errors.Is) when the blob's
// current version is not the one the caller named. Nothing was written.
var ErrPreconditionFailed = errors.New("catalogstore: blob version precondition failed")

// ErrConflict reports that a catalog in a PublishRequest was changed by
// another writer after the request's Base was loaded. The request was
// computed from state that no longer exists, so merging it would discard
// that writer's change: the caller must load again, recompute and call
// Publish again. Nothing of the request was committed. Returned as a
// *ConflictError; match with errors.Is(err, ErrConflict).
var ErrConflict = errors.New("catalogstore: catalog changed since it was loaded")

// ErrIndexBusy reports that the index kept changing under Publish until
// its retries ran out, although none of the request's own catalogs
// changed. Nothing of the request was committed; calling Publish again
// with the same request is safe.
var ErrIndexBusy = errors.New("catalogstore: index still changing after retries")

// ConflictError is ErrConflict with the catalogIds whose index entries
// changed underneath the request.
type ConflictError struct{ CatalogIDs []string }

func (e *ConflictError) Error() string {
	return fmt.Sprintf("%s: %s", ErrConflict.Error(), strings.Join(e.CatalogIDs, ", "))
}

// Is makes errors.Is(err, ErrConflict) true for a *ConflictError.
func (e *ConflictError) Is(target error) bool { return target == ErrConflict }

// ConditionalBlobStore is an optional capability of a BlobStore: reading
// a blob together with a version token, and writing it only while that
// token is still current (an object store's generation or ETag
// precondition; on a filesystem, a lock around compare-then-rename).
// Store detects it with a type assertion. With it, Publish is safe for
// any number of writers sharing the blob store; without it, Publish
// behaves as it always has and the caller must make sure there is only
// one writer.
//
// Like BlobStore, it uses only built-in types, so a caller's own
// blob-store type can satisfy it structurally.
type ConditionalBlobStore interface {
	BlobStore

	// GetVersioned returns the bytes at path and an opaque, non-empty
	// token naming that exact content generation, or ErrBlobNotFound.
	// Two reads that return the same token must have returned the same
	// bytes.
	GetVersioned(ctx context.Context, path string) (content []byte, version string, err error)

	// PutIfVersion writes content at path only if the blob's current
	// version is ifVersion, atomically with respect to every other
	// PutIfVersion on that path, and returns the new version. An empty
	// ifVersion means "only if nothing is stored at path". Otherwise it
	// writes nothing and returns ErrPreconditionFailed.
	PutIfVersion(ctx context.Context, path string, content []byte, ifVersion string) (newVersion string, err error)
}

// Base records, for each catalogId a caller loaded with LoadForPublish,
// what in the index its PublishRequest was computed from: the catalog's
// own entry (or the absence of one), and which other entries are stored
// under the same file names (CatalogState.NameConflicts), since whether
// an id may be published depends on that too. Opaque; pass it back
// unchanged in PublishRequest.Base.
//
// It does not cover a catalog's "latest" files (CatalogState.StoredLatest).
// Those are written after the index, so they can change while the entry
// stays as it is. A request computed from a "latest" that another writer
// was still bringing up to date rewrites it once more and moves
// entryVersion on; the file and the entry agree afterwards either way.
type Base struct{ revisions map[string]string }

// baseRevision identifies everything in the index that the state
// LoadCatalogs returns for catalogID was derived from.
func baseRevision(entries map[string]json.RawMessage, owners map[string][]nameOwner, catalogID string) string {
	rev := entryRevision(entries[catalogID])
	conflicts := conflictsFor(owners, catalogID)
	if len(conflicts) == 0 {
		return rev
	}
	shared := make([]string, 0, len(conflicts))
	for _, c := range conflicts {
		shared = append(shared, fmt.Sprintf("%s\x00%t", c.CatalogID, c.Retired))
	}
	sort.Strings(shared)
	sum := sha256.Sum256([]byte(rev + "\n" + strings.Join(shared, "\n")))
	return hex.EncodeToString(sum[:])
}

// entryRevision identifies one index entry's exact content: "" for no
// entry, otherwise a hash of its whitespace-normalised bytes.
func entryRevision(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	sum := sha256.Sum256(compactJSON(raw))
	return hex.EncodeToString(sum[:])
}

func compactJSON(raw []byte) []byte {
	var buf bytes.Buffer
	if json.Compact(&buf, raw) != nil {
		return raw
	}
	return buf.Bytes()
}

// changed returns the catalogIds of req for which the index no longer
// holds what this Base recorded, sorted.
func (b *Base) changed(req PublishRequest, entries map[string]json.RawMessage, order []string) []string {
	if b == nil {
		return nil
	}
	owners := nameOwners(entries, order)
	var ids []string
	check := func(us []CatalogUpdate) {
		for _, u := range us {
			want, ok := b.revisions[u.CatalogID]
			if ok && baseRevision(entries, owners, u.CatalogID) != want {
				ids = append(ids, u.CatalogID)
			}
		}
	}
	check(req.Updates)
	check(req.Retirements)
	sort.Strings(ids)
	return ids
}

// LoadForPublish is LoadCatalogs plus the Base to hand back in
// PublishRequest.Base, both taken from one read of the index. With a
// Base, Publish refuses (ErrConflict) to commit an update whose catalog
// another writer has changed in the meantime.
func (s *Store) LoadForPublish(ctx context.Context, catalogIDs []string) (map[string]CatalogState, *Base, error) {
	states, entries, order, err := s.loadCatalogs(ctx, catalogIDs)
	if err != nil {
		return nil, nil, err
	}
	owners := nameOwners(entries, order)
	base := &Base{revisions: make(map[string]string, len(catalogIDs))}
	for _, id := range catalogIDs {
		base.revisions[id] = baseRevision(entries, owners, id)
	}
	return states, base, nil
}

// RetryPolicy bounds Publish's compare-and-swap loop on a
// ConditionalBlobStore. Zero fields take the defaults.
type RetryPolicy struct {
	// MaxAttempts is how many times Publish tries to commit the index
	// before giving up with ErrIndexBusy. Each lost attempt means some
	// other writer committed, so N writers publishing at once need at
	// most N attempts each.
	MaxAttempts int
	// BaseBackoff and MaxBackoff bound the jittered, doubling pause
	// between attempts.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// TakeoverGrace is how long Publish watches the index before
	// replacing a versioned file that already exists with other content
	// and that no index entry references. Such a file is either left
	// over from a publish that died, or belongs to a writer about to
	// commit; the wait tells the two apart in the common case.
	TakeoverGrace time.Duration
}

var defaultRetryPolicy = RetryPolicy{MaxAttempts: 32, BaseBackoff: 5 * time.Millisecond, MaxBackoff: 200 * time.Millisecond, TakeoverGrace: 250 * time.Millisecond}

// WithRetryPolicy overrides the defaults. Returns s for chaining.
func (s *Store) WithRetryPolicy(p RetryPolicy) *Store {
	if p.MaxAttempts > 0 {
		s.retry.MaxAttempts = p.MaxAttempts
	}
	if p.BaseBackoff > 0 {
		s.retry.BaseBackoff = p.BaseBackoff
	}
	if p.MaxBackoff > 0 {
		s.retry.MaxBackoff = p.MaxBackoff
	}
	if p.TakeoverGrace > 0 {
		s.retry.TakeoverGrace = p.TakeoverGrace
	}
	return s
}

func (s *Store) backoff(ctx context.Context, attempt int) error {
	d := s.retry.BaseBackoff << min(attempt, 16)
	if d <= 0 || d > s.retry.MaxBackoff {
		d = s.retry.MaxBackoff
	}
	return sleepCtx(ctx, time.Duration(rand.Int64N(int64(d))+1))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// getVersioned is GetVersioned with "not found" folded into the empty
// version, the token PutIfVersion takes for "create only".
func getVersioned(ctx context.Context, cas ConditionalBlobStore, path string) ([]byte, string, error) {
	content, version, err := cas.GetVersioned(ctx, path)
	if errors.Is(err, ErrBlobNotFound) {
		return nil, "", nil
	}
	return content, version, err
}

// errRestart is internal: a pre-commit write found another writer active;
// start the attempt again from a fresh read of the index.
var errRestart = errors.New("catalogstore: restart attempt")

// publishTxn is what one Publish call has put in the blob store before
// its index commit.
type publishTxn struct {
	// written maps each path written before the commit to its bytes, so
	// a retry does not write it again and the commit can be checked.
	written map[string][]byte
	// undo lists files that held something else before, with what they
	// held and the version our write produced, so a call that does not
	// commit can put them back.
	undo      []undoWrite
	graceDone bool
}

type undoWrite struct {
	path      string
	previous  []byte
	myVersion string
}

// served returns the bytes writeFile would store for fw.
func served(fw *FileWrite) []byte {
	if fw.ServedContent != nil {
		return fw.ServedContent
	}
	return fw.Content
}

// publishConditional is Publish over a ConditionalBlobStore. The index
// write is the commit point and is a compare-and-swap:
//
//  1. read the index with its version; refuse (ErrConflict) if a catalog
//     of this request is no longer at its Base, or (checkNewCatalogIDs)
//     if a new catalogId would be stored over another catalog's files;
//  2. once per call, write what must exist before the commit: new
//     baseline and change files, created only if absent, and a
//     retirement's final "latest" tombstones (one per "latest" name);
//  3. merge and write the index if its version is unchanged; if it
//     changed, go back to 1 -- the request's own entries are still valid
//     as long as step 1 passes, so they are merged again as they are;
//  4. after the commit, check the files of step 2 still hold what the
//     committed entries reference, then bring each updated catalog's
//     "latest" file up to date.
//
// A call that does not commit puts back any file it replaced in step 2.
func (s *Store) publishConditional(ctx context.Context, cas ConditionalBlobStore, req PublishRequest) error {
	txn := &publishTxn{written: map[string][]byte{}}
	var lastErr error
	for attempt := range s.retry.MaxAttempts {
		if attempt > 0 {
			if err := s.backoff(ctx, attempt-1); err != nil {
				s.rollback(ctx, cas, txn)
				return fmt.Errorf("catalogstore: waiting to retry the index write: %w", err)
			}
		}
		entries, order, indexVersion, err := s.readIndex(ctx, cas)
		if err != nil {
			s.rollback(ctx, cas, txn)
			return err
		}
		if ids := req.Base.changed(req, entries, order); len(ids) > 0 {
			s.rollback(ctx, cas, txn)
			s.log.WarnContext(ctx, "catalogstore: catalogs changed since they were loaded, caller must recompute", "catalogIds", ids, "attempt", attempt+1)
			return &ConflictError{CatalogIDs: ids}
		}
		// On every attempt, against the index as just read: another writer
		// may have added the catalog this request's new id would collide
		// with since the last one.
		if err := checkNewCatalogIDs(entries, order, req); err != nil {
			s.rollback(ctx, cas, txn)
			s.log.ErrorContext(ctx, "catalogstore: publish refused", "error", err)
			return err
		}
		if err := s.writeBeforeCommit(ctx, cas, req, indexVersion, txn); err != nil {
			if errors.Is(err, errRestart) {
				lastErr = err
				continue
			}
			s.rollback(ctx, cas, txn)
			return err
		}
		indexBytes, count, err := mergeIndex(req, entries, order)
		if err != nil {
			s.rollback(ctx, cas, txn)
			return err
		}
		_, err = cas.PutIfVersion(ctx, IndexPath(), indexBytes, indexVersion)
		if err == nil {
			s.log.DebugContext(ctx, "catalogstore: index committed", "entries", count, "bytes", len(indexBytes), "attempt", attempt+1)
			return s.afterCommit(ctx, cas, req, txn)
		}
		if !errors.Is(err, ErrPreconditionFailed) {
			// The write may or may not have landed: leave everything in
			// place rather than pull files out from under a committed index.
			s.log.ErrorContext(ctx, "catalogstore: writing index failed", "error", err)
			return fmt.Errorf("catalogstore: writing index: %w", err)
		}
		lastErr = err
		s.log.DebugContext(ctx, "catalogstore: index changed under this publish, merging again", "attempt", attempt+1)
	}
	s.rollback(ctx, cas, txn)
	return fmt.Errorf("%w (%d attempts): %v", ErrIndexBusy, s.retry.MaxAttempts, lastErr)
}

// readIndex is readIndexEntries plus the index's version ("" when there
// is no index yet).
func (s *Store) readIndex(ctx context.Context, cas ConditionalBlobStore) (map[string]json.RawMessage, []string, string, error) {
	raw, version, err := getVersioned(ctx, cas, IndexPath())
	if err != nil {
		s.log.ErrorContext(ctx, "catalogstore: reading existing index failed", "error", err)
		return nil, nil, "", fmt.Errorf("catalogstore: reading existing index: %w", err)
	}
	if version == "" {
		return map[string]json.RawMessage{}, nil, "", nil
	}
	entries, order, err := s.parseIndex(ctx, raw)
	return entries, order, version, err
}

// mergeIndex applies req's entries over entries/order and returns the
// index document's bytes and its entry count.
func mergeIndex(req PublishRequest, entries map[string]json.RawMessage, order []string) ([]byte, int, error) {
	merge := func(us []CatalogUpdate) {
		for _, u := range us {
			if _, existed := entries[u.CatalogID]; !existed {
				order = append(order, u.CatalogID)
			}
			entries[u.CatalogID] = u.SignedEntry
		}
	}
	merge(req.Updates)
	merge(req.Retirements)
	ordered := make([]json.RawMessage, 0, len(order))
	for _, id := range order {
		ordered = append(ordered, entries[id])
	}
	indexBytes, err := json.Marshal(indexDoc{NodeID: req.NodeID, NextUpdate: req.NextUpdate, Catalogs: ordered})
	if err != nil {
		return nil, 0, fmt.Errorf("catalogstore: marshaling index: %w", err)
	}
	return indexBytes, len(ordered), nil
}

// writeBeforeCommit writes, once per Publish call, every file the index
// entries about to be committed reference and that cannot wait until
// after the commit.
//
// A baseline or change file must exist before an entry lists it. It is
// created only if absent, so a writer that goes on to lose never replaces
// a file some committed entry references.
//
// A retirement's final "latest" tombstones are also written first. Once
// the index says "retired", the only call that looks at those files again
// is a repeated retire; an update's "latest" is put right by any later
// publish. So the tombstone goes in before the commit, where a commit that
// then fails is corrected by the retry (or, if the catalog is published
// again instead, by that publish, which rewrites a "latest" carrying
// retiredAt under a live entry).
//
// An update's "latest" is written after the commit (see afterCommit).
func (s *Store) writeBeforeCommit(ctx context.Context, cas ConditionalBlobStore, req PublishRequest, indexVersion string, txn *publishTxn) error {
	for _, u := range req.Updates {
		if u.Baseline != nil {
			if err := s.createVersioned(ctx, cas, CatalogFilePath(u.CatalogID, u.Baseline.Version, "json", u.Baseline.Compressed), served(u.Baseline), indexVersion, txn); err != nil {
				return err
			}
		}
		if u.Change != nil {
			if err := s.createVersioned(ctx, cas, CatalogFilePath(u.CatalogID, u.Change.Version, "changes.json", u.Change.Compressed), served(u.Change), indexVersion, txn); err != nil {
				return err
			}
		}
	}
	for _, r := range req.Retirements {
		for _, fw := range latestWrites(r) {
			if err := s.replaceGuarded(ctx, cas, LatestFilePath(r.CatalogID, fw.Compressed), served(fw), txn); err != nil {
				return err
			}
		}
	}
	return nil
}

// latestWrites lists u's "latest" writes: Latest, then LatestAlt.
func latestWrites(u CatalogUpdate) []*FileWrite {
	var out []*FileWrite
	if u.Latest != nil {
		out = append(out, u.Latest)
	}
	if u.LatestAlt != nil {
		out = append(out, u.LatestAlt)
	}
	return out
}

// createVersioned writes a baseline or change file that must not replace
// one a committed entry references.
func (s *Store) createVersioned(ctx context.Context, cas ConditionalBlobStore, path string, content []byte, indexVersion string, txn *publishTxn) error {
	if _, done := txn.written[path]; done {
		return nil
	}
	_, err := cas.PutIfVersion(ctx, path, content, "")
	if err == nil {
		txn.written[path] = content
		s.log.DebugContext(ctx, "catalogstore: wrote file", "path", path, "bytes", len(content))
		return nil
	}
	if !errors.Is(err, ErrPreconditionFailed) {
		s.log.ErrorContext(ctx, "catalogstore: writing file failed", "path", path, "error", err)
		return fmt.Errorf("catalogstore: writing %s: %w", path, err)
	}
	// Something is already there. The Base check has passed, so no entry
	// of the index as just read lists this version of this catalog: the
	// file was left by a publish that never committed, or it belongs to
	// a writer that is about to. Give the latter time to show up in the
	// index (the restarted attempt then ends in ErrConflict), then take
	// the file over.
	if !txn.graceDone {
		changed, err := s.indexChangesWithin(ctx, cas, indexVersion, s.retry.TakeoverGrace)
		if err != nil {
			return err
		}
		if changed {
			return errRestart
		}
		txn.graceDone = true
	}
	return s.replaceGuarded(ctx, cas, path, content, txn)
}

// replaceGuarded overwrites path before the commit, remembering what it
// held so a call that does not commit can put it back. The write is
// conditional on the content just read: if another writer gets in
// between, the attempt restarts.
func (s *Store) replaceGuarded(ctx context.Context, cas ConditionalBlobStore, path string, content []byte, txn *publishTxn) error {
	if _, done := txn.written[path]; done {
		return nil
	}
	previous, version, err := getVersioned(ctx, cas, path)
	if err != nil {
		return fmt.Errorf("catalogstore: reading %s: %w", path, err)
	}
	if version != "" && bytes.Equal(previous, content) {
		txn.written[path] = content
		return nil
	}
	myVersion, err := cas.PutIfVersion(ctx, path, content, version)
	if errors.Is(err, ErrPreconditionFailed) {
		return errRestart
	}
	if err != nil {
		s.log.ErrorContext(ctx, "catalogstore: writing file failed", "path", path, "error", err)
		return fmt.Errorf("catalogstore: writing %s: %w", path, err)
	}
	txn.written[path] = content
	if version != "" {
		txn.undo = append(txn.undo, undoWrite{path: path, previous: previous, myVersion: myVersion})
	}
	s.log.DebugContext(ctx, "catalogstore: wrote file", "path", path, "bytes", len(content), "replaced", version != "")
	return nil
}

// indexChangesWithin polls the index version for up to d and reports
// whether it moved away from indexVersion.
func (s *Store) indexChangesWithin(ctx context.Context, cas ConditionalBlobStore, indexVersion string, d time.Duration) (bool, error) {
	const steps = 5
	for range steps {
		if err := sleepCtx(ctx, d/steps); err != nil {
			return false, err
		}
		_, version, err := getVersioned(ctx, cas, IndexPath())
		if err != nil {
			return false, fmt.Errorf("catalogstore: reading existing index: %w", err)
		}
		if version != indexVersion {
			return true, nil
		}
	}
	return false, nil
}

// rollback puts back every file this call replaced before a commit that
// did not happen. Each restore is conditional on the file still being
// this call's own write, so it never undoes another writer's. Best
// effort: a file that cannot be restored is logged; files this call
// created stay behind as orphans no entry references.
func (s *Store) rollback(ctx context.Context, cas ConditionalBlobStore, txn *publishTxn) {
	ctx = context.WithoutCancel(ctx)
	for i := len(txn.undo) - 1; i >= 0; i-- {
		u := txn.undo[i]
		if _, err := cas.PutIfVersion(ctx, u.path, u.previous, u.myVersion); err != nil && !errors.Is(err, ErrPreconditionFailed) {
			s.log.ErrorContext(ctx, "catalogstore: could not restore a file replaced by a publish that did not commit", "path", u.path, "error", err)
		}
	}
	txn.undo = nil
}

// afterCommit runs once the index holds this call's entries. The commit
// has happened, so it finishes even if the caller's context ends
// meanwhile: stopping here would leave "latest" behind the index for no
// benefit.
func (s *Store) afterCommit(ctx context.Context, cas ConditionalBlobStore, req PublishRequest, txn *publishTxn) error {
	ctx = context.WithoutCancel(ctx)
	// "latest" first: until it is written, a reader of this catalog sees
	// a latest.digest that does not match the file.
	for _, u := range req.Updates {
		for _, fw := range latestWrites(u) {
			if err := s.writeLatestAfterCommit(ctx, cas, u, fw); err != nil {
				return err
			}
		}
	}
	// A writer that read the index before this commit may have taken one
	// of this call's files for a leftover and replaced it. The committed
	// entry references this call's bytes, so they win.
	paths := make([]string, 0, len(txn.written))
	for p := range txn.written {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		got, err := cas.Get(ctx, p)
		if err == nil && bytes.Equal(got, txn.written[p]) {
			continue
		}
		s.log.WarnContext(ctx, "catalogstore: a committed file was replaced by another writer, writing it again", "path", p, "readError", err)
		if err := cas.Put(ctx, p, txn.written[p]); err != nil {
			return fmt.Errorf("catalogstore: index committed, but rewriting %s failed: %w", p, err)
		}
	}
	return nil
}

// writeLatestAfterCommit brings an updated catalog's "latest" file in
// line with the entry this call committed. "latest" is one file
// overwritten in place, so with several writers the last index commit
// must also be the last "latest" write. Reading the file's version
// first, then confirming this call's entry is still the one in the
// index, then writing only if the file has not moved, guarantees that: a
// writer that has been superseded either sees the newer entry and
// stops, or loses the conditional write to the newer writer.
func (s *Store) writeLatestAfterCommit(ctx context.Context, cas ConditionalBlobStore, u CatalogUpdate, fw *FileWrite) error {
	path := LatestFilePath(u.CatalogID, fw.Compressed)
	content := served(fw)
	mine := compactJSON(u.SignedEntry)
	for attempt := range s.retry.MaxAttempts {
		current, version, err := getVersioned(ctx, cas, path)
		if err != nil {
			return fmt.Errorf("catalogstore: index committed, but reading %s failed: %w", path, err)
		}
		if version != "" && bytes.Equal(current, content) {
			return nil
		}
		entries, _, _, err := s.readIndex(ctx, cas)
		if err != nil {
			return err
		}
		if !bytes.Equal(compactJSON(entries[u.CatalogID]), mine) {
			s.log.DebugContext(ctx, "catalogstore: entry superseded by a later publish, leaving latest to it", "catalogId", u.CatalogID)
			return nil
		}
		_, err = cas.PutIfVersion(ctx, path, content, version)
		if err == nil {
			s.log.DebugContext(ctx, "catalogstore: wrote file", "path", path, "bytes", len(content), "compressed", fw.Compressed)
			return nil
		}
		if !errors.Is(err, ErrPreconditionFailed) {
			s.log.ErrorContext(ctx, "catalogstore: writing file failed", "path", path, "error", err)
			return fmt.Errorf("catalogstore: index committed, but writing %s failed: %w", path, err)
		}
		if err := s.backoff(ctx, attempt); err != nil {
			return fmt.Errorf("catalogstore: index committed, but writing %s was interrupted: %w", path, err)
		}
	}
	return fmt.Errorf("catalogstore: index committed, but %s kept changing (%d attempts)", path, s.retry.MaxAttempts)
}
