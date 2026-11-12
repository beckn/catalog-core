// Package store is the one shared, backend-agnostic assembler for
// a publisher's catalog index: it understands how an index, its
// catalogs' baselines, change files, and "latest" pointers fit together
// per the decentralized-catalog file spec, built on top of whichever
// BlobStore is configured (local disk, S3, GCS, git, an authenticated
// CDN write root, ...).
//
// This is deliberately not meant to be swappable itself: there is
// exactly one correct way to assemble/reconstruct a catalog's file set
// per the spec, so nothing here should vary by backend -- only the
// BlobStore underneath it. A publisher tool (an HTTP handler, a CLI)
// talks to Store, never to a BlobStore directly.
//
// Store holds no signing key material and signs nothing: every entry it
// writes (CatalogUpdate.SignedEntry) arrives already self-signed by the
// caller. Store's only job is deciding where bytes live and merging new
// entries into the index that's already there.
package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/beckn/catalog-core/pkg/catalog"
)

// ErrBlobNotFound is the sentinel a BlobStore.Get implementation must
// return when path has never been written -- Store relies on this exact
// sentinel (via errors.Is) to tell "nothing here yet, start fresh" apart
// from a real backend failure.
var ErrBlobNotFound = errors.New("catalogstore: blob not found")

// ErrIndexUnreadable is wrapped by the error every read of the index
// returns when the index file exists but is empty or is not a JSON index
// document. Nothing can be published until it is restored: writing a new
// index in its place would drop every catalog it listed and restart their
// version lineages over files that are meant to be immutable.
var ErrIndexUnreadable = errors.New("catalogstore: the catalog index file is unreadable")

// BlobStore is the only backend-specific capability a catalog storage
// backend needs: read/write bytes at a path. It carries no catalog-file
// vocabulary at all -- every backend (local disk, S3, GCS, git, an
// authenticated CDN write root) implements exactly this and nothing
// more. A caller hosting Store as an onix plugin (or anything else)
// only needs its own blob-store type to satisfy this shape structurally
// -- no shared import with this package required.
type BlobStore interface {
	// Get returns the bytes stored at path, or ErrBlobNotFound if
	// nothing has been written there yet.
	Get(ctx context.Context, path string) ([]byte, error)

	// Put writes content at path, creating or overwriting it. Every
	// path Store passes is already content/version-addressed (a
	// versioned filename, or the fixed "latest"/index path), so a
	// backend needs no versioning or locking of its own beyond an
	// atomic per-path overwrite.
	//
	// That is enough for one writer. Where more than one process can
	// publish to the same blob store, the backend must also implement
	// ConditionalBlobStore, or their index writes drop each other's
	// entries.
	Put(ctx context.Context, path string, content []byte) error
}

// IndexFilename is the catalog index's filename.
const IndexFilename = "becknCatalogs.index.json"

// IndexDirName, CatalogsDirName, and ChangesDirName are the blob-key
// "directories" every path here is built under -- fixed by convention,
// matching the file spec's own example layout, not configurable per
// deployment (only where the blob store's root itself points is).
const (
	IndexDirName    = "index"
	CatalogsDirName = "catalogs"
	ChangesDirName  = "changes"
)

// IndexPath, CatalogFilePath, and LatestFilePath return "/"-separated blob
// keys -- CatalogBlobStore implementations translate these to whatever
// their backend needs (a filesystem path, an object key, ...).
func IndexPath() string { return path.Join(IndexDirName, IndexFilename) }

// LocalName returns catalogID with any "domain/" prefix stripped, matching
// the file spec's own example filenames (catalogId
// "open-economy.nfh.global/electronics-2026" -> "electronics-2026").
//
// Every file a catalog owns is named from this, so two catalogIds with the
// same LocalName would be written to the same files. FileNameKey is the
// value to compare to find that out, and ValidateCatalogID is the shape a
// catalogId must have before a first file is written under it.
func LocalName(catalogID string) string {
	if i := strings.LastIndex(catalogID, "/"); i != -1 {
		return catalogID[i+1:]
	}
	return catalogID
}

// FileNameKey identifies the set of file names catalogID's baseline, change
// and "latest" files are written under. Two catalogIds with the same key
// share those files: publishing one overwrites the other's. It is LocalName
// folded to lower case, because a blob store on a case-insensitive
// filesystem (the default on macOS and Windows) treats "CAT-1.v1.json" and
// "cat-1.v1.json" as one file.
//
// The key is only ever compared. File names and URLs keep using LocalName
// unchanged, so no published URL moves.
func FileNameKey(catalogID string) string { return strings.ToLower(LocalName(catalogID)) }

// MaxLocalNameBytes bounds a new catalogId's last path segment. File
// names are that segment plus a suffix of up to 37 bytes
// (".v<19 digits>.changes.json.gz"), and most filesystems cap a file name
// at 255 bytes.
const MaxLocalNameBytes = 200

// MaxCatalogIDBytes bounds a new catalogId as a whole.
const MaxCatalogIDBytes = 1024

// ValidateCatalogID reports why catalogID cannot be used for a catalog that
// has no files yet, or nil if it can. The rules exist because the id's last
// path segment is used, unescaped, as part of a file name and of a URL:
//
//   - not empty, at most MaxCatalogIDBytes, valid UTF-8, no white space or
//     control characters anywhere;
//   - no empty, "." or ".." path segment (so no leading, trailing or
//     doubled "/");
//   - the last segment uses only A-Z a-z 0-9 "-" "." "_" "~" (the URL
//     "unreserved" set: it means the same thing in a file name, in a URL
//     and after percent-decoding), does not start with ".", and is at most
//     MaxLocalNameBytes long.
//
// This is deliberately not applied to a catalogId that already has an
// index entry: such a catalog keeps its id, file names and URLs whatever
// they look like, and can still be updated and retired.
func ValidateCatalogID(catalogID string) error {
	if catalogID == "" {
		return errors.New("missing catalogId")
	}
	if len(catalogID) > MaxCatalogIDBytes {
		return fmt.Errorf("catalogId is %d bytes long; the limit is %d", len(catalogID), MaxCatalogIDBytes)
	}
	if !utf8.ValidString(catalogID) {
		return errors.New("catalogId is not valid UTF-8")
	}
	for _, r := range catalogID {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("catalogId %q contains white space or a control character", catalogID)
		}
	}
	segments := strings.Split(catalogID, "/")
	for _, seg := range segments {
		if seg == "" {
			return fmt.Errorf("catalogId %q has an empty path segment (a leading, trailing or doubled \"/\")", catalogID)
		}
		if seg == "." || seg == ".." {
			return fmt.Errorf("catalogId %q has a %q path segment", catalogID, seg)
		}
	}
	local := segments[len(segments)-1]
	if len(local) > MaxLocalNameBytes {
		return fmt.Errorf("the last path segment of catalogId %q is %d bytes long; the limit is %d (it becomes a file name)", truncateForError(catalogID), len(local), MaxLocalNameBytes)
	}
	if strings.HasPrefix(local, ".") {
		return fmt.Errorf("the last path segment of catalogId %q starts with \".\"; it becomes a file name", catalogID)
	}
	for _, r := range local {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.', r == '_', r == '~':
		default:
			return fmt.Errorf("the last path segment of catalogId %q contains %q; it becomes a file name and part of a URL, so only letters, digits, \"-\", \".\", \"_\" and \"~\" are allowed there", catalogID, r)
		}
	}
	return nil
}

// truncateForError keeps an over-long id from flooding an error message.
func truncateForError(s string) string {
	const limit = 80
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "..."
}

// CatalogFilePath returns the blob key for one catalog file: a baseline
// (suffix "json") sits directly under catalogs/, a change file (suffix
// "changes.json") under catalogs/changes/. compressed appends ".gz".
func CatalogFilePath(catalogID string, version int64, suffix string, compressed bool) string {
	if compressed {
		suffix += ".gz"
	}
	filename := fmt.Sprintf("%s.v%d.%s", LocalName(catalogID), version, suffix)
	if strings.HasPrefix(suffix, "changes.json") {
		return path.Join(CatalogsDirName, ChangesDirName, filename)
	}
	return path.Join(CatalogsDirName, filename)
}

// LatestFilePath returns the blob key for a catalog's "latest" pointer:
// unlike CatalogFilePath, its filename carries no version number -- every
// Publish call maintaining "latest" overwrites this same key in place.
func LatestFilePath(catalogID string, compressed bool) string {
	suffix := "json"
	if compressed {
		suffix += ".gz"
	}
	return path.Join(CatalogsDirName, fmt.Sprintf("%s.latest.%s", LocalName(catalogID), suffix))
}

// CatalogState is one catalog's reconstructed current state: the full
// content last published (baseline with every change file applied),
// enough for a caller to diff a new submission against, plus the
// entry-level metadata (distinct from file-lineage versioning) needed to
// detect a metadata-only change with no new file. BaselineFile/ChangeFiles/
// Dependencies reuse pkg/catalog's own wire types directly (the same ones
// catalog/crawler reads) rather than a second, independently-drifting
// copy -- see index.go.
type CatalogState struct {
	Catalog      json.RawMessage
	BaselineFile *catalog.FileEntry
	ChangeFiles  []catalog.FileEntry

	EntryVersion int64
	CatalogType  string
	NetworkIds   []string
	SchemaTypes  []string
	IsActive     bool
	Dependencies []catalog.MasterDependency
	CrawlHint    string

	// LatestPublished reports whether a "latest" full-catalog pointer was
	// previously published for this catalog -- independent of whether the
	// caller currently has that turned on, since retiring a catalog that
	// had one still needs a final write to that same stable URL.
	LatestPublished bool

	// LatestFile is the prior index entry's own "latest" reference,
	// verbatim (nil when the entry has none). "latest" is the one file
	// overwritten in place at a stable URL, so its name, encoding, digest
	// and version must come from what was last published -- never from
	// the current call's Gzip/PublishLatest settings, which may have
	// changed since. LatestPublished is LatestFile != nil.
	LatestFile *catalog.FileEntry

	// StoredLatest lists every "latest" file the blob store holds for
	// this catalog, under either of its two possible names
	// (LatestFilePath compressed and uncompressed), whether or not the
	// index entry still references it. A file can outlive its reference:
	// PublishLatest was switched off, the catalog was retired (a
	// tombstone entry never lists "latest"), or an earlier release wrote
	// it under the other name. A consumer may still hold that URL, so
	// retiring the catalog must reach every one of them (NFH-014
	// CON-TBD-38).
	StoredLatest []LatestFileState

	// Retired reports that this catalog's index entry is already a
	// tombstone (retiredAt set). Such a state carries only what the
	// tombstone itself does -- EntryVersion, CatalogType, NetworkIds,
	// SchemaTypes, RetiredAt -- plus SignedEntry and StoredLatest, and never
	// Catalog/BaselineFile/ChangeFiles: there is no content left to diff
	// against. It exists so a caller can tell an already-retired catalog
	// from one that never existed, and so EntryVersion keeps its lineage
	// (NFH-014 CON-TBD-03) instead of restarting from zero.
	Retired bool

	// RetiredAt is the tombstone entry's own retiredAt, verbatim; empty
	// unless Retired. A final "latest" file written late (a repeat
	// retire repairing one an earlier call missed) carries this instant,
	// not a new one.
	RetiredAt string

	// SignedEntry is the index entry exactly as stored, signature
	// included. A caller compares a freshly signed entry against it to
	// tell whether the entry really is unchanged; a repeat retire that has
	// to repair a "latest" file hands the same tombstone back to
	// Store.Publish.
	SignedEntry json.RawMessage

	// NameConflicts lists every other index entry whose files are written
	// under the same names as this catalogId's (same FileNameKey), in index
	// order. Empty in the normal case. For a catalogId that is not in the
	// index it means the id cannot be published: its first baseline would
	// overwrite another catalog's. For one that is, it means an index
	// written before this was checked already holds two catalogs sharing
	// files.
	NameConflicts []NameConflict

	// Unreadable, when not empty, says why this catalog's published content
	// could not be reconstructed from its own files (a baseline or change
	// file is missing or corrupt). Catalog is nil then, but EntryVersion,
	// the metadata fields, BaselineFile, ChangeFiles, LatestPublished,
	// LatestFile, StoredLatest and SignedEntry are still filled in, so the
	// catalog can be retired or given a fresh baseline with its version
	// lineage and its "latest" name intact. There is nothing to diff a
	// submission against.
	Unreadable string

	// EntryMalformed is set, together with Unreadable, when the index
	// entry itself does not parse. Nothing is known about the catalog
	// then -- not its entryVersion, not its files -- so it can be neither
	// published (a forced baseline would restart its version lineage and
	// overwrite files) nor retired until the index entry is repaired.
	// Only SignedEntry (the raw entry) and NameConflicts are filled in.
	EntryMalformed bool
}

// NameConflict is one other index entry that shares a catalogId's file
// names. Retired says whether that entry is a tombstone.
type NameConflict struct {
	CatalogID string
	Retired   bool
}

// Known reports whether s describes a catalogId that has an index entry,
// live or retired. LoadCatalogs can also return a state for an id that has
// none, carrying only NameConflicts; the zero CatalogState is not Known
// either.
func (s CatalogState) Known() bool {
	return s.Retired || s.Catalog != nil || s.BaselineFile != nil || s.EntryVersion > 0 || s.Unreadable != ""
}

// LatestFileState is one "latest" file as the blob store holds it.
type LatestFileState struct {
	// Compressed says which of the two names it is stored under:
	// "<id>.latest.json.gz" (true) or "<id>.latest.json" (false).
	Compressed bool

	// Readable is false when the stored bytes are not what a consumer
	// fetching that name needs: not gzip under the ".gz" name (or gzip
	// under the plain one), or not a CatalogFile. Every field below is
	// then empty.
	Readable bool

	// Content is the canonical (decompressed) signed CatalogFile; its
	// SHA-256 is what an index entry's latest.digest must equal.
	Content json.RawMessage
	// ServedSize is the size of the bytes as stored.
	ServedSize int64

	Version   int64
	Catalog   json.RawMessage
	RetiredAt *time.Time
}

// Stored returns the StoredLatest file under the given name, or nil.
func (s CatalogState) Stored(compressed bool) *LatestFileState {
	for i := range s.StoredLatest {
		if s.StoredLatest[i].Compressed == compressed {
			return &s.StoredLatest[i]
		}
	}
	return nil
}

// FileWrite is one file's content to persist. Version drives the
// versioned baseline/change path (LatestFilePath ignores it -- a fixed,
// unversioned key). Content is canonical (uncompressed) bytes;
// ServedContent is what's actually written when Compressed is true.
type FileWrite struct {
	Version       int64
	Content       json.RawMessage
	ServedContent []byte
	Compressed    bool
}

// CatalogUpdate is one catalog's new index entry -- already self-signed
// by the caller; Store never signs anything, it holds no key material --
// plus whichever new file content that entry references. Used for both
// an ordinary update (Baseline and/or Change, optionally Latest) and a
// retirement (SignedEntry is a tombstone; Latest set only when this
// catalog previously had one, for its one-time final tombstone write;
// Baseline/Change nil).
type CatalogUpdate struct {
	CatalogID   string
	SignedEntry json.RawMessage

	Baseline *FileWrite
	Change   *FileWrite
	Latest   *FileWrite

	// LatestAlt is a second "latest" write, under the other encoding's
	// name than Latest. Set only on a retirement of a catalog that has
	// "latest" files under both names, each of which needs the final
	// tombstone.
	LatestAlt *FileWrite
}

// PublishRequest is one publish call's outcome to persist. NodeID/
// NextUpdate are the index's top-level fields -- policy the caller owns
// (signing domain, freshness window), just data by the time it reaches
// Store.
type PublishRequest struct {
	NodeID      string
	NextUpdate  *time.Time
	Updates     []CatalogUpdate
	Retirements []CatalogUpdate

	// Base is what LoadForPublish returned alongside the prior state
	// this request was computed from. When set, Publish returns
	// ErrConflict instead of committing if another writer has since
	// changed any catalog in Updates or Retirements. When nil, entries
	// are merged whatever they replace.
	Base *Base
}

// Store is the one canonical assembler over any BlobStore.
type Store struct {
	blobs BlobStore
	log   *slog.Logger
	retry RetryPolicy
}

// New constructs a Store over blobs. Plain Go, no onix plugin machinery
// required. Logs via slog.Default() until WithLogger overrides it.
func New(blobs BlobStore) *Store {
	return &Store{blobs: blobs, log: slog.Default(), retry: defaultRetryPolicy}
}

// WithLogger sets the logger this Store uses for every subsequent call,
// e.g. to route it through an onix-hosted caller's own structured
// logging instead of slog's default handler. Returns s for chaining.
func (s *Store) WithLogger(logger *slog.Logger) *Store {
	if logger != nil {
		s.log = logger
	}
	return s
}

// indexDoc is the top-level index document's own shape: unlike an entry,
// nobody besides Store needs it as a typed value -- every reader treats
// "catalogs" as opaque per-entry bytes to merge or carry forward, and
// NextUpdate here is optional (omitted entirely when unset, per the file
// spec -- unlike catalog.Index's plain string, which a read-only caller
// like the crawler is content to leave as "" when absent). So this one
// wrapper stays Store's own, not shared with catalog.Index.
type indexDoc struct {
	NodeID     string            `json:"nodeId"`
	NextUpdate *time.Time        `json:"next_update,omitempty"`
	Catalogs   []json.RawMessage `json:"catalogs"`
}

// fileEnvelope unwraps a stored baseline/change file's self-signed
// envelope back to the bare catalog content, plus NextUpdate -- used to
// decide when a compaction's grace period has elapsed (see
// reconstructState). This is a file-content document, not an index entry
// -- index.go covers the latter, not the former, so there is nothing to
// share here yet (see pkg/catalog/publisher for where these get built and
// signed). No signature verification on read -- Store only ever reads
// back its own previously-written output, not externally-fetched content.
type fileEnvelope struct {
	NextUpdate time.Time       `json:"next_update"`
	Catalog    json.RawMessage `json:"catalog"`
}

// --- reads ------------------------------------------------------------

// LoadCatalogs reconstructs current state -- baseline with every change
// file applied -- for each of catalogIDs that has prior publish history.
// A requested catalogId that has no index entry and no name conflict is
// absent from the result: the caller starts a fresh baseline for it.
//
// Presence in the result does not by itself mean there is content to diff
// against. Use CatalogState.Known to tell whether the id has an index
// entry at all, and then:
//
//   - a retired catalogId has Retired set and no content (Catalog,
//     BaselineFile and ChangeFiles are empty);
//   - a live entry that lists no baseline has only its entry-level fields
//     (EntryVersion and metadata), so its version lineage continues;
//   - a live entry whose files cannot be read back has Unreadable set (see
//     CatalogState.Unreadable). Only content that is missing or corrupt is
//     reported this way; a blob store that fails for another reason still
//     fails the whole call, since nothing can be concluded from it;
//   - any id, in the index or not, whose file names are also used by
//     another index entry has NameConflicts set.
func (s *Store) LoadCatalogs(ctx context.Context, catalogIDs []string) (map[string]CatalogState, error) {
	result, _, _, err := s.loadCatalogs(ctx, catalogIDs)
	return result, err
}

// loadCatalogs is LoadCatalogs, also returning the index entries it read
// and their order (see LoadForPublish).
func (s *Store) loadCatalogs(ctx context.Context, catalogIDs []string) (map[string]CatalogState, map[string]json.RawMessage, []string, error) {
	s.log.DebugContext(ctx, "catalogstore: LoadCatalogs", "requested", len(catalogIDs))
	entries, order, err := s.readIndexEntries(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	owners := nameOwners(entries, order)

	result := make(map[string]CatalogState, len(catalogIDs))
	for _, id := range catalogIDs {
		conflicts := conflictsFor(owners, id)
		rawEntry, ok := entries[id]
		if !ok {
			if len(conflicts) > 0 {
				s.log.WarnContext(ctx, "catalogstore: catalog not in index, and its file names belong to another catalog", "catalogId", id, "sharedWith", conflicts[0].CatalogID)
				result[id] = CatalogState{NameConflicts: conflicts}
				continue
			}
			s.log.DebugContext(ctx, "catalogstore: catalog not in index, starting fresh", "catalogId", id)
			continue
		}
		var entry catalog.CatalogEntry
		if err := json.Unmarshal(rawEntry, &entry); err != nil {
			// The entry is in the index (parseIndex read its catalogId) but
			// is not an index entry. That is this catalog's problem only.
			s.log.ErrorContext(ctx, "catalogstore: index entry cannot be parsed", "catalogId", id, "error", err)
			result[id] = CatalogState{
				Unreadable:     fmt.Sprintf("catalogstore: the index entry for %s cannot be parsed: %v", id, err),
				EntryMalformed: true,
				SignedEntry:    rawEntry,
				NameConflicts:  conflicts,
			}
			continue
		}
		// Every "latest" file the blob store holds for this catalog,
		// whatever state its entry is in: retiring it has to reach them.
		stored, err := s.loadStoredLatest(ctx, id)
		if err != nil {
			return nil, nil, nil, err
		}
		if entry.IsRetired() {
			s.log.DebugContext(ctx, "catalogstore: catalog already retired, returning tombstone state", "catalogId", id, "entryVersion", entry.EntryVersion)
			result[id] = CatalogState{
				EntryVersion:  entry.EntryVersion,
				CatalogType:   entry.CatalogType,
				NetworkIds:    entry.NetworkIDs,
				SchemaTypes:   entry.SchemaTypes,
				Retired:       true,
				RetiredAt:     entry.RetiredAt,
				SignedEntry:   rawEntry,
				StoredLatest:  stored,
				NameConflicts: conflicts,
			}
			continue
		}
		if entry.Baseline.URL == "" {
			// Listed, not retired, but with nothing to fetch. There is no
			// content to diff against, so the caller publishes a fresh
			// baseline -- but from this entry's entryVersion, not from zero
			// (NFH-014 CON-TBD-03), and it can still be retired.
			s.log.DebugContext(ctx, "catalogstore: entry lists no baseline, returning entry-level state only", "catalogId", id, "entryVersion", entry.EntryVersion)
			state := entryLevelState(entry)
			state.NameConflicts = conflicts
			if state.Known() {
				state.SignedEntry = rawEntry
				state.StoredLatest = stored
				result[id] = state
			}
			continue
		}
		state, err := s.reconstructState(ctx, entry)
		var unreadable *unreadableError
		if errors.As(err, &unreadable) {
			s.log.ErrorContext(ctx, "catalogstore: published content cannot be read back", "catalogId", id, "error", err)
			broken := entryLevelState(entry)
			baseline := entry.Baseline
			broken.BaselineFile = &baseline
			broken.ChangeFiles = entry.Changes
			broken.Unreadable = err.Error()
			state = &broken
		} else if err != nil {
			return nil, nil, nil, err
		}
		state.LatestPublished = entry.Latest != nil
		state.LatestFile = entry.Latest
		state.StoredLatest = stored
		state.SignedEntry = rawEntry
		state.NameConflicts = conflicts
		result[id] = *state
	}
	s.log.DebugContext(ctx, "catalogstore: LoadCatalogs done", "requested", len(catalogIDs), "reconstructed", len(result))
	return result, entries, order, nil
}

// entryLevelState is the part of a CatalogState that comes from a live
// index entry alone, without reading any file.
func entryLevelState(entry catalog.CatalogEntry) CatalogState {
	isActive := true
	if entry.IsActive != nil {
		isActive = *entry.IsActive
	}
	var deps []catalog.MasterDependency
	if entry.Dependencies != nil {
		deps = entry.Dependencies.Masters
	}
	return CatalogState{
		EntryVersion: entry.EntryVersion,
		CatalogType:  entry.CatalogType,
		NetworkIds:   entry.NetworkIDs,
		SchemaTypes:  entry.SchemaTypes,
		IsActive:     isActive,
		Dependencies: deps,
		CrawlHint:    entry.CrawlHint,
	}
}

// unreadableError marks a reconstructState failure caused by the published
// content itself -- a file that is not there, or that does not decompress,
// parse or apply -- as opposed to the blob store failing to answer. Only
// the first kind is a fact about one catalog.
type unreadableError struct{ err error }

func (e *unreadableError) Error() string { return e.err.Error() }
func (e *unreadableError) Unwrap() error { return e.err }

// getContent is blobs.Get for a catalog's own file: a file that does not
// exist is an unreadableError; any other failure is returned as it is.
func (s *Store) getContent(ctx context.Context, path string) ([]byte, error) {
	raw, err := s.blobs.Get(ctx, path)
	if errors.Is(err, ErrBlobNotFound) {
		return nil, &unreadableError{fmt.Errorf("catalogstore: %s is listed in the index but missing from storage", path)}
	}
	if err != nil {
		return nil, fmt.Errorf("catalogstore: reading %s: %w", path, err)
	}
	return raw, nil
}

// nameOwner is one index entry as nameOwners sees it.
type nameOwner struct {
	catalogID string
	retired   bool
}

// nameOwners groups every index entry, live or retired, by FileNameKey, in
// index order. A retired entry still owns its file names: its baseline and
// change files are immutable (CON-TBD-23) and its final "latest" file is
// meant to stay served (CON-TBD-38).
func nameOwners(entries map[string]json.RawMessage, order []string) map[string][]nameOwner {
	owners := make(map[string][]nameOwner, len(order))
	seen := make(map[string]bool, len(order))
	for _, id := range order {
		if seen[id] {
			continue // an index that lists one catalogId twice: one owner, not two
		}
		seen[id] = true
		var probe struct {
			RetiredAt string `json:"retiredAt"`
		}
		_ = json.Unmarshal(entries[id], &probe)
		key := FileNameKey(id)
		owners[key] = append(owners[key], nameOwner{catalogID: id, retired: probe.RetiredAt != ""})
	}
	return owners
}

// conflictsFor returns the entries in owners that share catalogID's file
// names, leaving out catalogID itself.
func conflictsFor(owners map[string][]nameOwner, catalogID string) []NameConflict {
	var out []NameConflict
	for _, o := range owners[FileNameKey(catalogID)] {
		if o.catalogID != catalogID {
			out = append(out, NameConflict{CatalogID: o.catalogID, Retired: o.retired})
		}
	}
	return out
}

// readIndexEntries reads the current index (if any), returning every
// entry keyed by catalogId plus the catalogIds in their original index
// order -- so Publish can preserve stable ordering when it writes the
// merged index back out.
func (s *Store) readIndexEntries(ctx context.Context) (map[string]json.RawMessage, []string, error) {
	raw, err := s.blobs.Get(ctx, IndexPath())
	if errors.Is(err, ErrBlobNotFound) {
		s.log.DebugContext(ctx, "catalogstore: no existing index, starting empty")
		return map[string]json.RawMessage{}, nil, nil
	}
	if err != nil {
		s.log.ErrorContext(ctx, "catalogstore: reading existing index failed", "error", err)
		return nil, nil, fmt.Errorf("catalogstore: reading existing index: %w", err)
	}
	return s.parseIndex(ctx, raw)
}

// parseIndex splits an index document into its entries keyed by
// catalogId and the catalogIds in document order.
func (s *Store) parseIndex(ctx context.Context, raw []byte) (map[string]json.RawMessage, []string, error) {
	var doc indexDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		s.log.ErrorContext(ctx, "catalogstore: the index file is unreadable", "path", IndexPath(), "bytes", len(raw), "error", err)
		return nil, nil, fmt.Errorf("%w: %s holds %d bytes that are not a catalog index (%v); restore it from a copy, nothing can be published until then", ErrIndexUnreadable, IndexPath(), len(raw), err)
	}

	entries := make(map[string]json.RawMessage, len(doc.Catalogs))
	order := make([]string, 0, len(doc.Catalogs))
	for _, rawEntry := range doc.Catalogs {
		var probe struct {
			CatalogID string `json:"catalogId"`
		}
		if json.Unmarshal(rawEntry, &probe) != nil || probe.CatalogID == "" {
			s.log.WarnContext(ctx, "catalogstore: skipping malformed index entry")
			continue // tolerate a malformed stray entry rather than fail the whole read
		}
		entries[probe.CatalogID] = rawEntry
		order = append(order, probe.CatalogID)
	}
	s.log.DebugContext(ctx, "catalogstore: read existing index", "entries", len(entries), "bytes", len(raw))
	return entries, order, nil
}

func (s *Store) reconstructState(ctx context.Context, entry catalog.CatalogEntry) (*CatalogState, error) {
	baselineGzip := isGzipEncoded(entry.Baseline)
	baselinePath := CatalogFilePath(entry.CatalogID, entry.Baseline.Version, "json", baselineGzip)
	baselineBytes, err := s.getContent(ctx, baselinePath)
	if err != nil {
		s.log.ErrorContext(ctx, "catalogstore: reading baseline failed", "catalogId", entry.CatalogID, "path", baselinePath, "error", err)
		return nil, err
	}
	s.log.DebugContext(ctx, "catalogstore: read baseline", "catalogId", entry.CatalogID, "path", baselinePath, "bytes", len(baselineBytes))
	if baselineGzip {
		if baselineBytes, err = gunzip(baselineBytes); err != nil {
			return nil, &unreadableError{fmt.Errorf("catalogstore: decompressing %s: %w", baselinePath, err)}
		}
	}
	var wrapped fileEnvelope
	if err := json.Unmarshal(baselineBytes, &wrapped); err != nil {
		return nil, &unreadableError{fmt.Errorf("catalogstore: parsing %s: %w", baselinePath, err)}
	}

	// A change file's EffectiveVersion is only ever <= the current
	// baseline's own Version right after a re-baseline (a forced one, or
	// a compaction): every other baseline is created once, before any of
	// its changes[], so all of its changes carry strictly higher
	// versions. Such a "superseded" change describes how to get from an
	// older version to one the new baseline has already replaced. It is
	// not part of the catalog's current content and is never applied
	// here: the new baseline is whatever was submitted when it was made,
	// which need not be what the older changes add up to, so applying
	// them on top would put back a resource the baseline dropped or an
	// older value of one it changed -- and the next submission would be
	// diffed against content no consumer was served.
	//
	// It only needs to stay *listed* for one full next_update cycle after
	// the re-baseline (NFH-014 CON-TBD-32), measured against the
	// next_update stamped on the new baseline (wrapped.NextUpdate here),
	// so a consumer part-way through the old lineage can still reach the
	// baseline's version. Once that has elapsed, dropping it from the
	// returned ChangeFiles is what realizes compaction's index-payload
	// benefit -- Store's own retention rule, derived purely from data
	// already on disk, not a separate config knob.
	gracePeriodElapsed := time.Now().After(wrapped.NextUpdate)

	effective := wrapped.Catalog
	changeFiles := make([]catalog.FileEntry, 0, len(entry.Changes))
	for _, ch := range entry.Changes {
		if ch.EffectiveVersion() <= entry.Baseline.Version {
			if gracePeriodElapsed {
				s.log.DebugContext(ctx, "catalogstore: dropping superseded change file (grace period elapsed)",
					"catalogId", entry.CatalogID, "fromVersion", ch.FromVersion, "toVersion", ch.ToVersion)
				continue // grace period over: stop listing this pre-baseline change file
			}
			s.log.DebugContext(ctx, "catalogstore: keeping superseded change file listed, not applied", "catalogId", entry.CatalogID,
				"fromVersion", ch.FromVersion, "toVersion", ch.ToVersion)
			changeFiles = append(changeFiles, ch)
			continue
		}
		chGzip := isGzipEncoded(ch)
		chPath := CatalogFilePath(entry.CatalogID, ch.EffectiveVersion(), "changes.json", chGzip)
		raw, err := s.getContent(ctx, chPath)
		if err != nil {
			s.log.ErrorContext(ctx, "catalogstore: reading change file failed", "catalogId", entry.CatalogID, "path", chPath, "error", err)
			return nil, err
		}
		if chGzip {
			if raw, err = gunzip(raw); err != nil {
				return nil, &unreadableError{fmt.Errorf("catalogstore: decompressing %s: %w", chPath, err)}
			}
		}
		effective, err = catalog.Apply(effective, raw)
		if err != nil {
			return nil, &unreadableError{fmt.Errorf("catalogstore: applying %s: %w", chPath, err)}
		}
		s.log.DebugContext(ctx, "catalogstore: applied change file", "catalogId", entry.CatalogID,
			"fromVersion", ch.FromVersion, "toVersion", ch.ToVersion)
		changeFiles = append(changeFiles, ch)
	}

	if len(changeFiles) == 0 && entry.Changes == nil {
		// Keep "no change files" in the form the entry has it (null, as a
		// first baseline is published), so a republish that changes
		// nothing reproduces the entry byte for byte.
		changeFiles = nil
	}

	state := entryLevelState(entry)
	baseline := entry.Baseline
	state.Catalog = effective
	state.BaselineFile = &baseline
	state.ChangeFiles = changeFiles
	return &state, nil
}

// loadStoredLatest reads whatever the blob store holds under both of
// catalogID's "latest" names. Absence is normal and not an error. A file
// that declares a different catalogId is another catalog's (two ids can
// share a local name) and is left out: it is not this catalog's to
// rewrite.
func (s *Store) loadStoredLatest(ctx context.Context, catalogID string) ([]LatestFileState, error) {
	var out []LatestFileState
	for _, compressed := range []bool{true, false} {
		p := LatestFilePath(catalogID, compressed)
		raw, err := s.blobs.Get(ctx, p)
		if errors.Is(err, ErrBlobNotFound) {
			continue
		}
		if err != nil {
			s.log.ErrorContext(ctx, "catalogstore: reading latest file failed", "catalogId", catalogID, "path", p, "error", err)
			return nil, fmt.Errorf("catalogstore: reading %s: %w", p, err)
		}
		st := LatestFileState{Compressed: compressed, ServedSize: int64(len(raw))}
		content := raw
		isGzip := len(raw) >= 2 && raw[0] == 0x1f && raw[1] == 0x8b
		ok := isGzip == compressed
		if ok && compressed {
			if content, err = gunzip(raw); err != nil {
				ok = false
			}
		}
		var doc struct {
			CatalogID string          `json:"catalogId"`
			Version   int64           `json:"version"`
			Catalog   json.RawMessage `json:"catalog"`
			RetiredAt *time.Time      `json:"retiredAt"`
		}
		if ok && (json.Unmarshal(content, &doc) != nil || doc.CatalogID == "") {
			ok = false
		}
		if ok && doc.CatalogID != catalogID {
			s.log.WarnContext(ctx, "catalogstore: latest file belongs to another catalog, ignoring", "catalogId", catalogID, "path", p, "fileCatalogId", doc.CatalogID)
			continue
		}
		if ok {
			st.Readable, st.Content = true, content
			st.Version, st.Catalog, st.RetiredAt = doc.Version, doc.Catalog, doc.RetiredAt
		} else {
			s.log.WarnContext(ctx, "catalogstore: latest file is not servable as named", "catalogId", catalogID, "path", p, "bytes", len(raw))
		}
		out = append(out, st)
	}
	return out, nil
}

// --- writes -------------------------------------------------------------

// Publish merges req's catalog updates and retirements into the existing
// index, keyed by catalogId, replacing or appending as needed, and
// persists everything. A request with no updates and no retirements
// writes nothing at all.
//
// Over a ConditionalBlobStore the index write is a compare-and-swap,
// retried with the request merged again whenever another writer got in
// first, so any number of writers can publish at once without losing
// entries (see publishConditional for the order of writes). It can
// return ErrConflict (req.Base is stale: recompute and call again) or
// ErrIndexBusy (retries exhausted: call again).
//
// Over a plain BlobStore there is no such protection and the caller
// must be the only writer: every new file is written, then the merged
// index last, so a failure partway through never leaves the index
// pointing at a file that was never written.
func (s *Store) Publish(ctx context.Context, req PublishRequest) error {
	s.log.DebugContext(ctx, "catalogstore: Publish", "nodeId", req.NodeID, "updates", len(req.Updates), "retirements", len(req.Retirements))
	if len(req.Updates) == 0 && len(req.Retirements) == 0 {
		// Nothing to merge. Not even the index's next_update moves: a
		// call that changed nothing must leave nothing changed.
		s.log.DebugContext(ctx, "catalogstore: nothing to publish, index left as it is")
		return nil
	}
	if cas, ok := s.blobs.(ConditionalBlobStore); ok {
		return s.publishConditional(ctx, cas, req)
	}
	entries, order, err := s.readIndexEntries(ctx)
	if err != nil {
		return err
	}
	// Not atomic with the index write below, but it still catches a
	// change made between LoadForPublish and here.
	if ids := req.Base.changed(req, entries, order); len(ids) > 0 {
		return &ConflictError{CatalogIDs: ids}
	}
	// Before anything is written: see checkNewCatalogIDs.
	if err := checkNewCatalogIDs(entries, order, req); err != nil {
		s.log.ErrorContext(ctx, "catalogstore: publish refused", "error", err)
		return err
	}

	apply := func(u CatalogUpdate) error {
		if err := s.writeCatalogFiles(ctx, u); err != nil {
			return err
		}
		_, existed := entries[u.CatalogID]
		if !existed {
			order = append(order, u.CatalogID)
		}
		entries[u.CatalogID] = u.SignedEntry
		s.log.DebugContext(ctx, "catalogstore: merged entry", "catalogId", u.CatalogID, "replacedExisting", existed)
		return nil
	}
	for _, u := range req.Updates {
		if err := apply(u); err != nil {
			return err
		}
	}
	for _, r := range req.Retirements {
		if err := apply(r); err != nil {
			return err
		}
	}

	ordered := make([]json.RawMessage, 0, len(order))
	for _, id := range order {
		ordered = append(ordered, entries[id])
	}
	indexBytes, err := json.Marshal(indexDoc{NodeID: req.NodeID, NextUpdate: req.NextUpdate, Catalogs: ordered})
	if err != nil {
		return fmt.Errorf("catalogstore: marshaling index: %w", err)
	}
	if err := s.blobs.Put(ctx, IndexPath(), indexBytes); err != nil {
		s.log.ErrorContext(ctx, "catalogstore: writing index failed", "error", err)
		return fmt.Errorf("catalogstore: writing index: %w", err)
	}
	s.log.DebugContext(ctx, "catalogstore: Publish done", "entries", len(ordered), "bytes", len(indexBytes))
	return nil
}

// ErrCatalogIDNotAllowed is wrapped by the error Publish returns when a
// request would add a catalogId to the index that must not be added (see
// checkNewCatalogIDs).
var ErrCatalogIDNotAllowed = errors.New("catalogstore: catalogId not allowed")

// checkNewCatalogIDs is the store's own last check on the catalogIds a
// request would add to the index, made before any file is written. A
// caller that goes through LoadCatalogs and pkg/catalog/publisher never
// gets here with such a request -- the publisher has already reported it
// as a per-catalog error -- so a refusal here fails the whole request: it
// means the caller skipped that step, or the index changed in between.
//
// For an update whose catalogId has no index entry yet: the id must pass
// ValidateCatalogID, and its file names (FileNameKey) must not belong to
// another entry in the index or to another new id in the same request.
// Without this, the write below would silently replace another catalog's
// baseline and "latest" files (CON-TBD-23).
//
// An id that is already in the index is not re-checked here, and neither
// is a retirement: a tombstone writes no baseline or change file, and
// whether an id may be retired is the publisher's decision.
func checkNewCatalogIDs(entries map[string]json.RawMessage, order []string, req PublishRequest) error {
	owners := nameOwners(entries, order)
	claimed := make(map[string]string, len(req.Updates))
	for _, u := range req.Updates {
		if _, exists := entries[u.CatalogID]; exists {
			continue
		}
		if err := ValidateCatalogID(u.CatalogID); err != nil {
			return fmt.Errorf("%w: %w", ErrCatalogIDNotAllowed, err)
		}
		if conflicts := conflictsFor(owners, u.CatalogID); len(conflicts) > 0 {
			return fmt.Errorf("%w: %q would be stored under the same file names as %q, which is already in the catalog index", ErrCatalogIDNotAllowed, u.CatalogID, conflicts[0].CatalogID)
		}
		key := FileNameKey(u.CatalogID)
		if other, taken := claimed[key]; taken && other != u.CatalogID {
			return fmt.Errorf("%w: %q and %q, both new in this request, would be stored under the same file names", ErrCatalogIDNotAllowed, other, u.CatalogID)
		}
		claimed[key] = u.CatalogID
	}
	return nil
}

func (s *Store) writeCatalogFiles(ctx context.Context, u CatalogUpdate) error {
	if u.Baseline != nil {
		if err := s.writeFile(ctx, CatalogFilePath(u.CatalogID, u.Baseline.Version, "json", u.Baseline.Compressed), u.Baseline); err != nil {
			return err
		}
	}
	if u.Change != nil {
		if err := s.writeFile(ctx, CatalogFilePath(u.CatalogID, u.Change.Version, "changes.json", u.Change.Compressed), u.Change); err != nil {
			return err
		}
	}
	if u.Latest != nil {
		if err := s.writeFile(ctx, LatestFilePath(u.CatalogID, u.Latest.Compressed), u.Latest); err != nil {
			return err
		}
	}
	if u.LatestAlt != nil {
		if err := s.writeFile(ctx, LatestFilePath(u.CatalogID, u.LatestAlt.Compressed), u.LatestAlt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) writeFile(ctx context.Context, path string, fw *FileWrite) error {
	served := fw.ServedContent
	if served == nil {
		served = fw.Content // Compressed is false: ServedContent and Content are identical
	}
	if err := s.blobs.Put(ctx, path, served); err != nil {
		s.log.ErrorContext(ctx, "catalogstore: writing file failed", "path", path, "error", err)
		return fmt.Errorf("catalogstore: writing %s: %w", path, err)
	}
	s.log.DebugContext(ctx, "catalogstore: wrote file", "path", path, "bytes", len(served), "compressed", fw.Compressed)
	return nil
}

// --- helpers --------------------------------------------------------------

// isGzipEncoded reports whether fe's content is gzip-compressed: fe.Encoding
// if set, otherwise falling back to the URL's ".gz" suffix -- the same
// fallback convention catalog.FileEntry's own doc comment describes for a
// reader (see pkg/crawler/decode.EncodingFor). Read back from the entry
// itself rather than from any current compression setting, since a
// catalog's files may have been published under a different compression
// setting than whatever produced this call.
func isGzipEncoded(fe catalog.FileEntry) bool {
	if fe.Encoding != "" {
		return fe.Encoding == "gzip"
	}
	return strings.HasSuffix(fe.URL, ".gz")
}

// gunzip decompresses data written by a caller's own gzip.Writer.
func gunzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
