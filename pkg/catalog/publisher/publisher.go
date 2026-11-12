// Package publisher is the decentralized-catalog file spec's publish-side
// orchestration: given a publisher's catalog submissions and their prior
// state (as pkg/catalog/store already reconstructs it), diff, decide
// baseline/change/metadata/unchanged, and self-sign the resulting
// catalog-index entries and files -- RFC NFH-014's signing/diffing/
// versioning rules, all of it.
//
// Publish never touches storage: every entry and file it signs comes back
// in Result.Publish, ready to hand directly to a pkg/catalog/store.Store's
// own Publish call, which merges them into the index and persists
// everything. Publish also holds no key material between calls -- Params
// carries the signing key, key id, and domain a caller resolved (e.g. via
// an onix KeyManager) fresh each call, the same way it carries every other
// per-call value (compaction thresholds, gzip, publish-latest, the
// next-update window). Nothing here is loaded once and reused; nothing
// here is onix-specific.
package publisher

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/beckn/catalog-core/pkg/catalog"
	"github.com/beckn/catalog-core/pkg/catalog/store"
	"github.com/beckn/catalog-core/pkg/security/artifactsigner"
)

// defaultFileValidity is the fallback used for a catalog file's own
// required next_update when Params.NextUpdateIn is unset (zero) -- unlike
// the index's next_update (optional, omitted when unset), the file spec
// requires next_update on every CatalogFileDoc/ChangeFileDoc.
const defaultFileValidity = 24 * time.Hour

// Submission is one catalog's input to a publish call: the plain Beckn
// Catalog object (no context/message envelope) plus the publisher-
// declared metadata that only the publisher can know -- NetworkIds is
// never derived from the catalog content itself. Catalogs are public,
// unconditionally (RFC NFH-014, "Catalog access is public") -- there is
// no restricted-catalog or per-catalog-auth concept; NetworkIds is a
// Discovery-service relevance filter only, never an access control.
//
// IsActive is deliberately absent here: the index entry's isActive
// mirrors Catalog's own pre-existing isActive field, so Publish reads it
// out of Catalog itself rather than duplicating it as a second input
// that could disagree.
type Submission struct {
	// CatalogID is the full participant-scoped id, e.g.
	// "open-economy.nfh.global/electronics-2026".
	CatalogID string

	// CatalogType defaults to "REGULAR" when empty.
	CatalogType string

	SchemaTypes []string

	// NetworkIds scopes this catalog to specific networks; empty/nil
	// means public.
	NetworkIds []string

	// Dependencies lists, for a REGULAR catalog, every MASTER catalog any
	// of its resources currently extend (NFH-014 §10.3, CON-TBD-30).
	// Publish does not derive this from Catalog's own resource content --
	// a masterResourceId names a resource, not the catalog it lives in,
	// so resolving which catalog (and which index) owns it needs
	// cross-catalog knowledge only the caller has.
	Dependencies []catalog.MasterDependency

	// CrawlHint is an optional suggested crawl frequency a crawler MAY
	// honor. Empty means no hint is published.
	CrawlHint string

	Catalog json.RawMessage

	// Directives carries per-catalog publish controls -- unlike Params'
	// batch-wide knobs (compaction thresholds, Gzip, PublishLatest,
	// NextUpdateIn, ...), which apply uniformly across every catalog in a
	// call, these decide how this one catalog specifically gets
	// published.
	Directives PublishDirectives
}

// PublishDirectives carries per-catalog publish controls a caller sets
// independently for each Submission, rather than uniformly across a whole
// Publish call.
type PublishDirectives struct {
	// ForceBaseline bypasses diffing against PriorState and always emits
	// a fresh baseline for this catalog. For a catalog with no prior
	// state this is a no-op (already the default); for one with prior
	// state, this is how a caller triggers compaction -- a fresh baseline
	// at the next version, discarding the accumulated change list.
	ForceBaseline bool

	// Retire marks this catalogId RETIRED this call instead of
	// publishing it: a tombstone entry (retiredAt, no isActive/baseline/
	// changes) replaces whatever was there. Catalog is not required and
	// is ignored when this is set. This catalogId's prior CatalogType/
	// NetworkIds/SchemaTypes and EntryVersion still come from
	// PriorState[id] -- a tombstone keeps those, it only drops
	// isActive/baseline/changes.
	Retire bool
}

// Params is everything one Publish call needs, all as explicit values --
// no config is loaded or stored here, and no key material is held between
// calls. The caller resolves SigningKey/KeyID/Domain (e.g. via an onix
// KeyManager) and the compaction/gzip/publish-latest/next-update knobs
// (e.g. from its own onix plugin config) fresh each call.
type Params struct {
	Catalogs []Submission

	// PriorState supplies, per catalogId, what pkg/catalog/store's own
	// Store.LoadCatalogs already reconstructed for the catalogs actually
	// submitted in Catalogs -- the only way Publish can produce a change
	// file instead of a fresh baseline. A submitted catalogId absent from
	// this map is always published as a new baseline, same as when a
	// submission's Directives.ForceBaseline is set.
	PriorState map[string]store.CatalogState

	// CompactionChangeCountThreshold and CompactionSizeRatioThreshold
	// opt into automatically compacting a catalog's baseline instead of
	// publishing yet another change file. Both default to 0 (disabled).
	// Checked against prior.ChangeFiles only, before the new change file
	// would be created:
	//   - CompactionChangeCountThreshold: compact once the catalog
	//     already has this many pending change files.
	//   - CompactionSizeRatioThreshold: compact once the combined size of
	//     pending change files, divided by the baseline's own size, is at
	//     least this fraction (e.g. 0.5 for 50%).
	// Either threshold alone can trigger compaction; a submission's own
	// Directives.ForceBaseline still always triggers it regardless of
	// these. Unlike Directives, these thresholds apply uniformly to
	// every catalog in this call.
	CompactionChangeCountThreshold int
	CompactionSizeRatioThreshold   float64

	// Gzip opts into serving every file gzip-compressed, signaled by a
	// ".json.gz" URL extension and FileEntry.Encoding. Digest/signature
	// are always computed over the canonical, decompressed content
	// regardless of this setting.
	Gzip bool

	// PublishLatest opts into publishing/maintaining each catalog's
	// "latest" entry: a full CatalogFileDoc overwritten in place at a
	// stable URL, for consumers who want fully-current content without
	// ever applying changes[].
	PublishLatest bool

	// NextUpdateIn sets how far in the future the index's "next_update"
	// freshness window extends from the moment of publishing. Zero omits
	// the index's next_update entirely (a catalog file's own next_update
	// still gets defaultFileValidity, since the schema requires it).
	NextUpdateIn time.Duration

	// PublicBaseURL, if set, is the one public URL prefix everything this
	// publish writes gets addressed under -- the same root a
	// pkg/catalog/store-backed CatalogBlobStore serves its layout from. A
	// placeholder ("pending-artifact-store://...") is used when unset.
	PublicBaseURL string

	// SigningKey/KeyID/Domain are this call's resolved signing identity:
	// SigningKey signs every file/entry; KeyID is embedded as each
	// signature's keyId; Domain is stamped as the index's own nodeId.
	// Resolved by the caller (e.g. via KeyManager.Keyset) fresh every
	// call -- Publish holds none of this between calls.
	SigningKey ed25519.PrivateKey
	KeyID      string
	Domain     string

	// Logger receives every major-event log line Publish emits (mode
	// decisions, compaction triggers, signing steps, non-fatal errors).
	// Never logs key material or raw signature values. Defaults to
	// slog.Default() when unset -- there is no way to fully silence
	// logging short of passing a logger with a discarding handler, since
	// this package holds no global logging state of its own to disable.
	Logger *slog.Logger
}

// logger returns p.Logger, or slog.Default() when unset.
func (p Params) logger() *slog.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return slog.Default()
}

// Outcome reports what happened to one submitted catalog -- reporting
// only. Content/LatestContent are the canonical (uncompressed) bytes for
// programmatic inspection; the actual bytes to persist (served,
// compressed) travel via Result.Publish.Updates instead.
type Outcome struct {
	CatalogID string

	// Version is this catalog's new current file-lineage version after
	// this call, distinct from EntryVersion.
	Version int64

	// EntryVersion is this catalog's new entry-level version -- bumped
	// whenever Changed is true, content or metadata.
	EntryVersion int64

	Changed bool // false = no-op: diffed against PriorState and found no changes at all
	Digest  string

	// Mode is "baseline", "change", "metadata" (no file republished, but
	// entry-level metadata changed), or "unchanged".
	Mode    string
	Content json.RawMessage

	// LatestDigest is set whenever the entry lists "latest".
	// LatestContent is set only when this call (re)writes the file: an
	// unchanged or metadata-only outcome whose published "latest" is intact
	// keeps it as it is, so its digest changes only when its content does.
	LatestContent json.RawMessage
	LatestDigest  string
}

// PublishError is a non-fatal, per-catalog failure -- one bad submission
// must not fail the whole publish call.
type PublishError struct {
	CatalogID string
	Stage     string // "validate" | "load" | "diff" | "retire"
	Reason    string
	Fatal     bool
}

// Result is one Publish call's outcome. Publish is ready to hand directly
// to a pkg/catalog/store.Store's own Publish call -- this package never
// touches storage itself. Reports is per-catalog human-readable detail
// for a caller's own display/response purposes; Errors are non-fatal
// per-catalog failures.
type Result struct {
	PublishedAt time.Time
	Publish     store.PublishRequest
	Reports     []Outcome
	Errors      []PublishError

	// Retired reports every catalogId this call was asked to retire,
	// including one that was retired already (for which Publish.Retirements
	// has no update unless a "latest" file had to be repaired).
	Retired []RetireReport
}

// RetireReport is what a retire request left in place for one catalog: the
// tombstone's identity, and every "latest" file a consumer may still hold a
// URL for -- each of which must now serve a CatalogFile carrying retiredAt
// (NFH-014 CON-TBD-38). A caller that verifies its publishes re-fetches each
// one; that holds for a repeat retire too, which is how a retirement whose
// first attempt could not be verified gets verified.
type RetireReport struct {
	CatalogID    string
	EntryVersion int64
	RetiredAt    string // as in the index entry

	// AlreadyRetired is true when the index entry was a tombstone before
	// this call: no new tombstone was signed.
	AlreadyRetired bool

	Latest []RetiredLatestFile
}

// RetiredLatestFile is one "latest" file of a retired catalog.
type RetiredLatestFile struct {
	// File is the reference a consumer would verify it against: its stable
	// URL, the digest of Content, the served size, the encoding.
	File catalog.FileEntry
	// Compressed says which of the two "latest" names it is stored under.
	Compressed bool
	// Content is the canonical signed CatalogFile, retiredAt included.
	Content json.RawMessage
	// Written is true when this call writes it (Publish.Retirements carries
	// the bytes); false when the blob store already held exactly this.
	Written bool
}

// outcome is publishOne/finishEntry's full internal result -- Publish
// splits it into the public, reporting-only Outcome and the store-ready
// CatalogUpdate.
type outcome struct {
	CatalogID     string
	Version       int64
	EntryVersion  int64
	Changed       bool
	Digest        string
	Mode          string
	Content       json.RawMessage
	ServedContent []byte

	LatestContent       json.RawMessage
	LatestServedContent []byte
	LatestDigest        string
	// LatestCompressed is the encoding of the "latest" write: the one it
	// was last published under, which is not necessarily Params.Gzip.
	LatestCompressed bool
}

// IndexURL returns the configured index location under publicBaseURL, or
// a placeholder when no ArtifactStore-assigned location exists yet.
func IndexURL(publicBaseURL string) string {
	if publicBaseURL != "" {
		return joinURL(publicBaseURL, store.IndexDirName, store.IndexFilename)
	}
	return "pending-artifact-store://catalog-index.json"
}

// Publish validates each submission, diffs it against any PriorState for
// its catalogId, and signs the resulting catalog-index entry (baseline,
// change file, retirement tombstone, or a metadata-only/no-op update). A
// submission that fails validation or diffing is reported as a non-fatal
// PublishError and skipped; it does not fail the rest of the batch.
func Publish(ctx context.Context, p Params) (Result, error) {
	log := p.logger()
	log.DebugContext(ctx, "catalog/publisher: Publish", "domain", p.Domain, "keyId", p.KeyID, "catalogs", len(p.Catalogs))

	if len(p.SigningKey) != ed25519.PrivateKeySize {
		return Result{}, fmt.Errorf("catalog/publisher: invalid signing key (want %d bytes, got %d)", ed25519.PrivateKeySize, len(p.SigningKey))
	}
	if p.KeyID == "" {
		return Result{}, fmt.Errorf("catalog/publisher: KeyID is required")
	}

	now := time.Now()
	result := Result{PublishedAt: now, Publish: store.PublishRequest{NodeID: p.Domain}}

	fileValidityIn := p.NextUpdateIn
	if fileValidityIn <= 0 {
		fileValidityIn = defaultFileValidity
	}
	fileNextUpdate := now.Add(fileValidityIn)
	if p.NextUpdateIn > 0 {
		nextUpdate := fileNextUpdate
		result.Publish.NextUpdate = &nextUpdate
	}

	handled := make(map[string]bool, len(p.Catalogs))
	// claimed records, by store.FileNameKey, which catalogId this call has
	// already agreed to write files for -- so two ids in one call cannot
	// both be given the same file names (see admit).
	claimed := make(map[string]string, len(p.Catalogs))

	for _, sub := range p.Catalogs {
		if handled[sub.CatalogID] {
			continue // first entry for a catalogId in one call wins; later duplicates are ignored
		}
		handled[sub.CatalogID] = true
		prior := p.PriorState[sub.CatalogID]

		if sub.Directives.Retire {
			if err := p.retireOne(ctx, sub.CatalogID, now, fileNextUpdate, &result); err != nil {
				return result, err
			}
			continue
		}

		if err := validateSubmission(sub); err != nil {
			log.WarnContext(ctx, "catalog/publisher: submission rejected", "catalogId", sub.CatalogID, "stage", "validate", "reason", err.Error())
			result.Errors = append(result.Errors, PublishError{CatalogID: sub.CatalogID, Stage: "validate", Reason: err.Error()})
			continue
		}
		if stage, err := admit(sub, prior, claimed); err != nil {
			log.WarnContext(ctx, "catalog/publisher: submission rejected", "catalogId", sub.CatalogID, "stage", stage, "reason", err.Error())
			result.Errors = append(result.Errors, PublishError{CatalogID: sub.CatalogID, Stage: stage, Reason: err.Error()})
			continue
		}
		claimed[store.FileNameKey(sub.CatalogID)] = sub.CatalogID

		oc, entry, err := p.publishOne(ctx, sub, prior, fileNextUpdate)
		if err != nil {
			log.WarnContext(ctx, "catalog/publisher: submission rejected", "catalogId", sub.CatalogID, "stage", "diff", "reason", err.Error())
			result.Errors = append(result.Errors, PublishError{CatalogID: sub.CatalogID, Stage: "diff", Reason: err.Error()})
			continue
		}
		log.DebugContext(ctx, "catalog/publisher: catalog decided", "catalogId", sub.CatalogID, "mode", oc.Mode,
			"changed", oc.Changed, "version", oc.Version, "entryVersion", oc.EntryVersion)

		raw, err := p.signEntry(entry)
		if err != nil {
			log.ErrorContext(ctx, "catalog/publisher: signing catalog entry failed", "catalogId", sub.CatalogID, "error", err)
			return result, fmt.Errorf("catalog/publisher: signing catalog entry %q: %w", sub.CatalogID, err)
		}
		// "Unchanged" must mean the entry itself is unchanged: entryVersion
		// has to move on any change to the entry (NFH-014 Versioning), and
		// a consumer skips an entry whose entryVersion it has seen. The
		// submission can be identical and the entry still differ from the
		// one in the index -- its "latest" reference appeared, went away or
		// was repaired, superseded change files left the list after a
		// compaction's grace period, the signing key changed. Signing is
		// deterministic, so comparing with the stored entry settles it for
		// every such cause at once.
		if prior := p.PriorState[sub.CatalogID]; oc.Mode == "unchanged" && len(prior.SignedEntry) > 0 && !bytes.Equal(raw, prior.SignedEntry) {
			entry.EntryVersion = prior.EntryVersion + 1
			oc.EntryVersion, oc.Changed, oc.Mode = entry.EntryVersion, true, "metadata"
			log.DebugContext(ctx, "catalog/publisher: entry differs from the published one, entry version bumped", "catalogId", sub.CatalogID, "entryVersion", entry.EntryVersion)
			if raw, err = p.signEntry(entry); err != nil {
				return result, fmt.Errorf("catalog/publisher: signing catalog entry %q: %w", sub.CatalogID, err)
			}
		}

		update := store.CatalogUpdate{CatalogID: sub.CatalogID, SignedEntry: raw}
		if oc.Content != nil {
			fw := &store.FileWrite{Version: oc.Version, Content: oc.Content, ServedContent: oc.ServedContent, Compressed: p.Gzip}
			switch oc.Mode {
			case "baseline":
				update.Baseline = fw
			case "change":
				update.Change = fw
			}
		}
		if oc.LatestContent != nil {
			update.Latest = &store.FileWrite{Version: oc.Version, Content: oc.LatestContent, ServedContent: oc.LatestServedContent, Compressed: oc.LatestCompressed}
		}
		result.Publish.Updates = append(result.Publish.Updates, update)
		result.Reports = append(result.Reports, Outcome{
			CatalogID: oc.CatalogID, Version: oc.Version, EntryVersion: oc.EntryVersion,
			Changed: oc.Changed, Digest: oc.Digest, Mode: oc.Mode, Content: oc.Content,
			LatestContent: oc.LatestContent, LatestDigest: oc.LatestDigest,
		})
	}

	log.DebugContext(ctx, "catalog/publisher: Publish done", "published", len(result.Publish.Updates),
		"retired", len(result.Publish.Retirements), "errors", len(result.Errors))
	return result, nil
}

// admit decides whether a structurally valid submission may be published
// under its catalogId at all, given what the index already holds for that
// id (prior) and which file names this call has already given out
// (claimed). It returns the PublishError stage and reason for a refusal.
//
// These are the identity rules, checked before anything is diffed or
// signed, so a refused submission leaves the index and every file exactly
// as they were:
//
//   - A retired catalogId is never published again (NFH-014 CON-TBD-05,
//     CON-TBD-26). Doing so would un-retire the entry, restart
//     baseline.version (CON-TBD-04) and overwrite the retired catalog's
//     immutable files (CON-TBD-23).
//   - A catalog whose published content cannot be read back cannot be
//     diffed; only an explicit ForceBaseline (or a retire) moves it on.
//     One whose index entry itself cannot be read cannot be published at
//     all: a forced baseline would restart its entryVersion and overwrite
//     its files.
//   - A catalogId that has no index entry yet must be usable as a file
//     name and URL (store.ValidateCatalogID), and its file names must not
//     already belong to another catalogId, in the index or earlier in this
//     call. File names come from the id's last path segment only, so
//     "a.example/CAT-1" and "b.example/CAT-1" would be one set of files.
//   - A catalogId that is in the index, and shares its file names with
//     another entry that is still live, is refused too: that index was
//     written before this was checked, and every publish of either one
//     overwrites the other. Once the other one is retired, this one can be
//     published again.
//
// A catalogId that already has an index entry is otherwise not re-checked:
// it keeps its id, file names and URLs whatever they look like.
func admit(sub Submission, prior store.CatalogState, claimed map[string]string) (stage string, err error) {
	id := sub.CatalogID
	if prior.Retired {
		return "validate", fmt.Errorf("catalogId %q is retired and cannot be published again (NFH-014 CON-TBD-26); publish this catalog under a new catalogId", id)
	}
	if prior.EntryMalformed {
		return "load", fmt.Errorf("the index entry of catalogId %q cannot be read, so neither its entryVersion nor its files are known (%s); it cannot be published until the index entry is repaired", id, prior.Unreadable)
	}
	if prior.Unreadable != "" && !sub.Directives.ForceBaseline {
		return "load", fmt.Errorf("the published content of catalogId %q cannot be read back, so there is nothing to compare this submission with (%s); publish it with forceBaseline to start from a new baseline, or retire it", id, prior.Unreadable)
	}
	if !prior.Known() {
		if err := store.ValidateCatalogID(id); err != nil {
			return "validate", err
		}
		if len(prior.NameConflicts) > 0 {
			return "validate", fmt.Errorf("catalogId %q would be stored under the same file names as %q, which is already in the catalog index (file names come from the text after the last \"/\" of the id, compared without regard to case); use a catalogId whose last segment is different", id, prior.NameConflicts[0].CatalogID)
		}
	} else {
		for _, c := range prior.NameConflicts {
			if !c.Retired {
				return "validate", fmt.Errorf("catalogId %q shares its file names with %q, which is also published, so each one's files overwrite the other's; retire one of them, then publish the other with forceBaseline", id, c.CatalogID)
			}
		}
	}
	if other, taken := claimed[store.FileNameKey(id)]; taken && other != id {
		return "validate", fmt.Errorf("catalogId %q would be stored under the same file names as %q, which is published in the same call (file names come from the text after the last \"/\" of the id, compared without regard to case); use a catalogId whose last segment is different", id, other)
	}
	return "", nil
}

// shortID cuts an id that is not in the index -- and so can be anything a
// caller sent, of any length -- down to something that fits in an error
// message and a log line.
func shortID(id string) string {
	const limit = 120
	if len(id) <= limit {
		return id
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(id[cut]) {
		cut--
	}
	return id[:cut] + "..."
}

// sharesFilesWithLiveCatalog reports whether another, still live index
// entry is written under the same file names as this one.
func sharesFilesWithLiveCatalog(prior store.CatalogState) (string, bool) {
	for _, c := range prior.NameConflicts {
		if !c.Retired {
			return c.CatalogID, true
		}
	}
	return "", false
}

// retireOne signs a tombstone entry for id, replacing whatever entry was
// there -- prior CatalogType/NetworkIds/SchemaTypes/EntryVersion still
// come from PriorState[id], only isActive/baseline/changes are dropped.
//
// Only a catalogId that has an index entry can be retired. One that was
// never published (or is misspelt, or empty) is reported as a "retire"
// PublishError and nothing is signed: a tombstone is a permanent, signed
// statement that a catalog existed and was withdrawn.
func (p Params) retireOne(ctx context.Context, id string, now time.Time, fileNextUpdate time.Time, result *Result) error {
	log := p.logger()
	prior := p.PriorState[id]
	if !prior.Known() {
		reason := fmt.Sprintf("catalog not found: catalogId %q is not in the catalog index, so there is nothing to retire", shortID(id))
		if id == "" {
			reason = "catalog not found: missing catalogId"
		}
		log.WarnContext(ctx, "catalog/publisher: retire rejected", "catalogId", id, "stage", "retire", "reason", reason)
		result.Errors = append(result.Errors, PublishError{CatalogID: id, Stage: "retire", Reason: reason})
		return nil
	}
	if prior.EntryMalformed {
		reason := fmt.Sprintf("the index entry of catalogId %q cannot be read, so the entryVersion its tombstone must continue from is not known (%s); it cannot be retired until the index entry is repaired", id, prior.Unreadable)
		log.WarnContext(ctx, "catalog/publisher: retire rejected", "catalogId", id, "stage", "retire", "reason", reason)
		result.Errors = append(result.Errors, PublishError{CatalogID: id, Stage: "retire", Reason: reason})
		return nil
	}
	// Retirement is one-way and happens once (NFH-014 CON-TBD-05): the
	// tombstone already in the index is authoritative. Signing a second
	// one would move retiredAt, so it no longer matches the final
	// "latest" file, for no change in what a consumer should conclude.
	if prior.Retired {
		return p.recheckRetired(ctx, id, prior, fileNextUpdate, result)
	}

	// CON-TBD-38: a catalog that had "latest" published needs one final
	// write to that same stable URL, populating CatalogFileDoc.retiredAt
	// -- otherwise a consumer that only ever fetches "latest" directly,
	// never revisiting the index, has no way to learn the catalog is
	// gone. Where that write goes is decided by what was published, not by
	// today's Gzip or PublishLatest: the name the prior entry advertised,
	// and any other "latest" file the blob store still holds (PublishLatest
	// switched off since, or a file left under the other name), because a
	// consumer may hold any of those URLs.
	//
	// The exception is an index that already holds another live catalog
	// under the same file names (see admit): the "latest" file at that
	// name is the other catalog's as much as this one's, and writing a
	// tombstone into it would tell the other catalog's consumers that it
	// was retired.
	var targets []bool
	if sharedWith, shared := sharesFilesWithLiveCatalog(prior); shared {
		log.WarnContext(ctx, "catalog/publisher: not writing a final latest tombstone, the file is shared with a live catalog", "catalogId", id, "sharedWith", sharedWith)
	} else {
		targets = p.retireLatestTargets(prior)
	}
	// What the final file carries: the catalog as last published. When
	// that cannot be read back (CatalogState.Unreadable), the newest
	// readable "latest" file stands in for it. With neither, there is no
	// CatalogFile to put retiredAt on, and the retire is refused here,
	// before anything is signed: a tombstone in the index next to a
	// "latest" that still says live is the state this write exists to
	// prevent, and a retire that reports an error must not have happened.
	finalCatalog, finalVersion := prior.Catalog, currentVersion(prior)
	if len(targets) > 0 && finalCatalog == nil {
		if source := newestReadableLatest(prior.StoredLatest); source != nil {
			finalCatalog, finalVersion = source.Catalog, source.Version
		} else {
			reason := fmt.Sprintf("catalogId %q has a \"latest\" file that must be given a final copy carrying retiredAt, but neither its published content nor that file can be read back, so there is nothing to build the final copy from; publish it with forceBaseline first, then retire it", id)
			log.WarnContext(ctx, "catalog/publisher: retire rejected", "catalogId", id, "stage", "retire", "reason", reason)
			result.Errors = append(result.Errors, PublishError{CatalogID: id, Stage: "retire", Reason: reason})
			return nil
		}
	}

	retiredAt := now.Format(time.RFC3339Nano)
	raw, err := p.signEntry(catalog.CatalogEntry{
		CatalogID:    id,
		EntryVersion: prior.EntryVersion + 1,
		CatalogType:  prior.CatalogType,
		NetworkIDs:   prior.NetworkIds,
		SchemaTypes:  prior.SchemaTypes,
		RetiredAt:    retiredAt,
	})
	if err != nil {
		log.ErrorContext(ctx, "catalog/publisher: signing tombstone failed", "catalogId", id, "error", err)
		return fmt.Errorf("catalog/publisher: signing tombstone %q: %w", id, err)
	}
	log.DebugContext(ctx, "catalog/publisher: catalog retired", "catalogId", id, "entryVersion", prior.EntryVersion+1, "latestPublished", prior.LatestPublished)
	update := store.CatalogUpdate{CatalogID: id, SignedEntry: raw}
	report := RetireReport{CatalogID: id, EntryVersion: prior.EntryVersion + 1, RetiredAt: retiredAt}

	if len(targets) > 0 {
		content, err := p.signCatalogFile(id, finalVersion, fileNextUpdate, finalCatalog, &now)
		if err != nil {
			log.ErrorContext(ctx, "catalog/publisher: signing final latest tombstone failed", "catalogId", id, "error", err)
			return fmt.Errorf("catalog/publisher: signing final \"latest\" tombstone %q: %w", id, err)
		}
		for _, compressed := range targets {
			if err := p.addRetiredLatest(&update, &report, id, finalVersion, content, compressed); err != nil {
				return err
			}
			log.DebugContext(ctx, "catalog/publisher: wrote final latest tombstone", "catalogId", id, "compressed", compressed)
		}
	}
	result.Publish.Retirements = append(result.Publish.Retirements, update)
	result.Retired = append(result.Retired, report)
	return nil
}

// newestReadableLatest returns the stored "latest" file with the highest
// version that parses and carries a catalog, or nil.
func newestReadableLatest(stored []store.LatestFileState) *store.LatestFileState {
	var source *store.LatestFileState
	for i := range stored {
		if st := &stored[i]; st.Readable && len(st.Catalog) > 0 && (source == nil || st.Version > source.Version) {
			source = st
		}
	}
	return source
}

// retireLatestTargets lists the "latest" names (compressed or not) that
// must receive the final tombstone: the one the prior entry advertised
// first, then any other the blob store holds.
func (p Params) retireLatestTargets(prior store.CatalogState) []bool {
	var targets []bool
	add := func(compressed bool) {
		for _, t := range targets {
			if t == compressed {
				return
			}
		}
		targets = append(targets, compressed)
	}
	switch {
	case prior.LatestFile != nil:
		add(fileEntryCompressed(*prior.LatestFile))
	case prior.LatestPublished && len(prior.StoredLatest) == 0:
		// A caller that built PriorState by hand and says only that
		// "latest" was published: the current setting is all there is.
		add(p.Gzip)
	}
	for _, st := range prior.StoredLatest {
		add(st.Compressed)
	}
	return targets
}

// addRetiredLatest attaches one final "latest" write to update (a catalog
// has at most two "latest" names, so at most two writes) and reports it.
func (p Params) addRetiredLatest(update *store.CatalogUpdate, report *RetireReport, id string, version int64, content []byte, compressed bool) error {
	fe, served, err := p.buildLatestFileEntry(id, version, content, compressed)
	if err != nil {
		return fmt.Errorf("catalog/publisher: compressing final \"latest\" tombstone %q: %w", id, err)
	}
	fw := &store.FileWrite{Content: content, ServedContent: served, Compressed: compressed}
	if update.Latest == nil {
		update.Latest = fw
	} else {
		update.LatestAlt = fw
	}
	report.Latest = append(report.Latest, RetiredLatestFile{File: fe, Compressed: compressed, Content: content, Written: true})
	return nil
}

// recheckRetired handles a retire request for a catalog whose entry is
// already a tombstone. The entry is never signed again. What it does do is
// look at the "latest" files the blob store holds: one that already
// carries retiredAt is reported so the caller can verify what is being
// served; one that does not (an earlier release wrote the tombstone under
// the other name, or left a file behind) gets its final write now, with
// the retiredAt of the tombstone entry.
func (p Params) recheckRetired(ctx context.Context, id string, prior store.CatalogState, fileNextUpdate time.Time, result *Result) error {
	log := p.logger()
	report := RetireReport{CatalogID: id, EntryVersion: prior.EntryVersion, RetiredAt: prior.RetiredAt, AlreadyRetired: true}
	update := store.CatalogUpdate{CatalogID: id, SignedEntry: prior.SignedEntry}

	// Content to rebuild a missing tombstone from: whatever readable
	// "latest" file there is (a retired state carries no catalog itself).
	source := newestReadableLatest(prior.StoredLatest)
	for _, st := range prior.StoredLatest {
		if st.Readable && st.RetiredAt != nil {
			fe, _, err := p.buildLatestFileEntry(id, st.Version, st.Content, st.Compressed)
			if err != nil {
				return err
			}
			fe.Size = st.ServedSize
			report.Latest = append(report.Latest, RetiredLatestFile{File: fe, Compressed: st.Compressed, Content: st.Content})
			continue
		}
		retiredAt, err := time.Parse(time.RFC3339Nano, prior.RetiredAt)
		if err != nil || source == nil || len(prior.SignedEntry) == 0 {
			log.WarnContext(ctx, "catalog/publisher: retired catalog has a latest file without retiredAt that cannot be rebuilt", "catalogId", id, "compressed", st.Compressed)
			result.Errors = append(result.Errors, PublishError{
				CatalogID: id, Stage: "retire",
				Reason: "a \"latest\" file of this retired catalog does not carry retiredAt and there is nothing to rebuild it from",
			})
			continue
		}
		version, cat := source.Version, source.Catalog
		if st.Readable && len(st.Catalog) > 0 {
			version, cat = st.Version, st.Catalog
		}
		content, err := p.signCatalogFile(id, version, fileNextUpdate, cat, &retiredAt)
		if err != nil {
			return fmt.Errorf("catalog/publisher: signing final \"latest\" tombstone %q: %w", id, err)
		}
		if err := p.addRetiredLatest(&update, &report, id, version, content, st.Compressed); err != nil {
			return err
		}
		log.InfoContext(ctx, "catalog/publisher: repaired a latest file of an already retired catalog", "catalogId", id, "compressed", st.Compressed)
	}
	if update.Latest != nil {
		result.Publish.Retirements = append(result.Publish.Retirements, update)
	} else {
		log.DebugContext(ctx, "catalog/publisher: catalog already retired, nothing to write", "catalogId", id, "entryVersion", prior.EntryVersion)
	}
	result.Retired = append(result.Retired, report)
	return nil
}

// fileEntryCompressed reports whether fe names a gzip file: its Encoding
// when set, else the URL's ".gz" suffix (the same rule a reader applies).
func fileEntryCompressed(fe catalog.FileEntry) bool {
	if fe.Encoding != "" {
		return fe.Encoding == "gzip"
	}
	return strings.HasSuffix(fe.URL, ".gz")
}

// publishOne decides baseline vs. change-file vs. metadata-only vs. no-op
// for one submission and builds both its internal outcome and its
// catalog.CatalogEntry. The returned entry is not yet signed -- Publish
// signs every entry uniformly via signEntry.
func (p Params) publishOne(ctx context.Context, sub Submission, prior store.CatalogState, fileNextUpdate time.Time) (outcome, catalog.CatalogEntry, error) {
	log := p.logger()
	hasPrior := prior.Catalog != nil
	catalogType := sub.CatalogType
	if catalogType == "" {
		catalogType = "REGULAR"
	}
	isActive := catalogIsActive(sub.Catalog)

	entry := catalog.CatalogEntry{
		CatalogID:    sub.CatalogID,
		CatalogType:  catalogType,
		Dependencies: dependenciesToWire(sub.Dependencies),
		NetworkIDs:   sub.NetworkIds,
		SchemaTypes:  sub.SchemaTypes,
		IsActive:     &isActive,
		CrawlHint:    sub.CrawlHint,
	}

	metadataChanged := !hasPrior ||
		prior.CatalogType != catalogType ||
		prior.IsActive != isActive ||
		!stringSlicesEqual(prior.NetworkIds, sub.NetworkIds) ||
		!stringSlicesEqual(prior.SchemaTypes, sub.SchemaTypes) ||
		!masterDependenciesEqual(prior.Dependencies, sub.Dependencies) ||
		prior.CrawlHint != sub.CrawlHint

	if hasPrior {
		if prior.BaselineFile != nil {
			entry.Baseline = *prior.BaselineFile
		}
		entry.Changes = prior.ChangeFiles
	}

	// diff/changeCatalog are only meaningful once hasPrior &&
	// !Directives.ForceBaseline (an unconditional baseline republish
	// below never needs them); computed here, ahead of the
	// baseline-vs-change decision, so an automatic compaction trigger
	// (below) can be gated on contentChanged -- it must never force a
	// baseline republish on a call that changed nothing at all
	// content-wise.
	forceBaseline := sub.Directives.ForceBaseline
	var diff catalog.CatalogDiff
	var changeCatalog json.RawMessage
	var attributesRemoved []string
	contentChanged := true
	if hasPrior && !forceBaseline {
		var err error
		diff, changeCatalog, err = catalog.Diff(prior.Catalog, sub.Catalog)
		if err != nil {
			return outcome{}, catalog.CatalogEntry{}, err
		}
		// A change file can set a top-level attribute but not remove one
		// (see catalog.RemovedAttributes). A submission that drops one is
		// a content change that only a baseline can publish.
		if attributesRemoved, err = catalog.RemovedAttributes(prior.Catalog, sub.Catalog); err != nil {
			return outcome{}, catalog.CatalogEntry{}, err
		}
		contentChanged = !diff.Resources.IsEmpty() || !diff.Offers.IsEmpty() || changeCatalog != nil || len(attributesRemoved) > 0
		log.DebugContext(ctx, "catalog/publisher: diffed against prior state", "catalogId", sub.CatalogID,
			"upserts", len(diff.Resources.Upserts)+len(diff.Offers.Upserts), "removals", len(diff.Resources.Removals)+len(diff.Offers.Removals),
			"attributesChanged", changeCatalog != nil, "contentChanged", contentChanged)
	}

	compactionDue := hasPrior && !forceBaseline && contentChanged && p.compactionDue(ctx, sub.CatalogID, prior)
	if !hasPrior || forceBaseline || compactionDue || len(attributesRemoved) > 0 {
		reason := "no prior state"
		switch {
		case forceBaseline:
			reason = "ForceBaseline"
		case len(attributesRemoved) > 0:
			reason = "catalog attributes removed (" + strings.Join(attributesRemoved, ", ") + "), which a change file cannot express"
		case compactionDue:
			reason = "compaction threshold reached"
		}
		log.DebugContext(ctx, "catalog/publisher: publishing baseline", "catalogId", sub.CatalogID, "reason", reason)
		version := currentVersion(prior) + 1
		content, err := p.signCatalogFile(sub.CatalogID, version, fileNextUpdate, sub.Catalog, nil)
		if err != nil {
			return outcome{}, catalog.CatalogEntry{}, err
		}
		fe, served, err := p.buildFileEntry(sub.CatalogID, version, "json", content)
		if err != nil {
			return outcome{}, catalog.CatalogEntry{}, err
		}
		entry.Baseline = fe
		// entry.Changes is deliberately left as whatever prior.ChangeFiles
		// carried (set above), not reset to nil: on a forced re-baseline
		// (compaction), NFH-014 CON-TBD-32 requires the change files that
		// led up to the new baseline to stay *listed* -- not merely
		// hosted -- for a grace period, so a DS mid-lineage can still
		// reach equivalent content by applying them instead of fetching
		// the baseline. Publish holds no timer of its own; how long to
		// keep passing them here is pkg/catalog/store's own retention
		// policy, applied when it reconstructed PriorState.
		entry.EntryVersion = prior.EntryVersion + 1

		oc := outcome{
			CatalogID: sub.CatalogID, Version: version, EntryVersion: entry.EntryVersion, Changed: true, Digest: fe.Digest, Mode: "baseline",
			Content: content, ServedContent: served,
		}
		return p.finishEntry(ctx, sub, prior, entry, oc, fileNextUpdate)
	}

	if !contentChanged {
		version := currentVersion(prior)
		if !metadataChanged {
			log.DebugContext(ctx, "catalog/publisher: no-op, nothing changed", "catalogId", sub.CatalogID)
			entry.EntryVersion = prior.EntryVersion
			oc := outcome{CatalogID: sub.CatalogID, Version: version, EntryVersion: entry.EntryVersion, Changed: false, Mode: "unchanged"}
			return p.finishEntry(ctx, sub, prior, entry, oc, fileNextUpdate)
		}
		log.DebugContext(ctx, "catalog/publisher: metadata-only change, no new file", "catalogId", sub.CatalogID, "entryVersion", prior.EntryVersion+1)
		entry.EntryVersion = prior.EntryVersion + 1
		oc := outcome{CatalogID: sub.CatalogID, Version: version, EntryVersion: entry.EntryVersion, Changed: true, Mode: "metadata"}
		return p.finishEntry(ctx, sub, prior, entry, oc, fileNextUpdate)
	}
	log.DebugContext(ctx, "catalog/publisher: publishing change file", "catalogId", sub.CatalogID, "fromVersion", currentVersion(prior))

	fromVersion := currentVersion(prior)
	toVersion := fromVersion + 1
	changeDoc := catalog.ChangeFileDoc{
		CatalogID: sub.CatalogID, FromVersion: fromVersion, ToVersion: toVersion, NextUpdate: fileNextUpdate,
		Resources: diff.Resources, Offers: diff.Offers, Catalog: changeCatalog,
	}
	content, err := p.signChangeFile(changeDoc)
	if err != nil {
		return outcome{}, catalog.CatalogEntry{}, err
	}

	fe, served, err := p.buildFileEntry(sub.CatalogID, toVersion, "changes.json", content)
	if err != nil {
		return outcome{}, catalog.CatalogEntry{}, err
	}
	entry.Changes = append(entry.Changes, catalog.FileEntry{
		FromVersion: fromVersion, ToVersion: toVersion, URL: fe.URL, Size: fe.Size, Digest: fe.Digest, Encoding: fe.Encoding,
	})
	entry.EntryVersion = prior.EntryVersion + 1

	oc := outcome{
		CatalogID: sub.CatalogID, Version: toVersion, EntryVersion: entry.EntryVersion, Changed: true, Digest: fe.Digest, Mode: "change",
		Content: content, ServedContent: served,
	}
	return p.finishEntry(ctx, sub, prior, entry, oc, fileNextUpdate)
}

// finishEntry settles the entry's "latest" reference, which every
// publishOne return path funnels through.
//
// "latest" is one file overwritten in place at a stable URL, so everything
// about it follows what was last published (prior), not this call's
// settings:
//   - Its name. Once a catalog's "latest" has been advertised under one
//     encoding it stays there; switching Gzip changes the baseline and
//     change files from then on, not the URL consumers already hold.
//   - Whether to write. A baseline or change rewrites it. An unchanged or
//     metadata-only outcome leaves an intact file alone and carries its
//     reference forward: the file's content is what it was, so its digest
//     must be too (NFH-014 10.1), and the entry must not change under an
//     unchanged entryVersion.
//   - entryVersion. If an otherwise unchanged call does change the
//     reference (PublishLatest switched on or off, the file was missing
//     or damaged and is rewritten, the public base URL moved) the entry
//     has changed; Publish sees that when it signs the entry and bumps
//     entryVersion (see the "unchanged" check there).
//
// With PublishLatest off the entry lists no "latest". A file published
// earlier stays where it is, unrefreshed; retiring the catalog still
// writes its final tombstone (see retireOne).
func (p Params) finishEntry(ctx context.Context, sub Submission, prior store.CatalogState, entry catalog.CatalogEntry, oc outcome, fileNextUpdate time.Time) (outcome, catalog.CatalogEntry, error) {
	log := p.logger()
	if p.PublishLatest {
		compressed := p.latestCompressed(prior)
		if kept := p.intactLatest(sub.CatalogID, prior, oc, compressed); kept != nil {
			entry.Latest = kept
			oc.LatestDigest = kept.Digest
			log.DebugContext(ctx, "catalog/publisher: latest file intact, left as published", "catalogId", sub.CatalogID, "version", oc.Version)
		} else {
			content, err := p.signCatalogFile(sub.CatalogID, oc.Version, fileNextUpdate, sub.Catalog, nil)
			if err != nil {
				return outcome{}, catalog.CatalogEntry{}, err
			}
			fe, served, err := p.buildLatestFileEntry(sub.CatalogID, oc.Version, content, compressed)
			if err != nil {
				return outcome{}, catalog.CatalogEntry{}, err
			}
			entry.Latest = &fe
			oc.LatestContent = content
			oc.LatestServedContent = served
			oc.LatestDigest = fe.Digest
			oc.LatestCompressed = compressed
			log.DebugContext(ctx, "catalog/publisher: latest pointer updated", "catalogId", sub.CatalogID, "version", oc.Version, "compressed", compressed)
		}
	}

	return oc, entry, nil
}

// latestCompressed decides which of the two "latest" names this catalog
// uses: the one its entry already advertises; failing that, a file left in
// the blob store from before PublishLatest was switched off (preferring the
// one matching Gzip if both exist); and only for a catalog with no "latest"
// anywhere, Gzip.
func (p Params) latestCompressed(prior store.CatalogState) bool {
	if prior.LatestFile != nil {
		return fileEntryCompressed(*prior.LatestFile)
	}
	if prior.Stored(p.Gzip) == nil && prior.Stored(!p.Gzip) != nil {
		return !p.Gzip
	}
	return p.Gzip
}

// intactLatest returns the prior entry's "latest" reference when this call
// publishes no new content and the file it points at is still exactly what
// that reference says: same name and URL as this call would use, current
// version, stored bytes whose digest matches, and signed with the key this
// call signs with (after a key rotation the one file that can be re-signed
// in place is, so it does not depend on the old key staying registered).
// Otherwise nil, and the file is rewritten.
func (p Params) intactLatest(catalogID string, prior store.CatalogState, oc outcome, compressed bool) *catalog.FileEntry {
	if oc.Content != nil || prior.LatestFile == nil {
		return nil
	}
	ref := *prior.LatestFile
	st := prior.Stored(compressed)
	if st == nil || !st.Readable || st.RetiredAt != nil || st.Version != oc.Version || ref.Version != oc.Version {
		return nil
	}
	want, _, err := p.buildLatestFileEntry(catalogID, oc.Version, st.Content, compressed)
	if err != nil || want.URL != ref.URL || want.Digest != ref.Digest || st.ServedSize != ref.Size {
		return nil
	}
	var doc catalog.CatalogFileDoc
	if json.Unmarshal(st.Content, &doc) != nil || doc.Signature.KeyID != p.KeyID {
		return nil
	}
	return &ref
}

// catalogIsActive reads the submitted Catalog's own pre-existing
// "isActive" field -- the entry's isActive mirrors catalog.isActive, it
// is never a second, independently-supplied input -- defaulting to true
// when absent, matching Catalog's public-by-default posture.
func catalogIsActive(cat json.RawMessage) bool {
	var probe struct {
		IsActive *bool `json:"isActive"`
	}
	if json.Unmarshal(cat, &probe) != nil || probe.IsActive == nil {
		return true
	}
	return *probe.IsActive
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func masterDependenciesEqual(a, b []catalog.MasterDependency) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func dependenciesToWire(deps []catalog.MasterDependency) *catalog.Dependencies {
	if len(deps) == 0 {
		return nil
	}
	return &catalog.Dependencies{Masters: deps}
}

// signCatalogFile builds the file spec's self-signed CatalogFileDoc
// (baseline/latest): {catalog, signature}. "Avoiding circular signing":
// the signing input is the JCS canonicalization of this document with
// "signature" itself removed, so the pre-signing marshal's placeholder
// Signature value never matters -- it's stripped before signing anyway.
// retiredAt is nil for an ordinary baseline or "latest" refresh; it is set
// only for the one-time final write to a retired catalog's "latest" URL.
func (p Params) signCatalogFile(catalogID string, version int64, nextUpdate time.Time, cat json.RawMessage, retiredAt *time.Time) ([]byte, error) {
	doc := catalog.CatalogFileDoc{CatalogID: catalogID, Version: version, NextUpdate: nextUpdate, Catalog: cat, RetiredAt: retiredAt}
	unsigned, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("catalog/publisher: marshaling catalog file: %w", err)
	}
	sigValue, err := artifactsigner.SignJSON(unsigned, "signature", p.SigningKey)
	if err != nil {
		return nil, fmt.Errorf("catalog/publisher: signing catalog file: %w", err)
	}
	doc.Signature = catalog.FileSignature{KeyID: p.KeyID, Canonicalization: "JCS", Value: sigValue}
	return json.Marshal(doc)
}

// signChangeFile is signCatalogFile's counterpart for a ChangeFileDoc:
// doc's own fields are signed with "signature" removed, then the
// signature is attached and the final document re-marshaled.
func (p Params) signChangeFile(doc catalog.ChangeFileDoc) ([]byte, error) {
	unsigned, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("catalog/publisher: marshaling change file: %w", err)
	}
	sigValue, err := artifactsigner.SignJSON(unsigned, "signature", p.SigningKey)
	if err != nil {
		return nil, fmt.Errorf("catalog/publisher: signing change file: %w", err)
	}
	doc.Signature = catalog.FileSignature{KeyID: p.KeyID, Canonicalization: "JCS", Value: sigValue}
	return json.Marshal(doc)
}

// signEntry self-signs a catalog-index entry as a whole: every other
// field of entry, JCS-canonicalized with "signature" removed. Applies
// uniformly to ACTIVE and RETIRED (tombstone) entries alike.
func (p Params) signEntry(entry catalog.CatalogEntry) (json.RawMessage, error) {
	unsigned, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("catalog/publisher: marshaling catalog entry: %w", err)
	}
	sigValue, err := artifactsigner.SignJSON(unsigned, "signature", p.SigningKey)
	if err != nil {
		return nil, fmt.Errorf("catalog/publisher: signing catalog entry: %w", err)
	}
	entry.Signature = catalog.EntrySignature{KeyID: p.KeyID, Value: sigValue}
	return json.Marshal(entry)
}

// buildFileEntry computes a versioned URL, digest, and size for one
// already self-signed catalog file (content -- see signCatalogFile/
// signChangeFile): digest is always computed over content as given (the
// canonical, decompressed bytes a reader must verify against) -- content
// is never mutated -- while the returned served bytes and Size reflect
// what's actually written to storage, gzip-compressed with a
// ".gz"-suffixed filename/URL when Gzip is on.
func (p Params) buildFileEntry(catalogID string, version int64, suffix string, content []byte) (catalog.FileEntry, []byte, error) {
	served, fileSuffix, encoding, err := maybeCompress(content, suffix, p.Gzip)
	if err != nil {
		return catalog.FileEntry{}, nil, err
	}
	filename := fmt.Sprintf("%s.v%d.%s", store.LocalName(catalogID), version, fileSuffix)
	url := p.catalogPartURL(filename, fileSuffix)
	return catalog.FileEntry{
		Version:  version,
		URL:      url,
		Size:     int64(len(served)),
		Digest:   "sha-256:" + digestOf(content),
		Encoding: encoding,
	}, served, nil
}

// buildLatestFileEntry is buildFileEntry's counterpart for the "latest"
// pointer: unlike a baseline/change file, its filename carries no version
// number at all -- it is the one file this package overwrites in place on
// every publish, explicitly exempt from the immutable-URL rule every
// other file here follows. compressed is the caller's decision, taken from
// what was last published (see finishEntry), not from Params.Gzip.
func (p Params) buildLatestFileEntry(catalogID string, version int64, content []byte, compressed bool) (catalog.FileEntry, []byte, error) {
	served, fileSuffix, encoding, err := maybeCompress(content, "json", compressed)
	if err != nil {
		return catalog.FileEntry{}, nil, err
	}
	filename := fmt.Sprintf("%s.latest.%s", store.LocalName(catalogID), fileSuffix)
	url := p.catalogPartURL(filename, fileSuffix)
	return catalog.FileEntry{
		Version:  version,
		URL:      url,
		Size:     int64(len(served)),
		Digest:   "sha-256:" + digestOf(content),
		Encoding: encoding,
	}, served, nil
}

// maybeCompress gzip-compresses content and appends ".gz" to suffix (plus
// reports "gzip" as the encoding) when gzipEnabled is true, or returns
// content/suffix/"" unchanged otherwise -- shared by buildFileEntry and
// buildLatestFileEntry so the served-bytes/filename/encoding convention
// stays identical for every kind of catalog file.
func maybeCompress(content []byte, suffix string, gzipEnabled bool) (served []byte, fileSuffix string, encoding string, err error) {
	if !gzipEnabled {
		return content, suffix, "", nil
	}
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(content); err != nil {
		return nil, "", "", fmt.Errorf("catalog/publisher: gzip-compressing catalog file: %w", err)
	}
	if err := gw.Close(); err != nil {
		return nil, "", "", fmt.Errorf("catalog/publisher: gzip-compressing catalog file: %w", err)
	}
	return buf.Bytes(), suffix + ".gz", "gzip", nil
}

// currentVersion returns a catalog's implicit current content-lineage
// version: the last change file's version, or the baseline's version if
// there are no change files yet. Zero if there is no prior state at all.
//
// Only "live" change files -- those published after the current baseline
// (EffectiveVersion > baseline's own Version) -- count. Once a catalog
// has been compacted, prior.ChangeFiles legitimately still lists
// superseded (pre-compaction) entries for the CON-TBD-32 grace period;
// their versions predate the current baseline and must never be mistaken
// for the catalog's current version.
func currentVersion(prior store.CatalogState) int64 {
	var baselineVersion int64
	if prior.BaselineFile != nil {
		baselineVersion = prior.BaselineFile.Version
	}
	if n := len(prior.ChangeFiles); n > 0 {
		if last := prior.ChangeFiles[n-1].EffectiveVersion(); last > baselineVersion {
			return last
		}
	}
	return baselineVersion
}

// compactionDue reports whether an automatic compaction trigger
// (Params.CompactionChangeCountThreshold/CompactionSizeRatioThreshold)
// fires for prior -- checked against prior.ChangeFiles as they stand
// before this call's own new change file would be added. Both thresholds
// default to 0 (disabled); either alone can trigger compaction. False
// whenever there's no baseline to compare against yet.
//
// Counts only "live" change files (EffectiveVersion > baseline's own
// Version), same rationale as currentVersion.
func (p Params) compactionDue(ctx context.Context, catalogID string, prior store.CatalogState) bool {
	var baselineVersion, baselineSize int64
	if prior.BaselineFile != nil {
		baselineVersion = prior.BaselineFile.Version
		baselineSize = prior.BaselineFile.Size
	}
	var liveCount int
	var liveSize int64
	for _, cf := range prior.ChangeFiles {
		if cf.EffectiveVersion() > baselineVersion {
			liveCount++
			liveSize += cf.Size
		}
	}
	if p.CompactionChangeCountThreshold > 0 && liveCount >= p.CompactionChangeCountThreshold {
		p.logger().DebugContext(ctx, "catalog/publisher: compaction triggered by change count", "catalogId", catalogID, "liveChangeFiles", liveCount, "threshold", p.CompactionChangeCountThreshold)
		return true
	}
	if p.CompactionSizeRatioThreshold > 0 && baselineSize > 0 {
		ratio := float64(liveSize) / float64(baselineSize)
		if ratio >= p.CompactionSizeRatioThreshold {
			p.logger().DebugContext(ctx, "catalog/publisher: compaction triggered by size ratio", "catalogId", catalogID, "ratio", ratio, "threshold", p.CompactionSizeRatioThreshold)
			return true
		}
	}
	return false
}

// validateSubmission applies a shallow structural check: a Beckn Catalog
// object has no context/message envelope, just {id, descriptor, ...}. It
// also checks the two inputs that decide the entry's lifecycle state and
// crawl order, so that a value that cannot be read is refused rather than
// silently replaced by a default: catalog.isActive must be a boolean when
// present (a "false" in quotes would otherwise publish a catalog meant to
// be paused as ACTIVE), and CatalogType must be REGULAR, MASTER or empty.
//
// Whether the catalogId itself may be used is decided separately, by admit,
// because that depends on what is already published.
func validateSubmission(sub Submission) error {
	if sub.CatalogID == "" {
		return fmt.Errorf("missing catalogId")
	}
	switch sub.CatalogType {
	case "", "REGULAR", "MASTER":
	default:
		return fmt.Errorf("invalid catalogType %q (must be \"REGULAR\" or \"MASTER\")", sub.CatalogType)
	}
	var c struct {
		ID         string          `json:"id"`
		Descriptor json.RawMessage `json:"descriptor"`
		IsActive   json.RawMessage `json:"isActive"`
	}
	if err := json.Unmarshal(sub.Catalog, &c); err != nil {
		return fmt.Errorf("invalid catalog JSON: %w", err)
	}
	if c.ID == "" || len(c.Descriptor) == 0 {
		return fmt.Errorf("catalog missing id/descriptor")
	}
	if len(c.IsActive) > 0 && string(c.IsActive) != "null" {
		var b bool
		if err := json.Unmarshal(c.IsActive, &b); err != nil {
			return fmt.Errorf("catalog isActive must be true or false, got %s", c.IsActive)
		}
	}
	return nil
}

func (p Params) catalogPartURL(filename, suffix string) string {
	if p.PublicBaseURL == "" {
		return "pending-artifact-store://catalog/" + filename
	}
	if strings.HasPrefix(suffix, "changes.json") { // "changes.json" or gzip's "changes.json.gz"
		return joinURL(p.PublicBaseURL, store.CatalogsDirName, store.ChangesDirName, filename)
	}
	return joinURL(p.PublicBaseURL, store.CatalogsDirName, filename)
}

// joinURL appends parts to base, trimming exactly one "/" between each
// segment regardless of how base/parts are themselves slashed.
func joinURL(base string, parts ...string) string {
	out := strings.TrimRight(base, "/")
	for _, part := range parts {
		out += "/" + strings.Trim(part, "/")
	}
	return out
}

// digestOf returns the hex-encoded SHA-256 of body -- a plain hex digest,
// no prefix (the "sha-256:" prefix is added by the caller when building a
// FileEntry digest field, matching the file spec's convention).
func digestOf(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
