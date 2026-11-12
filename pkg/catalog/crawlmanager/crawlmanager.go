package crawlmanager

// crawlmanager.go — the two orchestration entry points: PollIndexes (find
// what changed) and SyncNext (resolve and hand off one changed catalog).
// A caller (a scheduler, an onix plugin, a CLI) drives these on whatever
// cadence/concurrency it wants; nothing here owns a scheduler loop itself.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/beckn/catalog-core/pkg/catalog"
	"github.com/beckn/catalog-core/pkg/catalog/crawler"
)

// DefaultRetryDelay is how long SyncNext waits before a transiently-failed
// item is eligible for another claim, when Params.RetryDelay is unset.
const DefaultRetryDelay = time.Minute

// DefaultMaxParkCount is how many RequeueOrAbandonParked sweeps a parked
// catalog gets before being abandoned, when Params.MaxParkCount is unset.
const DefaultMaxParkCount = 12

// FaultRollback and FaultRelisted classify an index entry SyncNext refuses
// to apply: one whose entryVersion or content version is behind the stored
// cursor (CON-TBD-11), and a live entry for a catalog already retired
// (CON-TBD-26). Both are publisher-side conditions that retrying the same
// entry does not fix, so both are permanent.
const (
	FaultRollback crawler.FaultClass = "rollback"
	FaultRelisted crawler.FaultClass = "relisted"
)

// Params are CrawlManager's injected collaborators.
type Params struct {
	Fetcher *catalog.Fetcher // fixed -- the one correct fetch/verify/decode
	Source  Source           // swappable discovery
	Sink    Sink             // swappable handoff
	Store   Store            // concrete for now
	// Networks is this deployment's own configured network scope, checked
	// against catalog.ResolveScope for every ActionSync decision -- a
	// network-scoped catalog whose networks don't intersect this list is not
	// enqueued at all. Empty means "no networks configured": only public
	// (unscoped) catalogs are taken. Retirement is unconditional regardless
	// of scope -- a catalog this deployment previously carried is wiped when
	// it retires even if Networks has since changed.
	Networks   []string
	RetryDelay time.Duration // 0 => DefaultRetryDelay
	// MaxAttempts caps how many times a transiently-failing item is
	// rescheduled before SyncNext gives up and parks it instead -- without
	// this, a permanently-unreachable-but-technically-transient host (DNS
	// gone, host decommissioned) retries forever. 0 => unlimited (no cap).
	MaxAttempts int
	// MaxParkCount caps how many times RequeueOrAbandonParked will revive a
	// given parked catalog before abandoning it instead -- the wrapper
	// service configures this once here rather than passing it at every
	// RequeueOrAbandonParked call. 0 => DefaultMaxParkCount. Use the
	// package-level DeriveMaxParkCount helper to compute this from the
	// cadence the wrapper actually runs RequeueOrAbandonParked at and the
	// total retry budget it wants to give a parked catalog (e.g.
	// DeriveMaxParkCount(5*time.Minute, 4*time.Hour) for a 5-minute sweep
	// cadence and a 4-hour budget).
	MaxParkCount int
	// ResolveBppURI resolves a publishing node's own registered URL, for
	// backstopping catalog.bppUri on resolved content that doesn't already
	// carry it (catalog.StampIdentity) -- distinct from key resolution
	// (Fetcher's own KeySource), which resolves a signing key, not a URL.
	// Optional: nil simply skips bppUri stamping (bppId is still stamped --
	// it costs no lookup, Fetcher already knows the publishing node's id).
	ResolveBppURI func(ctx context.Context, nodeID, keyID string) (string, error)
	Now           func() time.Time // 0 => time.Now; injected for tests
	Log           *slog.Logger     // nil => slog.Default()
}

func (p Params) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p Params) log() *slog.Logger {
	if p.Log != nil {
		return p.Log
	}
	return slog.Default()
}

func (p Params) retryDelay() time.Duration {
	if p.RetryDelay > 0 {
		return p.RetryDelay
	}
	return DefaultRetryDelay
}

func (p Params) maxParkCount() int {
	if p.MaxParkCount > 0 {
		return p.MaxParkCount
	}
	return DefaultMaxParkCount
}

// DeriveMaxParkCount computes a value for Params.MaxParkCount, given the
// cadence a caller runs RequeueOrAbandonParked at (sweepInterval) and how
// long they want to keep retrying a parked catalog in total
// (maxRetryDuration) -- e.g. a 5-minute sweep cadence and a 4-hour total
// retry budget yields 48. Named Derive-, distinctly from Params.MaxParkCount
// itself, so a call site (crawlmanager.DeriveMaxParkCount(...)) can't be
// misread as a struct literal for the field it's meant to feed.
//
// The returned threshold is compared with >= against a catalog's
// cumulative park count (see Store.RequeueOrAbandonParked), so the catalog
// is actually revived maxParkCount-1 times before the maxParkCount'th park
// abandons it outright -- a return value of 1 means no revivals at all,
// abandoning on the very first park. Always at least 1, so a
// misconfigured (zero or negative) sweepInterval still abandons eventually
// rather than never.
func DeriveMaxParkCount(sweepInterval, maxRetryDuration time.Duration) int {
	if sweepInterval <= 0 {
		return 1
	}
	if n := int(maxRetryDuration / sweepInterval); n > 1 {
		return n
	}
	return 1
}

// PollIndexes discovers index URLs via Source, conditionally fetches each
// (using Store's stored ETag/Last-Modified so an unchanged index costs a
// 304, not a full re-parse), and enqueues into Store any catalog entry
// whose declared version has advanced past its stored cursor.
//
// One index failing to poll is logged and does not stop the others -- a
// single unreachable or misbehaving publisher must not hide every healthy
// one behind it.
func (p Params) PollIndexes(ctx context.Context) error {
	refs, err := p.Source.Discover(ctx)
	if err != nil {
		return fmt.Errorf("crawlmanager: discovering indexes: %w", err)
	}
	p.log().InfoContext(ctx, "crawlmanager: poll tick starting", "indexesDiscovered", len(refs))
	for _, ref := range refs {
		if err := p.pollOne(ctx, ref); err != nil {
			p.log().ErrorContext(ctx, "crawlmanager: polling index failed", "indexUrl", ref.IndexURL, "error", err)
		}
	}
	return nil
}

func (p Params) pollOne(ctx context.Context, ref IndexRef) error {
	cond := catalog.IndexConditions{}
	if prior, err := p.Store.GetIndexCursor(ctx, ref.IndexURL); err != nil {
		return fmt.Errorf("loading index cursor for %s: %w", ref.IndexURL, err)
	} else if prior != nil {
		cond = catalog.IndexConditions{ETag: prior.ETag, LastModified: prior.LastModified}
	}

	p.log().InfoContext(ctx, "crawlmanager: fetching index", "indexUrl", ref.IndexURL, "participantId", ref.ParticipantID)
	res, err := p.Fetcher.FetchIndex(ctx, ref.IndexURL, cond)
	if err != nil {
		return fmt.Errorf("fetching index %s: %w", ref.IndexURL, err)
	}
	// incomplete: this poll did not get to a decision for every entry the
	// index lists, for a reason that may clear without the index changing.
	// The validators are then NOT stored, so the next poll reads the index
	// again instead of taking a 304 and skipping those entries until the
	// publisher happens to publish something.
	incomplete := false
	for _, dropped := range res.Dropped {
		p.log().WarnContext(ctx, "crawlmanager: dropped unverifiable index entry", "indexUrl", ref.IndexURL, "catalogId", dropped.CatalogID, "reason", dropped.Reason, "transient", dropped.Transient)
		if dropped.Transient {
			incomplete = true
		}
	}
	if res.NotModified {
		p.log().InfoContext(ctx, "crawlmanager: index unchanged since last poll", "indexUrl", ref.IndexURL)
		return nil
	}
	p.log().InfoContext(ctx, "crawlmanager: fetched index", "indexUrl", ref.IndexURL, "nodeId", res.Index.NodeID, "catalogs", len(res.Index.Catalogs), "dropped", len(res.Dropped))

	// One entry whose decision fails (a store error) does not stop the rest
	// of this index from being examined, any more than one index failing
	// stops the other indexes.
	var failed []error
	for _, entry := range res.Index.Catalogs {
		if err := p.decideCatalog(ctx, ref, entry); err != nil {
			failed = append(failed, fmt.Errorf("deciding %s: %w", entry.CatalogID, err))
		}
	}
	cursor := IndexCursor{IndexURL: ref.IndexURL, ParticipantID: ref.ParticipantID, ETag: res.ETag, LastModified: res.LastModified}
	if incomplete || len(failed) > 0 {
		cursor.ETag, cursor.LastModified = "", ""
	}
	if err := p.Store.UpsertIndexCursor(ctx, cursor); err != nil {
		failed = append(failed, err)
	}
	return errors.Join(failed...)
}

// cursorState adapts a stored cursor to catalog.Decide's input.
func cursorState(c CatalogCursor, seen bool) catalog.CursorState {
	return catalog.CursorState{Seen: seen, Retired: seen && c.Retired, EntryVersion: c.EntryVersion, ContentVersion: c.Version}
}

// flag makes a refused entry visible beyond the log: it is recorded as the
// catalog's latest failure reason, which a Store surfaces to an operator
// (CON-TBD-11: a regression MUST be flagged). The cursor is not touched.
func (p Params) flag(ctx context.Context, ref IndexRef, catalogID, reason string) {
	report := PassReport{CatalogID: catalogID, IndexURL: ref.IndexURL, ParticipantID: ref.ParticipantID, Error: reason, At: p.now()}
	if err := p.Store.RecordFailure(ctx, report); err != nil {
		p.log().ErrorContext(ctx, "crawlmanager: recording refused entry failed", "catalogId", catalogID, "error", err)
	}
}

func rollbackReason(entry catalog.CatalogEntry, cursor CatalogCursor) string {
	return fmt.Sprintf("crawlmanager: possible rollback, not applied: index declares entryVersion=%d version=%d, already synced entryVersion=%d version=%d",
		entry.EntryVersion, entry.LatestVersion(), cursor.EntryVersion, cursor.Version)
}

func relistedReason(entry catalog.CatalogEntry) string {
	return fmt.Sprintf("crawlmanager: catalog is listed again (entryVersion=%d) after it was retired, not applied: a retired catalogId cannot be reused",
		entry.EntryVersion)
}

// decideCatalog runs catalog.Decide against entry's stored cursor and acts
// on the result: ActionSync enqueues only if entry is in this deployment's
// network scope (catalog.ResolveScope); ActionRetire enqueues
// unconditionally (regardless of scope, and regardless of the tombstone's
// own entryVersion -- see catalog.Decide) but only if this catalog was ever
// seen -- nothing to retire otherwise; ActionRollback and ActionRelisted are
// flagged for an operator and NOT enqueued (CON-TBD-11: a regression is a
// publisher-side data problem, not something to silently apply; CON-TBD-26:
// a retired catalogId is not offered again); ActionSkipUnchanged does
// nothing.
func (p Params) decideCatalog(ctx context.Context, ref IndexRef, entry catalog.CatalogEntry) error {
	cursor, seen, err := p.Store.GetCatalogCursor(ctx, entry.CatalogID)
	if err != nil {
		return fmt.Errorf("loading cursor: %w", err)
	}
	d := catalog.Decide(entry, cursorState(cursor, seen))
	switch d.Action {
	case catalog.ActionSync:
		if take, _ := catalog.ResolveScope(entry, p.Networks); !take {
			p.log().DebugContext(ctx, "crawlmanager: catalog out of network scope, not enqueued", "catalogId", entry.CatalogID, "networkIds", entry.NetworkIDs)
			return nil
		}
		if err := p.Store.Enqueue(ctx, QueueItem{CatalogID: entry.CatalogID, IndexURL: ref.IndexURL, ParticipantID: ref.ParticipantID, Op: OpSync}); err != nil {
			return err
		}
		p.log().InfoContext(ctx, "crawlmanager: catalog changed, enqueued sync", "catalogId", entry.CatalogID, "toVersion", d.ToVersion, "entryVersion", d.EntryVersion, "everSynced", seen)
		return nil
	case catalog.ActionRetire:
		if !seen {
			// Never had it: nothing to retire -- unless its first sync is
			// still pending and about to deliver it (see PendingChecker).
			pc, ok := p.Store.(PendingChecker)
			if !ok {
				return nil
			}
			pending, err := pc.HasPending(ctx, entry.CatalogID)
			if err != nil {
				return fmt.Errorf("checking pending work: %w", err)
			}
			if !pending {
				return nil
			}
		}
		if err := p.Store.Enqueue(ctx, QueueItem{CatalogID: entry.CatalogID, IndexURL: ref.IndexURL, ParticipantID: ref.ParticipantID, Op: OpRetire}); err != nil {
			return err
		}
		p.log().InfoContext(ctx, "crawlmanager: catalog retired, enqueued wipe", "catalogId", entry.CatalogID)
		return nil
	case catalog.ActionRollback:
		p.log().WarnContext(ctx, "crawlmanager: rollback detected, not applying", "catalogId", entry.CatalogID,
			"storedVersion", cursor.Version, "declaredVersion", d.ToVersion, "storedEntryVersion", cursor.EntryVersion, "declaredEntryVersion", entry.EntryVersion)
		p.flag(ctx, ref, entry.CatalogID, rollbackReason(entry, cursor))
	case catalog.ActionRelisted:
		p.log().WarnContext(ctx, "crawlmanager: catalog listed again after retirement, not applying", "catalogId", entry.CatalogID, "declaredEntryVersion", entry.EntryVersion)
		p.flag(ctx, ref, entry.CatalogID, relistedReason(entry))
	case catalog.ActionSkipUnchanged:
		p.log().DebugContext(ctx, "crawlmanager: catalog unchanged", "catalogId", entry.CatalogID, "entryVersion", cursor.EntryVersion)
	}
	return nil
}

// SyncNext claims one queued item from Store, resolves that catalog's
// current content, and hands it to Sink. It settles the claim before
// returning: Complete on success, Reschedule on a transient failure, Park
// on a permanent one (crawler.IsPermanent) or once a transient one has
// exhausted MaxAttempts. Park is not the end of the line by itself --
// RequeueOrAbandonParked, run on its own separate cadence, is what later
// revives or abandons a parked item. Returns claimed=false when the queue
// was empty -- a caller loops on this to drain the queue.
func (p Params) SyncNext(ctx context.Context) (claimed bool, err error) {
	item, err := p.Store.ClaimNext(ctx)
	if err != nil {
		return false, fmt.Errorf("crawlmanager: claiming queue item: %w", err)
	}
	if item == nil {
		return false, nil
	}

	syncErr := p.syncOne(ctx, item)
	if syncErr == nil {
		return true, nil
	}

	report := PassReport{CatalogID: item.CatalogID, IndexURL: item.IndexURL, ParticipantID: item.ParticipantID, Error: syncErr.Error(), At: p.now()}
	if err := p.Store.RecordFailure(ctx, report); err != nil {
		p.log().ErrorContext(ctx, "crawlmanager: recording failure failed", "catalogId", item.CatalogID, "error", err)
	}

	// A permanent fault parks outright. A transient one also parks once it's
	// been retried MaxAttempts times -- past that point retrying is no
	// longer distinguishable from a permanent failure in practice (a host
	// that's been unreachable for MaxAttempts attempts in a row), and an
	// unlucky publisher shouldn't retry forever with no operator visibility.
	if crawler.IsPermanent(syncErr) || (p.MaxAttempts > 0 && item.Attempts+1 >= p.MaxAttempts) {
		p.log().ErrorContext(ctx, "crawlmanager: parking catalog", "catalogId", item.CatalogID, "attempts", item.Attempts+1, "error", syncErr)
		if err := p.Store.Park(ctx, item.ID, item.ClaimID); err != nil {
			return true, fmt.Errorf("crawlmanager: parking %s: %w", item.CatalogID, err)
		}
		return true, syncErr
	}
	p.log().WarnContext(ctx, "crawlmanager: rescheduling after transient error", "catalogId", item.CatalogID, "attempts", item.Attempts+1, "error", syncErr)
	if err := p.Store.Reschedule(ctx, item.ID, item.ClaimID, p.now().Add(p.retryDelay())); err != nil {
		return true, fmt.Errorf("crawlmanager: rescheduling %s: %w", item.CatalogID, err)
	}
	return true, syncErr
}

// RequeueOrAbandonParked drives Store.RequeueOrAbandonParked and logs the
// outcome, using Params.MaxParkCount (DefaultMaxParkCount if unset) as the
// abandon threshold. Run this on its own slow, independent cadence --
// separate from PollIndexes/SyncNext -- so a catalog that parked keeps
// getting another chance without needing its publisher to republish, while
// one that keeps failing past MaxParkCount stops retrying forever and
// becomes visible via ListAbandoned instead of silently retrying, or
// silently never retrying, forever.
func (p Params) RequeueOrAbandonParked(ctx context.Context, olderThan time.Duration) error {
	maxParkCount := p.maxParkCount()
	revived, abandoned, err := p.Store.RequeueOrAbandonParked(ctx, olderThan, maxParkCount)
	if err != nil {
		return fmt.Errorf("crawlmanager: requeuing parked catalogs: %w", err)
	}
	if revived > 0 || abandoned > 0 {
		p.log().InfoContext(ctx, "crawlmanager: parked catalog sweep", "revived", revived, "abandoned", abandoned, "olderThan", olderThan, "maxParkCount", maxParkCount)
	}
	return nil
}

// ListAbandoned returns every catalog RequeueOrAbandonParked has given up
// on, for an operator surface to review -- crawlmanager itself never reads
// this back or acts on it again.
func (p Params) ListAbandoned(ctx context.Context) ([]AbandonedCatalog, error) {
	return p.Store.ListAbandoned(ctx)
}

// syncOne dispatches a claimed item to its op: OpRetire wipes the catalog
// from Sink using its captured envelope (a retired index entry carries no
// content to re-fetch); OpSync (or unset, for a Store predating Op) resolves
// current content and pushes it.
func (p Params) syncOne(ctx context.Context, item *ClaimedItem) error {
	if item.Op == OpRetire {
		return p.retireOne(ctx, item)
	}
	return p.syncActive(ctx, item)
}

// syncActive re-fetches item's index (so it always acts on the catalog's
// current declared content, never a possibly-stale queued snapshot), locates
// the entry, resolves its baseline+changes via Fetcher.FetchFile and
// catalog.Resolve, hands the result to Sink, and settles the cursor --
// capturing the resolved content's own descriptor/provider envelope
// (catalog.ExtractEnvelope) so a later retirement can build a wipe push
// without needing to fetch anything (a retired entry drops every file
// reference by then).
func (p Params) syncActive(ctx context.Context, item *ClaimedItem) error {
	res, err := p.Fetcher.FetchIndex(ctx, item.IndexURL, catalog.IndexConditions{})
	if err != nil {
		return err
	}
	entry, ok := catalog.FindCatalog(res.Index, item.CatalogID)
	if !ok {
		return crawler.Permanentf("crawlmanager: %s no longer listed in index %s", item.CatalogID, item.IndexURL)
	}

	// The decision that queued this item was made against an earlier read
	// of the index, possibly long ago (a parked item revived by the sweep).
	// Decide again on the entry as it is NOW, so the checks PollIndexes
	// applies cannot be bypassed by timing: a sync never applies a
	// regressed or re-listed entry, and an entry retired in the meantime is
	// retired here instead of failing on a tombstone's missing files.
	cursor, seen, err := p.Store.GetCatalogCursor(ctx, item.CatalogID)
	if err != nil {
		return fmt.Errorf("crawlmanager: loading cursor for %s: %w", item.CatalogID, err)
	}
	if entry.IsRetired() {
		return p.retireOne(ctx, item)
	}
	switch catalog.Decide(entry, cursorState(cursor, seen)).Action {
	case catalog.ActionRollback:
		return crawler.PermanentFaultf(FaultRollback, "%s", rollbackReason(entry, cursor))
	case catalog.ActionRelisted:
		return crawler.PermanentFaultf(FaultRelisted, "%s", relistedReason(entry))
	}

	baselineContent, err := p.Fetcher.FetchFile(ctx, res.Index.NodeID, entry.CatalogID, entry.Baseline)
	if err != nil {
		return err
	}
	content, err := catalog.Resolve(baselineContent, entry.Baseline.Version, entry.Changes, func(ch catalog.FileEntry) ([]byte, error) {
		return p.Fetcher.FetchFile(ctx, res.Index.NodeID, entry.CatalogID, ch)
	}, nil)
	if err != nil {
		return err
	}
	content = p.stampIdentity(ctx, res.Index.NodeID, entry, content)

	outcome, err := p.Sink.Send(ctx, entry, content)
	if err != nil {
		return err
	}
	if !outcome.Accepted {
		return fmt.Errorf("crawlmanager: sink rejected %s: %s", entry.CatalogID, outcome.Reason)
	}

	descriptor, provider, err := catalog.ExtractEnvelope(content)
	if err != nil {
		p.log().WarnContext(ctx, "crawlmanager: extracting envelope failed, retirement won't be able to wipe this catalog's descriptor/provider", "catalogId", entry.CatalogID, "error", err)
	}

	// Complete both advances the cursor and settles the queue claim
	// atomically (Store's job to make that one transaction) -- a separate
	// UpsertCatalogCursor call here would let a crash between the two leave
	// the cursor advanced but the item still claimed, or vice versa.
	settled := CatalogCursor{
		CatalogID: entry.CatalogID, IndexURL: item.IndexURL, ParticipantID: item.ParticipantID,
		Version: entry.LatestVersion(), EntryVersion: entry.EntryVersion,
		Descriptor: descriptor, Provider: provider, CatalogType: entry.CatalogType,
		SyncedAt: p.now(),
	}
	if err := p.Store.Complete(ctx, item.ID, item.ClaimID, settled); err != nil {
		return err
	}
	p.log().InfoContext(ctx, "crawlmanager: catalog synced and pushed", "catalogId", entry.CatalogID, "version", settled.Version, "entryVersion", settled.EntryVersion, "contentBytes", len(content))
	return nil
}

// retireOne wipes a retired catalog from Sink: a push naming only id/
// descriptor/provider (no resources/offers container at all) replaces its
// content with nothing. The envelope comes from Store.GetCatalogEnvelope,
// not a fresh fetch -- a retired index entry drops every file reference
// (baseline/changes/latest), so there is nothing left in the index to fetch
// from by the time a retire reaches here.
//
// Settling as Retired only happens AFTER Sink acks the wipe (or after
// confirming there's nothing to wipe): a push failure returns an error the
// same way syncActive's does, so SyncNext routes it through the same
// park-vs-retry policy a normal sync failure gets, rather than marking the
// catalog retired while the destination still serves its last-known content.
// stampIdentity backstops content's bppId/bppUri (catalog.StampIdentity)
// using nodeID (always available -- no lookup cost) and, if ResolveBppURI is
// configured, that node's registered URL. Failures are logged and treated as
// "nothing to stamp" rather than failing the sync -- a missing bppUri is a
// degraded push, not a reason to park a healthy catalog.
func (p Params) stampIdentity(ctx context.Context, nodeID string, entry catalog.CatalogEntry, content []byte) []byte {
	var bppURI string
	if p.ResolveBppURI != nil {
		uri, err := p.ResolveBppURI(ctx, nodeID, entry.Signature.KeyID)
		if err != nil {
			p.log().WarnContext(ctx, "crawlmanager: resolving bppUri failed", "catalogId", entry.CatalogID, "error", err)
		} else {
			bppURI = uri
		}
	}
	stamped, err := catalog.StampIdentity(content, nodeID, bppURI)
	if err != nil {
		p.log().WarnContext(ctx, "crawlmanager: stamping bpp identity failed", "catalogId", entry.CatalogID, "error", err)
		return content
	}
	return stamped
}

func (p Params) retireOne(ctx context.Context, item *ClaimedItem) error {
	// The settled cursor keeps the version lineage and index URL it had: a
	// retired catalog's cursor is still what a later re-listing is judged
	// against, and is still what an operator reads.
	prior, seen, err := p.Store.GetCatalogCursor(ctx, item.CatalogID)
	if err != nil {
		return fmt.Errorf("crawlmanager: loading cursor for %s: %w", item.CatalogID, err)
	}
	retired := CatalogCursor{
		CatalogID: item.CatalogID, IndexURL: item.IndexURL, ParticipantID: item.ParticipantID,
		Version: prior.Version, EntryVersion: prior.EntryVersion, Retired: true, SyncedAt: p.now(),
	}
	if seen && prior.Retired {
		// Already wiped and settled (a second retire item for the same
		// catalog): settle the claim without pushing another wipe.
		return p.Store.Complete(ctx, item.ID, item.ClaimID, retired)
	}

	descriptor, provider, catalogType, participantID, ok, err := p.Store.GetCatalogEnvelope(ctx, item.CatalogID)
	if err != nil {
		return fmt.Errorf("crawlmanager: loading envelope for %s: %w", item.CatalogID, err)
	}
	if participantID != "" {
		retired.ParticipantID = participantID
	}
	if !ok {
		// No envelope was ever captured -- decideCatalog's !seen guard already
		// refuses to enqueue a retire for a catalog never synced, so this
		// shouldn't happen in practice, but defensively: nothing is known
		// about what Sink holds for this catalog, so there is nothing safe to
		// wipe. Settle as retired anyway rather than parking forever on a
		// catalog that was never actually delivered.
		if err := p.Store.Complete(ctx, item.ID, item.ClaimID, retired); err != nil {
			return err
		}
		p.log().InfoContext(ctx, "crawlmanager: catalog retired (nothing to wipe, no envelope was ever captured)", "catalogId", item.CatalogID)
		return nil
	}

	doc, err := catalog.BuildRetireDoc(item.CatalogID, descriptor, provider)
	if err != nil {
		return crawler.PermanentFaultf(crawler.FaultContentInvalid, "crawlmanager: building retire doc for %s: %v", item.CatalogID, err)
	}
	outcome, err := p.Sink.Send(ctx, catalog.CatalogEntry{CatalogID: item.CatalogID, CatalogType: catalogType}, doc)
	if err != nil {
		return err
	}
	if !outcome.Accepted {
		return fmt.Errorf("crawlmanager: sink rejected retiring %s: %s", item.CatalogID, outcome.Reason)
	}
	if err := p.Store.Complete(ctx, item.ID, item.ClaimID, retired); err != nil {
		return err
	}
	p.log().InfoContext(ctx, "crawlmanager: catalog retired and wiped", "catalogId", item.CatalogID)
	return nil
}
