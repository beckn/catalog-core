package catalog

// change.go — change detection: compares a catalog's index entry against a
// caller's stored cursors and yields a typed Decision (sync / skip_unchanged
// / retire / rollback). Pure, no I/O. Ported from the catalog-crawler
// prototype's own change.go.

// Action is what a caller should do with a catalog on one poll.
type Action string

const (
	ActionSync          Action = "sync"           // entryVersion (or content) advanced; enqueue a sync
	ActionSkipUnchanged Action = "skip_unchanged" // nothing to do: cursor already at latest, or already retired
	ActionRetire        Action = "retire"         // catalog carries retiredAt
	ActionRollback      Action = "rollback"       // entryVersion or content version went backwards; flag, don't apply
	ActionRelisted      Action = "relisted"       // a live entry for a catalog already retired; flag, don't apply
)

// Decision is the outcome of change detection for one catalog.
type Decision struct {
	Action       Action
	ToVersion    int64 // content-lineage target (CatalogEntry.LatestVersion())
	EntryVersion int64 // the entry-level cursor to persist once this decision is acted on
}

// CursorState is what a caller remembers about one catalog, as Decide
// needs it. The zero value means "never synced".
type CursorState struct {
	Seen           bool  // the caller has a stored cursor for this catalog
	Retired        bool  // the caller has already acted on this catalog's retirement
	EntryVersion   int64 // last-persisted entry-level cursor
	ContentVersion int64 // last-persisted content-lineage cursor
}

// DetectChange is Decide for a caller that does not track retirement: it
// compares entry against the stored entry-level and content-lineage
// cursors. seen=false means the caller has never synced this catalog.
// Prefer Decide -- without CursorState.Retired a caller cannot refuse a
// catalogId that is listed again after it was retired.
func DetectChange(entry CatalogEntry, entryCursor, contentCursor int64, seen bool) Decision {
	return Decide(entry, CursorState{Seen: seen, EntryVersion: entryCursor, ContentVersion: contentCursor})
}

// Decide compares a verified index entry against a caller's stored state
// and says what to do with it. Pure, no I/O. The order of the checks is the
// contract:
//
//  1. Retirement is terminal on the reader's side (CON-TBD-05/-26: a
//     publisher must never unset retiredAt and must never list a retired
//     catalogId again). Once the caller has retired a catalog, another
//     tombstone for it is ActionSkipUnchanged whatever its entryVersion or
//     retiredAt, and a live entry is ActionRelisted -- never applied, since
//     applying it would serve again something its publisher declared
//     withdrawn.
//
//  2. A tombstone always retires (CON-TBD-13), BEFORE any version
//     comparison. A tombstone carries no content, so the rule that a
//     regression must not be applied (CON-TBD-11) has nothing to protect
//     here; and because retirement is one-way, a validly signed tombstone
//     can never be stale -- replaying an old one only repeats a fact that is
//     still true. Publishers in the field do write tombstones whose
//     entryVersion is below what they published before (a repeated retire
//     restarted it at 1); treating that as a rollback left the retired
//     catalog served indefinitely.
//
//  3. Only then are the two cursors checked, independently (CON-TBD-11
//     covers a regression in EITHER one): entryVersion (bumps on ANY change
//     to the entry, content or metadata) is the cheap first check; the
//     content-lineage version is what decides how much content actually
//     needs resolving. A regression blocks applying content and blocks
//     moving the cursor; it does not block retirement (above), and it does
//     not remove what was already applied. A metadata-only change (e.g.
//     networkIds edited, no new file) bumps entryVersion with the content
//     version unchanged -- still ActionSync, so the entry is re-evaluated,
//     but a caller's own fold produces an empty delta range for it.
//
// The entry must already have passed signature verification: Decide acts
// on retiredAt as a fact.
func Decide(entry CatalogEntry, c CursorState) Decision {
	if c.Seen && c.Retired {
		if entry.IsRetired() {
			return Decision{Action: ActionSkipUnchanged, ToVersion: c.ContentVersion, EntryVersion: c.EntryVersion}
		}
		return Decision{Action: ActionRelisted, ToVersion: entry.LatestVersion(), EntryVersion: entry.EntryVersion}
	}
	if entry.IsRetired() {
		return Decision{Action: ActionRetire, EntryVersion: entry.EntryVersion}
	}
	latest := entry.LatestVersion()
	switch {
	case !c.Seen:
		return Decision{Action: ActionSync, ToVersion: latest, EntryVersion: entry.EntryVersion}
	case entry.EntryVersion < c.EntryVersion, latest < c.ContentVersion:
		return Decision{Action: ActionRollback, ToVersion: latest, EntryVersion: entry.EntryVersion}
	case entry.EntryVersion == c.EntryVersion:
		return Decision{Action: ActionSkipUnchanged, ToVersion: c.ContentVersion, EntryVersion: c.EntryVersion}
	default:
		return Decision{Action: ActionSync, ToVersion: latest, EntryVersion: entry.EntryVersion}
	}
}
