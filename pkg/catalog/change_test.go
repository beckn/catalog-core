package catalog

import "testing"

func TestDetectChange_NeverSeenIsSync(t *testing.T) {
	entry := CatalogEntry{CatalogID: "p/c", EntryVersion: 1, Baseline: FileEntry{Version: 1}}
	d := DetectChange(entry, 0, 0, false)
	if d.Action != ActionSync || d.ToVersion != 1 || d.EntryVersion != 1 {
		t.Fatalf("d = %+v, want sync to v1", d)
	}
}

func TestDetectChange_UnchangedEntryVersionIsSkip(t *testing.T) {
	entry := CatalogEntry{CatalogID: "p/c", EntryVersion: 1, Baseline: FileEntry{Version: 1}}
	d := DetectChange(entry, 1, 1, true)
	if d.Action != ActionSkipUnchanged {
		t.Fatalf("d = %+v, want skip_unchanged", d)
	}
}

func TestDetectChange_AdvancedEntryVersionIsSync(t *testing.T) {
	entry := CatalogEntry{CatalogID: "p/c", EntryVersion: 2, Baseline: FileEntry{Version: 1}}
	d := DetectChange(entry, 1, 1, true)
	if d.Action != ActionSync || d.ToVersion != 1 {
		t.Fatalf("d = %+v, want sync (metadata-only change, content unchanged)", d)
	}
}

func TestDetectChange_RetiredEntryIsRetire(t *testing.T) {
	entry := CatalogEntry{CatalogID: "p/c", EntryVersion: 2, RetiredAt: "2026-01-01T00:00:00Z"}
	d := DetectChange(entry, 1, 1, true)
	if d.Action != ActionRetire || d.EntryVersion != 2 {
		t.Fatalf("d = %+v, want retire", d)
	}
}

func TestDetectChange_EntryVersionRegressionIsRollback(t *testing.T) {
	entry := CatalogEntry{CatalogID: "p/c", EntryVersion: 1, Baseline: FileEntry{Version: 1}}
	d := DetectChange(entry, 3, 1, true) // stored cursor is ahead of what the index now declares
	if d.Action != ActionRollback {
		t.Fatalf("d = %+v, want rollback (entryVersion regressed)", d)
	}
}

func TestDetectChange_ContentVersionRegressionIsRollback(t *testing.T) {
	entry := CatalogEntry{CatalogID: "p/c", EntryVersion: 5, Baseline: FileEntry{Version: 1}} // latest=1
	d := DetectChange(entry, 4, 3, true)                                                      // stored content cursor (3) is ahead of latest (1)
	if d.Action != ActionRollback {
		t.Fatalf("d = %+v, want rollback (content version regressed)", d)
	}
}

// A tombstone whose entryVersion is below the stored cursor is still a
// tombstone. Publishers at catalog-core v0.2.0 rewrite a repeated retire at
// entryVersion 1; reading that as a rollback left the catalog served.
func TestDetectChange_RegressedTombstoneIsRetireNotRollback(t *testing.T) {
	entry := CatalogEntry{CatalogID: "p/c", EntryVersion: 1, RetiredAt: "2026-01-01T00:00:00Z"}
	d := DetectChange(entry, 3, 2, true)
	if d.Action != ActionRetire {
		t.Fatalf("d = %+v, want retire", d)
	}
}

func TestDecide_Table(t *testing.T) {
	live := func(ev, v int64) CatalogEntry {
		return CatalogEntry{CatalogID: "p/c", EntryVersion: ev, Baseline: FileEntry{Version: v, URL: "u"}}
	}
	tomb := func(ev int64) CatalogEntry {
		return CatalogEntry{CatalogID: "p/c", EntryVersion: ev, RetiredAt: "2026-01-01T00:00:00Z"}
	}
	active := CursorState{Seen: true, EntryVersion: 3, ContentVersion: 3}
	retired := CursorState{Seen: true, Retired: true, EntryVersion: 3, ContentVersion: 3}
	for _, tc := range []struct {
		name  string
		entry CatalogEntry
		state CursorState
		want  Action
	}{
		{"never seen, live", live(1, 1), CursorState{}, ActionSync},
		{"never seen, tombstone", tomb(1), CursorState{}, ActionRetire},
		{"active, advanced", live(4, 4), active, ActionSync},
		{"active, metadata only", live(4, 3), active, ActionSync},
		{"active, unchanged", live(3, 3), active, ActionSkipUnchanged},
		{"active, entryVersion regressed", live(2, 3), active, ActionRollback},
		{"active, content regressed", live(4, 2), active, ActionRollback},
		{"active, tombstone advanced", tomb(4), active, ActionRetire},
		{"active, tombstone same version", tomb(3), active, ActionRetire},
		{"active, tombstone regressed", tomb(1), active, ActionRetire},
		{"retired, tombstone again", tomb(4), retired, ActionSkipUnchanged},
		{"retired, tombstone regressed", tomb(1), retired, ActionSkipUnchanged},
		{"retired, re-listed restarting at 1", live(1, 1), retired, ActionRelisted},
		{"retired, re-listed continuing", live(4, 1), retired, ActionRelisted},
		{"retired, re-listed far ahead", live(9, 9), retired, ActionRelisted},
	} {
		if got := Decide(tc.entry, tc.state).Action; got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}
