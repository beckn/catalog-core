package publisher

// latest_state_test.go covers the single transitions the lifecycle table
// exercises in bulk, plus states only an earlier release could have left
// behind (a tombstone under the wrong name, gzip bytes under a plain name).

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"strings"
	"testing"

	"github.com/beckn/catalog-core/pkg/catalog"
	"github.com/beckn/catalog-core/pkg/catalog/store"
)

func gz(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write(b)
	zw.Close()
	return buf.Bytes()
}

func (w *lifecycleWorld) indexEntryRaw() []byte {
	w.t.Helper()
	var idx struct {
		Catalogs []json.RawMessage `json:"catalogs"`
	}
	if err := json.Unmarshal(w.blobs[store.IndexPath()], &idx); err != nil || len(idx.Catalogs) != 1 {
		w.t.Fatalf("index: err=%v entries=%d", err, len(idx.Catalogs))
	}
	return idx.Catalogs[0]
}

func retire() Submission {
	return Submission{CatalogID: lifecycleID, Directives: PublishDirectives{Retire: true}}
}

// An unchanged republish must leave an intact "latest" exactly as it is:
// nothing written, the entry byte-identical.
func TestLatest_UnchangedRepublishLeavesIntactFileAlone(t *testing.T) {
	w := newLifecycleWorld(t, true, true)
	w.call(w.submission())
	before, fileBefore := w.indexEntryRaw(), w.blobs[store.LatestFilePath(lifecycleID, true)]

	res := w.call(w.submission())
	if u := res.Publish.Updates[0]; u.Latest != nil {
		t.Errorf("unchanged republish rewrote latest")
	}
	if r := res.Reports[0]; r.Mode != "unchanged" || r.Changed || r.LatestDigest == "" || r.LatestContent != nil {
		t.Errorf("report = %+v, want unchanged with the kept digest and no latest content", r)
	}
	if !bytes.Equal(before, w.indexEntryRaw()) {
		t.Errorf("entry changed:\n before %s\n after  %s", before, w.indexEntryRaw())
	}
	if !bytes.Equal(fileBefore, w.blobs[store.LatestFilePath(lifecycleID, true)]) {
		t.Errorf("latest file bytes changed")
	}
}

// A "latest" file that is missing or no longer matches its reference is
// rewritten even though the content is unchanged, and because that changes
// the entry, entryVersion is bumped.
func TestLatest_UnchangedRepublishRepairsDamagedFile(t *testing.T) {
	for _, damage := range []string{"missing", "altered", "wrong encoding", "retired"} {
		t.Run(damage, func(t *testing.T) {
			w := newLifecycleWorld(t, true, true)
			w.call(w.submission())
			path := store.LatestFilePath(lifecycleID, true)
			switch damage {
			case "missing":
				delete(w.blobs, path)
			case "altered":
				w.blobs[path] = gz(t, []byte(`{"catalogId":"`+lifecycleID+`","version":1,"catalog":{"id":"X"}}`))
			case "wrong encoding":
				w.blobs[path] = []byte(`{"catalogId":"` + lifecycleID + `","version":1}`)
			case "retired":
				w.blobs[path] = gz(t, []byte(`{"catalogId":"`+lifecycleID+`","version":1,"catalog":{"id":"X"},"retiredAt":"2026-01-01T00:00:00Z"}`))
			}
			w.step = damage
			res := w.call(w.submission())
			if res.Publish.Updates[0].Latest == nil {
				t.Fatalf("damaged latest was not rewritten")
			}
			if r := res.Reports[0]; r.Mode != "metadata" || !r.Changed || r.EntryVersion != 2 {
				t.Errorf("report = mode %q changed %v entryVersion %d, want metadata/true/2", r.Mode, r.Changed, r.EntryVersion)
			}
			w.check()
		})
	}
}

// Gzip switched between publishes: the "latest" keeps the name it was
// advertised under; baseline and change files follow the new setting.
func TestLatest_KeepsItsNameWhenGzipChanges(t *testing.T) {
	for _, start := range []bool{true, false} {
		w := newLifecycleWorld(t, start, true)
		w.call(w.submission())
		w.gzip = !start
		w.items = append(w.items, "ITEM-2")
		res := w.call(w.submission())
		u := res.Publish.Updates[0]
		if u.Latest == nil || u.Latest.Compressed != start {
			t.Fatalf("start gzip=%v: latest write = %+v, want it under the original encoding", start, u.Latest)
		}
		if u.Change == nil || u.Change.Compressed == start {
			t.Errorf("start gzip=%v: change file did not follow the new gzip setting", start)
		}
		if _, other := w.blobs[store.LatestFilePath(lifecycleID, !start)]; other {
			t.Errorf("start gzip=%v: a second latest file appeared under the other name", start)
		}
		var entry catalog.CatalogEntry
		json.Unmarshal(w.indexEntryRaw(), &entry)
		if strings.HasSuffix(entry.Latest.URL, ".gz") != start {
			t.Errorf("start gzip=%v: latest url moved to %s", start, entry.Latest.URL)
		}
	}
}

// The first retire reports every latest file it writes; a repeat retire
// signs nothing, writes nothing, and reports the same files for the caller
// to verify.
func TestRetire_ReportsLatestFilesOnFirstAndRepeat(t *testing.T) {
	w := newLifecycleWorld(t, true, true)
	w.call(w.submission())
	first := w.call(retire())
	if len(first.Retired) != 1 || first.Retired[0].AlreadyRetired || len(first.Retired[0].Latest) != 1 || !first.Retired[0].Latest[0].Written {
		t.Fatalf("first retire report = %+v", first.Retired)
	}
	entry := w.indexEntryRaw()

	second := w.call(retire())
	if len(second.Publish.Retirements) != 0 {
		t.Errorf("repeat retire wrote something: %+v", second.Publish.Retirements)
	}
	if len(second.Retired) != 1 {
		t.Fatalf("repeat retire report = %+v", second.Retired)
	}
	rep, want := second.Retired[0], first.Retired[0]
	if !rep.AlreadyRetired || rep.RetiredAt != want.RetiredAt || rep.EntryVersion != want.EntryVersion {
		t.Errorf("repeat report = %+v, want AlreadyRetired with the first retire's retiredAt and entryVersion", rep)
	}
	if len(rep.Latest) != 1 || rep.Latest[0].Written || rep.Latest[0].File != want.Latest[0].File || !bytes.Equal(rep.Latest[0].Content, want.Latest[0].Content) {
		t.Errorf("repeat report latest = %+v\nwant the file the first retire wrote: %+v", rep.Latest, want.Latest)
	}
	if !bytes.Equal(entry, w.indexEntryRaw()) {
		t.Errorf("repeat retire changed the tombstone entry")
	}
}

// A retire done by a release that named the tombstone from the current gzip
// setting (onix #944) left the advertised file serving the live catalog. A
// repeat retire repairs it, with the tombstone entry's own retiredAt, and
// leaves the entry untouched.
func TestRetire_RepeatRepairsLatestLeftLiveByAnEarlierRelease(t *testing.T) {
	w := newLifecycleWorld(t, true, true)
	w.call(w.submission())
	live := append([]byte{}, w.blobs[store.LatestFilePath(lifecycleID, true)]...)
	w.call(retire())
	// Recreate what the old code left: tombstone under the plain name only.
	w.blobs[store.LatestFilePath(lifecycleID, false)] = func() []byte {
		zr, _ := gzip.NewReader(bytes.NewReader(w.blobs[store.LatestFilePath(lifecycleID, true)]))
		var out bytes.Buffer
		out.ReadFrom(zr)
		return out.Bytes()
	}()
	w.blobs[store.LatestFilePath(lifecycleID, true)] = live
	w.everLatest[w.srv.URL+"/"+store.LatestFilePath(lifecycleID, true)] = true
	w.everLatest[w.srv.URL+"/"+store.LatestFilePath(lifecycleID, false)] = true
	entry := w.indexEntryRaw()

	w.step = "repeat retire"
	res := w.call(retire())
	if len(res.Publish.Retirements) != 1 || res.Publish.Retirements[0].Latest == nil || !res.Publish.Retirements[0].Latest.Compressed {
		t.Fatalf("repeat retire did not rewrite the live .gz file: %+v", res.Publish.Retirements)
	}
	if !bytes.Equal(res.Publish.Retirements[0].SignedEntry, entry) || !bytes.Equal(entry, w.indexEntryRaw()) {
		t.Errorf("tombstone entry was re-signed or changed")
	}
	if n := len(res.Retired[0].Latest); n != 2 {
		t.Errorf("report lists %d latest files, want both", n)
	}
	w.prevEntryRaw = nil
	w.check() // both URLs now carry the entry's retiredAt
}

// An earlier release (onix #946) could leave gzip bytes under the plain
// "latest" name. Retiring writes a proper tombstone there too.
func TestRetire_TombstonesEveryStoredLatestIncludingUnservableOnes(t *testing.T) {
	w := newLifecycleWorld(t, true, true)
	w.call(w.submission())
	w.blobs[store.LatestFilePath(lifecycleID, false)] = append([]byte{}, w.blobs[store.LatestFilePath(lifecycleID, true)]...)
	w.everLatest[w.srv.URL+"/"+store.LatestFilePath(lifecycleID, true)] = true
	w.everLatest[w.srv.URL+"/"+store.LatestFilePath(lifecycleID, false)] = true

	w.step = "retire"
	res := w.call(retire())
	u := res.Publish.Retirements[0]
	if u.Latest == nil || u.LatestAlt == nil || u.Latest.Compressed == u.LatestAlt.Compressed || !u.Latest.Compressed {
		t.Fatalf("retire writes = %+v / %+v, want the advertised .gz first and the plain name second", u.Latest, u.LatestAlt)
	}
	w.check()
}

// PublishLatest switched off, a publish, then a retire: the entry no longer
// lists latest, but the file is still there for whoever holds its URL.
func TestRetire_TombstonesLatestLeftBehindWhenPublishLatestWasSwitchedOff(t *testing.T) {
	w := newLifecycleWorld(t, false, true)
	w.run([]string{"publish", "latest", "change"})
	var entry catalog.CatalogEntry
	json.Unmarshal(w.indexEntryRaw(), &entry)
	if entry.Latest != nil {
		t.Fatalf("entry still lists latest with PublishLatest off")
	}
	res := w.call(retire())
	if len(res.Retired[0].Latest) != 1 || res.Publish.Retirements[0].Latest == nil {
		t.Fatalf("retire did not write the final latest: %+v", res.Retired)
	}
	w.step = "retire"
	w.check()
}

// PublishLatest switched back on resumes at the URL consumers already
// hold, whatever Gzip is now.
func TestLatest_ResumesAtTheExistingNameWhenSwitchedBackOn(t *testing.T) {
	w := newLifecycleWorld(t, false, true)
	w.run([]string{"publish", "latest", "change", "gzip", "latest", "change"})
	var entry catalog.CatalogEntry
	json.Unmarshal(w.indexEntryRaw(), &entry)
	if entry.Latest == nil || strings.HasSuffix(entry.Latest.URL, ".gz") {
		t.Errorf("latest = %+v, want it back at the original plain name", entry.Latest)
	}
}
