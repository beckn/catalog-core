package publisher

// bothstores_test.go runs the publisher's end-to-end tables -- the
// "latest" lifecycle, the identity rules, the retire and repair cases --
// a second time over a blob store with a conditional write. Store.Publish
// has two write paths, chosen by what the blob store can do, and the
// adapter's local blob store takes the conditional one. A rule that holds
// on one path only is a defect on the other.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/beckn/catalog-core/pkg/catalog/store"
)

// worldConditional selects the blob store kind newWorldStore hands out.
// Tests in this package do not run in parallel.
var worldConditional bool

// newWorldStore is how every test world in this package builds its Store.
func newWorldStore(blobs memBlobs) *store.Store {
	if worldConditional {
		return store.New(casBlobs{blobs})
	}
	return store.New(blobs)
}

// casBlobs is memBlobs with store.ConditionalBlobStore on top; the version
// of a blob is the hash of its content.
type casBlobs struct{ memBlobs }

func blobVersion(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (c casBlobs) GetVersioned(ctx context.Context, path string) ([]byte, string, error) {
	b, err := c.memBlobs.Get(ctx, path)
	if err != nil {
		return nil, "", err
	}
	return b, blobVersion(b), nil
}

func (c casBlobs) PutIfVersion(ctx context.Context, path string, content []byte, ifVersion string) (string, error) {
	current := ""
	if b, ok := c.memBlobs[path]; ok {
		current = blobVersion(b)
	}
	if current != ifVersion {
		return "", store.ErrPreconditionFailed
	}
	if err := c.memBlobs.Put(ctx, path, content); err != nil {
		return "", err
	}
	return blobVersion(content), nil
}

// TestOnConditionalStore repeats every test that goes through a test world
// with the conditional write path underneath.
func TestOnConditionalStore(t *testing.T) {
	worldConditional = true
	defer func() { worldConditional = false }()
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"Lifecycle_LatestStaysConsistent", TestLifecycle_LatestStaysConsistent},
		{"Latest_UnchangedRepublishLeavesIntactFileAlone", TestLatest_UnchangedRepublishLeavesIntactFileAlone},
		{"Latest_UnchangedRepublishRepairsDamagedFile", TestLatest_UnchangedRepublishRepairsDamagedFile},
		{"Latest_KeepsItsNameWhenGzipChanges", TestLatest_KeepsItsNameWhenGzipChanges},
		{"Retire_ReportsLatestFilesOnFirstAndRepeat", TestRetire_ReportsLatestFilesOnFirstAndRepeat},
		{"Retire_RepeatRepairsLatestLeftLiveByAnEarlierRelease", TestRetire_RepeatRepairsLatestLeftLiveByAnEarlierRelease},
		{"Retire_TombstonesEveryStoredLatestIncludingUnservableOnes", TestRetire_TombstonesEveryStoredLatestIncludingUnservableOnes},
		{"Retire_TombstonesLatestLeftBehindWhenPublishLatestWasSwitchedOff", TestRetire_TombstonesLatestLeftBehindWhenPublishLatestWasSwitchedOff},
		{"Latest_ResumesAtTheExistingNameWhenSwitchedBackOn", TestLatest_ResumesAtTheExistingNameWhenSwitchedBackOn},
		{"Latest_UnchangedRepublishRewritesAfterKeyOrBaseURLChange", TestLatest_UnchangedRepublishRewritesAfterKeyOrBaseURLChange},
		{"IdentityDecisionTable", TestIdentityDecisionTable},
		{"Identity_RetiredIdStaysRetiredAcrossFurtherCalls", TestIdentity_RetiredIdStaysRetiredAcrossFurtherCalls},
		{"Identity_SharedLastSegment", TestIdentity_SharedLastSegment},
		{"Identity_SharedLastSegmentInOneCall", TestIdentity_SharedLastSegmentInOneCall},
		{"Identity_UnsafeNewIds", TestIdentity_UnsafeNewIds},
		{"Identity_SafeNewIds", TestIdentity_SafeNewIds},
		{"Identity_ExistingIdIsNotRevalidated", TestIdentity_ExistingIdIsNotRevalidated},
		{"Identity_RetireUnknown", TestIdentity_RetireUnknown},
		{"Identity_OneBadCatalogDoesNotPoisonTheBatch", TestIdentity_OneBadCatalogDoesNotPoisonTheBatch},
		{"Identity_FullLifecycleKeepsVersionsMonotonic", TestIdentity_FullLifecycleKeepsVersionsMonotonic},
		{"Identity_IndexThatAlreadySharesFiles", TestIdentity_IndexThatAlreadySharesFiles},
		{"Identity_UnreadableCatalogDoesNotPoisonTheBatch", TestIdentity_UnreadableCatalogDoesNotPoisonTheBatch},
		{"Identity_LiveEntryWithoutBaseline", TestIdentity_LiveEntryWithoutBaseline},
		{"DroppedAttribute_PublishedContentMatchesSubmission", TestDroppedAttribute_PublishedContentMatchesSubmission},
		{"Retire_UnreadableCatalog_BuildsFinalLatestFromStoredFile", TestRetire_UnreadableCatalog_BuildsFinalLatestFromStoredFile},
		{"Retire_UnreadableCatalogAndLatest_IsRefusedWithNothingSigned", TestRetire_UnreadableCatalogAndLatest_IsRefusedWithNothingSigned},
		{"ForceBaseline_UnreadableCatalog_KeepsLatestName", TestForceBaseline_UnreadableCatalog_KeepsLatestName},
		{"MalformedIndexEntry_IsRefusedPerCatalog", TestMalformedIndexEntry_IsRefusedPerCatalog},
		{"SupersededChangeFiles_AreNotAppliedOverTheNewBaseline", TestSupersededChangeFiles_AreNotAppliedOverTheNewBaseline},
	} {
		t.Run(tc.name, tc.run)
	}
}
