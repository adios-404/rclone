package sync

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/walk"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/lib/transform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testWatchOptions makes changes sync quickly, and only when reported
var testWatchOptions = WatchOptions{
	Settle:     20 * time.Millisecond,
	MaxDelay:   time.Second,
	DeleteMode: fs.DeleteModeDefault,
}

// fakeWatcher stands in for the WatchChanges feature of a backend
type fakeWatcher struct {
	mu     sync.Mutex
	notify func([]fs.ChangeEvent)
	err    error // returned from watch if set
}

// watch implements WatchChanges
func (w *fakeWatcher) watch(ctx context.Context, notify func([]fs.ChangeEvent)) error {
	if w.err != nil {
		return w.err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.notify = notify
	return nil
}

// changed reports that the files at paths have changed
func (w *fakeWatcher) changed(paths ...string) {
	w.send(fileEvents(paths...)...)
}

// send reports events
func (w *fakeWatcher) send(events ...fs.ChangeEvent) {
	w.mu.Lock()
	notify := w.notify
	w.mu.Unlock()
	notify(events)
}

// shortenWatchRetryDelay makes failed syncs retry quickly for this test.
func shortenWatchRetryDelay(t *testing.T) {
	oldMin, oldMax := watchRetryMinDelay, watchRetryMaxDelay
	watchRetryMinDelay, watchRetryMaxDelay = 20*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { watchRetryMinDelay, watchRetryMaxDelay = oldMin, oldMax })
}

// startWatch runs watch from r.Flocal to r.Fremote in the background
// until the test finishes, and fails the test if it returns an error.
func startWatch(t *testing.T, r *fstest.Run, opt WatchOptions, watchChanges func(context.Context, func([]fs.ChangeEvent)) error) {
	startWatchWith(context.Background(), t, r, opt, watchChanges)
}

// startWatchWith is startWatch using the config in ctx
func startWatchWith(ctx context.Context, t *testing.T, r *fstest.Run, opt WatchOptions, watchChanges func(context.Context, func([]fs.ChangeEvent)) error) {
	ctx, cancel := context.WithCancel(ctx)
	// Listing a missing directory while polling for results counts as an
	// error in the global stats, which stops a running sync from deleting.
	r.ForceMkdir(ctx, r.Flocal)
	r.ForceMkdir(ctx, r.Fremote)
	done := make(chan error, 1)
	go func() {
		done <- watch(ctx, r.Fremote, r.Flocal, opt, watchChanges)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(30 * time.Second):
			t.Error("watch did not return after being cancelled")
		}
	})
}

// waitForRemoteItems waits for r.Fremote to hold exactly items
func waitForRemoteItems(t *testing.T, r *fstest.Run, items ...fstest.Item) {
	t.Helper()
	ctx := context.Background()
	want := make(map[string]int64, len(items))
	for _, item := range items {
		want[item.Path] = item.Size
	}
	assert.Eventually(t, func() bool {
		objs, _, err := walk.GetAll(ctx, r.Fremote, "", true, -1)
		if err != nil || len(objs) != len(want) {
			return false
		}
		for _, o := range objs {
			if size, found := want[o.Remote()]; !found || size != o.Size() {
				return false
			}
		}
		return true
	}, 30*time.Second, 10*time.Millisecond, "destination never matched")
	r.CheckRemoteItems(t, items...)
}

// syncChanged syncs items from r.Flocal to r.Fremote
func syncChanged(ctx context.Context, r *fstest.Run, items ...watchItem) error {
	return syncChangedWith(ctx, r, testWatchOptions, items...)
}

// syncChangedWith syncs items from r.Flocal to r.Fremote using opt
func syncChangedWith(ctx context.Context, r *fstest.Run, opt WatchOptions, items ...watchItem) error {
	// Errors left by an earlier test would stop this deleting anything
	accounting.GlobalStats().ResetErrors()
	w := &watcher{fdst: r.Fremote, fsrc: r.Flocal, opt: opt}
	return w.syncChanged(ctx, items, nil)
}

// eachWatchMode runs test with a context which makes changed files be
// found by looking them up, then with one which makes them be found
// by listing the directories they are in.
func eachWatchMode(t *testing.T, test func(ctx context.Context, t *testing.T)) {
	t.Run("Lookup", func(t *testing.T) {
		ctx, ci := fs.AddConfig(context.Background())
		ci.NoUpdateDirModTime = true
		test(ctx, t)
	})
	t.Run("List", func(t *testing.T) {
		// Directories are listed to sync their modification times,
		// if the destination keeps them
		test(context.Background(), t)
	})
	t.Run("TrackRenames", func(t *testing.T) {
		ctx, ci := fs.AddConfig(context.Background())
		ci.TrackRenames = true
		test(ctx, t)
	})
}

// newMemoryFs returns an empty Fs which is case sensitive and keeps
// what is put in it in memory.
func newMemoryFs(t *testing.T) fs.Fs {
	f, err := fs.NewFs(context.Background(), ":memory:"+strings.ReplaceAll(t.Name(), "/", "-"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = operations.Purge(context.Background(), f, "") })
	return f
}

// putFile puts a file with content at remote in f
func putFile(t *testing.T, f fs.Fs, remote, content string) {
	info := object.NewStaticObjectInfo(remote, t1, int64(len(content)), true, nil, nil)
	_, err := f.Put(context.Background(), strings.NewReader(content), info)
	require.NoError(t, err)
}

// fileNames returns the names of the files in f in order
func fileNames(t *testing.T, f fs.Fs) (names []string) {
	objs, _, err := walk.GetAll(context.Background(), f, "", true, -1)
	require.NoError(t, err)
	for _, o := range objs {
		names = append(names, o.Remote())
	}
	slices.Sort(names)
	return names
}

func TestWatchSyncChangedOnlyTouchesChangedFiles(t *testing.T) {
	eachWatchMode(t, testWatchSyncChangedOnlyTouchesChangedFiles)
}

func testWatchSyncChangedOnlyTouchesChangedFiles(ctx context.Context, t *testing.T) {
	r := fstest.NewRun(t)
	added := r.WriteFile("added.txt", "new file", t1)
	updated := r.WriteFile("dir/updated.txt", "new content, and longer", t2)
	unlistedSrc := r.WriteFile("unlisted-src.txt", "not reported", t1)
	r.WriteObject(ctx, "dir/updated.txt", "old content", t1)
	r.WriteObject(ctx, "deleted.txt", "gone from the source", t1)
	unlistedDst := r.WriteObject(ctx, "unlisted-dst.txt", "only on the destination", t1)

	err := syncChanged(ctx, r, file("added.txt"), file("deleted.txt"), file("dir/updated.txt"), file("never-existed.txt"))
	require.NoError(t, err)

	r.CheckLocalItems(t, added, updated, unlistedSrc)
	r.CheckRemoteItems(t, added, updated, unlistedDst)
}

func TestWatchSyncChangedKeepsDirectoryOfDeletedFile(t *testing.T) {
	eachWatchMode(t, testWatchSyncChangedKeepsDirectoryOfDeletedFile)
}

func testWatchSyncChangedKeepsDirectoryOfDeletedFile(ctx context.Context, t *testing.T) {
	r := fstest.NewRun(t)
	require.NoError(t, operations.Mkdir(ctx, r.Flocal, "emptied"))
	kept := r.WriteBoth(ctx, "kept/file.txt", "still there", t1)
	r.WriteObject(ctx, "emptied/deleted.txt", "gone from the source", t1)

	require.NoError(t, syncChanged(ctx, r, file("emptied/deleted.txt")))

	// The directory is still in the source so it stays
	r.CheckRemoteListing(t, []fstest.Item{kept}, []string{"emptied", "kept"})
}

func TestWatchSyncChangedHonoursFilters(t *testing.T) {
	eachWatchMode(t, testWatchSyncChangedHonoursFilters)
}

func testWatchSyncChangedHonoursFilters(ctx context.Context, t *testing.T) {
	r := fstest.NewRun(t)
	wanted := r.WriteFile("wanted.txt", "wanted", t1)
	r.WriteFile("excluded.tmp", "excluded", t1)

	fi, err := filter.NewFilter(nil)
	require.NoError(t, err)
	require.NoError(t, fi.Add(false, "*.tmp"))
	ctx = filter.ReplaceConfig(ctx, fi)

	err = syncChanged(ctx, r, file("excluded.tmp"), file("wanted.txt"))
	require.NoError(t, err)
	r.CheckRemoteItems(t, wanted)

	// Nothing left to sync must not fall back to syncing everything
	other := r.WriteFile("other.txt", "not reported", t1)
	err = syncChanged(ctx, r, file("excluded.tmp"))
	require.NoError(t, err)
	r.CheckLocalItems(t, wanted, other, fstest.NewItem("excluded.tmp", "excluded", t1))
	r.CheckRemoteItems(t, wanted)

	// The same goes when the directory is listed to find it
	excludedInDir := r.WriteFile("dir/excluded.tmp", "excluded", t1)
	wantedInDir := r.WriteFile("dir/wanted.txt", "wanted", t1)
	err = syncChanged(ctx, r, tree("dir"), file("excluded.tmp"))
	require.NoError(t, err)
	r.CheckLocalItems(t, wanted, other, fstest.NewItem("excluded.tmp", "excluded", t1), excludedInDir, wantedInDir)
	r.CheckRemoteItems(t, wanted, wantedInDir)
}

func TestWatchSyncChangedHonoursSizeFilter(t *testing.T) {
	eachWatchMode(t, testWatchSyncChangedHonoursSizeFilter)
}

func testWatchSyncChangedHonoursSizeFilter(ctx context.Context, t *testing.T) {
	r := fstest.NewRun(t)
	small := r.WriteFile("small.txt", "small", t1)
	big := r.WriteFile("big.txt", "much too big to be included", t1)
	r.WriteObject(ctx, "small-deleted.txt", "tiny", t1)
	bigOnDst := r.WriteObject(ctx, "big-deleted.txt", "excluded so must be left alone", t1)

	fi, err := filter.NewFilter(nil)
	require.NoError(t, err)
	fi.Opt.MaxSize = 10
	ctx = filter.ReplaceConfig(ctx, fi)

	err = syncChanged(ctx, r, file("big-deleted.txt"), file("big.txt"), file("small-deleted.txt"), file("small.txt"))
	require.NoError(t, err)

	r.CheckLocalItems(t, small, big)
	r.CheckRemoteItems(t, small, bigOnDst)
}

func TestWatchSyncChangedHonoursFilesFrom(t *testing.T) {
	eachWatchMode(t, testWatchSyncChangedHonoursFilesFrom)
}

func testWatchSyncChangedHonoursFilesFrom(ctx context.Context, t *testing.T) {
	r := fstest.NewRun(t)
	listed := r.WriteFile("listed.txt", "in the list", t1)
	alsoListed := r.WriteFile("also-listed.txt", "in the list but not reported", t1)
	r.WriteFile("unlisted.txt", "not in the list", t1)

	fi, err := filter.NewFilter(nil)
	require.NoError(t, err)
	require.NoError(t, fi.AddFile("listed.txt"))
	require.NoError(t, fi.AddFile("also-listed.txt"))
	ctx = filter.ReplaceConfig(ctx, fi)

	require.NoError(t, syncChanged(ctx, r, file("listed.txt"), file("unlisted.txt")))
	r.CheckRemoteItems(t, listed)
	assert.Len(t, fi.Files(), 2, "the list of files was changed")

	require.NoError(t, syncChanged(ctx, r, tree("")))
	r.CheckRemoteItems(t, listed, alsoListed)
}

func TestWatchSyncChangedHonoursMaxDepth(t *testing.T) {
	eachWatchMode(t, testWatchSyncChangedHonoursMaxDepth)
}

func testWatchSyncChangedHonoursMaxDepth(ctx context.Context, t *testing.T) {
	ctx, ci := fs.AddConfig(ctx)
	ci.MaxDepth = 2
	r := fstest.NewRun(t)
	top := r.WriteFile("top.txt", "top", t1)
	inDir := r.WriteFile("dir/file.txt", "one down", t1)
	deep := r.WriteFile("dir/sub/deep.txt", "too deep", t1)

	require.NoError(t, syncChanged(ctx, r, file("dir/file.txt"), file("dir/sub/deep.txt"), file("top.txt")))
	r.CheckLocalItems(t, top, inDir, deep)
	r.CheckRemoteItems(t, top, inDir)
}

func TestWatchSyncChangedHonoursExcludeIfPresent(t *testing.T) {
	eachWatchMode(t, testWatchSyncChangedHonoursExcludeIfPresent)
}

func testWatchSyncChangedHonoursExcludeIfPresent(ctx context.Context, t *testing.T) {
	r := fstest.NewRun(t)
	wanted := r.WriteFile("wanted/file.txt", "wanted", t1)
	skipped := r.WriteFile("skipped/sub/file.txt", "in an excluded directory", t1)
	marker := r.WriteFile("skipped/.ignore", "", t1)

	fi, err := filter.NewFilter(nil)
	require.NoError(t, err)
	fi.Opt.ExcludeFile = []string{".ignore"}
	ctx = filter.ReplaceConfig(ctx, fi)

	require.NoError(t, syncChanged(ctx, r, file("skipped/sub/file.txt"), file("wanted/file.txt")))
	r.CheckLocalItems(t, wanted, skipped, marker)
	r.CheckRemoteItems(t, wanted)

	// Removing the file which excludes the directory brings what is in it back
	require.NoError(t, os.Remove(filepath.Join(r.LocalName, "skipped", ".ignore")))
	require.NoError(t, syncChanged(ctx, r, file("skipped/.ignore")))
	r.CheckRemoteItems(t, wanted, skipped)
}

func TestWatchSyncChangedDirectory(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	// A new directory
	inNew := r.WriteFile("new/file.txt", "in a new directory", t1)
	deepInNew := r.WriteFile("new/sub/deep/file.txt", "deep in a new directory", t2)
	// A directory with changes in it
	same := r.WriteBoth(ctx, "changed/same.txt", "no change", t1)
	updated := r.WriteFile("changed/updated.txt", "new content, and longer", t2)
	r.WriteObject(ctx, "changed/updated.txt", "old content", t1)
	r.WriteObject(ctx, "changed/sub/deleted.txt", "gone from the source", t1)
	// A directory which has gone
	r.WriteObject(ctx, "parent/gone/file.txt", "gone from the source", t1)
	r.WriteObject(ctx, "parent/gone/sub/file.txt", "gone from the source", t1)
	beside := r.WriteBoth(ctx, "parent/beside.txt", "next to a directory which has gone", t1)
	// Not reported so must be left alone
	unlistedSrc := r.WriteFile("unlisted/src.txt", "not reported", t1)
	unlistedDst := r.WriteObject(ctx, "unlisted/dst.txt", "only on the destination", t1)
	unlistedBeside := r.WriteObject(ctx, "parent/unlisted.txt", "only on the destination", t1)

	require.NoError(t, syncChanged(ctx, r, tree("changed"), tree("new"), tree("parent/gone")))

	r.CheckLocalItems(t, inNew, deepInNew, same, updated, beside, unlistedSrc)
	r.CheckRemoteListing(t,
		[]fstest.Item{inNew, deepInNew, same, updated, beside, unlistedDst, unlistedBeside},
		[]string{"new", "new/sub", "new/sub/deep", "changed", "parent", "unlisted"},
	)
}

func TestWatchSyncChangedDirectoryWhoseParentHasGone(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	kept := r.WriteBoth(ctx, "kept.txt", "still there", t1)
	r.WriteObject(ctx, "gone/sub/file.txt", "gone from the source", t1)
	r.WriteObject(ctx, "gone/other/file.txt", "gone from the source", t1)

	// A full sync would remove everything in the missing parent
	require.NoError(t, syncChanged(ctx, r, tree("gone/sub")))
	r.CheckRemoteListing(t, []fstest.Item{kept}, []string{})
}

func TestWatchSyncChangedFileAndDirectoryTogether(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	inDir := r.WriteFile("dir/file.txt", "in the directory", t1)
	changed := r.WriteFile("big/changed.txt", "new content, and longer", t2)
	r.WriteObject(ctx, "big/changed.txt", "old content", t1)
	r.WriteObject(ctx, "big/deleted.txt", "gone from the source", t1)
	unlisted := r.WriteObject(ctx, "big/unlisted.txt", "only on the destination", t1)

	require.NoError(t, syncChanged(ctx, r, file("big/changed.txt"), file("big/deleted.txt"), file("big/missing.txt"), tree("dir")))
	r.CheckRemoteItems(t, inDir, changed, unlisted)
}

func TestWatchSyncChangedWhenTypeOfWhatHasGoneIsUnknown(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	r.WriteObject(ctx, "was-dir/sub/file.txt", "gone from the source", t1)
	r.WriteObject(ctx, "was-file", "gone from the source", t1)
	other := r.WriteBoth(ctx, "other.txt", "not reported", t1)

	require.NoError(t, syncChanged(ctx, r, tree("was-dir"), tree("was-file")))
	r.CheckRemoteListing(t, []fstest.Item{other}, []string{})
}

func TestWatchSyncChangedWhenSpeltDifferentlyInDestination(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.IgnoreCaseSync = true
	r := fstest.NewRun(t)
	if r.Fremote.Features().CaseInsensitive {
		t.Skip("can't make names which differ only by case")
	}
	r.WriteObject(ctx, "Dir/Deleted.TXT", "gone from the source", t1)
	r.WriteObject(ctx, "Gone/Sub/File.txt", "gone from the source", t1)
	other := r.WriteObject(ctx, "Dir/Other.txt", "not reported", t1)
	r.WriteFile("dir/changed.txt", "changed", t2)
	r.WriteObject(ctx, "Dir/CHANGED.txt", "old", t1)

	require.NoError(t, syncChanged(ctx, r, file("dir/deleted.txt"), file("dir/changed.txt"), tree("gone")))
	r.CheckRemoteListing(t, []fstest.Item{other, fstest.NewItem("Dir/CHANGED.txt", "changed", t2)}, []string{"Dir"})
}

func TestWatchSyncChangedHonoursNameTransform(t *testing.T) {
	ctx, _ := fs.AddConfig(context.Background())
	require.NoError(t, transform.SetOptions(ctx, "all,replace=s-:d-"))
	r := fstest.NewRun(t)
	r.WriteObject(ctx, "d-dir/d-deleted.txt", "gone from the source", t1)
	r.WriteObject(ctx, "d-gone/d-sub/d-file.txt", "gone from the source", t1)
	other := r.WriteObject(ctx, "d-dir/d-other.txt", "not reported", t1)
	r.WriteFile("s-dir/s-changed.txt", "changed", t2)
	r.WriteObject(ctx, "d-dir/d-changed.txt", "old", t1)
	r.WriteFile("s-dir/s-new.txt", "new", t2)

	require.NoError(t, syncChanged(ctx, r, file("s-dir/s-deleted.txt"), file("s-dir/s-changed.txt"), file("s-dir/s-new.txt"), tree("s-gone")))
	r.CheckRemoteListing(t, []fstest.Item{other, fstest.NewItem("d-dir/d-changed.txt", "changed", t2), fstest.NewItem("d-dir/d-new.txt", "new", t2)}, []string{"d-dir"})
}

func TestWatchSyncChangedWhenDirectoryAboveIsNowAFile(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TrackRenames = true // changes are found by listing
	r := fstest.NewRun(t)
	if r.Fremote.Features().BucketBased || r.Fremote.Features().DuplicateFiles {
		t.Skip("a file and a directory of the same name can both be in the destination")
	}
	r.WriteObject(ctx, "was-dir/sub/file.txt", "gone from the source", t1)
	nowFile := r.WriteFile("was-dir", "a file now", t2)
	r.WriteFile("was-file/sub/file.txt", "new", t2)
	r.WriteObject(ctx, "was-file", "gone from the source", t1)

	// Only what was in them is reported as having changed
	require.NoError(t, syncChanged(ctx, r, file("was-dir/sub/file.txt"), file("was-file/sub/file.txt")))
	r.CheckRemoteItems(t, nowFile, fstest.NewItem("was-file/sub/file.txt", "new", t2))
}

func TestWatchSyncChangedDirectoryNewToDestination(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	file1 := r.WriteFile("new/file.txt", "in a new directory", t1)
	file2 := r.WriteFile("new/sub/file.txt", "in a new directory", t1)
	r.WriteFile("other/file.txt", "not reported", t1)

	// A change to the directory itself is all that was reported
	require.NoError(t, syncChanged(ctx, r, watchItem{path: "new", kind: watchDir}))
	r.CheckRemoteItems(t, file1, file2)
}

func TestWatchSyncChangedDeletesNothingIfSourceHasGone(t *testing.T) {
	eachWatchMode(t, func(ctx context.Context, t *testing.T) {
		r := fstest.NewRun(t)
		file1 := r.WriteBoth(ctx, "file.txt", "in both", t1)
		file2 := r.WriteBoth(ctx, "dir/file.txt", "in both", t1)
		require.NoError(t, os.RemoveAll(r.LocalName))

		err := syncChanged(ctx, r, file("dir/file.txt"))
		require.Error(t, err)
		r.CheckRemoteItems(t, file1, file2)
	})
}

func TestWatchSyncChangedWhenSourceLooksEmpty(t *testing.T) {
	eachWatchMode(t, func(ctx context.Context, t *testing.T) {
		r := fstest.NewRun(t)
		file1 := r.WriteObject(ctx, "file.txt", "gone from the source", t1)
		file2 := r.WriteObject(ctx, "dir/file.txt", "gone from the source", t1)
		file3 := r.WriteObject(ctx, "other/file.txt", "gone from the source", t1)
		r.ForceMkdir(ctx, r.Flocal)

		// A source which has been unmounted looks like this, and
		// not everything in the destination is said to have changed
		err := syncChanged(ctx, r, file("file.txt"), tree("dir"), file("other/file.txt"))
		require.ErrorIs(t, err, errWatchSourceEmpty)
		r.CheckRemoteItems(t, file1, file2, file3)

		// It is when everything in the source has been deleted
		require.NoError(t, syncChanged(ctx, r, file("file.txt"), tree("dir"), tree("other")))
		r.CheckRemoteListing(t, []fstest.Item{}, []string{})
	})
}

func TestWatchSyncChangedWhenDestinationIgnoresCase(t *testing.T) {
	eachWatchMode(t, func(ctx context.Context, t *testing.T) {
		r := fstest.NewRun(t)
		if !r.Fremote.Features().CaseInsensitive {
			t.Skip("needs a destination which ignores case")
		}
		fsrc := newMemoryFs(t)
		putFile(t, fsrc, "Foo.txt", "in both")
		r.WriteObject(ctx, "Foo.txt", "in both", t1)

		// Looking the file up in the destination by this name would
		// find the one which is still in the source
		accounting.GlobalStats().ResetErrors()
		w := &watcher{fdst: r.Fremote, fsrc: fsrc, opt: testWatchOptions}
		require.NoError(t, w.syncChanged(ctx, []watchItem{file("foo.txt")}, nil))
		assert.Equal(t, []string{"Foo.txt"}, fileNames(t, r.Fremote))
	})
}

func TestWatchSyncChangedWhenSourceIgnoresCase(t *testing.T) {
	eachWatchMode(t, func(ctx context.Context, t *testing.T) {
		r := fstest.NewRun(t)
		if !r.Flocal.Features().CaseInsensitive {
			t.Skip("needs a source which ignores case")
		}
		fdst := newMemoryFs(t)
		putFile(t, fdst, "foo.txt", "content")
		// Renamed from foo.txt, which the source would still find it by
		r.WriteFile("Foo.txt", "content", t1)

		accounting.GlobalStats().ResetErrors()
		w := &watcher{fdst: fdst, fsrc: r.Flocal, opt: testWatchOptions}
		require.NoError(t, w.syncChanged(ctx, []watchItem{file("Foo.txt"), file("foo.txt")}, nil))
		assert.Equal(t, []string{"Foo.txt"}, fileNames(t, fdst))
	})
}

func TestWatchSyncChangedHonoursExcludeIfPresentInDestination(t *testing.T) {
	eachWatchMode(t, func(ctx context.Context, t *testing.T) {
		fi, err := filter.NewFilter(nil)
		require.NoError(t, err)
		fi.Opt.ExcludeFile = []string{".ignore"}
		ctx = filter.ReplaceConfig(ctx, fi)
		r := fstest.NewRun(t)
		file1 := r.WriteBoth(ctx, "dir/kept.txt", "in both", t1)
		file2 := r.WriteObject(ctx, "dir/only-dst.txt", "only in the destination", t1)
		marker := r.WriteObject(ctx, "dir/.ignore", "", t1)

		// A full sync leaves a directory with the marker in it alone
		require.NoError(t, syncChanged(ctx, r, file("dir/only-dst.txt")))
		r.CheckRemoteItems(t, file1, file2, marker)
	})
}

func TestWatchSyncChangedLeavesOutWhatIsHeld(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TrackRenames = true // changes are found by listing
	r := fstest.NewRun(t)
	file1 := r.WriteFile("dir/file.txt", "new", t1)
	r.WriteFile("dir/held.txt", "failed earlier", t1)
	r.WriteFile("dir/held/file.txt", "in what failed earlier", t1)
	file2 := r.WriteFile("dir/held/changed/file.txt", "changed since it failed", t1)
	r.WriteObject(ctx, "dir/deleted.txt", "gone from the source", t1)
	// What is held is still deleted if it has gone from the source
	r.WriteObject(ctx, "dir/gone/file.txt", "gone from the source", t1)

	accounting.GlobalStats().ResetErrors()
	w := &watcher{fdst: r.Fremote, fsrc: r.Flocal, opt: testWatchOptions}
	require.NoError(t, w.syncChanged(ctx, []watchItem{tree("dir"), tree("dir/held/changed")}, []string{"dir/held.txt", "dir/held", "dir/gone/file.txt"}))
	r.CheckRemoteItems(t, file1, file2)
}

func TestWatchWidensChangeToExcludeMarker(t *testing.T) {
	fi, err := filter.NewFilter(nil)
	require.NoError(t, err)
	fi.Opt.ExcludeFile = []string{".ignore"}
	ctx := filter.ReplaceConfig(context.Background(), fi)
	w := new(watcher)

	// The directory the marker is in needs syncing, with everything in it
	items := w.widen(ctx, []watchItem{file("a/.ignore"), file("a/b/.ignore"), file("a/b/file.txt"), {path: "a/b", kind: watchDir}, file("c/.ignore.txt")})
	assert.Equal(t, []watchItem{tree("a"), tree("a/b"), file("a/b/file.txt"), file("c/.ignore.txt")}, items)

	// which is everything if it is in the root
	items = w.widen(ctx, []watchItem{file("a/file.txt"), file(".ignore")})
	assert.Equal(t, []watchItem{tree("")}, items)

	// Nothing changes without the flag
	items = []watchItem{file("a/.ignore")}
	assert.Equal(t, items, w.widen(context.Background(), items))
}

func TestWatchSyncChangedSyncsDirectoryWhichIsHeld(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	if r.Fremote.Features().DirSetModTime == nil {
		t.Skip("destination can't set the modification times of directories")
	}
	file1 := r.WriteBoth(ctx, "dir/file.txt", "in both", t1)
	file2 := r.WriteFile("dir/added.txt", "added", t2)
	_, err := operations.SetDirModTime(ctx, r.Flocal, nil, "dir", t2)
	require.NoError(t, err)

	// The directory failed to sync earlier, but is what a change is in now
	accounting.GlobalStats().ResetErrors()
	w := &watcher{fdst: r.Fremote, fsrc: r.Flocal, opt: testWatchOptions}
	require.NoError(t, w.syncChanged(ctx, []watchItem{file("dir/added.txt")}, []string{"dir"}))
	r.CheckRemoteItems(t, file1, file2)
	r.CheckDirectoryModTimes(t, "dir")
}

func TestWatchSyncChangedSetsDirectoryModTimes(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	if r.Fremote.Features().DirSetModTime == nil && r.Fremote.Features().MkdirMetadata == nil {
		t.Skip("destination can't set the modification times of directories")
	}
	kept := r.WriteBoth(ctx, "dir/sub/kept.txt", "kept", t1)
	r.WriteObject(ctx, "dir/sub/deleted.txt", "gone from the source", t1)
	other := r.WriteBoth(ctx, "other/file.txt", "not reported", t1)
	setModTime := func(f fs.Fs, dir string, when time.Time) {
		_, err := operations.SetDirModTime(ctx, f, nil, dir, when)
		require.NoError(t, err)
	}
	for _, dir := range []string{"dir", "dir/sub", "other"} {
		setModTime(r.Fremote, dir, t1)
	}
	setModTime(r.Flocal, "dir", t2)
	setModTime(r.Flocal, "other", t2)

	// Adding or removing a file changes the modification time of
	// the directory it is in, so that is synced with the file
	added := r.WriteFile("dir/sub/added.txt", "new file", t1)
	setModTime(r.Flocal, "dir/sub", t3)
	require.NoError(t, syncChanged(ctx, r, file("dir/sub/added.txt"), file("dir/sub/deleted.txt")))
	r.CheckRemoteItems(t, kept, added, other)
	r.CheckDirectoryModTimes(t, "dir/sub")
	got := fstest.NewDirectory(ctx, t, r.Fremote, "dir")
	fstest.CheckDirModTime(ctx, t, r.Fremote, got, t1)

	// A change to a directory itself is synced without what is in it
	unreported := r.WriteFile("dir/unreported.txt", "not reported", t1)
	setModTime(r.Flocal, "dir", t2)
	require.NoError(t, syncChanged(ctx, r, watchItem{path: "dir", kind: watchDir}))
	r.CheckLocalItems(t, kept, added, other, unreported)
	r.CheckRemoteItems(t, kept, added, other)
	r.CheckDirectoryModTimes(t, "dir", "dir/sub")
	got = fstest.NewDirectory(ctx, t, r.Fremote, "other")
	fstest.CheckDirModTime(ctx, t, r.Fremote, got, t1)
}

func TestWatchSyncChangedCopiesEmptyDirectories(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	if !r.Fremote.Features().CanHaveEmptyDirectories {
		t.Skip("destination can't have empty directories")
	}
	require.NoError(t, operations.Mkdir(ctx, r.Flocal, "empty/sub"))
	opt := testWatchOptions
	opt.CopyEmptySrcDirs = true

	require.NoError(t, syncChangedWith(ctx, r, opt, tree("empty")))
	r.CheckRemoteListing(t, []fstest.Item{}, []string{"empty", "empty/sub"})
}

func TestWatchSyncChangedCopyModeDeletesNothing(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	added := r.WriteFile("dir/added.txt", "new file", t1)
	onlyDst := r.WriteObject(ctx, "dir/only-dst.txt", "gone from the source", t1)
	onlyDstDir := r.WriteObject(ctx, "gone/file.txt", "gone from the source", t1)
	opt := testWatchOptions
	opt.DeleteMode = fs.DeleteModeOff

	require.NoError(t, syncChangedWith(ctx, r, opt, file("dir/added.txt"), file("dir/only-dst.txt")))
	r.CheckRemoteItems(t, added, onlyDst, onlyDstDir)
	require.NoError(t, syncChangedWith(ctx, r, opt, tree("dir"), tree("gone")))
	r.CheckRemoteItems(t, added, onlyDst, onlyDstDir)
}

func TestWatchSyncChangedDeleteBefore(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	added := r.WriteFile("dir/added.txt", "new file", t1)
	r.WriteObject(ctx, "dir/deleted.txt", "gone from the source", t1)
	r.WriteObject(ctx, "deleted.txt", "gone from the source", t1)
	top := r.WriteFile("top.txt", "new file", t1)
	opt := testWatchOptions
	opt.DeleteMode = fs.DeleteModeBefore

	require.NoError(t, syncChangedWith(ctx, r, opt, file("deleted.txt"), file("top.txt")))
	require.NoError(t, syncChangedWith(ctx, r, opt, tree("dir")))
	r.CheckRemoteItems(t, added, top)
}

func TestWatchSyncChangedTracksRenames(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TrackRenames = true
	r := fstest.NewRun(t)
	canTrackRenames := operations.CanServerSideMove(r.Fremote) && r.Fremote.Hashes().Overlap(r.Flocal.Hashes()).Count() > 0
	r.WriteObject(ctx, "old-dir/a.txt", "content of a", t1)
	r.WriteObject(ctx, "old-dir/sub/b.txt", "content of b, which is longer", t1)
	r.WriteObject(ctx, "old-name.txt", "content of a file", t1)
	a := r.WriteFile("new-dir/a.txt", "content of a", t1)
	b := r.WriteFile("new-dir/sub/b.txt", "content of b, which is longer", t1)
	c := r.WriteFile("new-name.txt", "content of a file", t1)

	// A renamed file
	accounting.GlobalStats().ResetCounters()
	require.NoError(t, syncChanged(ctx, r, file("new-name.txt"), file("old-name.txt")))
	if canTrackRenames {
		assert.Equal(t, int64(1), accounting.GlobalStats().Renames(0))
	}

	// A renamed directory
	accounting.GlobalStats().ResetCounters()
	require.NoError(t, syncChanged(ctx, r, tree("new-dir"), tree("old-dir")))
	if canTrackRenames {
		assert.Equal(t, int64(2), accounting.GlobalStats().Renames(0))
	}
	r.CheckRemoteListing(t, []fstest.Item{a, b, c}, []string{"new-dir", "new-dir/sub"})
}

func TestWatchRunsFullSyncFirst(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	file1 := r.WriteFile("existing.txt", "there before the watch started", t1)
	r.WriteObject(ctx, "stale.txt", "only on the destination", t1)

	startWatch(t, r, testWatchOptions, new(fakeWatcher).watch)

	waitForRemoteItems(t, r, file1)
}

func TestWatchSyncsNewAndChangedFiles(t *testing.T) {
	r := fstest.NewRun(t)
	file1 := r.WriteFile("existing.txt", "there before the watch started", t1)
	watcher := new(fakeWatcher)
	startWatch(t, r, testWatchOptions, watcher.watch)
	waitForRemoteItems(t, r, file1)

	file2 := r.WriteFile("dir/new.txt", "created while watching", t2)
	watcher.changed("dir/new.txt")
	waitForRemoteItems(t, r, file1, file2)

	file1 = r.WriteFile("existing.txt", "changed while watching", t3)
	watcher.changed("existing.txt")
	waitForRemoteItems(t, r, file1, file2)
}

func TestWatchSyncsDeletedFile(t *testing.T) {
	r := fstest.NewRun(t)
	file1 := r.WriteFile("keep.txt", "keep", t1)
	r.WriteFile("delete.txt", "delete me", t1)
	watcher := new(fakeWatcher)
	startWatch(t, r, testWatchOptions, watcher.watch)
	waitForRemoteItems(t, r, file1, fstest.NewItem("delete.txt", "delete me", t1))

	require.NoError(t, os.Remove(filepath.Join(r.LocalName, "delete.txt")))
	watcher.changed("delete.txt")
	waitForRemoteItems(t, r, file1)
}

func TestWatchSyncsDirectories(t *testing.T) {
	r := fstest.NewRun(t)
	file1 := r.WriteFile("dir/sub/file.txt", "there before the watch started", t1)
	watcher := new(fakeWatcher)
	startWatch(t, r, testWatchOptions, watcher.watch)
	waitForRemoteItems(t, r, file1)

	unreported := r.WriteFile("unreported.txt", "nothing said this changed", t1)
	require.NoError(t, os.Rename(filepath.Join(r.LocalName, "dir"), filepath.Join(r.LocalName, "renamed")))
	file1.Path = "renamed/sub/file.txt"
	watcher.send(dirEvents("dir", "renamed")...)
	waitForRemoteItems(t, r, file1)

	require.NoError(t, os.RemoveAll(filepath.Join(r.LocalName, "renamed")))
	watcher.send(dirEvents("renamed")...)
	waitForRemoteItems(t, r)
	r.CheckLocalItems(t, unreported)
	r.CheckRemoteListing(t, []fstest.Item{}, []string{})
}

func TestWatchOnlySyncsReportedPathsUntilRecursiveEvent(t *testing.T) {
	r := fstest.NewRun(t)
	// Shows that the first full sync has finished
	first := r.WriteFile("first.txt", "there before the watch started", t1)
	watcher := new(fakeWatcher)
	startWatch(t, r, testWatchOptions, watcher.watch)
	waitForRemoteItems(t, r, first)

	unreported := r.WriteFile("unreported.txt", "nothing said this changed", t1)
	reported := r.WriteFile("reported.txt", "reported", t1)
	watcher.changed("reported.txt")
	waitForRemoteItems(t, r, first, reported)

	watcher.send(dirEvents("")...)
	waitForRemoteItems(t, r, first, reported, unreported)
}

func TestWatchFullCheckWithoutChangeReports(t *testing.T) {
	shortenWatchRetryDelay(t)
	r := fstest.NewRun(t)
	opt := testWatchOptions
	opt.FullCheck = 50 * time.Millisecond
	startWatch(t, r, opt, nil)
	waitForRemoteItems(t, r)

	file1 := r.WriteFile("found-by-full-check.txt", "no watcher to report this", t1)
	waitForRemoteItems(t, r, file1)
}

func TestWatchFallsBackToFullCheckIfWatchingFails(t *testing.T) {
	shortenWatchRetryDelay(t)
	r := fstest.NewRun(t)
	opt := testWatchOptions
	opt.FullCheck = 50 * time.Millisecond
	watcher := &fakeWatcher{err: assert.AnError}
	startWatch(t, r, opt, watcher.watch)
	waitForRemoteItems(t, r)

	file1 := r.WriteFile("found-by-full-check.txt", "the watcher failed to start", t1)
	waitForRemoteItems(t, r, file1)
}

func TestWatchNeedsChangeReportsOrFullCheck(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	file1 := r.WriteFile("file.txt", "content", t1)

	err := watch(ctx, r.Fremote, r.Flocal, testWatchOptions, nil)
	require.Error(t, err)
	assert.True(t, fserrors.IsFatalError(err))

	watcher := &fakeWatcher{err: assert.AnError}
	err = watch(ctx, r.Fremote, r.Flocal, testWatchOptions, watcher.watch)
	assert.ErrorIs(t, err, assert.AnError)

	r.CheckLocalItems(t, file1)
	r.CheckRemoteItems(t)
}

func TestWatchReturnsFatalError(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	r.WriteFile("file.txt", "content", t1)

	// Syncing a directory onto itself is refused with a fatal error
	err := watch(ctx, r.Flocal, r.Flocal, testWatchOptions, new(fakeWatcher).watch)
	assert.ErrorIs(t, err, fs.ErrorOverlapping)
}

func TestWatchRecoversAfterFailedBatch(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("can't make a file unreadable")
	}
	shortenWatchRetryDelay(t)
	r := fstest.NewRun(t)
	file1 := r.WriteFile("other.txt", "deleted later", t1)
	watcher := new(fakeWatcher)
	startWatch(t, r, testWatchOptions, watcher.watch)
	waitForRemoteItems(t, r, file1)

	unreadable := r.WriteFile("unreadable.txt", "can't be read at first", t2)
	unreadablePath := filepath.Join(r.LocalName, "unreadable.txt")
	require.NoError(t, os.Chmod(unreadablePath, 0))
	watcher.changed("unreadable.txt")

	// A delete must still go through once a batch has failed
	time.Sleep(4 * testWatchOptions.Settle)
	require.NoError(t, os.Remove(filepath.Join(r.LocalName, "other.txt")))
	watcher.changed("other.txt")
	waitForRemoteItems(t, r)

	// The failed path is retried without a new report
	require.NoError(t, os.Chmod(unreadablePath, 0o644))
	waitForRemoteItems(t, r, unreadable)
}

func TestWatchRetriesFileWhichIsDirectoryInDestination(t *testing.T) {
	shortenWatchRetryDelay(t)
	eachWatchMode(t, func(ctx context.Context, t *testing.T) {
		r := fstest.NewRun(t)
		if !canReplaceTypes(r.Fremote) {
			t.Skip("destination can have a file and a directory with the same name")
		}
		file1 := r.WriteFile("name/sub/file.txt", "in a directory", t1)
		watcher := new(fakeWatcher)
		startWatchWith(ctx, t, r, testWatchOptions, watcher.watch)
		waitForRemoteItems(t, r, file1)

		// As when a symlink to a directory is replaced by a file
		// while symlinks are being followed
		require.NoError(t, os.RemoveAll(filepath.Join(r.LocalName, "name")))
		file2 := r.WriteFile("name", "a file now", t2)
		watcher.changed("name")
		waitForRemoteItems(t, r, file2)
	})
}

func TestWatchWontEmptyDestinationIfSourceLooksEmpty(t *testing.T) {
	shortenWatchRetryDelay(t)
	r := fstest.NewRun(t)
	file1 := r.WriteFile("dir/file.txt", "there before the watch started", t1)
	file2 := r.WriteFile("file.txt", "there before the watch started", t1)
	watcher := new(fakeWatcher)
	startWatch(t, r, testWatchOptions, watcher.watch)
	waitForRemoteItems(t, r, file1, file2)

	// What an unmounted source looks like
	hidden := r.LocalName + "-hidden"
	require.NoError(t, os.Rename(r.LocalName, hidden))
	t.Cleanup(func() { _ = os.RemoveAll(hidden) })
	require.NoError(t, os.Mkdir(r.LocalName, 0o777))
	watcher.send(dirEvents("")...)
	watcher.send(dirEvents("dir")...)
	watcher.changed("file.txt")
	time.Sleep(10 * testWatchOptions.Settle)
	r.CheckRemoteItems(t, file1, file2)

	// Syncing carries on when the source is back
	require.NoError(t, os.Remove(r.LocalName))
	require.NoError(t, os.Rename(hidden, r.LocalName))
	require.NoError(t, os.Remove(filepath.Join(r.LocalName, "file.txt")))
	waitForRemoteItems(t, r, file1)
}

func TestWatchWontEmptyDestinationIfFirstSyncFailed(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("can't make a file unreadable")
	}
	shortenWatchRetryDelay(t)
	ctx := context.Background()
	r := fstest.NewRun(t)
	file1 := r.WriteBoth(ctx, "file.txt", "in both", t1)
	r.WriteFile("unreadable.txt", "can't be read", t1)
	unreadablePath := filepath.Join(r.LocalName, "unreadable.txt")
	require.NoError(t, os.Chmod(unreadablePath, 0))
	watcher := new(fakeWatcher)
	startWatch(t, r, testWatchOptions, watcher.watch)
	time.Sleep(10 * testWatchOptions.Settle)

	// What an unmounted source looks like
	hidden := r.LocalName + "-hidden"
	require.NoError(t, os.Rename(r.LocalName, hidden))
	t.Cleanup(func() { _ = os.RemoveAll(hidden) })
	require.NoError(t, os.Mkdir(r.LocalName, 0o777))
	watcher.send(dirEvents("")...)
	time.Sleep(10 * testWatchOptions.Settle)
	r.CheckRemoteItems(t, file1)
}

func TestWatchSyncsChangesWhileFullSyncKeepsFailing(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("can't make a file unreadable")
	}
	// Long enough that a retry of the full sync isn't what syncs the change
	oldMin, oldMax := watchRetryMinDelay, watchRetryMaxDelay
	watchRetryMinDelay, watchRetryMaxDelay = time.Hour, time.Hour
	t.Cleanup(func() { watchRetryMinDelay, watchRetryMaxDelay = oldMin, oldMax })
	r := fstest.NewRun(t)
	r.WriteFile("unreadable.txt", "can't be read", t1)
	require.NoError(t, os.Chmod(filepath.Join(r.LocalName, "unreadable.txt"), 0))
	watcher := new(fakeWatcher)
	startWatch(t, r, testWatchOptions, watcher.watch)
	time.Sleep(10 * testWatchOptions.Settle)

	added := r.WriteFile("dir/added.txt", "added while the full sync is failing", t2)
	watcher.changed("dir/added.txt")
	waitForRemoteItems(t, r, added)
}

func TestWatchStopsAtMaxTransfer(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.MaxTransfer = 10
	ci.CutoffMode = fs.CutoffModeCautious
	accounting.GlobalStats().ResetCounters()
	t.Cleanup(accounting.GlobalStats().ResetErrors)
	r := fstest.NewRun(t)
	r.WriteFile("big.txt", "more than can be transferred", t1)

	err := watch(ctx, r.Fremote, r.Flocal, testWatchOptions, new(fakeWatcher).watch)
	assert.ErrorIs(t, err, accounting.ErrorMaxTransferLimitReached)
	r.CheckRemoteItems(t)
}

func TestWatchStopsAtMaxDuration(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.MaxDuration = fs.Duration(500 * time.Millisecond)
	r := fstest.NewRun(t)
	file1 := r.WriteFile("file.txt", "content", t1)

	start := time.Now()
	require.NoError(t, watch(ctx, r.Fremote, r.Flocal, testWatchOptions, new(fakeWatcher).watch))
	assert.GreaterOrEqual(t, time.Since(start), 500*time.Millisecond)
	r.CheckRemoteItems(t, file1)
}

func TestWatchStoppingAtMaxDurationLeavesNoErrors(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("can't make a file unreadable")
	}
	shortenWatchRetryDelay(t)
	ctx, ci := fs.AddConfig(context.Background())
	ci.MaxDuration = fs.Duration(500 * time.Millisecond)
	r := fstest.NewRun(t)
	r.WriteFile("unreadable.txt", "can't be read", t1)
	require.NoError(t, os.Chmod(filepath.Join(r.LocalName, "unreadable.txt"), 0))

	// Errors left behind would make the command be run again
	require.NoError(t, watch(ctx, r.Fremote, r.Flocal, testWatchOptions, new(fakeWatcher).watch))
	assert.False(t, accounting.Stats(ctx).Errored())
}

func TestWatchWontDeleteIfSourceHasGone(t *testing.T) {
	for name, mode := range map[string]fs.DeleteMode{"During": fs.DeleteModeDuring, "Before": fs.DeleteModeBefore, "After": fs.DeleteModeAfter} {
		t.Run(name, func(t *testing.T) {
			shortenWatchRetryDelay(t)
			ctx := context.Background()
			r := fstest.NewRun(t)
			file1 := r.WriteBoth(ctx, "dir/file.txt", "in both", t1)
			file2 := r.WriteBoth(ctx, "file.txt", "in both", t1)
			opt := testWatchOptions
			opt.DeleteMode = mode
			watcher := new(fakeWatcher)
			startWatch(t, r, opt, watcher.watch)
			time.Sleep(10 * testWatchOptions.Settle)

			// As when the drive the source is on is unplugged
			hidden := r.LocalName + "-hidden"
			require.NoError(t, os.Rename(r.LocalName, hidden))
			t.Cleanup(func() { _ = os.RemoveAll(hidden) })
			watcher.send(dirEvents("")...)
			watcher.send(dirEvents("dir")...)
			watcher.changed("file.txt")
			time.Sleep(10 * testWatchOptions.Settle)
			r.CheckRemoteItems(t, file1, file2)
			require.NoError(t, os.Rename(hidden, r.LocalName))
		})
	}
}

func TestWatchFailsIfThereIsNoSourceToStartWith(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	file1 := r.WriteObject(ctx, "file.txt", "in the destination", t1)
	fsrc, err := fs.NewFs(ctx, r.LocalName+"/missing")
	require.NoError(t, err)

	err = watch(ctx, r.Fremote, fsrc, testWatchOptions, new(fakeWatcher).watch)
	assert.ErrorIs(t, err, fs.ErrorDirNotFound)
	accounting.GlobalStats().ResetErrors()
	r.CheckRemoteItems(t, file1)
}

func TestWatchKeepsChangesWhenFullSyncFails(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("can't make a file unreadable")
	}
	// Long enough that a retry of the full sync isn't what syncs the change
	oldMin, oldMax := watchRetryMinDelay, watchRetryMaxDelay
	watchRetryMinDelay, watchRetryMaxDelay = time.Hour, time.Hour
	t.Cleanup(func() { watchRetryMinDelay, watchRetryMaxDelay = oldMin, oldMax })
	ctx := context.Background()
	r := fstest.NewRun(t)
	r.WriteBoth(ctx, "deleted.txt", "deleted later", t1)
	r.WriteFile("unreadable.txt", "can't be read", t1)
	require.NoError(t, os.Chmod(filepath.Join(r.LocalName, "unreadable.txt"), 0))
	opt := testWatchOptions
	opt.Settle = 500 * time.Millisecond
	watcher := new(fakeWatcher)

	// Reported before the first full sync is taken, so it is waiting
	// for that when it fails
	require.NoError(t, os.Remove(filepath.Join(r.LocalName, "deleted.txt")))
	startWatch(t, r, opt, func(ctx context.Context, notify func([]fs.ChangeEvent)) error {
		err := watcher.watch(ctx, notify)
		notify(fileEvents("deleted.txt"))
		return err
	})
	waitForRemoteItems(t, r)
}

func TestWatchFailingFileDoesNotHoldUpDirectoryItIsIn(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("can't make a file unreadable")
	}
	// Long enough that a retry isn't what syncs the changes
	oldMin, oldMax := watchRetryMinDelay, watchRetryMaxDelay
	watchRetryMinDelay, watchRetryMaxDelay = time.Hour, time.Hour
	t.Cleanup(func() { watchRetryMinDelay, watchRetryMaxDelay = oldMin, oldMax })
	r := fstest.NewRun(t)
	file1 := r.WriteFile("dir/old.txt", "deleted later", t1)
	watcher := new(fakeWatcher)
	startWatch(t, r, testWatchOptions, watcher.watch)
	waitForRemoteItems(t, r, file1)

	// As when a directory is moved in with a file in it which can't be read
	r.WriteFile("dir/unreadable.txt", "can't be read", t1)
	require.NoError(t, os.Chmod(filepath.Join(r.LocalName, "dir", "unreadable.txt"), 0))
	file2 := r.WriteFile("dir/sub/new.txt", "new", t2)
	watcher.send(dirEvents("dir")...)
	waitForRemoteItems(t, r, file1, file2)

	// What else changes in the directory is still synced, deletes included
	require.NoError(t, os.Remove(filepath.Join(r.LocalName, "dir", "old.txt")))
	file3 := r.WriteFile("dir/added.txt", "added", t2)
	watcher.changed("dir/old.txt", "dir/added.txt")
	waitForRemoteItems(t, r, file2, file3)
	watcher.send(dirEvents("dir")...)
	file4 := r.WriteFile("dir/sub/another.txt", "another", t2)
	waitForRemoteItems(t, r, file2, file3, file4)
}

func TestWatchFirstSyncOfEmptySourceEmptiesDestination(t *testing.T) {
	ctx := context.Background()
	r := fstest.NewRun(t)
	r.WriteObject(ctx, "stale.txt", "only on the destination", t1)

	startWatch(t, r, testWatchOptions, new(fakeWatcher).watch)
	waitForRemoteItems(t, r)
}

func TestWatchMaxDeleteAppliesToEachPass(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.MaxDelete = 1
	accounting.GlobalStats().ResetCounters()
	// The fatal error this ends with would stop later tests deleting
	t.Cleanup(accounting.GlobalStats().ResetErrors)
	r := fstest.NewRun(t)
	kept := r.WriteBoth(ctx, "kept.txt", "kept", t1)
	a := r.WriteObject(ctx, "a.txt", "gone from the source", t1)
	b := r.WriteObject(ctx, "b.txt", "gone from the source", t1)
	r.WriteObject(ctx, "c.txt", "gone from the source", t1)
	r.WriteObject(ctx, "d.txt", "gone from the source", t1)
	w := &watcher{fdst: r.Fremote, fsrc: r.Flocal, opt: testWatchOptions}

	// One delete in each of two passes is within the limit
	require.NoError(t, w.sync(ctx, []watchItem{file("c.txt")}, nil, false, time.Time{}))
	require.NoError(t, w.sync(ctx, []watchItem{file("d.txt")}, nil, false, time.Time{}))
	r.CheckRemoteItems(t, kept, a, b)

	// Two in one pass is not
	err := w.sync(ctx, []watchItem{file("a.txt"), file("b.txt")}, nil, false, time.Time{})
	require.Error(t, err)
	assert.True(t, fserrors.IsFatalError(err))
}

func TestWatchWithLocalWatcher(t *testing.T) {
	r := fstest.NewRun(t)
	if r.Flocal.Features().WatchChanges == nil {
		t.Skip("source can't report changes")
	}
	file1 := r.WriteFile("dir/existing.txt", "there before the watch started", t1)

	ctx, cancel := context.WithCancel(context.Background())
	r.ForceMkdir(ctx, r.Fremote)
	done := make(chan error, 1)
	go func() {
		done <- Watch(ctx, r.Fremote, r.Flocal, testWatchOptions)
	}()
	defer func() {
		cancel()
		assert.NoError(t, <-done)
	}()
	waitForRemoteItems(t, r, file1)

	file2 := r.WriteFile("dir/new.txt", "created while watching", t2)
	waitForRemoteItems(t, r, file1, file2)

	file1 = r.WriteFile("dir/existing.txt", "changed while watching", t3)
	waitForRemoteItems(t, r, file1, file2)

	require.NoError(t, os.Remove(filepath.Join(r.LocalName, "dir", "new.txt")))
	waitForRemoteItems(t, r, file1)

	// A new directory, filled before it can have been noticed
	file3 := r.WriteFile("new/sub/file.txt", "in a new directory", t1)
	waitForRemoteItems(t, r, file1, file3)

	require.NoError(t, os.Rename(filepath.Join(r.LocalName, "new"), filepath.Join(r.LocalName, "renamed")))
	file3.Path = "renamed/sub/file.txt"
	waitForRemoteItems(t, r, file1, file3)

	require.NoError(t, os.RemoveAll(filepath.Join(r.LocalName, "renamed")))
	waitForRemoteItems(t, r, file1)
	r.CheckRemoteListing(t, []fstest.Item{file1}, []string{"dir"})
}
