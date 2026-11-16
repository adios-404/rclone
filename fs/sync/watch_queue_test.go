package sync

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSettle   = 2 * time.Second
	testMaxDelay = time.Minute
)

var watchT0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// fileEvents returns events for changes to the files at paths
func fileEvents(paths ...string) []fs.ChangeEvent {
	events := make([]fs.ChangeEvent, len(paths))
	for i, path := range paths {
		events[i] = fs.ChangeEvent{Path: path, EntryType: fs.EntryObject}
	}
	return events
}

// dirEvents returns events for everything below the directories at
// paths needing to be looked at.
func dirEvents(paths ...string) []fs.ChangeEvent {
	events := make([]fs.ChangeEvent, len(paths))
	for i, path := range paths {
		events[i] = fs.ChangeEvent{Path: path, EntryType: fs.EntryDirectory, Recursive: true}
	}
	return events
}

func file(path string) watchItem { return watchItem{path: path} }
func tree(path string) watchItem { return watchItem{path: path, kind: watchTree} }

func TestWatchQueueReleasesAfterSettle(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	q.add(watchT0, fileEvents("a.txt"))

	assert.Empty(t, q.take(watchT0.Add(testSettle-time.Nanosecond)))
	assert.Equal(t, []watchItem{file("a.txt")}, q.take(watchT0.Add(testSettle)))
	assert.Empty(t, q.take(watchT0.Add(time.Hour)), "released twice")
}

func TestWatchQueueRepeatedChangesRestartTheWait(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	q.add(watchT0, fileEvents("a.txt"))
	q.add(watchT0.Add(time.Second), fileEvents("a.txt"))

	assert.Empty(t, q.take(watchT0.Add(testSettle)))
	assert.Equal(t, []watchItem{file("a.txt")}, q.take(watchT0.Add(time.Second+testSettle)))
}

func TestWatchQueueMaxDelay(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	// A file which never goes quiet
	for now := watchT0; now.Before(watchT0.Add(testMaxDelay)); now = now.Add(time.Second) {
		q.add(now, fileEvents("busy.log"))
		assert.Empty(t, q.take(now))
	}
	assert.Equal(t, []watchItem{file("busy.log")}, q.take(watchT0.Add(testMaxDelay)))
}

func TestWatchQueueReleasesOnlyWhatIsDue(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	q.add(watchT0, fileEvents("b.txt", "a.txt", "b.txt"))
	q.add(watchT0.Add(time.Second), fileEvents("later.txt"))

	assert.Equal(t, []watchItem{file("a.txt"), file("b.txt")}, q.take(watchT0.Add(testSettle)))
	assert.Equal(t, []watchItem{file("later.txt")}, q.take(watchT0.Add(time.Second+testSettle)))
}

func TestWatchQueueDirectoryCoversWhatIsBelowIt(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	q.add(watchT0, fileEvents("dir/a.txt", "dir/sub/b.txt", "dir", "directory.txt", "other/c.txt"))
	q.add(watchT0, dirEvents("dir/sub"))
	q.add(watchT0.Add(time.Second), dirEvents("dir"))
	// Covered by the directory, whose wait it restarts
	q.add(watchT0.Add(2*time.Second), fileEvents("dir/new.txt"))
	q.add(watchT0.Add(2*time.Second), dirEvents("dir/sub/deeper"))

	assert.Equal(t, []watchItem{file("directory.txt"), file("other/c.txt")}, q.take(watchT0.Add(testSettle)))
	assert.Empty(t, q.take(watchT0.Add(time.Second+testSettle)))
	assert.Equal(t, []watchItem{tree("dir")}, q.take(watchT0.Add(2*time.Second+testSettle)))
}

func TestWatchQueueDirectoryKeepsFirstChangeForMaxDelay(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	q.add(watchT0, fileEvents("dir/busy.log"))
	for now := watchT0.Add(time.Second); now.Before(watchT0.Add(testMaxDelay)); now = now.Add(time.Second) {
		q.add(now, dirEvents("dir"))
		assert.Empty(t, q.take(now))
	}
	assert.Equal(t, []watchItem{tree("dir")}, q.take(watchT0.Add(testMaxDelay)))
}

func TestWatchQueueChangeToDirectoryItself(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	self := []fs.ChangeEvent{{Path: "dir", EntryType: fs.EntryDirectory}}
	q.add(watchT0, self)
	q.add(watchT0, fileEvents("dir/file.txt"))
	assert.Equal(t, []watchItem{{path: "dir", kind: watchDir}, file("dir/file.txt")}, q.take(watchT0.Add(testSettle)))

	// A change to everything in it covers the directory too
	q.add(watchT0, self)
	q.add(watchT0, dirEvents("dir"))
	q.add(watchT0, self)
	assert.Equal(t, []watchItem{tree("dir")}, q.take(watchT0.Add(testSettle)))

	// If it isn't clear whether it is a file or a directory, all of it is looked at
	q.add(watchT0, self)
	q.add(watchT0, fileEvents("dir", "dir/file.txt"))
	assert.Equal(t, []watchItem{tree("dir")}, q.take(watchT0.Add(testSettle)))
}

func TestWatchQueueRootMeansFullSync(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	q.add(watchT0, fileEvents("a.txt"))
	q.add(watchT0.Add(time.Second), dirEvents(""))
	q.add(watchT0.Add(time.Second), fileEvents("b.txt"))

	// The changes wait for the full sync, which covers them
	assert.Empty(t, q.take(watchT0.Add(testSettle)))
	items := q.take(watchT0.Add(time.Second + testSettle))
	require.Equal(t, []watchItem{tree("")}, items)
	assert.True(t, items[0].isFull())
	assert.False(t, file("").isFull())
	assert.False(t, tree("a").isFull())

	// They are forgotten once it has worked, unless they changed again
	q.add(watchT0.Add(time.Minute), fileEvents("b.txt"))
	q.synced(watchT0.Add(time.Second + testSettle))
	assert.Equal(t, []watchItem{file("b.txt")}, q.take(watchT0.Add(time.Hour)))
	assert.Empty(t, q.entries)
}

func TestWatchQueueAddFullIsDueAtOnce(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	q.add(watchT0, fileEvents("a.txt"))
	q.addFull(watchT0.Add(time.Second))
	next, ok := q.next(watchT0)
	require.True(t, ok)
	assert.Equal(t, watchT0.Add(time.Second), next)
	assert.Equal(t, []watchItem{tree("")}, q.take(watchT0.Add(time.Second)))
}

func TestWatchQueueTooManyChangesAreMergedIntoDirectories(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	// 10 files in each of 1,000 directories
	for i := range watchMaxItems {
		q.add(watchT0, fileEvents(fmt.Sprintf("top%d/dir%d/file%d", i%10, i%1000, i)))
	}
	assert.Len(t, q.entries, watchMaxItems)

	// One more and the deepest are merged into the directories they are in
	q.add(watchT0.Add(time.Second), fileEvents("top0/dir0/one too many"))
	assert.Nil(t, q.full)
	items := q.take(watchT0.Add(time.Second + testSettle))
	require.Len(t, items, 1000)
	assert.Equal(t, tree("top0/dir0"), items[0])
	for _, item := range items {
		assert.Equal(t, watchTree, item.kind)
		assert.Equal(t, 1, strings.Count(item.path, "/"), item.path)
	}
	assert.Empty(t, q.entries)

	// If they are all in the root it takes a full sync
	for i := range watchMaxItems + 1 {
		q.add(watchT0, fileEvents(fmt.Sprintf("file%d", i)))
	}
	assert.Equal(t, []watchItem{tree("")}, q.take(watchT0.Add(testSettle)))
}

func TestWatchQueueNext(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	_, ok := q.next(watchT0)
	assert.False(t, ok)

	q.add(watchT0.Add(time.Second), fileEvents("b.txt"))
	q.add(watchT0, fileEvents("a.txt"))
	next, ok := q.next(watchT0)
	require.True(t, ok)
	assert.Equal(t, watchT0.Add(testSettle), next)

	q.take(watchT0.Add(testSettle))
	next, ok = q.next(watchT0)
	require.True(t, ok)
	assert.Equal(t, watchT0.Add(time.Second+testSettle), next)
}

func TestWatchQueueRetry(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	delay := func(item watchItem) time.Duration { return time.Duration(item.failures) * 10 * time.Second }
	blamed := func(watchItem) bool { return false }
	q.add(watchT0, fileEvents("a.txt"))
	q.add(watchT0, dirEvents("dir"))
	takenAt := watchT0.Add(testSettle)
	items := q.take(takenAt)
	require.Len(t, items, 2)

	failedAt := takenAt.Add(time.Second)
	q.retry(failedAt, takenAt, items, blamed, delay)
	assert.ElementsMatch(t, []string{"a.txt", "dir"}, q.held(failedAt))
	assert.Empty(t, q.take(failedAt.Add(10*time.Second-time.Nanosecond)))
	next, ok := q.next(failedAt)
	require.True(t, ok)
	assert.Equal(t, failedAt.Add(10*time.Second), next)
	takenAt = failedAt.Add(10 * time.Second)
	assert.Empty(t, q.held(takenAt))
	items = q.take(takenAt)
	// It is looked at as a tree in case it isn't the file it was thought to be
	assert.Equal(t, []watchItem{{path: "a.txt", kind: watchTree, failures: 1}, {path: "dir", kind: watchTree, failures: 1}}, items)

	// The delay depends on how many times it has failed
	failedAt = takenAt
	q.retry(failedAt, takenAt, items, blamed, delay)
	assert.Empty(t, q.take(failedAt.Add(20*time.Second-time.Nanosecond)))
	items = q.take(failedAt.Add(20 * time.Second))
	assert.Equal(t, []watchItem{{path: "a.txt", kind: watchTree, failures: 2}, {path: "dir", kind: watchTree, failures: 2}}, items)

	// It is forgotten once it has been synced
	q.add(failedAt.Add(time.Minute), fileEvents("a.txt"))
	assert.Equal(t, []watchItem{file("a.txt")}, q.take(failedAt.Add(time.Minute+testSettle)))
}

func TestWatchQueueWhatFailedDoesNotHoldUpChanges(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	delay := func(item watchItem) time.Duration { return time.Duration(item.failures) * time.Hour }
	blamed := func(watchItem) bool { return false }
	q.add(watchT0, fileEvents("dir/failing.txt"))
	takenAt := watchT0.Add(testSettle)
	q.retry(takenAt, takenAt, q.take(takenAt), blamed, delay)

	// A change to it is a reason to try it again
	q.add(watchT0.Add(time.Minute), fileEvents("dir/failing.txt", "dir/other.txt"))
	takenAt = watchT0.Add(time.Minute + testSettle)
	items := q.take(takenAt)
	assert.Equal(t, []watchItem{file("dir/failing.txt"), file("dir/other.txt")}, items)
	// If it fails again that is counted with the times it failed before
	q.retry(takenAt, takenAt, items, func(item watchItem) bool { return item.path == "dir/other.txt" }, delay)
	assert.Equal(t, []watchItem{{path: "dir/other.txt", quick: 1}}, q.take(takenAt.Add(testSettle)))
	assert.Equal(t, []string{"dir/failing.txt"}, q.held(takenAt.Add(time.Hour)))
	assert.Equal(t, []watchItem{{path: "dir/failing.txt", kind: watchTree, failures: 2}}, q.take(takenAt.Add(2*time.Hour)))

	// What failed inside a tree is kept apart from the tree
	q.add(takenAt, dirEvents("tree"))
	q.blame(takenAt, "tree/sub/failing.txt", delay)
	q.blame(takenAt.Add(time.Second), "tree/sub/failing.txt", delay)
	assert.Equal(t, []watchItem{tree("tree")}, q.take(takenAt.Add(testSettle)))
	assert.Equal(t, []string{"tree/sub/failing.txt"}, q.held(takenAt.Add(testSettle)))
	assert.Equal(t, []watchItem{{path: "tree/sub/failing.txt", kind: watchTree, failures: 2}}, q.take(takenAt.Add(time.Second+2*time.Hour)))

	// A full sync which works has synced whatever had failed
	q.retry(takenAt, takenAt, []watchItem{file("again.txt")}, blamed, delay)
	q.synced(takenAt.Add(time.Second))
	assert.Empty(t, q.held(takenAt.Add(time.Second)))
	_, ok := q.next(takenAt)
	assert.False(t, ok)
}

func TestWatchQueueRetryWhenWhatFailedIsUnknown(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	delay := func(item watchItem) time.Duration { return time.Duration(item.failures) * 10 * time.Second }
	q.add(watchT0, fileEvents("a.txt"))
	takenAt := watchT0.Add(testSettle)
	items := q.take(takenAt)

	// The first failure isn't delayed as it may not have been this which failed
	q.retry(takenAt, takenAt, items, nil, delay)
	takenAt = takenAt.Add(testSettle)
	items = q.take(takenAt)
	require.Equal(t, []watchItem{{path: "a.txt", kind: watchTree, failures: 1}}, items)

	// After that it is delayed as if it had failed one time fewer
	q.retry(takenAt, takenAt, items, nil, delay)
	assert.Empty(t, q.take(takenAt.Add(10*time.Second-time.Nanosecond)))
	takenAt = takenAt.Add(10 * time.Second)
	items = q.take(takenAt)
	require.Equal(t, []watchItem{{path: "a.txt", kind: watchTree, failures: 2}}, items)
	q.retry(takenAt, takenAt, items, nil, delay)
	assert.Empty(t, q.take(takenAt.Add(20*time.Second-time.Nanosecond)))
	assert.Equal(t, []watchItem{{path: "a.txt", kind: watchTree, failures: 3}}, q.take(takenAt.Add(20*time.Second)))
}

func TestWatchQueueRetryOfWhatChangedMeanwhile(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	delay := func(item watchItem) time.Duration { return time.Duration(item.failures) * 10 * time.Second }
	q.add(watchT0, fileEvents("changed.txt", "quiet.txt", "dir/quiet.txt"))
	q.add(watchT0, dirEvents("changed-dir", "quiet-dir", "covered/dir"))
	takenAt := watchT0.Add(testSettle)
	items := q.take(takenAt)
	require.Len(t, items, 6)

	// What changes while it is being synced probably failed because of that
	q.add(takenAt.Add(time.Second), fileEvents("changed.txt", "changed-dir/sub/file.txt", "dir/other.txt", "quiet-dir-not/file.txt"))
	q.add(takenAt.Add(time.Second), dirEvents("covered"))
	failedAt := takenAt.Add(2 * time.Second)
	q.retry(failedAt, takenAt, items, func(watchItem) bool { return false }, delay)

	quick := func(item watchItem) watchItem {
		item.quick = 1
		return item
	}
	assert.Equal(t, []watchItem{quick(tree("changed-dir")), quick(file("changed.txt")), quick(tree("covered")), file("dir/other.txt"), file("quiet-dir-not/file.txt")},
		q.take(failedAt.Add(testSettle)))
	assert.Equal(t, []watchItem{{path: "dir/quiet.txt", kind: watchTree, failures: 1}, {path: "quiet-dir", kind: watchTree, failures: 1}, {path: "quiet.txt", kind: watchTree, failures: 1}},
		q.take(failedAt.Add(10*time.Second)))

	// What is known not to have failed is released like a change too
	q.add(watchT0, fileEvents("blamed.txt", "blameless.txt"))
	items = q.take(watchT0.Add(testSettle))
	require.Len(t, items, 2)
	q.retry(watchT0.Add(3*time.Second), watchT0.Add(testSettle), items, func(item watchItem) bool { return item.path == "blameless.txt" }, delay)
	assert.Equal(t, []watchItem{quick(file("blameless.txt"))}, q.take(watchT0.Add(3*time.Second+testSettle)))
	assert.Equal(t, []watchItem{{path: "blamed.txt", kind: watchTree, failures: 1}}, q.take(watchT0.Add(13*time.Second)))

	// It is only retried like this a few times, in case that isn't why it failed
	q = newWatchQueue(testSettle, testMaxDelay)
	item := file("busy.txt")
	items = []watchItem{item}
	for i := range watchQuickRetries {
		q.add(watchT0.Add(time.Second), fileEvents("busy.txt"))
		q.retry(watchT0.Add(2*time.Second), watchT0, items, nil, delay)
		items = q.take(watchT0.Add(2*time.Second + testSettle))
		item.quick = i + 1
		require.Equal(t, []watchItem{item}, items)
	}
	q.retry(watchT0.Add(2*time.Second), watchT0, items, func(watchItem) bool { return false }, delay)
	assert.Empty(t, q.take(watchT0.Add(12*time.Second-time.Nanosecond)))
	assert.Equal(t, []watchItem{{path: "busy.txt", kind: watchTree, failures: 1}}, q.take(watchT0.Add(12*time.Second)))
}

func TestWatchQueueRetryOfWhatIsNotShownToHaveFailed(t *testing.T) {
	delay := func(item watchItem) time.Duration { return time.Duration(item.failures) * 10 * time.Second }
	// takeInnocent takes what is due, leaving out the file which keeps changing
	takeInnocent := func(q *watchQueue, now time.Time) []watchItem {
		return slices.DeleteFunc(q.take(now), func(item watchItem) bool { return item.path == "busy.txt" })
	}
	for name, blameless := range map[string]func(watchItem) bool{
		"Blameless":                 func(watchItem) bool { return true },
		"UnknownWhileOthersChanged": nil,
	} {
		t.Run(name, func(t *testing.T) {
			q := newWatchQueue(testSettle, testMaxDelay)
			q.add(watchT0, fileEvents("innocent.txt"))
			now := watchT0.Add(testSettle)
			items := takeInnocent(q, now)
			// However many times it is in a sync which fails, the wait doesn't grow
			for range 5 {
				for i := range watchQuickRetries {
					q.add(now, fileEvents("busy.txt"))
					q.retry(now, now, items, blameless, delay)
					now = now.Add(testSettle)
					items = takeInnocent(q, now)
					require.Equal(t, []watchItem{{path: "innocent.txt", quick: i + 1}}, items)
				}
				q.add(now, fileEvents("busy.txt"))
				q.retry(now, now, items, blameless, delay)
				assert.Equal(t, []string{"innocent.txt"}, q.held(now))
				assert.Empty(t, takeInnocent(q, now.Add(10*time.Second-time.Nanosecond)))
				now = now.Add(10 * time.Second)
				items = takeInnocent(q, now)
				require.Equal(t, []watchItem{file("innocent.txt")}, items)
			}
		})
	}
}

func TestWatchQueueFullSyncBeingRetriedDoesNotHoldUpChanges(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	delay := func(item watchItem) time.Duration { return time.Hour }
	q.add(watchT0, fileEvents("pending.txt"))
	q.addFull(watchT0)
	items := q.take(watchT0)
	require.Equal(t, []watchItem{tree("")}, items)
	// A change while it runs doesn't make it be retried any sooner
	q.add(watchT0, fileEvents("busy.txt"))
	q.retry(watchT0, watchT0, items, nil, delay)

	// Changes are synced on their own until the full sync is due
	// again, including those which were waiting for it
	q.add(watchT0.Add(time.Minute), fileEvents("a.txt"))
	q.add(watchT0.Add(time.Minute), dirEvents("dir"))
	assert.True(t, q.fullHeld(watchT0.Add(time.Minute)))
	next, ok := q.next(watchT0.Add(time.Minute))
	require.True(t, ok)
	assert.Equal(t, watchT0.Add(testSettle), next)
	assert.Equal(t, []watchItem{file("a.txt"), file("busy.txt"), tree("dir"), file("pending.txt")}, q.take(watchT0.Add(time.Minute+testSettle)))
	next, ok = q.next(watchT0.Add(2 * time.Minute))
	require.True(t, ok)
	assert.Equal(t, watchT0.Add(time.Hour), next)

	// After which they wait for it, as it covers them
	q.add(watchT0.Add(time.Hour-time.Second), fileEvents("b.txt"))
	assert.False(t, q.fullHeld(watchT0.Add(time.Hour)))
	assert.Equal(t, []watchItem{{kind: watchTree, failures: 1}}, q.take(watchT0.Add(time.Hour)))
	q.synced(watchT0.Add(time.Hour))
	_, ok = q.next(watchT0.Add(time.Hour))
	assert.False(t, ok)

	// A change to everything while it is held back waits with it
	q.retry(watchT0.Add(time.Hour), watchT0.Add(time.Hour), []watchItem{{kind: watchTree, failures: 1}}, nil, delay)
	q.add(watchT0.Add(time.Hour+time.Minute), dirEvents(""))
	q.add(watchT0.Add(time.Hour+time.Minute), fileEvents("c.txt"))
	assert.Equal(t, []watchItem{file("c.txt")}, q.take(watchT0.Add(time.Hour+time.Minute+testSettle)))
	assert.Equal(t, []watchItem{{kind: watchTree, failures: 2}}, q.take(watchT0.Add(2*time.Hour)))
}

func TestWatchQueueTreeIsNotHeldUpByWhatFailedBelowIt(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	delay := func(item watchItem) time.Duration { return time.Hour }
	q.add(watchT0, fileEvents("dir/failing.txt"))
	items := q.take(watchT0.Add(testSettle))
	q.retry(watchT0.Add(testSettle), watchT0.Add(testSettle), items, func(watchItem) bool { return false }, delay)
	assert.Empty(t, q.take(watchT0.Add(time.Minute)))

	q.add(watchT0.Add(time.Minute), dirEvents("dir"))
	assert.Equal(t, []watchItem{tree("dir")}, q.take(watchT0.Add(time.Minute+testSettle)))
	assert.Equal(t, []string{"dir/failing.txt"}, q.held(watchT0.Add(time.Minute+testSettle)))
}

func TestWatchQueueIgnoresChangeToRootItself(t *testing.T) {
	q := newWatchQueue(testSettle, testMaxDelay)
	q.add(watchT0, []fs.ChangeEvent{{Path: "", EntryType: fs.EntryDirectory}})
	_, ok := q.next(watchT0)
	assert.False(t, ok)
	assert.Empty(t, q.take(watchT0.Add(time.Hour)))
}

func TestWatchRetryDelay(t *testing.T) {
	w := &watcher{opt: WatchOptions{FullCheck: time.Hour}}
	delay := func(failures int) time.Duration { return w.retryDelay(watchItem{path: "file.txt", failures: failures}) }
	assert.Equal(t, 5*time.Second, delay(1))
	assert.Equal(t, 10*time.Second, delay(2))
	assert.Equal(t, 160*time.Second, delay(6))
	assert.Equal(t, 5*time.Minute, delay(7))
	assert.Equal(t, 5*time.Minute, delay(1000))

	// A full sync isn't retried more often than it is run
	delay = func(failures int) time.Duration { return w.retryDelay(watchItem{kind: watchTree, failures: failures}) }
	assert.Equal(t, 5*time.Second, delay(1))
	assert.Equal(t, 2560*time.Second, delay(10))
	assert.Equal(t, time.Hour, delay(11))
	assert.Equal(t, time.Hour, delay(1000))
	w.opt.FullCheck = 0
	assert.Equal(t, 5*time.Minute, delay(1000))
}
