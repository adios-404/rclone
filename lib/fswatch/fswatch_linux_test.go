//go:build linux

package fswatch

import (
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// testWatcher is a watcher running in the background which can be
// stopped to look at its state.
type testWatcher struct {
	*watcher
	c       *collector
	stopped chan struct{}
}

// startWatcher runs a watcher on root after applying setup to it
func startWatcher(t *testing.T, root string, opt Options, setup func(w *watcher)) *testWatcher {
	tw := newTestWatcher(t, root, opt, setup)
	tw.goRun()
	return tw
}

// newTestWatcher starts a watcher on root after applying setup to
// it. Changes aren't reported until goRun is called.
func newTestWatcher(t *testing.T, root string, opt Options, setup func(w *watcher)) *testWatcher {
	c := new(collector)
	w := newWatcher(root, opt, c.add)
	if setup != nil {
		setup(w)
	}
	require.NoError(t, w.start())
	tw := &testWatcher{watcher: w, c: c, stopped: make(chan struct{})}
	t.Cleanup(tw.stop)
	return tw
}

// goRun starts reading changes, as Watch does straight away
func (tw *testWatcher) goRun() {
	go func() {
		tw.run()
		close(tw.stopped)
	}()
}

// stopAndWait stops the watcher, after which its state can be read
func (tw *testWatcher) stopAndWait() {
	tw.stop()
	<-tw.stopped
}

// requireMatchesTree checks that a stopped watcher has a node, at the
// right path, for every directory in the tree and no others.
func (tw *testWatcher) requireMatchesTree(t *testing.T) {
	t.Helper()
	tw.requireConsistent(t)
	var want []string
	require.NoError(t, filepath.WalkDir(tw.root, func(path string, d os.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			want = append(want, path)
		}
		return err
	}))
	var got []string
	addPath := func(idx uint32) {
		path, known := tw.path(idx)
		require.True(t, known, "node %d isn't in the tree", idx)
		got = append(got, path)
	}
	for _, idx := range tw.byWd {
		addPath(idx)
	}
	for idx := range tw.unwatched {
		addPath(idx)
	}
	slices.Sort(want)
	slices.Sort(got)
	assert.Equal(t, want, got)
}

// requireConsistent checks the nodes of a stopped watcher make sense
func (tw *testWatcher) requireConsistent(t *testing.T) {
	t.Helper()
	live := make(map[uint32]bool)
	for wd, idx := range tw.byWd {
		require.Equal(t, wd, tw.nodes[idx].wd)
		live[idx] = true
	}
	for idx := range tw.unwatched {
		require.Equal(t, int32(noWatch), tw.nodes[idx].wd)
		live[idx] = true
	}
	require.True(t, live[rootNode])
	require.Empty(t, tw.lost, "nodes are still waiting to be found")
	require.Empty(t, tw.oldLost, "nodes are still waiting to be found")
	linked := 0
	for idx := range live {
		// Every node is reached from the root by its name
		steps := 0
		for at := idx; at != rootNode; at = tw.nodes[at].parent {
			n := tw.nodes[at]
			require.True(t, live[n.parent], "node %d has a parent which isn't in use", at)
			require.Equal(t, at, tw.children[childKey{n.parent, n.name}], "node %d can't be found by its name", at)
			steps++
			require.LessOrEqual(t, steps, len(tw.nodes), "node %d is above itself", idx)
		}
		// Every node in it says so
		for child := tw.nodes[idx].first; child != noNode; child = tw.nodes[child].next {
			require.True(t, live[child], "node %d has a node in it which isn't in use", idx)
			require.Equal(t, idx, tw.nodes[child].parent, "node %d is in a node which isn't its parent", child)
			linked++
			require.LessOrEqual(t, linked, len(tw.nodes), "nodes in node %d go round in a circle", idx)
		}
	}
	require.Equal(t, len(live)-1, linked)
	require.Len(t, tw.children, len(live)-1)
	require.Len(t, tw.free, len(tw.nodes)-len(live))
}

// limitWatches makes w fail to watch any more directories while
// *atLimit is set, as the kernel does at the limit on watches.
func limitWatches(w *watcher, mu *sync.Mutex, atLimit *bool) {
	watched := make(map[int]bool)
	w.addWatch = func(fd int, path string, mask uint32) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		wd, err := unix.InotifyAddWatch(fd, path, mask)
		if err != nil {
			return wd, err
		}
		// Watching a directory which is already watched works at the
		// limit, and so does the first watch, which is on the root
		if *atLimit && !watched[wd] && len(watched) > 0 {
			_, _ = unix.InotifyRmWatch(fd, uint32(wd))
			return 0, unix.ENOSPC
		}
		watched[wd] = true
		return wd, nil
	}
}

func TestWatchDirectoryRenameMovesWatches(t *testing.T) {
	shortenRetryDelay(t)
	root := tempRoot(t)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "old", "sub", "deep"), 0o777))
	require.NoError(t, os.Mkdir(filepath.Join(root, "other"), 0o777))
	tw := startWatcher(t, root, Options{}, nil)

	newDir := filepath.Join(root, "other", "renamed")
	require.NoError(t, os.Rename(filepath.Join(root, "old"), newDir))
	tw.c.requireEvent(t, "new name", rescanAt(newDir))
	requireReportedSoon(t, tw.c, filepath.Join(newDir, "sub", "deep"))

	// Wait long enough for it to have been taken for a move out of the tree
	time.Sleep(2 * moveWait)
	requireReportedSoon(t, tw.c, filepath.Join(newDir, "sub"))
	assert.False(t, tw.c.has(rescanAt(root)), "watching was restarted")
	tw.stopAndWait()
	tw.requireMatchesTree(t)
	assert.Len(t, tw.byWd, 5)
	assert.Len(t, tw.children, 4)
	assert.Empty(t, tw.lost)
	assert.Empty(t, tw.oldLost)
}

func TestWatchDirectoryRenameOverEmptyDirectory(t *testing.T) {
	root := tempRoot(t)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "from", "sub"), 0o777))
	require.NoError(t, os.Mkdir(filepath.Join(root, "to"), 0o777))
	tw := startWatcher(t, root, Options{}, nil)

	// os.Rename refuses to replace a directory
	to := filepath.Join(root, "to")
	require.NoError(t, unix.Rename(filepath.Join(root, "from"), to))
	tw.c.requireEvent(t, "new name", rescanAt(to))
	requireReportedSoon(t, tw.c, filepath.Join(to, "sub"))
	requireReportedSoon(t, tw.c, to)
	tw.stopAndWait()
	tw.requireMatchesTree(t)
	assert.Len(t, tw.byWd, 3)
	assert.Len(t, tw.children, 2)
}

func TestWatchDirectoryMovedOutIsNoLongerWatched(t *testing.T) {
	parent := tempRoot(t)
	root := filepath.Join(parent, "root")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "dir", "sub", "deep"), 0o777))
	require.NoError(t, os.Mkdir(filepath.Join(root, "stays"), 0o777))
	tw := startWatcher(t, root, Options{}, nil)

	require.NoError(t, os.Rename(filepath.Join(root, "dir"), filepath.Join(parent, "outside")))
	tw.c.requireEvent(t, "directory moved out", dirAt(filepath.Join(root, "dir")))
	time.Sleep(2 * moveWait)
	requireReportedSoon(t, tw.c, filepath.Join(root, "stays"))
	tw.stopAndWait()
	tw.requireMatchesTree(t)
	assert.Len(t, tw.byWd, 2)
	assert.Len(t, tw.children, 1)
	assert.Empty(t, tw.oldLost)
}

func TestWatchDeletedDirectoriesAreForgotten(t *testing.T) {
	root := tempRoot(t)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "a", "b", "c"), 0o777))
	tw := startWatcher(t, root, Options{}, nil)

	require.NoError(t, os.RemoveAll(filepath.Join(root, "a")))
	tw.c.requireEvent(t, "directory delete", dirAt(filepath.Join(root, "a")))
	requireReportedSoon(t, tw.c, root)
	tw.stopAndWait()
	tw.requireMatchesTree(t)
	assert.Len(t, tw.byWd, 1)
	assert.Empty(t, tw.children)
	assert.Len(t, tw.free, 3)
}

func TestWatchRootAtWatchLimit(t *testing.T) {
	root := tempRoot(t)
	w := newWatcher(root, Options{}, func([]Event) {})
	w.addWatch = func(fd int, path string, mask uint32) (int, error) {
		return 0, unix.ENOSPC // what the kernel returns at the limit on watches
	}
	err := w.start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fs.inotify.max_user_watches")
}

func TestWatchDirectoriesOverWatchLimitAreRescanned(t *testing.T) {
	shortenRetryDelay(t)
	root := tempRoot(t)
	sub := filepath.Join(root, "sub")
	require.NoError(t, os.MkdirAll(filepath.Join(sub, "deep"), 0o777))
	var mu sync.Mutex
	atLimit := true
	tw := startWatcher(t, root, Options{}, func(w *watcher) { limitWatches(w, &mu, &atLimit) })

	// Changes in a directory which can't be watched are found by
	// looking at all of it every so often
	tw.c.requireEvent(t, "rescan of unwatched directory", rescanAt(sub))
	tw.c.reset()
	tw.c.requireEvent(t, "second rescan of unwatched directory", rescanAt(sub))
	assert.False(t, tw.c.has(rescanAt(root)), "watching was restarted")

	// A new directory can't be watched either
	added := filepath.Join(root, "added")
	require.NoError(t, os.Mkdir(added, 0o777))
	tw.c.requireEvent(t, "rescan of new directory", rescanAt(added))
	tw.c.reset()
	tw.c.requireEvent(t, "second rescan of new directory", rescanAt(added))

	// It is found under its new name once renamed
	renamed := filepath.Join(root, "renamed")
	require.NoError(t, os.Rename(added, renamed))
	tw.c.requireEvent(t, "rescan of renamed directory", rescanAt(renamed))
	time.Sleep(2 * moveWait)
	tw.c.reset()
	tw.c.requireEvent(t, "second rescan of renamed directory", rescanAt(renamed))
	assert.False(t, tw.c.has(dirAt(added)), "reported under the old name")

	// It is forgotten once deleted
	tw.c.reset()
	require.NoError(t, os.Remove(renamed))
	tw.c.requireEvent(t, "directory delete", func(ev Event) bool { return ev.Path == renamed && !ev.Rescan })
	tw.c.reset()
	tw.c.requireEvent(t, "rescan of unwatched directory", rescanAt(sub))
	tw.c.reset()
	tw.c.requireEvent(t, "second rescan of unwatched directory", rescanAt(sub))
	assert.False(t, tw.c.has(dirAt(renamed)), "deleted directory still being rescanned")

	// When there is room again everything is watched
	mu.Lock()
	atLimit = false
	mu.Unlock()
	requireReportedSoon(t, tw.c, filepath.Join(sub, "deep"))
	tw.stopAndWait()
	tw.requireMatchesTree(t)
	assert.Empty(t, tw.unwatched)
	assert.Len(t, tw.byWd, 3)
}

func TestWatchUnwatchedDirectoryMovedOutIsForgotten(t *testing.T) {
	shortenRetryDelay(t)
	parent := tempRoot(t)
	root := filepath.Join(parent, "root")
	sub := filepath.Join(root, "sub")
	require.NoError(t, os.MkdirAll(sub, 0o777))
	var mu sync.Mutex
	atLimit := true
	tw := startWatcher(t, root, Options{}, func(w *watcher) { limitWatches(w, &mu, &atLimit) })
	tw.c.requireEvent(t, "rescan of unwatched directory", rescanAt(sub))

	require.NoError(t, os.Rename(sub, filepath.Join(parent, "outside")))
	tw.c.requireEvent(t, "directory moved out", func(ev Event) bool { return ev.Path == sub && !ev.Rescan })
	time.Sleep(2 * moveWait)
	requireReportedSoon(t, tw.c, root)
	tw.stopAndWait()
	tw.requireMatchesTree(t)
	assert.Empty(t, tw.unwatched)
	assert.Empty(t, tw.children)
}

func TestWatchOneFileSystem(t *testing.T) {
	root := tempRoot(t)
	other := filepath.Join(root, "other")
	require.NoError(t, os.MkdirAll(filepath.Join(other, "sub"), 0o777))
	require.NoError(t, os.Mkdir(filepath.Join(root, "same"), 0o777))
	otherID, err := identify(other)
	require.NoError(t, err)
	onOtherDevice := func(w *watcher) {
		w.device = func(fd int) (uint64, error) {
			id, err := identify(procFd + "/" + strconv.Itoa(fd))
			if id == otherID {
				return 2, err
			}
			return 1, err
		}
	}

	tw := startWatcher(t, root, Options{OneFileSystem: true}, onOtherDevice)
	require.NoError(t, os.WriteFile(filepath.Join(other, "file.txt"), nil, 0o666))
	requireReportedSoon(t, tw.c, filepath.Join(root, "same"))
	assert.False(t, tw.c.has(fileAt(filepath.Join(other, "file.txt"))), "change on another file system reported")
	tw.stopAndWait()
	tw.requireConsistent(t)
	assert.Len(t, tw.byWd, 2)

	tw = startWatcher(t, root, Options{}, onOtherDevice)
	requireReportedSoon(t, tw.c, filepath.Join(other, "sub"))
}

func TestWatchUnmount(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("mounting needs root")
	}
	root := tempRoot(t)
	mountPoint := filepath.Join(root, "mnt")
	require.NoError(t, os.Mkdir(mountPoint, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(mountPoint, "hidden.txt"), nil, 0o666))
	require.NoError(t, exec.Command("mount", "-t", "tmpfs", "tmpfs", mountPoint).Run())
	unmount := func() error { return exec.Command("umount", mountPoint).Run() }
	t.Cleanup(func() { _ = unmount() })
	require.NoError(t, os.Mkdir(filepath.Join(mountPoint, "sub"), 0o777))
	tw := startWatcher(t, root, Options{}, nil)
	requireReportedSoon(t, tw.c, filepath.Join(mountPoint, "sub"))

	// The directory which was mounted on is watched instead
	require.NoError(t, unmount())
	tw.c.requireEvent(t, "unmount", rescanAt(mountPoint))
	requireReportedSoon(t, tw.c, mountPoint)
	tw.stopAndWait()
	tw.requireMatchesTree(t)
	assert.Len(t, tw.byWd, 2)
}

func TestWatchQueueOverflowRescansRootAndKeepsWatching(t *testing.T) {
	root := tempRoot(t)
	require.NoError(t, os.Mkdir(filepath.Join(root, "sub"), 0o777))
	c := new(collector)
	w := newWatcher(root, Options{}, c.add)
	require.NoError(t, w.start())
	t.Cleanup(w.stop)

	// What the kernel sends when its event queue has overflowed
	events, restarted := w.handle(nil, -1, unix.IN_Q_OVERFLOW, 0, "")
	assert.True(t, restarted)
	assert.Equal(t, []Event{{Path: root, IsDir: true, Known: true, Rescan: true}}, events)

	go w.run()
	requireReportedSoon(t, c, filepath.Join(root, "sub"))
}

func TestWatchRootOfFileSystem(t *testing.T) {
	w := &watcher{root: "/", nodes: []node{{}, {parent: rootNode, name: "a"}, {parent: 1, name: "b"}}}
	path, known := w.path(rootNode)
	assert.True(t, known)
	assert.Equal(t, "", path)
	path, known = w.path(2)
	assert.True(t, known)
	assert.Equal(t, "/a/b", path)
	assert.Equal(t, "/", w.rescanRoot().Path)
}

func TestWatcherRunReturnsWhenStopped(t *testing.T) {
	tw := startWatcher(t, tempRoot(t), Options{}, nil)
	tw.stop()
	select {
	case <-tw.stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after stop")
	}
}

func TestWatcherRunReturnsWhenStoppedWhileRecovering(t *testing.T) {
	root := tempRoot(t)
	tw := startWatcher(t, root, Options{}, nil)
	require.NoError(t, os.Remove(root))
	tw.c.requireEvent(t, "failure to watch a missing root", rescanAt(root))

	tw.stop()
	select {
	case <-tw.stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after stop")
	}
}

func TestWatchDirectoryMovedOutAndBackInKeepsNodes(t *testing.T) {
	parent := tempRoot(t)
	root := filepath.Join(parent, "root")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "dir", "sub", "deep"), 0o777))
	tw := startWatcher(t, root, Options{}, nil)

	// Out and back in the same read, so it is still being watched when it returns
	outside, back := filepath.Join(parent, "outside"), filepath.Join(root, "back")
	require.NoError(t, os.Rename(filepath.Join(root, "dir"), outside))
	require.NoError(t, os.Rename(outside, back))
	tw.c.requireEvent(t, "directory moved back in", rescanAt(back))
	time.Sleep(2 * moveWait)
	requireReportedSoon(t, tw.c, filepath.Join(back, "sub", "deep"))

	// Renaming it again finds the right nodes
	again := filepath.Join(root, "again")
	require.NoError(t, os.Mkdir(filepath.Join(root, "other"), 0o777))
	require.NoError(t, os.Rename(back, again))
	tw.c.requireEvent(t, "directory renamed", rescanAt(again))
	requireReportedSoon(t, tw.c, filepath.Join(again, "sub", "deep"))
	requireReportedSoon(t, tw.c, filepath.Join(root, "other"))
	tw.stopAndWait()
	tw.requireMatchesTree(t)
	assert.Len(t, tw.byWd, 5)
}

// The tests below make changes before the watcher reads any of them,
// as happens when it is behind, so that it looks for what changed
// after things have moved on.

func TestWatchDirectoryCreatedThenParentRenamed(t *testing.T) {
	root := tempRoot(t)
	require.NoError(t, os.Mkdir(filepath.Join(root, "a"), 0o777))
	tw := newTestWatcher(t, root, Options{}, nil)

	require.NoError(t, os.MkdirAll(filepath.Join(root, "a", "new", "deep"), 0o777))
	require.NoError(t, os.Rename(filepath.Join(root, "a"), filepath.Join(root, "b")))
	tw.goRun()
	tw.c.requireEvent(t, "new name", rescanAt(filepath.Join(root, "b")))
	requireReportedSoon(t, tw.c, filepath.Join(root, "b", "new", "deep"))
	tw.stopAndWait()
	tw.requireMatchesTree(t)
}

func TestWatchDirectoryCreatedInDirectoryMovedOutAndBack(t *testing.T) {
	parent := tempRoot(t)
	root := filepath.Join(parent, "root")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "dir"), 0o777))
	tw := newTestWatcher(t, root, Options{}, nil)

	outside := filepath.Join(parent, "outside")
	require.NoError(t, os.Rename(filepath.Join(root, "dir"), outside))
	require.NoError(t, os.Mkdir(filepath.Join(outside, "new"), 0o777))
	require.NoError(t, os.Rename(outside, filepath.Join(root, "back")))
	tw.goRun()
	tw.c.requireEvent(t, "moved back", rescanAt(filepath.Join(root, "back")))
	requireReportedSoon(t, tw.c, filepath.Join(root, "back", "new"))
	tw.stopAndWait()
	tw.requireMatchesTree(t)
}

func TestWatchDirectoryCreatedThenParentReplacedByAnother(t *testing.T) {
	root := tempRoot(t)
	require.NoError(t, os.Mkdir(filepath.Join(root, "a"), 0o777))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "b", "x"), 0o777))
	tw := newTestWatcher(t, root, Options{}, nil)

	// By the time the new a/x is looked for, a/x is what was b/x
	require.NoError(t, os.Mkdir(filepath.Join(root, "a", "x"), 0o777))
	require.NoError(t, os.Rename(filepath.Join(root, "a"), filepath.Join(root, "old")))
	require.NoError(t, os.Rename(filepath.Join(root, "b"), filepath.Join(root, "a")))
	tw.goRun()
	requireReportedSoon(t, tw.c, filepath.Join(root, "a", "x"))
	requireReportedSoon(t, tw.c, filepath.Join(root, "old", "x"))
	tw.stopAndWait()
	tw.requireMatchesTree(t)
}

func TestWatchDirectoriesExchanged(t *testing.T) {
	root := tempRoot(t)
	a, b := filepath.Join(root, "a"), filepath.Join(root, "b")
	require.NoError(t, os.MkdirAll(filepath.Join(a, "in-a"), 0o777))
	require.NoError(t, os.MkdirAll(filepath.Join(b, "in-b"), 0o777))
	tw := startWatcher(t, root, Options{}, nil)

	err := unix.Renameat2(unix.AT_FDCWD, a, unix.AT_FDCWD, b, unix.RENAME_EXCHANGE)
	if err != nil {
		t.Skipf("can't exchange directories on this file system: %v", err)
	}
	tw.c.requireEvent(t, "new a", rescanAt(a))
	tw.c.requireEvent(t, "new b", rescanAt(b))
	time.Sleep(2 * moveWait)
	requireReportedSoon(t, tw.c, filepath.Join(a, "in-b"))
	requireReportedSoon(t, tw.c, filepath.Join(b, "in-a"))

	// Each can still be told apart when it moves again
	require.NoError(t, os.Rename(a, filepath.Join(root, "c")))
	tw.c.requireEvent(t, "renamed", rescanAt(filepath.Join(root, "c")))
	requireReportedSoon(t, tw.c, filepath.Join(root, "c", "in-b"))
	tw.stopAndWait()
	tw.requireMatchesTree(t)
}

func TestWatchDirectoryMadeReadable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read anything")
	}
	root := tempRoot(t)
	dir := filepath.Join(root, "dir")
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o777))
	require.NoError(t, os.Chmod(dir, 0))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o777) })
	tw := startWatcher(t, root, Options{}, nil)

	require.NoError(t, os.Chmod(dir, 0o777))
	tw.c.requireEvent(t, "made readable", rescanAt(dir))
	requireReportedSoon(t, tw.c, filepath.Join(dir, "sub"))
	tw.stopAndWait()
	tw.requireMatchesTree(t)
}

func TestWatchDirectoryMadeSearchable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read anything")
	}
	root := tempRoot(t)
	dir := filepath.Join(root, "dir")
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub", "deep"), 0o777))
	// It can be listed, but what is in it can't be opened
	require.NoError(t, os.Chmod(dir, 0o444))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o777) })
	tw := startWatcher(t, root, Options{}, nil)

	require.NoError(t, os.Chmod(dir, 0o777))
	tw.c.requireEvent(t, "made searchable", rescanAt(dir))
	requireReportedSoon(t, tw.c, filepath.Join(dir, "sub", "deep"))
	tw.stopAndWait()
	tw.requireMatchesTree(t)
}

func TestWatchRecoversFromFailingToOpenDirectories(t *testing.T) {
	shortenRetryDelay(t)
	root := tempRoot(t)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "old", "sub"), 0o777))
	tw := startWatcher(t, root, Options{}, nil)

	// Use up the file descriptors, leaving one to open the root with
	var was unix.Rlimit
	require.NoError(t, unix.Getrlimit(unix.RLIMIT_NOFILE, &was))
	limit := was
	limit.Cur = 256
	require.NoError(t, unix.Setrlimit(unix.RLIMIT_NOFILE, &limit))
	restore := func() { _ = unix.Setrlimit(unix.RLIMIT_NOFILE, &was) }
	t.Cleanup(restore)
	var fds []int
	for {
		fd, err := unix.Open("/dev/null", unix.O_RDONLY, 0)
		if err != nil {
			break
		}
		fds = append(fds, fd)
	}
	_ = unix.Close(fds[len(fds)-1])
	fds = fds[:len(fds)-1]
	added := filepath.Join(root, "added")
	require.NoError(t, unix.Mkdir(added, 0o777))
	require.NoError(t, unix.Rename(filepath.Join(root, "old"), filepath.Join(root, "renamed")))
	time.Sleep(200 * time.Millisecond)
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
	restore()

	// The directories which couldn't be opened are found before long
	tw.c.requireEvent(t, "watching restarted", rescanAt(root))
	requireReportedSoon(t, tw.c, added)
	requireReportedSoon(t, tw.c, filepath.Join(root, "renamed", "sub"))
	tw.stopAndWait()
	tw.requireMatchesTree(t)
}

func TestWatchAtWatchLimitDoesNotStartAgain(t *testing.T) {
	shortenRetryDelay(t)
	root := tempRoot(t)
	require.NoError(t, os.Mkdir(filepath.Join(root, "d"), 0o777))
	var mu sync.Mutex
	atLimit := false
	tw := newTestWatcher(t, root, Options{}, func(w *watcher) { limitWatches(w, &mu, &atLimit) })

	// The directory a change was in has gone by the time the change is
	// handled, and what couldn't be watched takes its place in the nodes
	require.NoError(t, os.Mkdir(filepath.Join(root, "d", "x"), 0o777))
	require.NoError(t, os.Remove(filepath.Join(root, "d", "x")))
	require.NoError(t, os.Remove(filepath.Join(root, "d")))
	mu.Lock()
	atLimit = true
	mu.Unlock()
	require.NoError(t, os.Mkdir(filepath.Join(root, "u"), 0o777))
	tw.goRun()
	tw.c.requireEvent(t, "unwatched", rescanAt(filepath.Join(root, "u")))
	time.Sleep(500 * time.Millisecond)
	assert.False(t, tw.c.has(rescanAt(root)), "watching was started again from scratch")
}

func TestWatchRemount(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("mounting needs root")
	}
	shortenRetryDelay(t)
	root := tempRoot(t)
	mnt := filepath.Join(root, "mnt")
	require.NoError(t, os.Mkdir(mnt, 0o777))
	if err := unix.Mount("tmpfs", mnt, "tmpfs", 0, ""); err != nil {
		t.Skipf("can't mount: %v", err)
	}
	t.Cleanup(func() { _ = unix.Unmount(mnt, unix.MNT_DETACH) })
	require.NoError(t, os.Mkdir(filepath.Join(mnt, "sub"), 0o777))
	tw := startWatcher(t, root, Options{}, nil)
	requireReportedSoon(t, tw.c, filepath.Join(mnt, "sub"))

	// A different file system mounted in place of the first
	require.NoError(t, unix.Unmount(mnt, 0))
	require.NoError(t, unix.Mount("tmpfs", mnt, "tmpfs", 0, ""))
	require.NoError(t, os.MkdirAll(filepath.Join(mnt, "new", "deep"), 0o777))
	tw.c.requireEvent(t, "remounted", rescanAt(root))
	requireReportedSoon(t, tw.c, filepath.Join(mnt, "new", "deep"))
	tw.stopAndWait()
	tw.requireMatchesTree(t)
}

func TestWatchDirectoryInTwoPlaces(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("mounting needs root")
	}
	shortenRetryDelay(t)
	root := tempRoot(t)
	a, b := filepath.Join(root, "a"), filepath.Join(root, "b")
	require.NoError(t, os.MkdirAll(filepath.Join(a, "sub"), 0o777))
	require.NoError(t, os.Mkdir(b, 0o777))
	if err := unix.Mount(a, b, "", unix.MS_BIND, ""); err != nil {
		t.Skipf("can't mount: %v", err)
	}
	t.Cleanup(func() { _ = unix.Unmount(b, unix.MNT_DETACH) })
	tw := startWatcher(t, root, Options{}, nil)

	// It is watched in one place and rescanned in the other
	require.NoError(t, os.WriteFile(filepath.Join(a, "sub", "file.txt"), nil, 0o666))
	covered := func(place string) func(Event) bool {
		return func(event Event) bool {
			return fileAt(filepath.Join(place, "sub", "file.txt"))(event) || rescanAt(place)(event) || rescanAt(filepath.Join(place, "sub"))(event)
		}
	}
	tw.c.requireEvent(t, "changed in one place", covered(a))
	tw.c.requireEvent(t, "changed in the other place", covered(b))
}

func TestWatchSkipsUnwantedDirectories(t *testing.T) {
	root := tempRoot(t)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "wanted", "skipped", "deep"), 0o777))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "skipped", "sub"), 0o777))
	skip := func(path string) bool { return filepath.Base(path) == "skipped" }
	tw := startWatcher(t, root, Options{Skip: skip}, nil)

	// Only the root and the wanted directory are watched
	require.NoError(t, os.WriteFile(filepath.Join(root, "skipped", "sub", "file.txt"), nil, 0o666))
	require.NoError(t, os.Mkdir(filepath.Join(root, "wanted", "new"), 0o777))
	requireReportedSoon(t, tw.c, filepath.Join(root, "wanted", "new"))
	assert.False(t, tw.c.has(fileAt(filepath.Join(root, "skipped", "sub", "file.txt"))))

	// A directory is watched once it is renamed to something which is wanted
	require.NoError(t, os.Rename(filepath.Join(root, "skipped"), filepath.Join(root, "kept")))
	tw.c.requireEvent(t, "renamed to a wanted name", rescanAt(filepath.Join(root, "kept")))
	requireReportedSoon(t, tw.c, filepath.Join(root, "kept", "sub"))
	// and not once it is renamed to something which isn't
	require.NoError(t, os.Rename(filepath.Join(root, "wanted", "new"), filepath.Join(root, "wanted", "skipped", "new")))
	requireReportedSoon(t, tw.c, filepath.Join(root, "wanted"))
	// It is kept for a while in case it turns up somewhere else
	time.Sleep(3 * moveWait)
	tw.stopAndWait()
	tw.requireConsistent(t)
	assert.Len(t, tw.byWd, 4, "the root, wanted, kept and kept/sub")
}

func TestWatchMountsBelow(t *testing.T) {
	// There is always something mounted at or below the root
	assert.NotEmpty(t, mountsBelow("/"))
	assert.Empty(t, mountsBelow(tempRoot(t)))
}

// TestWatchRandomChanges makes random changes to the directories in
// a tree, some of them while the watcher is held up, and checks that
// it ends up watching exactly the directories which are there.
func TestWatchRandomChanges(t *testing.T) {
	for seed := range uint64(20) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			parent := tempRoot(t)
			root, outside := filepath.Join(parent, "root"), filepath.Join(parent, "outside")
			require.NoError(t, os.Mkdir(root, 0o777))
			require.NoError(t, os.Mkdir(outside, 0o777))
			r := rand.New(rand.NewPCG(seed, 1))
			// Holding this stops the watcher reporting, and so reading, changes
			var hold sync.Mutex
			tw := newTestWatcher(t, root, Options{}, nil)
			report := tw.fn
			tw.fn = func(events []Event) {
				hold.Lock()
				defer hold.Unlock()
				report(events)
			}
			tw.goRun()

			dirs := []string{root}
			var out []string
			next := 0
			name := func() string {
				next++
				return fmt.Sprintf("d%d", next)
			}
			// remove takes the directories at and below dir out of dirs
			remove := func(dir string) {
				dirs = slices.DeleteFunc(dirs, func(d string) bool {
					return d == dir || strings.HasPrefix(d, dir+"/")
				})
			}
			// add puts the directories at and below dir into dirs
			add := func(dir string) {
				require.NoError(t, filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
					if err == nil && d.IsDir() {
						dirs = append(dirs, path)
					}
					return err
				}))
			}
			pick := func() string { return dirs[r.IntN(len(dirs))] }
			for range 300 {
				if r.IntN(10) == 0 {
					// Let it get behind for a while
					hold.Lock()
					time.AfterFunc(time.Duration(r.IntN(20))*time.Millisecond, hold.Unlock)
				}
				dir := pick()
				switch op := r.IntN(10); {
				case op < 4 || len(dirs) < 3:
					newDir := filepath.Join(dir, name())
					require.NoError(t, os.Mkdir(newDir, 0o777))
					dirs = append(dirs, newDir)
				case dir == root:
				case op < 6:
					// Rename, to somewhere not inside itself
					to := pick()
					if to == dir || strings.HasPrefix(to, dir+"/") {
						to = root
					}
					to = filepath.Join(to, name())
					remove(dir)
					require.NoError(t, os.Rename(dir, to))
					add(to)
				case op < 7:
					other := pick()
					if other == root || strings.HasPrefix(other, dir+"/") || strings.HasPrefix(dir, other+"/") || other == dir {
						continue
					}
					if err := unix.Renameat2(unix.AT_FDCWD, dir, unix.AT_FDCWD, other, unix.RENAME_EXCHANGE); err != nil {
						continue
					}
					remove(dir)
					remove(other)
					add(dir)
					add(other)
				case op < 8:
					remove(dir)
					require.NoError(t, os.RemoveAll(dir))
				case op < 9:
					to := filepath.Join(outside, name())
					remove(dir)
					require.NoError(t, os.Rename(dir, to))
					out = append(out, to)
				case len(out) > 0:
					// Move one back in, with something new in it
					from := out[len(out)-1]
					out = out[:len(out)-1]
					require.NoError(t, os.Mkdir(filepath.Join(from, name()), 0o777))
					to := filepath.Join(dir, name())
					require.NoError(t, os.Rename(from, to))
					add(to)
				}
			}

			// Once it has caught up it is watching every directory
			hold.Lock()
			hold.Unlock() //nolint:staticcheck // waits for any hold to end
			probe := filepath.Join(root, "caught-up")
			require.NoError(t, os.WriteFile(probe, nil, 0o666))
			tw.c.requireEvent(t, "last change", fileAt(probe))
			time.Sleep(2 * moveWait)
			tw.stopAndWait()
			tw.requireMatchesTree(t)
		})
	}
}
