//go:build linux

package fswatch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collector gathers the events reported by a watcher
type collector struct {
	mu     sync.Mutex
	events []Event
}

func (c *collector) add(events []Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, events...)
}

// reset forgets the events seen so far
func (c *collector) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = nil
}

// has reports whether an event matching want has been seen
func (c *collector) has(want func(Event) bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ev := range c.events {
		if want(ev) {
			return true
		}
	}
	return false
}

// requireEvent waits for an event matching want
func (c *collector) requireEvent(t *testing.T, what string, want func(Event) bool) {
	t.Helper()
	require.Eventually(t, func() bool { return c.has(want) }, 10*time.Second, 5*time.Millisecond, "no event for %s", what)
}

// fileAt matches a change to the file at path
func fileAt(path string) func(Event) bool {
	return func(ev Event) bool { return ev.Path == path && !ev.Rescan && (!ev.Known || !ev.IsDir) }
}

// dirAt matches a change to the directory at path
func dirAt(path string) func(Event) bool {
	return func(ev Event) bool { return ev.Path == path && (!ev.Known || ev.IsDir) }
}

// rescanAt matches a request to look at everything at and below path
func rescanAt(path string) func(Event) bool {
	return func(ev Event) bool { return ev.Path == path && ev.Rescan && (!ev.Known || ev.IsDir) }
}

// tempRoot makes a temporary directory, returning its real path
func tempRoot(t *testing.T) string {
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	return root
}

// startWatch watches a new temporary directory, in which setup has been run
func startWatch(t *testing.T, setup func(root string)) (root string, c *collector) {
	root = tempRoot(t)
	if setup != nil {
		setup(root)
	}
	return root, watch(t, root)
}

// watch watches root until the test finishes.
//
// It waits for a change to be reported and then forgets what has been
// reported, as changes made just before watching started can be.
func watch(t *testing.T, root string) *collector {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := new(collector)
	require.NoError(t, Watch(ctx, root, Options{}, c.add))
	requireReportedSoon(t, c, root)
	c.reset()
	return c
}

// requireReportedSoon creates new files in dir until one is reported,
// for use when watching dir may not have been established yet.
func requireReportedSoon(t *testing.T, c *collector, dir string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		path := filepath.Join(dir, fmt.Sprintf("probe%d", i))
		if err := os.WriteFile(path, nil, 0o666); err != nil {
			time.Sleep(time.Millisecond)
			continue
		}
		reported := time.Now().Add(250 * time.Millisecond)
		for time.Now().Before(reported) {
			if c.has(fileAt(path)) {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	t.Fatalf("changes in %s are not being reported", dir)
}

// shortenRetryDelay makes a failed watcher retry quickly for this test
func shortenRetryDelay(t *testing.T) {
	old := retryDelay
	retryDelay = 20 * time.Millisecond
	t.Cleanup(func() { retryDelay = old })
}

func TestSupported(t *testing.T) {
	assert.True(t, Supported())
}

func TestWatchMissingRoot(t *testing.T) {
	err := Watch(context.Background(), filepath.Join(t.TempDir(), "missing"), Options{}, func([]Event) {})
	assert.Error(t, err)
}

func TestWatchReportsChangeMadeAsSoonAsWatching(t *testing.T) {
	root := tempRoot(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := new(collector)
	require.NoError(t, Watch(ctx, root, Options{}, c.add))
	path := filepath.Join(root, "file.txt")
	require.NoError(t, os.WriteFile(path, nil, 0o666))
	c.requireEvent(t, "create", fileAt(path))
}

func TestWatchReportsFileCreateWriteDelete(t *testing.T) {
	root, c := startWatch(t, nil)
	path := filepath.Join(root, "file.txt")

	require.NoError(t, os.WriteFile(path, []byte("one"), 0o666))
	c.requireEvent(t, "create", fileAt(path))

	c.reset()
	require.NoError(t, os.WriteFile(path, []byte("two"), 0o666))
	c.requireEvent(t, "write", fileAt(path))

	c.reset()
	require.NoError(t, os.Remove(path))
	c.requireEvent(t, "delete", fileAt(path))
}

func TestWatchReportsModTimeChange(t *testing.T) {
	var path string
	_, c := startWatch(t, func(root string) {
		path = filepath.Join(root, "file.txt")
		require.NoError(t, os.WriteFile(path, []byte("one"), 0o666))
	})
	when := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	require.NoError(t, os.Chtimes(path, when, when))
	c.requireEvent(t, "modification time change", fileAt(path))
}

func TestWatchReportsFileInExistingSubdirectory(t *testing.T) {
	root, c := startWatch(t, func(root string) {
		require.NoError(t, os.MkdirAll(filepath.Join(root, "a", "b"), 0o777))
	})
	path := filepath.Join(root, "a", "b", "file.txt")
	require.NoError(t, os.WriteFile(path, nil, 0o666))
	c.requireEvent(t, "create in subdirectory", fileAt(path))
}

func TestWatchReportsFileRename(t *testing.T) {
	var oldPath string
	root, c := startWatch(t, func(root string) {
		oldPath = filepath.Join(root, "old.txt")
		require.NoError(t, os.WriteFile(oldPath, []byte("one"), 0o666))
	})
	newPath := filepath.Join(root, "new.txt")
	require.NoError(t, os.Rename(oldPath, newPath))
	c.requireEvent(t, "old name", fileAt(oldPath))
	c.requireEvent(t, "new name", fileAt(newPath))
}

func TestWatchUnusualNames(t *testing.T) {
	root, c := startWatch(t, nil)
	// The accented names are in different Unicode forms. They differ in
	// more than that as some file systems take both forms of a name to
	// be the same file.
	for _, name := range []string{"with space.txt", "caf\u00e9.txt", "the\u0301.txt", "\u65e5\u672c\u8a9e.txt", "#hash &amp.txt"} {
		path := filepath.Join(root, name)
		require.NoError(t, os.WriteFile(path, nil, 0o666))
		c.requireEvent(t, name, fileAt(path))
	}
}

func TestWatchNewDirectoryIsRescannedAndWatched(t *testing.T) {
	root, c := startWatch(t, nil)
	dir := filepath.Join(root, "new")
	require.NoError(t, os.Mkdir(dir, 0o777))
	// Anything put in the directory before it was noticed may not be reported
	c.requireEvent(t, "new directory", rescanAt(dir))
	path := filepath.Join(dir, "file.txt")
	require.NoError(t, os.WriteFile(path, nil, 0o666))
	c.requireEvent(t, "create in new directory", fileAt(path))
}

func TestWatchNewDirectoryTreeIsWatched(t *testing.T) {
	root, c := startWatch(t, nil)
	deep := filepath.Join(root, "x", "y", "z")
	require.NoError(t, os.MkdirAll(deep, 0o777))
	requireReportedSoon(t, c, deep)
}

func TestWatchReportsDirectoryDelete(t *testing.T) {
	root, c := startWatch(t, func(root string) {
		require.NoError(t, os.Mkdir(filepath.Join(root, "gone"), 0o777))
	})
	dir := filepath.Join(root, "gone")
	require.NoError(t, os.Remove(dir))
	c.requireEvent(t, "directory delete", dirAt(dir))
}

func TestWatchDirectoryRename(t *testing.T) {
	root, c := startWatch(t, func(root string) {
		require.NoError(t, os.MkdirAll(filepath.Join(root, "old", "sub"), 0o777))
	})
	oldDir, newDir := filepath.Join(root, "old"), filepath.Join(root, "renamed")
	require.NoError(t, os.Rename(oldDir, newDir))
	c.requireEvent(t, "old name", dirAt(oldDir))
	c.requireEvent(t, "new name", rescanAt(newDir))

	// Changes below the renamed directory come out under its new name
	c.reset()
	requireReportedSoon(t, c, filepath.Join(newDir, "sub"))
	requireReportedSoon(t, c, newDir)
	assert.False(t, c.has(func(ev Event) bool {
		return ev.Path == oldDir || strings.HasPrefix(ev.Path, oldDir+string(filepath.Separator))
	}), "change reported under the old name")
}

func TestWatchDirectoryMovedIn(t *testing.T) {
	// On the same file system as the root so that it can be renamed in
	parent := tempRoot(t)
	root, outside := filepath.Join(parent, "root"), filepath.Join(parent, "outside")
	require.NoError(t, os.Mkdir(root, 0o777))
	require.NoError(t, os.MkdirAll(filepath.Join(outside, "sub"), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(outside, "sub", "file.txt"), nil, 0o666))
	c := watch(t, root)

	// What was already in the directory is not reported item by item
	dir := filepath.Join(root, "moved")
	require.NoError(t, os.Rename(outside, dir))
	c.requireEvent(t, "directory moved in", rescanAt(dir))
	requireReportedSoon(t, c, filepath.Join(dir, "sub"))
}

func TestWatchDirectoryMovedOut(t *testing.T) {
	parent := tempRoot(t)
	root := filepath.Join(parent, "root")
	dir := filepath.Join(root, "dir")
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o777))
	c := watch(t, root)

	outside := filepath.Join(parent, "outside")
	require.NoError(t, os.Rename(dir, outside))
	c.requireEvent(t, "directory moved out", dirAt(dir))

	// Changes to it stop being reported, though not necessarily at once
	stopped := false
	for i := 0; i < 40 && !stopped; i++ {
		c.reset()
		require.NoError(t, os.WriteFile(filepath.Join(outside, "sub", fmt.Sprintf("file%d", i)), nil, 0o666))
		requireReportedSoon(t, c, root)
		stopped = !c.has(func(ev Event) bool {
			return ev.Path != root && !strings.HasPrefix(ev.Path, root+string(filepath.Separator)+"probe")
		})
	}
	assert.True(t, stopped, "changes to a directory moved out are still being reported")
}

func TestWatchDirectoryMovedOutAndBackIn(t *testing.T) {
	parent := tempRoot(t)
	root := filepath.Join(parent, "root")
	dir := filepath.Join(root, "dir")
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o777))
	c := watch(t, root)

	outside, back := filepath.Join(parent, "outside"), filepath.Join(root, "back")
	require.NoError(t, os.Rename(dir, outside))
	require.NoError(t, os.Rename(outside, back))
	c.requireEvent(t, "directory moved back in", rescanAt(back))

	// It is still watched once it could have been given up on
	time.Sleep(500 * time.Millisecond)
	c.reset()
	requireReportedSoon(t, c, filepath.Join(back, "sub"))
	requireReportedSoon(t, c, back)
}

func TestWatchRootIsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks need privileges on Windows")
	}
	real := tempRoot(t)
	root := filepath.Join(filepath.Dir(real), filepath.Base(real)+"-link")
	require.NoError(t, os.Symlink(real, root))
	t.Cleanup(func() { _ = os.Remove(root) })
	c := watch(t, root)

	require.NoError(t, os.WriteFile(filepath.Join(real, "file.txt"), nil, 0o666))
	c.requireEvent(t, "create under a symlinked root", fileAt(filepath.Join(root, "file.txt")))
}

func TestWatchSymlinkToDirectoryIsNotFollowed(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks need privileges on Windows")
	}
	outside := tempRoot(t)
	root, c := startWatch(t, func(root string) {
		require.NoError(t, os.Symlink(outside, filepath.Join(root, "link")))
	})
	require.NoError(t, os.WriteFile(filepath.Join(outside, "file.txt"), nil, 0o666))
	requireReportedSoon(t, c, root)
	assert.False(t, c.has(func(ev Event) bool { return filepath.Dir(ev.Path) == filepath.Join(root, "link") }))
}

func TestWatchRecoversWhenRootComesBack(t *testing.T) {
	shortenRetryDelay(t)
	parent := tempRoot(t)
	root := filepath.Join(parent, "root")
	require.NoError(t, os.Mkdir(root, 0o777))
	c := watch(t, root)

	// Everything must be looked at again when the root is replaced
	require.NoError(t, os.Rename(root, filepath.Join(parent, "old-root")))
	c.requireEvent(t, "root gone", rescanAt(root))
	c.reset()
	require.NoError(t, os.Mkdir(root, 0o777))
	c.requireEvent(t, "root back", rescanAt(root))
	requireReportedSoon(t, c, root)
}

func TestWatchRecoversWhenDirectoryAboveRootIsReplaced(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows doesn't allow a directory above one which is being watched to be renamed")
	}
	shortenRetryDelay(t)
	top := tempRoot(t)
	root := filepath.Join(top, "parent", "root")
	require.NoError(t, os.MkdirAll(root, 0o777))
	c := watch(t, root)

	require.NoError(t, os.Rename(filepath.Join(top, "parent"), filepath.Join(top, "old-parent")))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "sub"), 0o777))
	c.requireEvent(t, "root replaced", rescanAt(root))
	requireReportedSoon(t, c, filepath.Join(root, "sub"))

	// Changes to what used to be the root stop being reported
	stopped := false
	for i := 0; i < 40 && !stopped; i++ {
		c.reset()
		name := fmt.Sprintf("old%d", i)
		require.NoError(t, os.WriteFile(filepath.Join(top, "old-parent", "root", name), nil, 0o666))
		requireReportedSoon(t, c, root)
		stopped = !c.has(fileAt(filepath.Join(root, name)))
	}
	assert.True(t, stopped, "changes to the directory which was at the root are still being reported")
}

func TestWatchStopsReportingAfterCancel(t *testing.T) {
	root := tempRoot(t)
	ctx, cancel := context.WithCancel(context.Background())
	c := new(collector)
	require.NoError(t, Watch(ctx, root, Options{}, c.add))
	requireReportedSoon(t, c, root)

	cancel()
	// Cancelling takes effect asynchronously, so keep making changes
	// until one goes unreported.
	deadline := time.Now().Add(10 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		path := filepath.Join(root, fmt.Sprintf("late%d", i))
		require.NoError(t, os.WriteFile(path, nil, 0o666))
		time.Sleep(250 * time.Millisecond)
		if !c.has(fileAt(path)) {
			return
		}
	}
	t.Fatal("changes still being reported after cancel")
}

func TestWatchCancelledBeforeStarting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Watch(ctx, tempRoot(t), Options{}, func([]Event) {})
	assert.ErrorIs(t, err, context.Canceled)
}
