//go:build darwin && cgo

package fswatch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Flags which convert doesn't need to know about
const (
	flagItemInodeMetaMod = 0x400  // kFSEventStreamEventFlagItemInodeMetaMod
	flagItemModified     = 0x1000 // kFSEventStreamEventFlagItemModified
)

func TestConvert(t *testing.T) {
	const (
		root     = "/link/to/root"
		realRoot = "/private/real/root"
	)
	rescanRoot := Event{Path: root, IsDir: true, Known: true, Rescan: true}
	for _, test := range []struct {
		name  string
		path  string
		flags uint32
		want  Event
		drop  bool
	}{
		{
			name:  "file created",
			path:  realRoot + "/file.txt",
			flags: flagItemIsFile | flagItemCreated | flagItemModified,
			want:  Event{Path: root + "/file.txt", Known: true},
		},
		{
			name:  "file in subdirectory removed",
			path:  realRoot + "/a/b/file.txt",
			flags: flagItemIsFile | flagItemRemoved,
			want:  Event{Path: root + "/a/b/file.txt", Known: true},
		},
		{
			name:  "symlink created",
			path:  realRoot + "/link",
			flags: flagItemIsSymlink | flagItemCreated,
			want:  Event{Path: root + "/link", Known: true},
		},
		{
			name:  "hard link created",
			path:  realRoot + "/hard",
			flags: flagItemIsHardlink | flagItemCreated,
			want:  Event{Path: root + "/hard", Known: true},
		},
		{
			name:  "last hard link removed",
			path:  realRoot + "/hard",
			flags: flagItemIsLastHardlink | flagItemRemoved,
			want:  Event{Path: root + "/hard", Known: true},
		},
		{
			name:  "unknown kind",
			path:  realRoot + "/what",
			flags: flagItemModified,
			want:  Event{Path: root + "/what"},
		},
		{
			name:  "directory created",
			path:  realRoot + "/dir",
			flags: flagItemIsDir | flagItemCreated,
			want:  Event{Path: root + "/dir", IsDir: true, Known: true, Rescan: true},
		},
		{
			name:  "directory renamed",
			path:  realRoot + "/a/dir",
			flags: flagItemIsDir | flagItemRenamed,
			want:  Event{Path: root + "/a/dir", IsDir: true, Known: true, Rescan: true},
		},
		{
			name:  "directory removed",
			path:  realRoot + "/dir",
			flags: flagItemIsDir | flagItemRemoved,
			want:  Event{Path: root + "/dir", IsDir: true, Known: true},
		},
		{
			name:  "directory created, renamed and removed",
			path:  realRoot + "/dir",
			flags: flagItemIsDir | flagItemCreated | flagItemRenamed | flagItemRemoved,
			want:  Event{Path: root + "/dir", IsDir: true, Known: true, Rescan: true},
		},
		{
			name:  "directory metadata changed",
			path:  realRoot + "/dir",
			flags: flagItemIsDir | flagItemInodeMetaMod | flagItemModified,
			want:  Event{Path: root + "/dir", IsDir: true, Known: true},
		},
		{
			name:  "file and directory",
			path:  realRoot + "/swap",
			flags: flagItemIsFile | flagItemIsDir | flagItemCreated,
			want:  Event{Path: root + "/swap", IsDir: true, Known: true, Rescan: true},
		},
		{
			name:  "must scan below a directory",
			path:  realRoot + "/a",
			flags: flagMustScanSubDirs,
			want:  Event{Path: root + "/a", IsDir: true, Known: true, Rescan: true},
		},
		{
			name:  "must scan below the root",
			path:  realRoot,
			flags: flagMustScanSubDirs,
			want:  rescanRoot,
		},
		{
			name:  "must scan above the root",
			path:  "/private/real",
			flags: flagMustScanSubDirs,
			want:  rescanRoot,
		},
		{
			name:  "events dropped by this process",
			path:  realRoot + "/a",
			flags: flagMustScanSubDirs | flagUserDropped,
			want:  rescanRoot,
		},
		{
			name:  "events dropped by the kernel",
			path:  realRoot + "/a",
			flags: flagMustScanSubDirs | flagKernelDropped,
			want:  rescanRoot,
		},
		{
			name:  "root changed",
			path:  realRoot,
			flags: flagRootChanged,
			want:  rescanRoot,
		},
		{
			name:  "volume mounted",
			path:  realRoot + "/mnt",
			flags: flagMount,
			want:  Event{Path: root + "/mnt", IsDir: true, Known: true, Rescan: true},
		},
		{
			name:  "volume unmounted",
			path:  realRoot + "/mnt",
			flags: flagUnmount,
			want:  Event{Path: root + "/mnt", IsDir: true, Known: true, Rescan: true},
		},
		{
			name:  "root itself",
			path:  realRoot,
			flags: flagItemIsDir | flagItemCreated | flagItemInodeMetaMod,
			drop:  true,
		},
		{
			name:  "root known by another name",
			path:  "/System/Volumes/Data" + realRoot + "/a/file.txt",
			flags: flagItemIsFile | flagItemCreated,
			want:  Event{Path: root + "/a/file.txt", Known: true},
		},
		{
			name:  "outside the root",
			path:  "/private/real/other/file.txt",
			flags: flagItemIsFile | flagItemCreated,
			want:  rescanRoot,
		},
		{
			name:  "name starting with that of the root",
			path:  realRoot + "2/file.txt",
			flags: flagItemIsFile | flagItemCreated,
			want:  rescanRoot,
		},
		{
			name:  "end of history",
			path:  realRoot,
			flags: flagHistoryDone,
			drop:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := convert(root, []string{realRoot, "/System/Volumes/Data" + realRoot}, test.path, test.flags)
			assert.Equal(t, !test.drop, ok)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestConvertRootOfFileSystem(t *testing.T) {
	got, ok := convert("/", []string{""}, "/dir/file.txt", flagItemIsFile|flagItemModified)
	assert.True(t, ok)
	assert.Equal(t, Event{Path: "/dir/file.txt", Known: true}, got)

	_, ok = convert("/", []string{""}, "/", flagItemIsDir|flagItemInodeMetaMod)
	assert.False(t, ok)
}

func TestWatchRootIsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file.txt")
	require.NoError(t, os.WriteFile(path, nil, 0o666))
	err := Watch(context.Background(), path, Options{}, func([]Event) {})
	assert.Error(t, err)
}

func TestWatchRootInAnotherCase(t *testing.T) {
	real := filepath.Join(tempRoot(t), "Root")
	require.NoError(t, os.Mkdir(real, 0o777))
	root := strings.ToUpper(real)
	if _, err := os.Stat(root); err != nil {
		t.Skip("file system is case sensitive")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := new(collector)
	require.NoError(t, Watch(ctx, root, Options{}, c.add))

	// FSEvents reports paths in the case they have on disk
	require.NoError(t, os.WriteFile(filepath.Join(real, "file.txt"), nil, 0o666))
	c.requireEvent(t, "create under a root in another case", fileAt(root+"/file.txt"))
}

func TestWatchRootDeleted(t *testing.T) {
	shortenRetryDelay(t)
	parent := tempRoot(t)
	root := filepath.Join(parent, "root")
	require.NoError(t, os.Mkdir(root, 0o777))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := new(collector)
	require.NoError(t, Watch(ctx, root, Options{}, c.add))

	require.NoError(t, os.Remove(root))
	c.requireEvent(t, "root gone", rescanAt(root))
	c.reset()
	require.NoError(t, os.Mkdir(root, 0o777))
	c.requireEvent(t, "root back", rescanAt(root))
	requireReportedSoon(t, c, root)
}

// watchers returns how many watchers are running
func watchers() int {
	buf := make([]byte, 1<<20)
	return strings.Count(string(buf[:runtime.Stack(buf, true)]), "created by github.com/rclone/rclone/lib/fswatch.Watch ")
}

func TestWatchStartStopLeaksNoGoroutines(t *testing.T) {
	root := tempRoot(t)
	// Keep the watchers busy so that they are stopped with changes to report
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
			_ = os.WriteFile(filepath.Join(root, fmt.Sprintf("file%d", i%10)), nil, 0o666)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, Watch(ctx, root, Options{}, func([]Event) {}))
	require.Positive(t, watchers())
	cancel()
	for range 200 {
		ctx, cancel := context.WithCancel(context.Background())
		require.NoError(t, Watch(ctx, root, Options{}, func([]Event) {}))
		cancel()
	}
	// The watchers stop asynchronously, as do those of earlier tests
	assert.Eventually(t, func() bool { return watchers() == 0 }, 10*time.Second, 10*time.Millisecond)
}
