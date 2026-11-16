package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/lib/fswatch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newWatchTestFs makes an Fs rooted at a new temporary directory
func newWatchTestFs(t *testing.T) *Fs {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	f, err := NewFs(context.Background(), "local", dir, configmap.Simple{})
	require.NoError(t, err)
	return f.(*Fs)
}

// newWatchTestFsWith is newWatchTestFs with config set
func newWatchTestFsWith(t *testing.T, config configmap.Simple) *Fs {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	f, err := NewFs(context.Background(), "local", dir, config)
	require.NoError(t, err)
	return f.(*Fs)
}

func TestWatchAppendChangeEvents(t *testing.T) {
	f := newWatchTestFs(t)
	require.NoError(t, os.MkdirAll(filepath.Join(f.root, "a", "b"), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(f.root, "a", "file.txt"), nil, 0o666))
	file := func(remote string) fs.ChangeEvent { return fs.ChangeEvent{Path: remote, EntryType: fs.EntryObject} }
	everything := func(remote string) fs.ChangeEvent {
		return fs.ChangeEvent{Path: remote, EntryType: fs.EntryDirectory, Recursive: true}
	}
	for _, test := range []struct {
		name string
		in   fswatch.Event
		want []fs.ChangeEvent
	}{
		{
			name: "file",
			in:   fswatch.Event{Path: filepath.Join(f.root, "file.txt"), Known: true},
			want: []fs.ChangeEvent{file("file.txt")},
		},
		{
			name: "nested file",
			in:   fswatch.Event{Path: filepath.Join(f.root, "a", "b", "file.txt"), Known: true},
			want: []fs.ChangeEvent{file("a/b/file.txt")},
		},
		{
			name: "directory which is still there",
			in:   fswatch.Event{Path: filepath.Join(f.root, "a", "b"), Known: true, IsDir: true},
			want: []fs.ChangeEvent{{Path: "a/b", EntryType: fs.EntryDirectory}},
		},
		{
			name: "directory which has gone",
			in:   fswatch.Event{Path: filepath.Join(f.root, "a", "gone"), Known: true, IsDir: true},
			want: []fs.ChangeEvent{everything("a/gone")},
		},
		{
			name: "directory which is now a file",
			in:   fswatch.Event{Path: filepath.Join(f.root, "a", "file.txt"), Known: true, IsDir: true},
			want: []fs.ChangeEvent{everything("a/file.txt"), file("a/file.txt")},
		},
		{
			name: "rescan of a directory",
			in:   fswatch.Event{Path: filepath.Join(f.root, "a"), Known: true, IsDir: true, Rescan: true},
			want: []fs.ChangeEvent{everything("a")},
		},
		{
			name: "rescan of the root",
			in:   fswatch.Event{Path: f.root, Known: true, IsDir: true, Rescan: true},
			want: []fs.ChangeEvent{everything("")},
		},
		{
			name: "unknown which is a file",
			in:   fswatch.Event{Path: filepath.Join(f.root, "a", "file.txt")},
			want: []fs.ChangeEvent{file("a/file.txt")},
		},
		{
			name: "unknown which is a directory",
			in:   fswatch.Event{Path: filepath.Join(f.root, "a", "b")},
			want: []fs.ChangeEvent{everything("a/b")},
		},
		{
			name: "unknown which has gone",
			in:   fswatch.Event{Path: filepath.Join(f.root, "a", "gone")},
			want: []fs.ChangeEvent{everything("a/gone"), file("a/gone")},
		},
		{
			name: "outside the root",
			in:   fswatch.Event{Path: filepath.Dir(f.root), Known: true},
			want: []fs.ChangeEvent{everything("")},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, f.appendChangeEvents(nil, test.in))
		})
	}
}

func TestWatchAppendChangeEventsSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks need privileges on Windows")
	}
	file := func(remote string) fs.ChangeEvent { return fs.ChangeEvent{Path: remote, EntryType: fs.EntryObject} }
	everything := func(remote string) fs.ChangeEvent {
		return fs.ChangeEvent{Path: remote, EntryType: fs.EntryDirectory, Recursive: true}
	}
	setup := func(f *Fs) {
		require.NoError(t, os.Mkdir(filepath.Join(f.root, "dir"), 0o777))
		require.NoError(t, os.WriteFile(filepath.Join(f.root, "file.txt"), nil, 0o666))
		require.NoError(t, os.Symlink("file.txt", filepath.Join(f.root, "file-link")))
		require.NoError(t, os.Symlink("dir", filepath.Join(f.root, "dir-link")))
	}
	event := func(f *Fs, name string) []fs.ChangeEvent {
		return f.appendChangeEvents(nil, fswatch.Event{Path: filepath.Join(f.root, name), Known: true})
	}

	// Symlinks are listed with a suffix, so a change could be to either name
	f := newWatchTestFsWith(t, configmap.Simple{"links": "true"})
	setup(f)
	assert.Equal(t, []fs.ChangeEvent{file("file-link"), file("file-link" + fs.LinkSuffix)}, event(f, "file-link"))
	assert.Equal(t, []fs.ChangeEvent{file("file.txt"), file("file.txt" + fs.LinkSuffix)}, event(f, "file.txt"))
	assert.Equal(t, []fs.ChangeEvent{file("gone"), file("gone" + fs.LinkSuffix)}, event(f, "gone"))

	// Symlinks are listed as what they point to
	f = newWatchTestFsWith(t, configmap.Simple{"copy_links": "true"})
	setup(f)
	assert.Equal(t, []fs.ChangeEvent{file("file-link")}, event(f, "file-link"))
	assert.Equal(t, []fs.ChangeEvent{everything("dir-link")}, event(f, "dir-link"))
	assert.Equal(t, []fs.ChangeEvent{everything("gone"), file("gone")}, event(f, "gone"))
}

func TestWatchAppendChangeEventsMatchesListing(t *testing.T) {
	ctx := context.Background()
	f := newWatchTestFs(t)
	checked := 0
	for i, name := range []string{
		"plain.txt",
		"with space.txt",
		"e\u0301 decomposed.txt",
		"\u00e9 composed.txt",
		"fullwidth\uFF0Fslash",
		"trailing dot.",
		"tab\tname",
		"\u2400 control picture",
	} {
		dir := fmt.Sprintf("dir%d", i)
		require.NoError(t, os.Mkdir(filepath.Join(f.root, dir), 0o777))
		path := filepath.Join(f.root, dir, name)
		if err := os.WriteFile(path, nil, 0o666); err != nil {
			continue // not a valid name on this OS
		}
		entries, err := f.List(ctx, dir)
		require.NoError(t, err)
		require.Len(t, entries, 1, "%q", name)
		got := f.appendChangeEvents(nil, fswatch.Event{Path: path, Known: true})
		require.Len(t, got, 1, "%q", name)
		assert.Equal(t, entries[0].Remote(), got[0].Path, "%q", name)
		checked++
	}
	assert.NotZero(t, checked)
}

func TestWatchChangesFeature(t *testing.T) {
	f := newWatchTestFs(t)
	assert.Equal(t, fswatch.Supported(), f.Features().WatchChanges != nil)
}

func TestWatchChanges(t *testing.T) {
	f := newWatchTestFs(t)
	watch := f.Features().WatchChanges
	if watch == nil {
		t.Skip("local backend can't watch for changes on this platform")
	}
	require.NoError(t, os.Mkdir(filepath.Join(f.root, "sub"), 0o777))

	var mu sync.Mutex
	var got []fs.ChangeEvent
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, watch(ctx, func(events []fs.ChangeEvent) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, events...)
	}))

	require.NoError(t, os.WriteFile(filepath.Join(f.root, "sub", "file.txt"), []byte("hello"), 0o666))
	want := fs.ChangeEvent{Path: "sub/file.txt", EntryType: fs.EntryObject}
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, event := range got {
			if event == want {
				return true
			}
		}
		return false
	}, 10*time.Second, 5*time.Millisecond)
}

func TestWatchNetworkFileSystem(t *testing.T) {
	// The temporary directory is taken to be on a local disk
	assert.Equal(t, "", networkFileSystem(t.TempDir()))
}

func TestWatchChangesLeavesOutExcludedDirectories(t *testing.T) {
	f := newWatchTestFs(t)
	watch := f.Features().WatchChanges
	if watch == nil {
		t.Skip("local backend can't watch for changes on this platform")
	}
	require.NoError(t, os.MkdirAll(filepath.Join(f.root, "excluded", "sub"), 0o777))
	require.NoError(t, os.Mkdir(filepath.Join(f.root, "included"), 0o777))
	fi, err := filter.NewFilter(nil)
	require.NoError(t, err)
	require.NoError(t, fi.AddRule("- /excluded/**"))
	ctx, cancel := context.WithCancel(filter.SetUseFilter(filter.ReplaceConfig(context.Background(), fi), true))
	defer cancel()

	var mu sync.Mutex
	var got []fs.ChangeEvent
	require.NoError(t, watch(ctx, func(events []fs.ChangeEvent) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, events...)
	}))
	reported := func(remote string) func() bool {
		return func() bool {
			mu.Lock()
			defer mu.Unlock()
			return slices.ContainsFunc(got, func(event fs.ChangeEvent) bool { return event.Path == remote })
		}
	}

	require.NoError(t, os.WriteFile(filepath.Join(f.root, "excluded", "sub", "file.txt"), []byte("hello"), 0o666))
	require.NoError(t, os.WriteFile(filepath.Join(f.root, "included", "file.txt"), []byte("hello"), 0o666))
	require.Eventually(t, reported("included/file.txt"), 10*time.Second, 5*time.Millisecond)
	if runtime.GOOS == "linux" {
		// Elsewhere the whole tree is watched whatever is in it
		assert.False(t, reported("excluded/sub/file.txt")())
	}
}
