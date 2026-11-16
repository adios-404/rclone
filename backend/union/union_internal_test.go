package union

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/union/upstream"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/fstest/fstests"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/lib/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MakeTestDirs makes directories in /tmp for testing
func MakeTestDirs(t *testing.T, n int) (dirs []string) {
	for i := 1; i <= n; i++ {
		dir := t.TempDir()
		dirs = append(dirs, dir)
	}
	return dirs
}

func (f *Fs) TestInternalReadOnly(t *testing.T) {
	if f.name != "TestUnionRO" {
		t.Skip("Only on RO union")
	}
	dir := "TestInternalReadOnly"
	ctx := context.Background()
	rofs := f.upstreams[len(f.upstreams)-1]
	assert.False(t, rofs.IsWritable())

	// Put a file onto the read only fs
	contents := random.String(50)
	file1 := fstest.NewItem(dir+"/file.txt", contents, time.Now())
	obj1 := fstests.PutTestContents(ctx, t, rofs, &file1, contents, true)

	// Check read from readonly fs via union
	o, err := f.NewObject(ctx, file1.Path)
	require.NoError(t, err)
	assert.Equal(t, int64(50), o.Size())

	// Now call Update on the union Object with new data
	contents2 := random.String(100)
	file2 := fstest.NewItem(dir+"/file.txt", contents2, time.Now())
	in := bytes.NewBufferString(contents2)
	src := object.NewStaticObjectInfo(file2.Path, file2.ModTime, file2.Size, true, nil, nil)
	err = o.Update(ctx, in, src)
	require.NoError(t, err)
	assert.Equal(t, int64(100), o.Size())

	// Check we read the new object via the union
	o, err = f.NewObject(ctx, file1.Path)
	require.NoError(t, err)
	assert.Equal(t, int64(100), o.Size())

	// Remove the object
	assert.NoError(t, o.Remove(ctx))

	// Check we read the old object in the read only layer now
	o, err = f.NewObject(ctx, file1.Path)
	require.NoError(t, err)
	assert.Equal(t, int64(50), o.Size())

	// Remove file and dir from read only fs
	assert.NoError(t, obj1.Remove(ctx))
	assert.NoError(t, rofs.Rmdir(ctx, dir))
}

func (f *Fs) InternalTest(t *testing.T) {
	t.Run("ReadOnly", f.TestInternalReadOnly)
}

var _ fstests.InternalTester = (*Fs)(nil)

// This specifically tests a union of local which can Move but not
// Copy and :memory: which can Copy but not Move to makes sure that
// the resulting union can Move
func TestMoveCopy(t *testing.T) {
	if *fstest.RemoteName != "" {
		t.Skip("Skipping as -remote set")
	}
	ctx := context.Background()
	dirs := MakeTestDirs(t, 1)
	fsString := fmt.Sprintf(":union,upstreams='%s :memory:bucket':", dirs[0])
	f, err := fs.NewFs(ctx, fsString)
	require.NoError(t, err)

	unionFs := f.(*Fs)
	fLocal := unionFs.upstreams[0].Fs
	fMemory := unionFs.upstreams[1].Fs

	if runtime.GOOS == "darwin" {
		// need to disable as this test specifically tests a local that can't Copy
		f.Features().Disable("Copy")
		fLocal.Features().Disable("Copy")
	}

	t.Run("Features", func(t *testing.T) {
		assert.NotNil(t, f.Features().Move)
		assert.Nil(t, f.Features().Copy)

		// Check underlying are as we are expect
		assert.NotNil(t, fLocal.Features().Move)
		assert.Nil(t, fLocal.Features().Copy)
		assert.Nil(t, fMemory.Features().Move)
		assert.NotNil(t, fMemory.Features().Copy)
	})

	// Put a file onto the local fs
	contentsLocal := random.String(50)
	fileLocal := fstest.NewItem("local.txt", contentsLocal, time.Now())
	_ = fstests.PutTestContents(ctx, t, fLocal, &fileLocal, contentsLocal, true)
	objLocal, err := f.NewObject(ctx, fileLocal.Path)
	require.NoError(t, err)

	// Put a file onto the memory fs
	contentsMemory := random.String(60)
	fileMemory := fstest.NewItem("memory.txt", contentsMemory, time.Now())
	_ = fstests.PutTestContents(ctx, t, fMemory, &fileMemory, contentsMemory, true)
	objMemory, err := f.NewObject(ctx, fileMemory.Path)
	require.NoError(t, err)

	fstest.CheckListing(t, f, []fstest.Item{fileLocal, fileMemory})

	t.Run("MoveLocal", func(t *testing.T) {
		fileLocal.Path = "local-renamed.txt"
		_, err := operations.Move(ctx, f, nil, fileLocal.Path, objLocal)
		require.NoError(t, err)
		fstest.CheckListing(t, f, []fstest.Item{fileLocal, fileMemory})

		// Check can retrieve object from union
		obj, err := f.NewObject(ctx, fileLocal.Path)
		require.NoError(t, err)
		assert.Equal(t, fileLocal.Size, obj.Size())

		// Check can retrieve object from underlying
		obj, err = fLocal.NewObject(ctx, fileLocal.Path)
		require.NoError(t, err)
		assert.Equal(t, fileLocal.Size, obj.Size())

		t.Run("MoveMemory", func(t *testing.T) {
			fileMemory.Path = "memory-renamed.txt"
			_, err := operations.Move(ctx, f, nil, fileMemory.Path, objMemory)
			require.NoError(t, err)
			fstest.CheckListing(t, f, []fstest.Item{fileLocal, fileMemory})

			// Check can retrieve object from union
			obj, err := f.NewObject(ctx, fileMemory.Path)
			require.NoError(t, err)
			assert.Equal(t, fileMemory.Size, obj.Size())

			// Check can retrieve object from underlying
			obj, err = fMemory.NewObject(ctx, fileMemory.Path)
			require.NoError(t, err)
			assert.Equal(t, fileMemory.Size, obj.Size())
		})
	})
}

// watchedUpstream stands in for the WatchChanges of an upstream
type watchedUpstream struct {
	err    error                  // returned by watchChanges
	ctx    context.Context        // passed to watchChanges
	notify func([]fs.ChangeEvent) // passed to watchChanges
}

// watchChanges implements WatchChanges
func (w *watchedUpstream) watchChanges(ctx context.Context, notify func([]fs.ChangeEvent)) error {
	w.ctx, w.notify = ctx, notify
	return w.err
}

// newWatchedFs returns an Fs watched by w, or one which can't be
// watched if w is nil
func newWatchedFs(t *testing.T, w *watchedUpstream) fs.Fs {
	f, err := mockfs.NewFs(context.Background(), "mock", "", nil)
	require.NoError(t, err)
	if w != nil {
		f.Features().WatchChanges = w.watchChanges
	}
	return f
}

func TestWatchChanges(t *testing.T) {
	// newFs returns a union of an upstream for each of ws
	newFs := func(t *testing.T, ws ...*watchedUpstream) (f *Fs) {
		f = &Fs{}
		for _, w := range ws {
			uFs := newWatchedFs(t, w)
			f.upstreams = append(f.upstreams, &upstream.Fs{Fs: uFs, RootFs: uFs})
		}
		return f
	}
	file := []fs.ChangeEvent{{Path: "dir/file.txt", EntryType: fs.EntryObject}}
	rescan := []fs.ChangeEvent{{Path: "", EntryType: fs.EntryDirectory, Recursive: true}}

	t.Run("AllUpstreams", func(t *testing.T) {
		w1, w2 := &watchedUpstream{}, &watchedUpstream{}
		f := newFs(t, w1, w2)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var got [][]fs.ChangeEvent
		require.NoError(t, f.WatchChanges(ctx, func(events []fs.ChangeEvent) {
			got = append(got, events)
		}))
		w1.notify(file)
		w2.notify(rescan)
		assert.Equal(t, [][]fs.ChangeEvent{file, rescan}, got)

		// notify must not be called by both upstreams at once
		var wg sync.WaitGroup
		for _, w := range []*watchedUpstream{w1, w2} {
			wg.Go(func() {
				for range 100 {
					w.notify(file)
				}
			})
		}
		wg.Wait()
		assert.Len(t, got, 202)

		assert.NoError(t, w1.ctx.Err())
		assert.NoError(t, w2.ctx.Err())
		cancel()
		assert.Error(t, w1.ctx.Err())
		assert.Error(t, w2.ctx.Err())
	})

	t.Run("UpstreamFails", func(t *testing.T) {
		errFailed := errors.New("failed to watch")
		w1, w2 := &watchedUpstream{}, &watchedUpstream{err: errFailed}
		f := newFs(t, w1, w2)
		assert.Equal(t, errFailed, f.WatchChanges(context.Background(), nil))
		assert.Error(t, w1.ctx.Err(), "the upstream which started must be stopped")
	})

	t.Run("UpstreamCantWatch", func(t *testing.T) {
		w1 := &watchedUpstream{}
		f := newFs(t, w1, nil)
		assert.Equal(t, fs.ErrorNotImplemented, f.WatchChanges(context.Background(), nil))
		assert.Error(t, w1.ctx.Err(), "the upstream which started must be stopped")
	})

	t.Run("RootMissing", func(t *testing.T) {
		// The root doesn't exist on the second upstream so it can
		// only be watched from the root of the upstream remote
		w1, w2, w2Root := &watchedUpstream{}, &watchedUpstream{err: errors.New("no such directory")}, &watchedUpstream{}
		f := newFs(t, w1, w2)
		f.root = "dir/root"
		f.upstreams[1].RootFs = newWatchedFs(t, w2Root)
		var got [][]fs.ChangeEvent
		require.NoError(t, f.WatchChanges(context.Background(), func(events []fs.ChangeEvent) {
			got = append(got, events)
		}))
		w1.notify(file)
		w2Root.notify([]fs.ChangeEvent{
			{Path: "dir/root/sub/file.txt", EntryType: fs.EntryObject},
			{Path: "dir/root", EntryType: fs.EntryDirectory, Recursive: true},
			{Path: "dir", EntryType: fs.EntryObject},
			{Path: "", EntryType: fs.EntryDirectory, Recursive: true},
			{Path: "dir/other/file.txt", EntryType: fs.EntryObject},
			{Path: "dir/rooted", EntryType: fs.EntryObject},
		})
		w2Root.notify([]fs.ChangeEvent{
			{Path: "elsewhere", EntryType: fs.EntryObject},
		})
		assert.Equal(t, [][]fs.ChangeEvent{file, {
			{Path: "sub/file.txt", EntryType: fs.EntryObject},
			{Path: "", EntryType: fs.EntryDirectory, Recursive: true},
			{Path: "", EntryType: fs.EntryDirectory, Recursive: true},
			{Path: "", EntryType: fs.EntryDirectory, Recursive: true},
		}}, got)
	})
}
