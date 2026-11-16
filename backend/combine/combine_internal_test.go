package combine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdjustmentDo(t *testing.T) {
	for _, test := range []struct {
		root       string
		mountpoint string
		in         string
		want       string
		wantErr    error
	}{
		{
			root:       "",
			mountpoint: "mountpoint",
			in:         "path/to/file.txt",
			want:       "mountpoint/path/to/file.txt",
		},
		{
			root:       "mountpoint",
			mountpoint: "mountpoint",
			in:         "path/to/file.txt",
			want:       "path/to/file.txt",
		},
		{
			root:       "mountpoint/path",
			mountpoint: "mountpoint",
			in:         "path/to/file.txt",
			want:       "to/file.txt",
		},
		{
			root:       "mountpoint/path",
			mountpoint: "mountpoint",
			in:         "wrongpath/to/file.txt",
			want:       "",
			wantErr:    errNotUnderRoot,
		},
	} {
		what := fmt.Sprintf("%+v", test)
		a := newAdjustment(test.root, test.mountpoint)
		got, gotErr := a.do(test.in)
		assert.Equal(t, test.wantErr, gotErr)
		assert.Equal(t, test.want, got, what)
	}

}

func TestAdjustmentUndo(t *testing.T) {
	for _, test := range []struct {
		root       string
		mountpoint string
		in         string
		want       string
		wantErr    error
	}{
		{
			root:       "",
			mountpoint: "mountpoint",
			in:         "mountpoint/path/to/file.txt",
			want:       "path/to/file.txt",
		},
		{
			root:       "mountpoint",
			mountpoint: "mountpoint",
			in:         "path/to/file.txt",
			want:       "path/to/file.txt",
		},
		{
			root:       "mountpoint/path",
			mountpoint: "mountpoint",
			in:         "to/file.txt",
			want:       "path/to/file.txt",
		},
		{
			root:       "wrongmountpoint/path",
			mountpoint: "mountpoint",
			in:         "to/file.txt",
			want:       "",
			wantErr:    errNotUnderRoot,
		},
	} {
		what := fmt.Sprintf("%+v", test)
		a := newAdjustment(test.root, test.mountpoint)
		got, gotErr := a.undo(test.in)
		assert.Equal(t, test.wantErr, gotErr)
		assert.Equal(t, test.want, got, what)
	}

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
	// newFs returns a combine at root of an upstream for each of ws,
	// mounted at dir1, dir2...
	newFs := func(t *testing.T, root string, ws ...*watchedUpstream) (f *Fs) {
		f = &Fs{root: root, upstreams: map[string]*upstream{}}
		for i, w := range ws {
			dir := fmt.Sprintf("dir%d", i+1)
			f.upstreams[dir] = &upstream{
				f:              newWatchedFs(t, w),
				parent:         f,
				dir:            dir,
				pathAdjustment: newAdjustment(root, dir),
			}
		}
		return f
	}
	file := []fs.ChangeEvent{{Path: "sub/file.txt", EntryType: fs.EntryObject}}
	rescan := []fs.ChangeEvent{{Path: "", EntryType: fs.EntryDirectory, Recursive: true}}

	t.Run("AllUpstreams", func(t *testing.T) {
		w1, w2 := &watchedUpstream{}, &watchedUpstream{}
		f := newFs(t, "", w1, w2)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var got [][]fs.ChangeEvent
		require.NoError(t, f.WatchChanges(ctx, func(events []fs.ChangeEvent) {
			got = append(got, events)
		}))
		w1.notify(file)
		w2.notify(rescan)
		assert.Equal(t, [][]fs.ChangeEvent{
			{{Path: "dir1/sub/file.txt", EntryType: fs.EntryObject}},
			{{Path: "dir2", EntryType: fs.EntryDirectory, Recursive: true}},
		}, got)

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

	t.Run("RootInUpstream", func(t *testing.T) {
		w1, w2 := &watchedUpstream{}, &watchedUpstream{}
		f := newFs(t, "dir1/sub", w1, w2)
		var got [][]fs.ChangeEvent
		require.NoError(t, f.WatchChanges(context.Background(), func(events []fs.ChangeEvent) {
			got = append(got, events)
		}))
		w1.notify([]fs.ChangeEvent{
			{Path: "sub/file.txt", EntryType: fs.EntryObject},
			{Path: "sub", EntryType: fs.EntryDirectory},
			{Path: "other/file.txt", EntryType: fs.EntryObject},
			{Path: "subway", EntryType: fs.EntryObject},
		})
		// a change to a parent of the root
		w1.notify(rescan)
		// changes which can't be seen from the root
		w1.notify([]fs.ChangeEvent{{Path: "other", EntryType: fs.EntryDirectory, Recursive: true}})
		w2.notify(file)
		w2.notify(rescan)
		assert.Equal(t, [][]fs.ChangeEvent{
			{
				{Path: "file.txt", EntryType: fs.EntryObject},
				{Path: "", EntryType: fs.EntryDirectory},
			},
			rescan,
		}, got)
	})

	t.Run("UpstreamFails", func(t *testing.T) {
		errFailed := errors.New("failed to watch")
		// The upstreams are started in a random order so try enough
		// times to see the failing one started last
		for range 20 {
			w1, w2 := &watchedUpstream{}, &watchedUpstream{err: errFailed}
			f := newFs(t, "", w1, w2)
			assert.Equal(t, errFailed, f.WatchChanges(context.Background(), nil))
			if w1.ctx != nil {
				assert.Error(t, w1.ctx.Err(), "the upstream which started must be stopped")
			}
		}
	})

	t.Run("UpstreamCantWatch", func(t *testing.T) {
		for range 20 {
			w1 := &watchedUpstream{}
			f := newFs(t, "", w1, nil)
			assert.Equal(t, fs.ErrorNotImplemented, f.WatchChanges(context.Background(), nil))
			if w1.ctx != nil {
				assert.Error(t, w1.ctx.Err(), "the upstream which started must be stopped")
			}
		}
	})
}
