package fs_test

import (
	"context"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// watchingFs is a mock Fs which can report changes
type watchingFs struct {
	fs.Fs
	events   []fs.ChangeEvent
	features *fs.Features
}

// Features returns the features of the watching Fs
func (f *watchingFs) Features() *fs.Features {
	return f.features
}

// WatchChanges reports the canned events
func (f *watchingFs) WatchChanges(ctx context.Context, notify func([]fs.ChangeEvent)) error {
	notify(f.events)
	return nil
}

func TestFeaturesFillWatchChanges(t *testing.T) {
	ctx := context.Background()
	plain, err := mockfs.NewFs(ctx, "mock", "", nil)
	require.NoError(t, err)
	assert.Nil(t, (&fs.Features{}).Fill(ctx, plain).WatchChanges, "an Fs without WatchChanges must not get the feature")

	want := []fs.ChangeEvent{
		{Path: "dir/file.txt", EntryType: fs.EntryObject},
		{Path: "", EntryType: fs.EntryDirectory, Recursive: true},
	}
	f := &watchingFs{Fs: plain, events: want}
	ft := (&fs.Features{}).Fill(ctx, f)
	require.NotNil(t, ft.WatchChanges)

	var got []fs.ChangeEvent
	require.NoError(t, ft.WatchChanges(ctx, func(events []fs.ChangeEvent) { got = append(got, events...) }))
	assert.Equal(t, want, got)
}

func TestFeaturesMaskWatchChanges(t *testing.T) {
	ctx := context.Background()
	plain, err := mockfs.NewFs(ctx, "mock", "", nil)
	require.NoError(t, err)
	watching := &watchingFs{Fs: plain}
	watching.features = (&fs.Features{}).Fill(ctx, watching)

	ft := (&fs.Features{}).Fill(ctx, watching)
	ft.Mask(ctx, plain)
	assert.Nil(t, ft.WatchChanges, "masking with an Fs that can't watch must clear the feature")

	ft = (&fs.Features{}).Fill(ctx, watching)
	ft.Mask(ctx, watching)
	assert.NotNil(t, ft.WatchChanges)
}
