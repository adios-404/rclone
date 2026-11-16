package local

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/lib/fswatch"
)

// WatchChanges reports changes under the root to notify until ctx is cancelled.
//
// It returns once watching is established.
//
// Changes below a directory which is a symlink are not reported, and
// those in directories which the filters in ctx exclude may not be if
// ctx says to use them.
func (f *Fs) WatchChanges(ctx context.Context, notify func([]fs.ChangeEvent)) error {
	opt := fswatch.Options{OneFileSystem: f.opt.OneFileSystem}
	if fi := filter.GetConfig(ctx); filter.GetUseFilter(ctx) && !fi.InActive() {
		// Whether a directory has a file in it which excludes it can
		// change, so those are watched
		rules := *fi
		rules.Opt.ExcludeFile = nil
		includeDirectory := rules.IncludeDirectory(ctx, f)
		opt.Skip = func(path string) bool {
			rel, err := filepath.Rel(f.root, path)
			if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return false
			}
			remote := ""
			for name := range strings.SplitSeq(rel, string(filepath.Separator)) {
				remote = f.cleanRemote(remote, name)
			}
			include, err := includeDirectory(remote)
			return err == nil && !include
		}
	}
	err := fswatch.Watch(ctx, f.root, opt, func(events []fswatch.Event) {
		out := make([]fs.ChangeEvent, 0, len(events))
		for _, event := range events {
			out = f.appendChangeEvents(out, event)
		}
		if len(out) > 0 {
			notify(out)
		}
	})
	if err != nil {
		return err
	}
	if kind := networkFileSystem(f.root); kind != "" {
		fs.Logf(f, "Watching a %s file system: changes made to it by other computers may not be noticed until it is next synced in full", kind)
	}
	return nil
}

// appendChangeEvents appends event to out, converted to use paths as
// returned by List.
func (f *Fs) appendChangeEvents(out []fs.ChangeEvent, event fswatch.Event) []fs.ChangeEvent {
	rel, err := filepath.Rel(f.root, event.Path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return append(out, fs.ChangeEvent{EntryType: fs.EntryDirectory, Recursive: true})
	}
	remote := ""
	if rel != "." {
		for name := range strings.SplitSeq(rel, string(filepath.Separator)) {
			remote = f.cleanRemote(remote, name)
		}
		if f.onOtherDevice(filepath.Dir(event.Path)) {
			return out
		}
	}
	dir := fs.ChangeEvent{Path: remote, EntryType: fs.EntryDirectory}
	everything := fs.ChangeEvent{Path: remote, EntryType: fs.EntryDirectory, Recursive: true}
	if event.Rescan {
		return append(out, everything)
	}
	appendFile := func() []fs.ChangeEvent {
		out = append(out, fs.ChangeEvent{Path: remote, EntryType: fs.EntryObject})
		if f.opt.TranslateSymlinks {
			// It may be, or have been, a symlink
			out = append(out, fs.ChangeEvent{Path: remote + fs.LinkSuffix, EntryType: fs.EntryObject})
		}
		return out
	}
	isFile := event.Known && !event.IsDir
	if isFile && !f.opt.FollowSymlinks {
		return appendFile()
	}

	// Find out what is there as the event doesn't say enough
	stat := os.Lstat
	if f.opt.FollowSymlinks {
		stat = os.Stat
	}
	fi, err := stat(event.Path)
	switch {
	case err != nil:
		// Whatever it was has gone. A symlink being followed could
		// have been to a directory.
		if !isFile || f.opt.FollowSymlinks {
			out = append(out, everything)
		}
		if !event.Known || !event.IsDir {
			out = appendFile()
		}
		return out
	case !fi.IsDir():
		if event.Known && event.IsDir {
			// A file has replaced the directory
			out = append(out, everything)
		}
		return appendFile()
	case event.Known && event.IsDir:
		// The directory is still there so it is the directory itself which changed
		return append(out, dir)
	}
	return append(out, everything)
}

// onOtherDevice reports whether the directory at path is on a
// different device to the root with --one-file-system in use, in
// which case what is in it isn't listed.
func (f *Fs) onOtherDevice(path string) bool {
	if f.dev == devUnset {
		return false
	}
	fi, err := os.Lstat(path)
	return err == nil && readDevice(fi, f.opt.OneFileSystem) != f.dev
}
