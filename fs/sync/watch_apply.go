package sync

import (
	"context"
	"strings"
	"sync"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/march"
	"github.com/rclone/rclone/lib/transform"
	"golang.org/x/text/unicode/norm"
)

// syncChanged syncs just items, none of which is the root, leaving
// out held.
//
// Changed files are looked up individually if that is all there is
// to do. Otherwise the directories on the way to each item are
// listed to find it, which is what it takes to sync a directory, its
// modification time or a rename, and to find something which may be
// spelt differently in the destination, or which is where it is
// decided whether a directory is excluded.
func (w *watcher) syncChanged(ctx context.Context, items []watchItem, held []string) error {
	ctx, ci := fs.AddConfig(ctx)
	fi := filter.GetConfig(ctx)
	// The modification time of a directory changes when a file in it is
	// added or removed, so the directory needs syncing too. Names
	// can only be looked up if they are spelt the same on both sides.
	listDirs := ci.TrackRenames || w.syncsDirModTimes(ctx) ||
		ci.IgnoreCaseSync || ci.FixCase || transform.Transforming(ctx) ||
		w.fsrc.Features().CaseInsensitive || w.fdst.Features().CaseInsensitive ||
		len(fi.Opt.ExcludeFile) > 0
	for _, item := range items {
		listDirs = listDirs || item.kind != watchFile
	}
	if listDirs {
		// Listing recursively would list far more than is needed
		ci.UseListR = false
		ci.NoTraverse = false
		return w.syncLimited(ctx, func(s *syncCopyMove) march.Marcher {
			return newWatchMarcher(w, s, items, held, false)
		})
	}

	// A files-from list overrides every other filter, so apply them here first
	included, err := filter.NewFilter(nil)
	if err != nil {
		return err
	}
	included.Opt.DeleteExcluded = fi.Opt.DeleteExcluded
	includes := newWatchFilter(ctx, w.fdst, w.fsrc)
	for _, item := range items {
		include, err := includes.include(item.path)
		if err != nil {
			return err
		}
		if !include {
			continue
		}
		if err := included.AddFile(item.path); err != nil {
			return err
		}
	}
	if !included.HaveFilesFrom() {
		return nil
	}
	// Look each file up rather than listing the directories they are in
	ci.NoTraverse = true
	return w.syncLimited(filter.ReplaceConfig(ctx, included), func(s *syncCopyMove) march.Marcher {
		return newWatchMarcher(w, s, items, nil, true)
	})
}

// syncsDirModTimes reports whether a sync sets the modification
// times of directories in the destination.
func (w *watcher) syncsDirModTimes(ctx context.Context) bool {
	dst := w.fdst.Features()
	return !fs.GetConfig(ctx).NoUpdateDirModTime && w.fsrc.Features().CanHaveEmptyDirectories &&
		(dst.WriteDirSetModTime || dst.MkdirMetadata != nil || dst.DirSetModTime != nil)
}

// syncLimited runs a sync with what it syncs limited by the
// march.Marcher returned by limit.
func (w *watcher) syncLimited(ctx context.Context, limit func(s *syncCopyMove) march.Marcher) error {
	deleteMode := w.opt.DeleteMode
	if deleteMode == fs.DeleteModeBefore {
		// Delete in a pass of its own first, as a full sync does
		if err := w.syncLimitedPass(ctx, fs.DeleteModeOnly, limit); err != nil {
			return err
		}
		deleteMode = fs.DeleteModeOff
	}
	return w.syncLimitedPass(ctx, deleteMode, limit)
}

// syncLimitedPass does one pass of syncLimited
func (w *watcher) syncLimitedPass(ctx context.Context, deleteMode fs.DeleteMode, limit func(s *syncCopyMove) march.Marcher) error {
	s, err := newSyncCopyMove(ctx, w.fdst, w.fsrc, deleteMode, false, false, w.opt.CopyEmptySrcDirs, false)
	if err != nil {
		return err
	}
	s.marcher = limit(s)
	s.logger = w.logFailed(s.logger)
	return s.run()
}

// watchFilter decides whether changed files should be synced, as a
// files-from list doesn't.
type watchFilter struct {
	ctx        context.Context
	fi         *filter.Filter
	fdst, fsrc fs.Fs
	maxDepth   int
	usesObject bool // set if the filters need more than the name of a file
}

// newWatchFilter makes a watchFilter using the filters in ctx
func newWatchFilter(ctx context.Context, fdst, fsrc fs.Fs) *watchFilter {
	fi := filter.GetConfig(ctx)
	opt := &fi.Opt
	return &watchFilter{
		ctx:      ctx,
		fi:       fi,
		fdst:     fdst,
		fsrc:     fsrc,
		maxDepth: fs.GetConfig(ctx).MaxDepth,
		usesObject: !fi.ModTimeFrom.IsZero() || !fi.ModTimeTo.IsZero() || opt.MinSize >= 0 || opt.MaxSize >= 0 ||
			len(opt.MetaRules.FilterRule)+len(opt.MetaRules.FilterFrom)+
				len(opt.MetaRules.ExcludeRule)+len(opt.MetaRules.ExcludeFrom)+
				len(opt.MetaRules.IncludeRule)+len(opt.MetaRules.IncludeFrom) > 0,
	}
}

// include reports whether the file at remote should be synced.
//
// A source file excluded by its size, age or metadata is skipped,
// leaving any copy on the destination for the next full sync to
// deal with.
func (f *watchFilter) include(remote string) (bool, error) {
	if f.maxDepth >= 0 && strings.Count(remote, "/") >= f.maxDepth {
		return false, nil
	}
	if !f.fi.IncludeRemote(remote) {
		return false, nil
	}
	if !f.usesObject {
		return true, nil
	}
	if src, err := f.fsrc.NewObject(f.ctx, remote); err == nil {
		return f.fi.IncludeObject(f.ctx, src), nil
	}
	// Gone from the source, so only delete it from the destination
	// if a full sync would have seen it there
	dst, err := f.fdst.NewObject(f.ctx, remote)
	return err == nil && f.fi.IncludeObject(f.ctx, dst), nil
}

// watchMarcher passes on what a march finds to a sync if it is one
// of the changes being synced.
//
// Paths are kept in the form a march matches names in, as what is in
// the destination may not be spelt as it is in the source.
type watchMarcher struct {
	w     *watcher
	s     *syncCopyMove
	lower bool                // set if names are matched ignoring case
	all   bool                // set if everything the march finds is being synced
	dirs  map[string]struct{} // directories to sync, but not what is in them
	above map[string]struct{} // directories containing something being synced
	top   map[string]struct{} // what has changed in the root
	held  map[string]struct{} // what is left out, along with everything below it

	mu    sync.Mutex          // protects trees
	trees map[string]struct{} // what to sync along with everything below it

	checkSource sync.Once // for sourceErr
	sourceErr   error     // set if nothing should be deleted from the destination
}

// newWatchMarcher makes a watchMarcher for s which syncs items other
// than what is in held, or everything the march finds if all is set.
func newWatchMarcher(w *watcher, s *syncCopyMove, items []watchItem, held []string, all bool) *watchMarcher {
	m := &watchMarcher{
		w:     w,
		s:     s,
		lower: s.fdst.Features().CaseInsensitive || s.ci.IgnoreCaseSync,
		all:   all,
		dirs:  make(map[string]struct{}),
		above: make(map[string]struct{}),
		top:   make(map[string]struct{}),
		held:  make(map[string]struct{}),
		trees: make(map[string]struct{}),
	}
	for _, remote := range held {
		for _, isDir := range []bool{true, false} {
			m.held[m.key(remote, true, isDir)] = struct{}{}
		}
	}
	for _, item := range items {
		// It isn't known whether a tree is a directory, which can change how it is spelt
		for _, isDir := range []bool{true, false} {
			if item.kind != watchTree && isDir != (item.kind == watchDir) {
				continue
			}
			remote := m.key(item.path, true, isDir)
			if item.kind == watchDir {
				m.dirs[remote] = struct{}{}
			} else {
				m.trees[remote] = struct{}{}
			}
			if !strings.Contains(remote, "/") {
				m.top[remote] = struct{}{}
			}
		}
		for dir, ok := watchParent(item.path); ok; dir, ok = watchParent(dir) {
			m.above[m.key(dir, true, true)] = struct{}{}
		}
		if dir, ok := watchParent(item.path); ok && dir != "" && item.kind != watchDir {
			// Adding or removing something changes the modification
			// time of the directory it is in
			m.dirs[m.key(dir, true, true)] = struct{}{}
		}
	}
	return m
}

// key returns remote in the form a march matches names in. isSrc
// says whether it is in the source rather than the destination.
func (m *watchMarcher) key(remote string, isSrc, isDir bool) string {
	if isSrc {
		remote = transform.Path(m.s.ctx, remote, isDir)
	}
	if !m.s.noUnicodeNormalization {
		remote = norm.NFC.String(remote)
	}
	if m.lower {
		remote = strings.ToLower(remote)
	}
	return remote
}

// Where a directory entry is relative to the changes being synced
const (
	watchOutside = iota // nothing to do with them
	watchAbove          // a directory containing some
	watchItself         // a directory to sync, but not what is in it
	watchInside         // one of them, or below one
)

// where returns where entry is relative to the changes being synced
// along with its key.
func (m *watchMarcher) where(entry fs.DirEntry, isSrc bool) (where int, remote string) {
	if m.all {
		return watchInside, ""
	}
	_, isDir := entry.(fs.Directory)
	remote = m.key(entry.Remote(), isSrc, isDir)
	m.mu.Lock()
	defer m.mu.Unlock()
	// What is held is only left out of a tree it is in, not when
	// it, or something in it, is one of the changes itself. Nor is
	// it if it is only in the destination, as deleting it from there
	// isn't what failed.
	held, inTree := false, false
	for dir, ok := remote, true; ok && !inTree; dir, ok = watchParent(dir) {
		if _, found := m.trees[dir]; found {
			inTree = true
		} else if _, found := m.held[dir]; found && isSrc {
			held = true
		}
	}
	switch _, isAbove := m.above[remote]; {
	case inTree && !held:
		return watchInside, remote
	case inTree && isAbove:
		// It is on the way to a change which isn't held
		return watchAbove, remote
	case inTree:
		return watchOutside, remote
	}
	if _, found := m.dirs[remote]; found {
		return watchItself, remote
	}
	if _, found := m.above[remote]; found {
		return watchAbove, remote
	}
	return watchOutside, remote
}

// addTree makes everything at and below remote be synced
func (m *watchMarcher) addTree(remote string) {
	m.mu.Lock()
	m.trees[remote] = struct{}{}
	m.mu.Unlock()
}

// mayDelete reports whether what isn't in the source can be deleted
// from the destination, which it can't if the source has gone.
func (m *watchMarcher) mayDelete() bool {
	m.checkSource.Do(func() {
		m.sourceErr = m.w.checkSource(m.s.ctx, func(name string) bool {
			_, found := m.top[m.key(name, false, false)]
			return found
		})
		if m.sourceErr != nil {
			m.sourceErr = fs.CountError(m.s.ctx, m.sourceErr)
			fs.Errorf(m.s.fsrc, "%v", m.sourceErr)
			m.s.processError(m.sourceErr)
		}
	})
	return m.sourceErr == nil
}

// SrcOnly is called for a DirEntry found only in the source
func (m *watchMarcher) SrcOnly(src fs.DirEntry) (recurse bool) {
	_, isDir := src.(fs.Directory)
	where, remote := m.where(src, true)
	switch {
	case where == watchOutside:
		return false
	case where == watchAbove && isDir:
		return true
	case where != watchInside:
		// It is new to the destination, so everything in it is too
		m.addTree(remote)
	}
	return m.s.SrcOnly(src)
}

// DstOnly is called for a DirEntry found only in the destination
func (m *watchMarcher) DstOnly(dst fs.DirEntry) (recurse bool) {
	_, isDir := dst.(fs.Directory)
	where, remote := m.where(dst, false)
	if where == watchOutside {
		return false
	}
	if m.s.deleteMode != fs.DeleteModeOff && !m.mayDelete() {
		return false
	}
	if isDir && m.all && m.s.deleteMode != fs.DeleteModeOff {
		// The directory may only be missing from the list of files
		// being synced, so delete the files in it but not the
		// directory.
		return true
	}
	if where != watchInside {
		// It has gone from the source along with everything in it
		m.addTree(remote)
	}
	return m.s.DstOnly(dst)
}

// Match is called for a DirEntry found both in the source and destination
func (m *watchMarcher) Match(ctx context.Context, dst, src fs.DirEntry) (recurse bool) {
	_, srcIsDir := src.(fs.Directory)
	_, dstIsDir := dst.(fs.Directory)
	where, remote := m.where(src, true)
	switch {
	case where == watchOutside:
		return false
	case where == watchAbove && srcIsDir && dstIsDir:
		return true
	case where != watchInside && !(srcIsDir && dstIsDir):
		// It isn't the directory it was thought to be, so all of it needs syncing
		m.addTree(remote)
	}
	return m.s.Match(ctx, dst, src)
}
