package sync

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/operations"
)

// How long to wait before retrying a sync which failed, which
// doubles from the minimum to the maximum each time it fails again.
var (
	watchRetryMinDelay = 5 * time.Second
	watchRetryMaxDelay = 5 * time.Minute
)

// errWatchSourceEmpty is returned instead of syncing an empty source
// over a destination which isn't empty.
var errWatchSourceEmpty = errors.New("source is empty: not syncing as this would delete everything in the destination - restart rclone if that is what is wanted")

// errWatchSourceMissing is returned instead of deleting files from
// the destination when the source can't be listed.
var errWatchSourceMissing = errors.New("source can't be listed: not deleting anything from the destination")

// WatchOptions configures Watch
type WatchOptions struct {
	Settle           time.Duration // sync a change once it has been quiet for this long
	MaxDelay         time.Duration // sync something which keeps changing after this long
	FullCheck        time.Duration // run a full sync this often, 0 for never
	DeleteMode       fs.DeleteMode // how to delete files from fdst, fs.DeleteModeOff to copy only
	CopyEmptySrcDirs bool          // copy empty source directories, as for Sync
}

// Watch syncs fsrc into fdst, then keeps fdst up to date as fsrc
// changes until ctx is cancelled.
//
// Just what has changed is synced when fsrc reports changes. A full
// sync is run every opt.FullCheck as well, which is the only way
// changes are noticed if fsrc can't report them.
//
// It returns nil when ctx is cancelled. Errors from individual syncs
// are logged and retried, unless they are fatal.
func Watch(ctx context.Context, fdst, fsrc fs.Fs, opt WatchOptions) error {
	return watch(ctx, fdst, fsrc, opt, fsrc.Features().WatchChanges)
}

// watcher is the state of a running Watch
type watcher struct {
	fdst, fsrc fs.Fs
	opt        WatchOptions
	wake       chan struct{} // signalled when queue has been added to

	mu    sync.Mutex // protects queue
	queue *watchQueue

	failedMu sync.Mutex          // protects failed
	failed   map[string]struct{} // what the sync in progress has had an error syncing

	sourceEmpty bool // set if a full sync found the source empty and the destination not
}

// watch implements Watch, watching for changes with watchChanges
// which may be nil if fsrc can't report them.
func watch(ctx context.Context, fdst, fsrc fs.Fs, opt WatchOptions, watchChanges func(context.Context, func([]fs.ChangeEvent)) error) error {
	w := &watcher{
		fdst:  fdst,
		fsrc:  fsrc,
		opt:   opt,
		wake:  make(chan struct{}, 1),
		queue: newWatchQueue(opt.Settle, opt.MaxDelay),
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // stops the backend watching
	// The limit on how long to run for is for all the syncs together
	var end time.Time
	if maxDuration := time.Duration(fs.GetConfig(ctx).MaxDuration); maxDuration > 0 {
		end = time.Now().Add(maxDuration)
	}

	switch {
	case watchChanges == nil && opt.FullCheck <= 0:
		return fserrors.FatalError(errors.New("can't watch: source can't report changes and full checks are disabled"))
	case watchChanges == nil:
		fs.Logf(fsrc, "Source can't report changes - relying on a full sync every %v", opt.FullCheck)
	default:
		// A backend which can use the filters needn't watch what they exclude
		err := watchChanges(filter.SetUseFilter(ctx, fsrc.Features().FilterAware), w.notify)
		if err != nil && opt.FullCheck <= 0 {
			return fserrors.FatalError(fmt.Errorf("failed to watch for changes: %w", err))
		}
		if err != nil {
			fs.Errorf(fsrc, "Failed to watch for changes - relying on a full sync every %v: %v", opt.FullCheck, err)
		} else {
			fs.Infof(fsrc, "Watching for changes")
		}
	}

	// Watching was established before this first full sync, so
	// nothing which changes while it runs is missed.
	nextFull := time.Now() // when the next full sync is due, zero if none is
	for first := true; ctx.Err() == nil; {
		now := time.Now()
		if !end.IsZero() && !now.Before(end) {
			fs.Logf(fsrc, "Stopping watching: %v", ErrorMaxDurationReached)
			// What failed would have been retried, and it would be
			// taken as a reason to run the command again
			accounting.Stats(ctx).ResetErrors()
			return nil
		}
		w.mu.Lock()
		if !nextFull.IsZero() && !now.Before(nextFull) {
			w.queue.addFull(now)
			nextFull = time.Time{}
		}
		items := w.widen(ctx, w.queue.take(now))
		held := w.queue.held(now)
		w.mu.Unlock()
		if len(items) > 0 {
			err := w.sync(ctx, items, held, first, end)
			full := items[0].isFull()
			switch {
			case ctx.Err() != nil:
				return nil
			case fserrors.IsFatalError(err), errors.Is(err, accounting.ErrorMaxTransferLimitReached):
				// A limit on how much to transfer is for all the syncs together
				return err
			case first && errors.Is(err, fs.ErrorDirNotFound):
				// As a sync without watching does if there is no source
				return err
			case err != nil:
				w.retry(ctx, items, now, err)
			case full:
				w.mu.Lock()
				w.queue.synced(now)
				w.mu.Unlock()
				if opt.FullCheck > 0 {
					nextFull = time.Now().Add(opt.FullCheck)
				}
			}
			first = false
		}
		w.wait(ctx, nextFull, end)
	}
	return nil
}

// widen returns items with any change to a file which marks the
// directory it is in as excluded replaced by that directory and
// everything in it, as it changes whether they are synced.
func (w *watcher) widen(ctx context.Context, items []watchItem) []watchItem {
	excludeFile := filter.GetConfig(ctx).Opt.ExcludeFile
	if len(excludeFile) == 0 {
		return items
	}
	widened := false
	for i := range items {
		item := &items[i]
		if item.kind != watchFile || !slices.Contains(excludeFile, path.Base(item.path)) {
			continue
		}
		item.path, _ = watchParent(item.path)
		item.kind = watchTree
		if item.isFull() {
			return []watchItem{*item}
		}
		widened = true
	}
	if !widened {
		return items
	}
	// The directory may be one of the items already
	slices.SortStableFunc(items, func(a, b watchItem) int {
		return cmp.Or(strings.Compare(a.path, b.path), cmp.Compare(b.kind, a.kind))
	})
	return slices.CompactFunc(items, func(a, b watchItem) bool { return a.path == b.path })
}

// notify queues events from the backend to be synced
func (w *watcher) notify(events []fs.ChangeEvent) {
	w.mu.Lock()
	w.queue.add(time.Now(), events)
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// wait blocks until there may be something to do, which there is
// at nextFull and end if they aren't zero.
func (w *watcher) wait(ctx context.Context, nextFull, end time.Time) {
	w.mu.Lock()
	next, ok := w.queue.next(time.Now())
	w.mu.Unlock()
	for _, at := range []time.Time{nextFull, end} {
		if !at.IsZero() && (!ok || at.Before(next)) {
			next, ok = at, true
		}
	}
	var due <-chan time.Time
	if ok {
		delay := time.Until(next)
		if delay <= 0 {
			return
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		due = timer.C
	}
	select {
	case <-ctx.Done():
	case <-w.wake:
	case <-due:
	}
}

// retryDelay returns how long to wait before retrying item, which
// has failed to sync item.failures times.
//
// A full sync which keeps failing ends up being run as often as it
// would be if it worked.
func (w *watcher) retryDelay(item watchItem) time.Duration {
	maxDelay := watchRetryMaxDelay
	if item.isFull() {
		maxDelay = max(maxDelay, w.opt.FullCheck)
	}
	delay := watchRetryMinDelay
	for failures := item.failures; failures > 1 && delay < maxDelay; failures-- {
		delay *= 2
	}
	return min(delay, maxDelay)
}

// retry queues items, which were taken at taken, to be synced again
// after they failed with err.
func (w *watcher) retry(ctx context.Context, items []watchItem, taken time.Time, err error) {
	blameless, culprits := w.blame(ctx, items)
	w.mu.Lock()
	now := time.Now()
	delay := w.queue.retry(now, taken, items, blameless, w.retryDelay)
	for _, remote := range culprits {
		delay = max(delay, w.queue.blame(now, remote, w.retryDelay))
	}
	w.mu.Unlock()
	what := fmt.Sprintf("%d changes", len(items))
	if items[0].isFull() {
		what = "everything"
	}
	if delay >= watchRetryMinDelay {
		fs.Errorf(w.fdst, "Failed to sync %s - will retry within %v: %v", what, delay.Round(time.Second), err)
	} else {
		fs.Infof(w.fdst, "Failed to sync %s - will retry: %v", what, err)
	}
}

// blame works out which of items, which have just failed to sync,
// had the errors.
//
// blameless reports whether an item wasn't what had an error. It is
// nil if it isn't known what had the errors. culprits are what had
// an error inside an item which is a tree, which isn't blamed for
// them.
//
// A sync which has an error doesn't delete anything, so the items
// which didn't have one may still need syncing.
func (w *watcher) blame(ctx context.Context, items []watchItem) (blameless func(item watchItem) bool, culprits []string) {
	w.failedMu.Lock()
	failed := w.failed
	w.failedMu.Unlock()
	if len(failed) == 0 || int64(len(failed)) < accounting.Stats(ctx).GetErrors() {
		return nil, nil
	}
	kinds := make(map[string]watchKind, len(items))
	for _, item := range items {
		kinds[item.path] = item.kind
	}
	blamed := make(map[string]struct{}, len(failed))
	for remote := range failed {
		if _, isItem := kinds[remote]; isItem {
			blamed[remote] = struct{}{}
			continue
		}
		found := false
		for dir, ok := watchParent(remote); ok && !found; dir, ok = watchParent(dir) {
			kind, isItem := kinds[dir]
			found = isItem && kind == watchTree
		}
		if !found {
			return nil, nil
		}
		culprits = append(culprits, remote)
	}
	return func(item watchItem) bool {
		_, isBlamed := blamed[item.path]
		return !isBlamed
	}, culprits
}

// logFailed returns logger wrapped to note what has an error in failed
func (w *watcher) logFailed(logger operations.LoggerFn) operations.LoggerFn {
	return func(ctx context.Context, sigil operations.Sigil, src, dst fs.DirEntry, err error) {
		logger(ctx, sigil, src, dst, err)
		// Files which aren't deleted because of an earlier error are logged too
		if sigil != operations.TransferError || err == nil || errors.Is(err, fs.ErrorNotDeleting) {
			return
		}
		entry := src
		if entry == nil {
			entry = dst
		}
		if entry == nil {
			return
		}
		w.failedMu.Lock()
		if w.failed == nil {
			w.failed = make(map[string]struct{})
		}
		w.failed[entry.Remote()] = struct{}{}
		w.failedMu.Unlock()
	}
}

// sync syncs items, leaving out held which is what failed to sync
// and is to be retried later. first is set for the first sync, which
// is of everything, and end is when to stop if it isn't zero.
func (w *watcher) sync(ctx context.Context, items []watchItem, held []string, first bool, end time.Time) error {
	stats := accounting.Stats(ctx)
	// Errors from an earlier pass would stop this one deleting anything
	stats.ResetErrors()
	w.failedMu.Lock()
	w.failed = nil
	w.failedMu.Unlock()
	// The limits on deletes apply to each pass
	ctx, ci := fs.AddConfig(ctx)
	if ci.MaxDelete >= 0 {
		ci.MaxDelete += stats.GetDeletes()
	}
	if ci.MaxDeleteSize >= 0 {
		ci.MaxDeleteSize += fs.SizeSuffix(stats.GetDeletesSize())
	}
	if !end.IsZero() {
		ci.MaxDuration = fs.Duration(max(time.Until(end), time.Nanosecond))
	}
	if !items[0].isFull() {
		fs.Debugf(w.fsrc, "Syncing %d changes", len(items))
		return w.syncChanged(ctx, items, held)
	}
	// The first sync does what a sync without watching would
	if !first {
		if err := w.checkSource(ctx, nil); err != nil {
			return err
		}
	}
	return runSyncCopyMove(ctx, w.fdst, w.fsrc, w.opt.DeleteMode, false, false, w.opt.CopyEmptySrcDirs, false)
}

// checkSource returns an error if deleting from the destination
// what isn't in the source looks like a mistake.
//
// A source which has been unmounted looks like an empty directory,
// which a sync would empty the destination to match. If everything
// in the source really has been deleted, each of those changes was
// reported, so changed is called with the name of each thing in the
// root of the destination, if it isn't nil, and they must all have
// changed for that to be believed.
func (w *watcher) checkSource(ctx context.Context, changed func(name string) bool) error {
	if w.opt.DeleteMode == fs.DeleteModeOff {
		return nil
	}
	srcEntries, err := w.fsrc.List(ctx, "")
	switch {
	case err != nil:
		// Files which can't be found in the source would be taken
		// to have been deleted from it
		return fmt.Errorf("%w: %w", errWatchSourceMissing, err)
	case len(srcEntries) > 0:
		w.sourceEmpty = false
		return nil
	}
	dstEntries, err := w.fdst.List(ctx, "")
	if err != nil || len(dstEntries) == 0 {
		return nil
	}
	if changed != nil && !w.sourceEmpty {
		allChanged := true
		for _, entry := range dstEntries {
			allChanged = allChanged && changed(entry.Remote())
		}
		if allChanged {
			return nil
		}
	}
	w.sourceEmpty = true
	return errWatchSourceEmpty
}
