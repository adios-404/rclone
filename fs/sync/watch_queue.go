package sync

import (
	"path"
	"slices"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
)

// watchMaxItems is the most pending changes which are kept. Above
// that they are merged into the directories they are in, which
// costs less to sync than that many changes one by one and stops
// the memory used growing without limit.
const watchMaxItems = 10000

// watchKind is how much needs syncing for a change
type watchKind uint8

// These are in order, so that the greater of two covers both
const (
	watchFile watchKind = iota // the file at the path
	watchDir                   // the directory at the path itself, not what is in it
	watchTree                  // everything at and below the path
)

// watchItem is something which has changed and needs syncing
type watchItem struct {
	path     string // relative to the root of the source, "" for the root
	kind     watchKind
	failures int // how many times in a row syncing it has failed and been delayed
	quick    int // how many times in a row syncing it has failed and not been delayed
}

// isFull reports whether syncing item means syncing everything
func (item watchItem) isFull() bool {
	return item.kind == watchTree && item.path == ""
}

// watchEntry records when something changed
type watchEntry struct {
	first     time.Time // first change not yet synced
	last      time.Time // most recent change
	notBefore time.Time // earliest time to sync it, zero if none
	kind      watchKind
	failures  int // as for watchItem
	quick     int // as for watchItem
}

// due returns when e should be synced
func (e *watchEntry) due(settle, maxDelay time.Duration) time.Time {
	due := e.last.Add(settle)
	if limit := e.first.Add(maxDelay); limit.Before(due) {
		due = limit
	}
	if e.notBefore.After(due) {
		due = e.notBefore
	}
	return due
}

// touch records a change at now
func (e *watchEntry) touch(now time.Time) {
	if e.first.IsZero() {
		e.first = now
	}
	if now.After(e.last) {
		e.last = now
	}
}

// merge adds when other changed to e
func (e *watchEntry) merge(other *watchEntry) {
	if e.first.IsZero() || other.first.Before(e.first) {
		e.first = other.first
	}
	if other.last.After(e.last) {
		e.last = other.last
	}
}

// watchQueue collects changes until they have been quiet for long
// enough to sync.
//
// What has failed to sync is kept apart from the changes while it
// waits to be retried, so that it doesn't hold up changes to
// anything else, such as other files in the same directory.
//
// It is not safe for concurrent use.
type watchQueue struct {
	settle   time.Duration
	maxDelay time.Duration
	entries  map[string]*watchEntry // changes, none of which is at or below a watchTree entry
	failed   map[string]*watchEntry // what failed to sync and is waiting to be retried
	full     *watchEntry            // set if everything needs syncing
	last     time.Time              // when a change was last added
}

// newWatchQueue makes a watchQueue which releases a change once it
// has been quiet for settle, or maxDelay after it was first seen.
func newWatchQueue(settle, maxDelay time.Duration) *watchQueue {
	return &watchQueue{
		settle:   settle,
		maxDelay: maxDelay,
		entries:  make(map[string]*watchEntry),
		failed:   make(map[string]*watchEntry),
	}
}

// watchParent returns the directory remote is in, and false if
// remote is the root.
func watchParent(remote string) (parent string, ok bool) {
	if remote == "" {
		return "", false
	}
	parent = path.Dir(remote)
	if parent == "." {
		parent = ""
	}
	return parent, true
}

// add records events seen at now
func (q *watchQueue) add(now time.Time, events []fs.ChangeEvent) {
	if now.After(q.last) {
		q.last = now
	}
	for _, event := range events {
		kind := watchFile
		switch {
		case event.Recursive:
			kind = watchTree
		case event.Path == "":
			// The root itself is only synced along with everything in it
			continue
		case event.EntryType == fs.EntryDirectory:
			kind = watchDir
		}
		q.entry(event.Path, kind).touch(now)
	}
}

// addFull makes a full sync due at now
func (q *watchQueue) addFull(now time.Time) {
	e := q.entry("", watchTree)
	e.touch(now)
	e.first = now.Add(-q.maxDelay)
}

// watchQuickRetries is how many times in a row something which
// failed to sync is retried without being delayed, when it looks as
// if it would work this time. More than that and it doesn't.
const watchQuickRetries = 3

// retry puts items back which failed to sync at now, having been
// taken at taken.
//
// blameless reports whether an item is known not to have been what
// failed. It is nil if that isn't known for any of them.
//
// An item which changed again in the meantime, or which isn't shown
// to have been what failed, is released as any change is, a few
// times, and then after delay(item) for an item which has failed
// once. It isn't shown to have been what failed if blameless says it
// wasn't, or if that isn't known and something else changed in the
// meantime, as a sync often fails when what it is syncing is changing.
//
// Otherwise it is released delay(item) from now, where
// item.failures is how many times it has failed like this, and as a
// tree if it was a file in case it isn't what it was thought to be,
// such as a file in the source where the destination has a
// directory. If it isn't known what failed, its first failure isn't
// delayed.
//
// It returns the longest any of them was delayed by.
func (q *watchQueue) retry(now, taken time.Time, items []watchItem, blameless func(item watchItem) bool, delay func(item watchItem) time.Duration) (longest time.Duration) {
	churn := !q.last.Before(taken)
	for _, item := range items {
		if item.isFull() {
			// Changes are synced on their own while it waits
			e := q.entry("", watchTree)
			e.touch(now)
			e.failures = max(e.failures, item.failures) + 1
			item.failures = e.failures
			if notBefore := now.Add(delay(item)); notBefore.After(e.notBefore) {
				e.notBefore = notBefore
			}
			longest = max(longest, e.notBefore.Sub(now))
			continue
		}
		innocent := churn
		if blameless != nil {
			innocent = blameless(item)
		}
		if (innocent || q.changedSince(item, taken)) && item.quick < watchQuickRetries {
			e := q.entry(item.path, item.kind)
			e.touch(now)
			e.quick = max(e.quick, item.quick+1)
			e.failures = max(e.failures, item.failures)
			continue
		}
		if failed := q.failed[item.path]; failed != nil {
			item.failures = max(item.failures, failed.failures)
		}
		if innocent {
			// Often enough to notice when whatever is failing stops,
			// without it counting against this
			longest = max(longest, q.fail(now, item, delay(watchItem{path: item.path, kind: item.kind, failures: 1})))
			continue
		}
		item.failures++
		if item.kind == watchFile {
			item.kind = watchTree
		}
		if blameless == nil && item.failures == 1 {
			e := q.entry(item.path, item.kind)
			e.touch(now)
			e.quick = 0
			e.failures = max(e.failures, item.failures)
			continue
		}
		wait := delay(item)
		if blameless == nil {
			wait = delay(watchItem{path: item.path, kind: item.kind, failures: item.failures - 1})
		}
		longest = max(longest, q.fail(now, item, wait))
	}
	return longest
}

// blame records that remote, which is inside something which was
// being synced, failed to sync at now, so that it is retried on its
// own later and left out when what it is inside is synced.
//
// It returns how long it is until then.
func (q *watchQueue) blame(now time.Time, remote string, delay func(item watchItem) time.Duration) time.Duration {
	item := watchItem{path: remote, kind: watchTree, failures: 1}
	if failed := q.failed[remote]; failed != nil {
		item.failures = failed.failures + 1
	}
	return q.fail(now, item, delay(item))
}

// fail records that item failed to sync at now and is to be retried
// after delay.
//
// It returns how long it is until it is retried.
func (q *watchQueue) fail(now time.Time, item watchItem, delay time.Duration) time.Duration {
	failed := q.failed[item.path]
	if failed == nil {
		failed = &watchEntry{kind: item.kind}
		q.failed[item.path] = failed
	}
	failed.touch(now)
	failed.kind = max(failed.kind, item.kind)
	failed.failures = max(failed.failures, item.failures)
	if notBefore := now.Add(delay); notBefore.After(failed.notBefore) {
		failed.notBefore = notBefore
	}
	return failed.notBefore.Sub(now)
}

// held returns what failed to sync and isn't due to be retried at
// now, which is left out of what is synced until it is.
func (q *watchQueue) held(now time.Time) (held []string) {
	for remote, failed := range q.failed {
		if failed.notBefore.After(now) {
			held = append(held, remote)
		}
	}
	return held
}

// synced records that a full sync taken at taken worked, so
// everything which changed before then has been synced.
func (q *watchQueue) synced(taken time.Time) {
	for remote, e := range q.entries {
		if e.last.Before(taken) {
			delete(q.entries, remote)
		}
	}
	clear(q.failed)
}

// changedSince reports whether anything item covers has changed at
// or after since.
func (q *watchQueue) changedSince(item watchItem, since time.Time) bool {
	for dir, ok := item.path, true; ok; dir, ok = watchParent(dir) {
		e := q.entries[dir]
		if e != nil && (dir == item.path || e.kind == watchTree) && !e.last.Before(since) {
			return true
		}
	}
	if item.kind != watchTree {
		return false
	}
	for other, e := range q.entries {
		if strings.HasPrefix(other, item.path+"/") && !e.last.Before(since) {
			return true
		}
	}
	return false
}

// entry returns the entry which covers a change of kind to remote,
// making one if needed.
func (q *watchQueue) entry(remote string, kind watchKind) *watchEntry {
	if remote == "" {
		if q.full == nil {
			q.full = &watchEntry{kind: watchTree}
		}
		return q.full
	}
	// A tree above remote covers it
	for dir, ok := watchParent(remote); ok && dir != ""; dir, ok = watchParent(dir) {
		if e := q.entries[dir]; e != nil && e.kind == watchTree {
			return e
		}
	}
	e := q.entries[remote]
	switch {
	case e == nil && len(q.entries) >= watchMaxItems:
		q.compact()
		return q.entry(remote, kind)
	case e == nil:
		e = &watchEntry{kind: kind}
		q.entries[remote] = e
	case e.kind != kind:
		// It isn't clear what is at remote, so look at all of it
		kind = watchTree
	}
	if kind == watchTree {
		q.makeTree(remote, e)
	}
	return e
}

// makeTree makes e, which is the entry for remote, into a tree,
// which covers everything below it.
func (q *watchQueue) makeTree(remote string, e *watchEntry) {
	e.kind = watchTree
	for other, otherEntry := range q.entries {
		if strings.HasPrefix(other, remote+"/") {
			e.merge(otherEntry)
			delete(q.entries, other)
		}
	}
}

// compact merges entries into trees for the directories they are in,
// starting with those deepest in the tree, until there are half as
// many as there can be.
//
// This keeps what is synced in proportion to what changed: 10,000
// files changing in 1,000 directories become those 1,000 directories.
func (q *watchQueue) compact() {
	for len(q.entries) > watchMaxItems/2 {
		depth := 0
		for remote := range q.entries {
			depth = max(depth, strings.Count(remote, "/"))
		}
		if depth == 0 {
			// They are all in the root, so everything needs syncing
			full := q.entry("", watchTree)
			for remote, e := range q.entries {
				full.merge(e)
				delete(q.entries, remote)
			}
			return
		}
		parents := make(map[string]*watchEntry)
		for remote, e := range q.entries {
			if strings.Count(remote, "/") != depth {
				continue
			}
			dir, _ := watchParent(remote)
			parent := parents[dir]
			if parent == nil {
				parent = &watchEntry{kind: watchTree}
				parents[dir] = parent
			}
			parent.merge(e)
			delete(q.entries, remote)
		}
		for dir, parent := range parents {
			if e := q.entries[dir]; e != nil {
				parent.merge(e)
			}
			q.entries[dir] = parent
		}
	}
}

// fullHeld reports whether everything needs syncing but can't be
// yet at now, because it has failed and is being retried later.
func (q *watchQueue) fullHeld(now time.Time) bool {
	return q.full != nil && q.full.notBefore.After(now)
}

// take removes and returns what is due at now in path order.
//
// When everything needs syncing that is all that is returned, as it
// covers every other change, which wait for it and are forgotten
// when synced is called. If it is being held back after failing, the
// other changes are returned without it so that they aren't held
// back too.
func (q *watchQueue) take(now time.Time) (items []watchItem) {
	if q.full != nil && !q.fullHeld(now) {
		if q.full.due(q.settle, q.maxDelay).After(now) {
			return nil
		}
		items = []watchItem{{kind: watchTree, failures: q.full.failures}}
		q.full = nil
		return items
	}
	due := make(map[string]watchItem)
	for remote, e := range q.entries {
		if !e.due(q.settle, q.maxDelay).After(now) {
			due[remote] = watchItem{path: remote, kind: e.kind, failures: e.failures, quick: e.quick}
			delete(q.entries, remote)
		}
	}
	for remote, failed := range q.failed {
		if !failed.notBefore.After(now) {
			item := due[remote]
			due[remote] = watchItem{path: remote, kind: failed.kind, failures: max(item.failures, failed.failures)}
			delete(q.failed, remote)
		}
	}
	for _, item := range due {
		items = append(items, item)
	}
	slices.SortFunc(items, func(a, b watchItem) int { return strings.Compare(a.path, b.path) })
	return items
}

// next returns the earliest time at which take will return
// something, given that it is now, and false if nothing is pending.
func (q *watchQueue) next(now time.Time) (next time.Time, ok bool) {
	if q.full != nil {
		next, ok = q.full.due(q.settle, q.maxDelay), true
		if !q.fullHeld(now) {
			return next, ok
		}
	}
	for _, e := range q.entries {
		if due := e.due(q.settle, q.maxDelay); !ok || due.Before(next) {
			next, ok = due, true
		}
	}
	for _, failed := range q.failed {
		if !ok || failed.notBefore.Before(next) {
			next, ok = failed.notBefore, true
		}
	}
	return next, ok
}
