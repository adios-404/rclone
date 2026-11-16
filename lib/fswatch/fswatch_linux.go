//go:build linux

package fswatch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/rclone/rclone/fs"
	"golang.org/x/sys/unix"
)

// watchMask is what is asked for on every watched directory
const watchMask = unix.IN_CREATE | unix.IN_DELETE | unix.IN_MOVED_FROM | unix.IN_MOVED_TO |
	unix.IN_CLOSE_WRITE | unix.IN_MODIFY | unix.IN_ATTRIB | unix.IN_DELETE_SELF | unix.IN_MOVE_SELF |
	unix.IN_ONLYDIR | unix.IN_EXCL_UNLINK

// openDir is how directories are opened, other than the root which
// may be a symlink.
const openDir = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC

// procFd is where the files this process has open can be found by
// their file descriptors.
const procFd = "/proc/self/fd"

const (
	rootNode = 0          // index of the node for the root
	noNode   = ^uint32(0) // in place of the index of a node when there isn't one
	noWatch  = -1         // watch descriptor of a directory which isn't watched
)

// moveWait is how long to wait for a directory which isn't where it
// was to be found somewhere else before deciding it has left the tree.
const moveWait = 100 * time.Millisecond

// How long to wait before looking again at a name which couldn't be
// looked at, and how many times to look before giving up.
const (
	recheckWait  = 20 * time.Millisecond
	recheckTries = 5
)

// node is a directory in the tree.
//
// A node holds its own name and its parent rather than its path, so
// that the memory used does not grow with the depth of the tree and
// renaming a directory changes one node.
type node struct {
	parent uint32 // index of the node this is in, or noNode for the root and for a directory which isn't where it was
	wd     int32  // inotify watch descriptor, or noWatch
	name   string // leaf name, unused for the root
	first  uint32 // index of the first node in this one, or noNode
	next   uint32 // index of the next node in parent, or noNode
	prev   uint32 // index of the previous node in parent, or noNode
}

// childKey finds a node from its parent and name
type childKey struct {
	parent uint32
	name   string
}

// recheck is a name which needs looking at again
type recheck struct {
	tries int  // how many times it has been looked at since
	loop  bool // set if what was found there was a directory it is itself in
}

// watcher watches a directory tree with inotify.
//
// inotify has no recursive watch, so there is one watch per
// directory, and a node for each which says where it is in the tree.
//
// An event gives the watch of the directory something happened in
// and a name in it. Events are handled some time after they happened
// and directories can be renamed or replaced in between, so an event
// about a directory is only taken as a reason to look at what has
// that name now. What is found is recognised by its watch, which
// stays with a directory wherever it is moved to, and the nodes are
// made to match. Whatever changes a name is followed by an event for
// it, so the last look at a name is at what is still there.
type watcher struct {
	root     string
	prefix   string // root without a trailing slash
	opt      Options
	fn       func([]Event)
	addWatch func(fd int, path string, mask uint32) (int, error) // unix.InotifyAddWatch unless testing
	device   func(fd int) (uint64, error)                        // device number of an open directory
	done     chan struct{}                                       // closed by stop
	retry    time.Duration                                       // how often to retry what failed

	mu      sync.Mutex // protects file and stopped
	file    *os.File   // the inotify instance, nil if there isn't one
	stopped bool

	// The fields below are only used by the goroutine running run,
	// or before it has started.
	fd        int // of the inotify instance, kept as file.Fd() would make it blocking
	dev       uint64
	realRoot  string    // root with symlinks resolved
	rootID    dirID     // the directory root was when watching started
	mounts    string    // what was mounted at and below the root when watching started
	checked   time.Time // when rootID and mounts were last checked
	nodes     []node
	free      []uint32 // unused indexes in nodes
	byWd      map[int32]uint32
	children  map[childKey]uint32
	lost      map[uint32]struct{}  // nodes found not to be where they were in the current read
	oldLost   map[uint32]struct{}  // the same for the read before
	lostAt    time.Time            // when a node was last added to lost
	rechecks  map[childKey]recheck // names which couldn't be looked at
	unwatched map[uint32]struct{}  // nodes which couldn't be watched, such as when at the limit on watches
	logged    bool                 // set while there are unwatched nodes which have been logged
	nextScan  time.Time            // when to report the unwatched nodes next
	corrupt   bool                 // set if the nodes are found not to make sense
	suspect   bool                 // set if the nodes may not match the tree
	failed    bool                 // set if watching failed and needs retrying
	dirBuf    []byte               // for reading directories into
}

// dirID identifies a directory
type dirID struct {
	dev, ino uint64
}

// Supported reports whether this build can watch for changes
func Supported() bool {
	return true
}

// Watch reports changes under root to fn until ctx is cancelled.
//
// It returns once watching is established, or an error if it can't
// be or ctx is cancelled first. fn is called from a single goroutine,
// which may be in the middle of a call when ctx is cancelled.
//
// Directories which can't be watched, because of the limit on the
// number of watches or because they are in the tree in another place
// as well, are reported with Rescan set every minute instead. If
// watching fails later on then Rescan is reported for root, and
// again once watching has been re-established.
//
// Something being mounted or unmounted at or below root, and the
// directory at root being replaced by renaming one above it, are
// noticed within a minute rather than at once.
func Watch(ctx context.Context, root string, opt Options, fn func([]Event)) error {
	w := newWatcher(root, opt, fn)
	// Stopping it here lets ctx interrupt watching a big tree
	context.AfterFunc(ctx, w.stop)
	err := w.start()
	if ctx.Err() != nil {
		w.stop()
		return ctx.Err()
	}
	if err != nil {
		w.stop()
		return err
	}
	go w.run()
	return nil
}

// newWatcher makes a watcher of root which start sets going
func newWatcher(root string, opt Options, fn func([]Event)) *watcher {
	return &watcher{
		root:     root,
		prefix:   strings.TrimSuffix(root, "/"),
		opt:      opt,
		fn:       fn,
		addWatch: unix.InotifyAddWatch,
		device:   device,
		done:     make(chan struct{}),
		retry:    retryDelay,
	}
}

// device returns the device number of the directory open as fd
func device(fd int) (uint64, error) {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return 0, err
	}
	return uint64(st.Dev), nil // nolint: unconvert
}

// identify returns what identifies the directory at path
func identify(path string) (id dirID, err error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return id, err
	}
	return dirID{dev: uint64(st.Dev), ino: st.Ino}, nil // nolint: unconvert
}

// mountsBelow returns a description of what is mounted at and below
// dir, which must have no symlinks in it, or "" if that can't be
// found out.
//
// inotify doesn't report a file system being mounted, after which
// the watches are on directories which can no longer be reached.
func mountsBelow(dir string) string {
	mountInfo, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return ""
	}
	var below strings.Builder
	for line := range strings.SplitSeq(string(mountInfo), "\n") {
		// The fields are the mount ID, parent ID, device, root and mount point
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		// Spaces and the like in the mount point are written in octal
		mountPoint := fields[4]
		if strings.Contains(mountPoint, `\`) {
			if unquoted, err := strconv.Unquote(`"` + mountPoint + `"`); err == nil {
				mountPoint = unquoted
			}
		}
		if mountPoint == dir || strings.HasPrefix(mountPoint, strings.TrimSuffix(dir, "/")+"/") {
			below.WriteString(fields[0] + " " + fields[4] + "\n")
		}
	}
	return below.String()
}

// start makes a new inotify instance watching the whole tree.
//
// Any existing instance is closed first, whether or not this
// succeeds, as its watches no longer match the tree.
func (w *watcher) start() error {
	if !w.setFile(nil) {
		return os.ErrClosed
	}
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		return fmt.Errorf("failed to start inotify: %w", err)
	}
	// Reads on a non-blocking os.File can have a deadline and are interrupted by Close
	file := os.NewFile(uintptr(fd), "inotify")
	w.fd = fd
	w.nodes = w.nodes[:0]
	w.free = w.free[:0]
	w.byWd = make(map[int32]uint32)
	w.children = make(map[childKey]uint32)
	w.lost, w.oldLost = nil, nil
	w.rechecks = make(map[childKey]recheck)
	w.corrupt, w.suspect = false, false
	w.unwatched = make(map[uint32]struct{})
	w.logged = false
	if _, err = os.Stat(procFd); err != nil {
		err = fmt.Errorf("can't watch without %s: %w", procFd, err)
	} else if w.realRoot, err = filepath.EvalSymlinks(w.root); err == nil {
		w.mounts = mountsBelow(w.realRoot)
		w.checked = time.Now()
		err = w.addRoot()
	}
	if err != nil {
		_ = file.Close()
		return err
	}
	if !w.setFile(file) {
		_ = file.Close()
		return os.ErrClosed
	}
	return nil
}

// setFile makes file the inotify instance in use and closes the one
// it replaces. It returns false if the watcher has been stopped.
func (w *watcher) setFile(file *os.File) bool {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return false
	}
	old := w.file
	w.file = file
	w.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return true
}

// stop stops watching
func (w *watcher) stop() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	file := w.file
	w.mu.Unlock()
	close(w.done)
	if file != nil {
		_ = file.Close()
	}
}

// addRoot watches the root and every directory below it
func (w *watcher) addRoot() error {
	fd, err := unix.Open(w.root, openDir&^unix.O_NOFOLLOW, 0)
	if err == nil {
		defer func() { _ = unix.Close(fd) }()
		w.rootID, err = identify(procFd + "/" + strconv.Itoa(fd))
	}
	if err == nil && w.opt.OneFileSystem {
		w.dev, err = w.device(fd)
	}
	var wd int32
	if err == nil {
		wd, err = w.watchFd(fd)
	}
	if errors.Is(err, os.ErrClosed) {
		return err
	}
	if err != nil {
		if errors.Is(err, unix.ENOSPC) {
			err = w.limitError()
		}
		return fmt.Errorf("failed to watch %q: %w", w.root, err)
	}
	_, err = w.scan(fd, w.newNode(noNode, wd, ""))
	return err
}

// What open found
const (
	openOK    = iota // the directory, which is open
	openMoved        // something else, or nothing, where the node says it is
	openNever        // that the directory can't be opened by its path, such as when that is too long
)

// open opens the directory of the node at idx.
//
// It is found by where the node says it is, which may be out of
// date, so what is opened is checked to have the watch of the node.
//
// It returns an error only if the watcher was stopped.
func (w *watcher) open(idx uint32) (fd int, found int, err error) {
	path, flags, known := w.root, openDir&^unix.O_NOFOLLOW, true
	if idx != rootNode {
		path, known = w.path(idx)
		flags = openDir
	}
	if !known || w.nodes[idx].wd == noWatch {
		return -1, openMoved, nil
	}
	fd, err = unix.Open(path, flags, 0)
	if errors.Is(err, unix.ENAMETOOLONG) {
		return -1, openNever, nil
	}
	if err != nil {
		w.unexpected(err)
		return -1, openMoved, nil
	}
	// Watching a directory which is already watched gives the same watch
	wd, err := w.watchFd(fd)
	if err == nil {
		node, isWatched := w.byWd[wd]
		if isWatched && node == idx {
			return fd, openOK, nil
		}
		if !isWatched {
			w.rmWatch(wd)
		}
	}
	_ = unix.Close(fd)
	if errors.Is(err, os.ErrClosed) {
		return -1, openMoved, err
	}
	return -1, openMoved, nil
}

// unexpected notes that err, from opening or reading a directory,
// isn't one which says there is nothing there which can be watched,
// so directories may have been missed.
func (w *watcher) unexpected(err error) {
	switch {
	case err == nil, errors.Is(err, unix.ENOENT), errors.Is(err, unix.ENOTDIR), errors.Is(err, unix.ELOOP),
		errors.Is(err, unix.EACCES), errors.Is(err, unix.EPERM), errors.Is(err, unix.ENAMETOOLONG):
	default:
		w.suspect = true
	}
}

// check looks at what name is now in the directory of the node
// parent and makes the nodes match.
//
// If the directory of parent can't be found, name is looked at again
// once it has been.
//
// It returns changed set if there is a directory there which wasn't
// known to be, so anything in it may have been missed. It returns an
// error only if the watcher was stopped.
func (w *watcher) check(parent uint32, name string) (changed bool, err error) {
	fd, found, err := w.open(parent)
	if err != nil {
		return false, err
	}
	key := childKey{parent, name}
	switch found {
	case openMoved:
		if _, found := w.rechecks[key]; !found {
			w.rechecks[key] = recheck{}
		}
		return false, nil
	case openNever:
		// Nor could what is in it be listed
		delete(w.rechecks, key)
		return false, nil
	}
	delete(w.rechecks, key)
	changed, err = w.bind(fd, parent, name)
	_ = unix.Close(fd)
	return changed, err
}

// bind makes the nodes match what name is in the directory open as
// parentFd, which is that of the node parent.
//
// Directories are opened relative to the one they are in and watched
// while open, so that one can't be mistaken for another if they are
// being renamed at the time.
//
// It returns as check does.
func (w *watcher) bind(parentFd int, parent uint32, name string) (changed bool, err error) {
	key := childKey{parent, name}
	old, hadOld := w.children[key]
	fd, err := unix.Openat(parentFd, name, openDir, 0)
	w.unexpected(err)
	if err == nil && w.opt.OneFileSystem {
		dev, devErr := w.device(fd)
		w.unexpected(devErr)
		if devErr != nil || dev != w.dev {
			_ = unix.Close(fd)
			err = unix.EXDEV
		}
	}
	if err == nil && w.opt.Skip != nil {
		if path, known := w.path(parent); known && w.opt.Skip(path+"/"+name) {
			_ = unix.Close(fd)
			err = unix.ENOENT
		}
	}
	if err != nil {
		// There is nothing there to watch, or it isn't a directory,
		// or it can't be read, as it couldn't be for a listing, or
		// it isn't wanted.
		if hadOld {
			w.lose(old)
		}
		return false, nil
	}
	defer func() { _ = unix.Close(fd) }()
	wd, err := w.watchFd(fd)
	if errors.Is(err, os.ErrClosed) {
		return false, err
	}
	if err != nil {
		// A directory which is already watched can be watched again,
		// even at the limit, so this isn't one which is.
		if hadOld && w.nodes[old].wd == noWatch {
			return false, nil
		}
		if hadOld {
			w.lose(old)
		}
		if errors.Is(err, unix.ENOSPC) {
			err = w.limitError()
		}
		w.addUnwatched(parent, name, err)
		return true, nil
	}
	idx, isWatched := w.byWd[wd]
	switch {
	case !isWatched:
		if hadOld {
			w.lose(old)
		}
		// The watch is added before the directory is read so that
		// anything created after the read is reported as an event.
		_, err = w.scan(fd, w.newNode(parent, wd, name))
		return true, err
	case hadOld && idx == old:
		return false, nil
	}
	if hadOld {
		w.lose(old)
	}
	if idx == rootNode || w.below(parent, idx) {
		// A directory can only be in itself if it is mounted there as
		// well, in which case it is left where it was, unless the
		// nodes above parent are out of date.
		if _, found := w.rechecks[key]; !found {
			w.rechecks[key] = recheck{loop: true}
		}
		return false, nil
	}
	if w.nodes[idx].parent != noNode {
		// If it is still where it was too then it is in two places,
		// which a directory mounted somewhere else as well is. It can
		// only be watched in one of them.
		elsewhere, found, err := w.open(idx)
		if err != nil {
			return false, err
		}
		if found == openOK {
			_ = unix.Close(elsewhere)
			if path, known := w.path(idx); known {
				w.addUnwatched(parent, name, fmt.Errorf("%q is the same directory as %q", name, path))
			}
			return true, nil
		}
	}
	// It has been moved here from where it was
	w.unlink(idx)
	delete(w.lost, idx)
	delete(w.oldLost, idx)
	w.link(idx, parent, name)
	return true, nil
}

// scan watches the directories in the directory open as fd, which is
// that of the node at idx, and every directory below them.
//
// It returns changed set if there was a directory there which
// wasn't known to be. It returns an error only if the watcher was
// stopped.
func (w *watcher) scan(fd int, idx uint32) (changed bool, err error) {
	names, err := w.subdirs(fd)
	w.unexpected(err)
	for _, name := range names {
		bound, err := w.bind(fd, idx, name)
		if err != nil {
			return changed, err
		}
		changed = changed || bound
	}
	return changed, nil
}

// rescan watches any directories in the directory of the node at
// idx which aren't known about, as scan does.
func (w *watcher) rescan(idx uint32) (changed bool, err error) {
	fd, found, err := w.open(idx)
	if err != nil || found != openOK {
		return false, err
	}
	defer func() { _ = unix.Close(fd) }()
	return w.scan(fd, idx)
}

// watchFd adds a watch on the directory open as fd, returning its
// watch descriptor.
func (w *watcher) watchFd(fd int) (wd int32, err error) {
	// Holding the lock stops the instance being closed, and its file
	// descriptor reused, in the middle of this.
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return 0, os.ErrClosed
	}
	added, err := w.addWatch(w.fd, procFd+"/"+strconv.Itoa(fd), watchMask)
	return int32(added), err
}

// subdirs returns the names of the entries in the directory open as
// fd which may be directories.
func (w *watcher) subdirs(fd int) (names []string, err error) {
	if w.dirBuf == nil {
		w.dirBuf = make([]byte, 32*1024)
	}
	nameOffset := int(unsafe.Offsetof(unix.Dirent{}.Name))
	for {
		n, err := unix.ReadDirent(fd, w.dirBuf)
		if err == unix.EINTR {
			continue
		}
		if err != nil || n <= 0 {
			return names, err
		}
		for buf := w.dirBuf[:n]; len(buf) >= nameOffset; {
			entry := (*unix.Dirent)(unsafe.Pointer(&buf[0]))
			size := int(entry.Reclen)
			if size < nameOffset || size > len(buf) {
				return names, nil
			}
			name := buf[nameOffset:size]
			buf = buf[size:]
			if i := bytes.IndexByte(name, 0); i >= 0 {
				name = name[:i]
			}
			// Some file systems don't say what kind of entry it is
			if entry.Type != unix.DT_DIR && entry.Type != unix.DT_UNKNOWN {
				continue
			}
			if s := string(name); s != "." && s != ".." {
				names = append(names, s)
			}
		}
	}
}

// limitError describes the limit on the number of watches being reached
func (w *watcher) limitError() error {
	return fmt.Errorf("too many directories to watch (%d watched): raise the limit with \"sysctl fs.inotify.max_user_watches=<number>\"", len(w.byWd))
}

// addUnwatched records that the directory name in parent, which must
// not have a node, can't be watched because of err, such as the limit
// on the number of watches being reached.
func (w *watcher) addUnwatched(parent uint32, name string, err error) {
	if !w.logged {
		fs.Logf(w.root, "Can't watch every directory for changes - checking the rest every %v instead, or less often if there are a lot of them: %v", w.retry, err)
		w.logged = true
		w.nextScan = time.Now().Add(w.retry)
	}
	w.newNode(parent, noWatch, name)
}

// scanDelay returns how long to leave between reporting the
// unwatched nodes as needing a rescan.
//
// Each one is a whole tree to look through, so the more there are
// the longer they are left: a minute up to 6,000 of them, and an
// hour for 360,000.
func (w *watcher) scanDelay() time.Duration {
	return max(w.retry, w.retry/6000*time.Duration(len(w.unwatched)))
}

// newNode makes a node for the directory name in the node parent,
// which must not have one, and returns its index. parent is noNode
// for the root.
func (w *watcher) newNode(parent uint32, wd int32, name string) (idx uint32) {
	n := node{parent: noNode, wd: wd, first: noNode, next: noNode, prev: noNode}
	if last := len(w.free) - 1; last >= 0 {
		idx = w.free[last]
		w.free = w.free[:last]
		w.nodes[idx] = n
	} else {
		idx = uint32(len(w.nodes))
		w.nodes = append(w.nodes, n)
	}
	if wd == noWatch {
		w.unwatched[idx] = struct{}{}
	} else {
		w.byWd[wd] = idx
	}
	if parent != noNode {
		w.link(idx, parent, name)
	}
	return idx
}

// isNode reports whether there is a node at idx
func (w *watcher) isNode(idx uint32) bool {
	if int(idx) >= len(w.nodes) {
		return false
	}
	if wd := w.nodes[idx].wd; wd != noWatch {
		return w.byWd[wd] == idx
	}
	_, isUnwatched := w.unwatched[idx]
	return isUnwatched
}

// link puts the node at idx, which must not be in a node, in the node
// parent as name, which must not be taken.
func (w *watcher) link(idx, parent uint32, name string) {
	n, p := &w.nodes[idx], &w.nodes[parent]
	n.parent, n.name = parent, name
	n.prev, n.next = noNode, p.first
	if p.first != noNode {
		w.nodes[p.first].prev = idx
	}
	p.first = idx
	w.children[childKey{parent, name}] = idx
}

// unlink takes the node at idx out of the node it is in, if it is in one
func (w *watcher) unlink(idx uint32) {
	n := &w.nodes[idx]
	if n.parent == noNode {
		return
	}
	if n.prev != noNode {
		w.nodes[n.prev].next = n.next
	} else {
		w.nodes[n.parent].first = n.next
	}
	if n.next != noNode {
		w.nodes[n.next].prev = n.prev
	}
	delete(w.children, childKey{n.parent, n.name})
	n.parent, n.prev, n.next = noNode, noNode, noNode
}

// lose takes the node at idx out of the node it is in, as its
// directory isn't there any more.
//
// The node is kept for a while with what is in it, as the directory
// may be found to have been moved somewhere else in the tree.
func (w *watcher) lose(idx uint32) {
	if w.nodes[idx].wd == noWatch {
		// It couldn't be recognised without a watch
		w.dropNode(idx)
		return
	}
	w.unlink(idx)
	if w.lost == nil {
		w.lost = make(map[uint32]struct{})
	}
	w.lost[idx] = struct{}{}
	w.lostAt = time.Now()
}

// dropNode forgets the node at idx, which must not be the root, if
// it hasn't been forgotten already.
func (w *watcher) dropNode(idx uint32) {
	if !w.isNode(idx) {
		return
	}
	w.unlink(idx)
	// What was in a directory which has gone can't be there any more
	for child := w.nodes[idx].first; child != noNode; child = w.nodes[idx].first {
		w.lose(child)
	}
	w.forget(idx)
}

// dropTree forgets the node at idx, which must not be the root, and
// every node below it, and stops watching their directories.
func (w *watcher) dropTree(idx uint32) {
	w.unlink(idx)
	for todo := []uint32{idx}; len(todo) > 0; {
		last := len(todo) - 1
		idx, todo = todo[last], todo[:last]
		n := &w.nodes[idx]
		for child := n.first; child != noNode; child = w.nodes[child].next {
			delete(w.children, childKey{idx, w.nodes[child].name})
			todo = append(todo, child)
		}
		if n.wd != noWatch {
			w.rmWatch(n.wd)
		}
		w.forget(idx)
	}
}

// forget frees the node at idx, which must not be in a node or have
// nodes in it which are to be kept.
func (w *watcher) forget(idx uint32) {
	n := &w.nodes[idx]
	if n.wd != noWatch {
		delete(w.byWd, n.wd)
	}
	delete(w.unwatched, idx)
	delete(w.lost, idx)
	delete(w.oldLost, idx)
	// Its index may be used for another directory
	for key := range w.rechecks {
		if key.parent == idx {
			delete(w.rechecks, key)
		}
	}
	*n = node{parent: noNode, wd: noWatch, first: noNode, next: noNode, prev: noNode}
	w.free = append(w.free, idx)
}

// dropLost forgets the nodes in oldLost and everything below them,
// as their directories weren't found anywhere else so have left the
// tree. If one is found later it is watched as a new directory is.
func (w *watcher) dropLost() {
	for idx := range w.oldLost {
		w.dropTree(idx)
	}
	w.oldLost = nil
}

// below reports whether the node at idx is the node at dir or below it
func (w *watcher) below(idx, dir uint32) bool {
	for steps := 0; idx != dir; idx = w.nodes[idx].parent {
		if idx == noNode {
			return false
		}
		if steps++; steps > len(w.nodes) {
			// A node can't be above itself
			w.corrupt = true
			return false
		}
	}
	return true
}

// path returns the path of the node at idx, without a trailing slash.
//
// It returns false if that isn't known, as the node or one above it
// isn't where it was and hasn't been found yet.
func (w *watcher) path(idx uint32) (string, bool) {
	var names []string
	size := len(w.prefix)
	for idx != rootNode {
		n := &w.nodes[idx]
		if n.parent == noNode {
			return "", false
		}
		if len(names) > len(w.nodes) {
			// A node can't be above itself
			w.corrupt = true
			return "", false
		}
		names = append(names, n.name)
		size += 1 + len(n.name)
		idx = n.parent
	}
	var out strings.Builder
	out.Grow(size)
	out.WriteString(w.prefix)
	for i := len(names) - 1; i >= 0; i-- {
		out.WriteByte('/')
		out.WriteString(names[i])
	}
	return out.String(), true
}

// run reads events and reports them until the watcher is stopped
func (w *watcher) run() {
	buf := make([]byte, 64*1024)
	for {
		if w.failed {
			if !w.recover() {
				return
			}
			continue
		}
		deadline := w.checked.Add(w.retry)
		switch {
		case len(w.rechecks) > 0:
			deadline = time.Now().Add(recheckWait)
		case len(w.lost) > 0:
			deadline = time.Now().Add(moveWait)
		case len(w.unwatched) > 0 && w.nextScan.Before(deadline):
			deadline = w.nextScan
		}
		// Only this goroutine changes w.file once it is running
		_ = w.file.SetReadDeadline(deadline)
		n, err := w.file.Read(buf)
		if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			if !w.fail(err) {
				return
			}
			continue
		}
		// Where a directory was moved to is nearly always reported
		// straight after where it was moved from, so a directory which
		// isn't found by the end of the read after the one it was lost
		// in is taken to have left the tree. Waking up to look at
		// names again doesn't count as a read.
		rotated := n > 0 || time.Since(w.lostAt) >= moveWait
		if rotated {
			w.oldLost, w.lost = w.lost, nil
		}
		var events []Event
		restarted := false
		for offset := 0; offset+unix.SizeofInotifyEvent <= n && !restarted; {
			raw := (*unix.InotifyEvent)(unsafe.Pointer(&buf[offset]))
			nameStart := offset + unix.SizeofInotifyEvent
			offset = nameStart + int(raw.Len)
			if offset > n {
				break
			}
			name := string(buf[nameStart:offset])
			if i := strings.IndexByte(name, 0); i >= 0 {
				name = name[:i]
			}
			// Events left in the buffer after a restart are from the old inotify instance
			events, restarted = w.handle(events, raw.Wd, raw.Mask, raw.Cookie, name)
		}
		if !restarted {
			if rotated {
				w.dropLost()
			}
			// Everything which has happened is known about once the events have all been read
			events = w.recheck(events, n < len(buf)/2)
			events = w.scanUnwatched(events)
			if w.corrupt {
				events, restarted = w.restart(events)
			}
		}
		if !restarted && time.Since(w.checked) >= w.retry {
			w.checked = time.Now()
			if w.suspect || w.rootChanged() {
				events, _ = w.restart(events)
			}
		}
		if len(events) > 0 {
			w.fn(events)
		}
	}
}

// fail reports that watching has stopped working because of err,
// and returns false if that is because the watcher was stopped.
func (w *watcher) fail(err error) bool {
	if !w.setFile(nil) {
		return false
	}
	w.setFailed(err)
	w.fn([]Event{w.rescanRoot()})
	return true
}

// setFailed records whether watching has failed, which it has if err is not nil
func (w *watcher) setFailed(err error) {
	w.failed = err != nil
	if w.failed {
		fs.Errorf(w.root, "Failed to watch for changes - will retry every %v: %v", w.retry, err)
	}
}

// recover retries watching until it works.
//
// It returns false if the watcher was stopped first.
func (w *watcher) recover() bool {
	for {
		timer := time.NewTimer(w.retry)
		select {
		case <-w.done:
			timer.Stop()
			return false
		case <-timer.C:
		}
		err := w.start()
		if errors.Is(err, os.ErrClosed) {
			return false
		}
		if err == nil {
			fs.Logf(w.root, "Watching for changes again")
			w.failed = false
			w.fn([]Event{w.rescanRoot()})
			return true
		}
	}
}

// rootChanged reports whether the root is a different directory to
// the one being watched, or something has been mounted or unmounted
// at or below it, neither of which inotify reports.
func (w *watcher) rootChanged() bool {
	id, err := identify(w.root)
	return err != nil || id != w.rootID || mountsBelow(w.realRoot) != w.mounts
}

// rescanRoot is reported when changes may have been missed anywhere
func (w *watcher) rescanRoot() Event {
	return Event{Path: w.root, IsDir: true, Known: true, Rescan: true}
}

// handle works out what to report for one inotify event, appending
// it to events.
//
// It returns restarted set if it closed the inotify instance the
// event came from, in which case any more events read from it must
// be discarded.
func (w *watcher) handle(events []Event, wd int32, mask, _ uint32, name string) (_ []Event, restarted bool) {
	if mask&unix.IN_Q_OVERFLOW != 0 {
		// Events were lost, so the nodes may no longer match the tree
		return w.restart(events)
	}
	idx, found := w.byWd[wd]
	if !found {
		return events, false
	}
	if idx == rootNode && mask&(unix.IN_DELETE_SELF|unix.IN_MOVE_SELF|unix.IN_UNMOUNT|unix.IN_IGNORED) != 0 {
		return w.restart(events)
	}
	if mask&unix.IN_UNMOUNT != 0 {
		// Something else may be mounted here straight afterwards,
		// which can't be told from what was, so start again soon
		w.suspect = true
		// Watch what the file system which was mounted here was hiding
		n := w.nodes[idx]
		path, known := w.path(idx)
		w.dropNode(idx)
		// Directories below the one which was mounted on have gone
		// along with the nodes for the directories they were in
		if n.parent == noNode {
			return events, false
		}
		if _, err := w.check(n.parent, n.name); err != nil || !known {
			return events, false
		}
		return append(events, Event{Path: path, IsDir: true, Known: true, Rescan: true}), false
	}
	if mask&unix.IN_IGNORED != 0 {
		w.dropNode(idx)
		return events, false
	}
	if name == "" {
		// A change to a watched directory itself, which the watch on
		// its parent reports too
		return events, false
	}
	// What happens in a directory which isn't where it was can't be
	// reported, as it isn't known where it is, but is covered by the
	// rescan reported for the directory when it is found.
	path, known := w.path(idx)
	path += "/" + name
	if mask&unix.IN_ISDIR == 0 {
		if known {
			events = append(events, Event{Path: path, Known: true})
		}
		return events, false
	}
	event := Event{Path: path, IsDir: true, Known: true}
	child, hasNode := w.children[childKey{idx, name}]
	// Anything put in a new directory before it was watched was not
	// reported, and nor is what is in a directory moved here
	arrived := mask&(unix.IN_CREATE|unix.IN_MOVED_TO) != 0
	// A directory which wasn't known about needs nothing doing when it goes
	left := mask&(unix.IN_DELETE|unix.IN_MOVED_FROM) != 0 && hasNode
	// A directory which couldn't be read may be able to be now, or
	// what is in one which could only be listed may be
	permissions := mask&unix.IN_ATTRIB != 0
	switch {
	case arrived || left || permissions && !hasNode:
		changed, err := w.check(idx, strings.Clone(name))
		if err != nil {
			return events, false
		}
		event.Rescan = arrived || changed
	case permissions && w.nodes[child].wd != noWatch:
		changed, err := w.rescan(child)
		if err != nil {
			return events, false
		}
		event.Rescan = changed
	}
	if known {
		events = append(events, event)
	}
	return events, false
}

// recheck looks again at the names which couldn't be looked at,
// reporting any directories found.
//
// caughtUp says whether everything which has happened is known
// about. If a name still can't be looked at when it has been several
// times, something isn't as the nodes say it is.
func (w *watcher) recheck(events []Event, caughtUp bool) []Event {
	if len(w.rechecks) == 0 {
		return events
	}
	keys := make([]childKey, 0, len(w.rechecks))
	for key := range w.rechecks {
		keys = append(keys, key)
	}
	for _, key := range keys {
		was, found := w.rechecks[key]
		if !found {
			continue
		}
		delete(w.rechecks, key)
		// What is in a directory which isn't watched is rescanned anyway
		if !w.isNode(key.parent) || w.nodes[key.parent].wd == noWatch {
			continue
		}
		changed, err := w.check(key.parent, key.name)
		if err != nil {
			return events
		}
		if path, known := w.path(key.parent); changed && known {
			events = append(events, Event{Path: path + "/" + key.name, IsDir: true, Known: true, Rescan: true})
		}
		now, again := w.rechecks[key]
		if !again {
			continue
		}
		// A directory which isn't where it was is either found or forgotten
		if caughtUp && w.below(key.parent, rootNode) {
			was.tries++
		}
		if was.tries < recheckTries {
			now.tries = was.tries
			w.rechecks[key] = now
			continue
		}
		delete(w.rechecks, key)
		// A directory which is mounted inside itself stays that way
		if !now.loop {
			w.suspect = true
		}
	}
	return events
}

// rmWatch removes the watch wd from the inotify instance
func (w *watcher) rmWatch(wd int32) {
	// Holding the lock stops the instance being closed, and its file
	// descriptor reused, in the middle of this.
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.stopped {
		_, _ = unix.InotifyRmWatch(w.fd, uint32(wd))
	}
}

// scanUnwatched reports the directories which aren't being watched
// as needing a rescan if that is due, after trying to watch them
// again.
func (w *watcher) scanUnwatched(events []Event) []Event {
	if len(w.unwatched) == 0 || time.Now().Before(w.nextScan) {
		return events
	}
	w.nextScan = time.Now().Add(w.scanDelay())
	unwatched := make([]uint32, 0, len(w.unwatched))
	for idx := range w.unwatched {
		unwatched = append(unwatched, idx)
	}
	for _, idx := range unwatched {
		if !w.isNode(idx) || w.nodes[idx].wd != noWatch {
			continue
		}
		n := w.nodes[idx]
		if path, known := w.path(idx); known {
			events = append(events, Event{Path: path, IsDir: true, Known: true, Rescan: true})
		}
		if _, err := w.check(n.parent, n.name); err != nil {
			break
		}
	}
	if len(w.unwatched) == 0 && w.logged {
		fs.Logf(w.root, "Watching every directory for changes again")
		w.logged = false
	}
	return events
}

// restart starts watching from scratch when the watches no longer
// match the tree, and reports that anything may have changed.
func (w *watcher) restart(events []Event) (_ []Event, restarted bool) {
	fs.Infof(w.root, "Changes may have been missed - starting to watch again")
	err := w.start()
	if !errors.Is(err, os.ErrClosed) {
		w.setFailed(err)
	}
	return append(events, w.rescanRoot()), true
}
