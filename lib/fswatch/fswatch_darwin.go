//go:build darwin && cgo

package fswatch

/*
#cgo LDFLAGS: -framework CoreServices

#include <CoreServices/CoreServices.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/param.h>
#include <unistd.h>

void fswatchCallback(uintptr_t handle, size_t n, char **paths, FSEventStreamEventFlags *flags);

// fswatchEvents passes the events FSEvents reports on to Go.
//
// info is passed as an integer because it is a handle, not a pointer,
// and Go must not see it as one.
static void fswatchEvents(ConstFSEventStreamRef stream, void *info, size_t n, void *paths,
		const FSEventStreamEventFlags *flags, const FSEventStreamEventId *ids) {
	fswatchCallback((uintptr_t)info, n, paths, (FSEventStreamEventFlags *)flags);
}

// fswatchStream is a running stream and the queue its callback runs on
typedef struct {
	FSEventStreamRef stream;
	dispatch_queue_t queue;
} fswatchStream;

// fswatchStart starts reporting changes below path to fswatchCallback,
// which is passed handle. The stream returned is NULL if it failed.
static fswatchStream fswatchStart(const char *path, uintptr_t handle, CFTimeInterval latency) {
	fswatchStream s = {NULL, NULL};
	CFStringRef cfPath = CFStringCreateWithCString(NULL, path, kCFStringEncodingUTF8);
	if (cfPath == NULL) {
		return s;
	}
	CFArrayRef paths = CFArrayCreate(NULL, (const void **)&cfPath, 1, &kCFTypeArrayCallBacks);
	FSEventStreamContext context = {0, (void *)handle, NULL, NULL, NULL};
	s.stream = FSEventStreamCreate(NULL, fswatchEvents, &context, paths,
		kFSEventStreamEventIdSinceNow, latency,
		kFSEventStreamCreateFlagFileEvents | kFSEventStreamCreateFlagNoDefer | kFSEventStreamCreateFlagWatchRoot);
	CFRelease(paths);
	CFRelease(cfPath);
	if (s.stream == NULL) {
		return s;
	}
	s.queue = dispatch_queue_create("rclone.fswatch", DISPATCH_QUEUE_SERIAL);
	FSEventStreamSetDispatchQueue(s.stream, s.queue);
	if (!FSEventStreamStart(s.stream)) {
		FSEventStreamInvalidate(s.stream);
		FSEventStreamRelease(s.stream);
		dispatch_release(s.queue);
		s.stream = NULL;
	}
	return s;
}

// fswatchPath puts the path the system knows what is at path by in
// out, which must have room for MAXPATHLEN bytes. It returns -1 if
// it fails.
static int fswatchPath(const char *path, char *out) {
	int fd = open(path, O_EVTONLY);
	if (fd < 0) {
		return -1;
	}
	int err = fcntl(fd, F_GETPATH, out);
	close(fd);
	return err;
}

static void fswatchInvalidate(void *stream) {
	FSEventStreamStop(stream);
	FSEventStreamInvalidate(stream);
}

// fswatchStop stops a stream started by fswatchStart and frees it.
//
// The stream is stopped on its queue, which runs one thing at a time,
// so that no callback is running or yet to run when this returns.
static void fswatchStop(FSEventStreamRef stream, dispatch_queue_t queue) {
	dispatch_sync_f(queue, stream, fswatchInvalidate);
	FSEventStreamRelease(stream);
	dispatch_release(queue);
}
*/
import "C"

import (
	"context"
	"fmt"
	"os"
	"runtime/cgo"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/rclone/rclone/fs"
)

// latency is how long, in seconds, FSEvents waits after reporting
// changes before it reports any more.
//
// The first change after a quiet spell is reported straight away.
// Those made during the wait are reported when it ends, with repeated
// changes to a path merged into one event. It is small compared to
// the time a caller waits for changes to settle.
const latency = 0.05

// Flags which FSEvents sets on an event
const (
	flagMustScanSubDirs    = C.kFSEventStreamEventFlagMustScanSubDirs
	flagUserDropped        = C.kFSEventStreamEventFlagUserDropped
	flagKernelDropped      = C.kFSEventStreamEventFlagKernelDropped
	flagHistoryDone        = C.kFSEventStreamEventFlagHistoryDone
	flagRootChanged        = C.kFSEventStreamEventFlagRootChanged
	flagMount              = C.kFSEventStreamEventFlagMount
	flagUnmount            = C.kFSEventStreamEventFlagUnmount
	flagItemCreated        = C.kFSEventStreamEventFlagItemCreated
	flagItemRemoved        = C.kFSEventStreamEventFlagItemRemoved
	flagItemRenamed        = C.kFSEventStreamEventFlagItemRenamed
	flagItemIsFile         = C.kFSEventStreamEventFlagItemIsFile
	flagItemIsDir          = C.kFSEventStreamEventFlagItemIsDir
	flagItemIsSymlink      = C.kFSEventStreamEventFlagItemIsSymlink
	flagItemIsHardlink     = C.kFSEventStreamEventFlagItemIsHardlink
	flagItemIsLastHardlink = C.kFSEventStreamEventFlagItemIsLastHardlink
)

// watcher watches a directory tree with FSEvents.
//
// One stream covers the whole tree, so nothing is kept in memory for
// the files and directories in it.
type watcher struct {
	root    string // as passed to Watch
	fn      func([]Event)
	retry   time.Duration // how long to wait before retrying when the root has gone
	batches chan batch    // from the callback of the running stream to run
	outside sync.Once     // for logging a change which isn't in the root once
}

// batch is what FSEvents reported in one callback
type batch struct {
	events      []Event
	rootChanged bool // set if the root was deleted, renamed or replaced
}

// stream is a running FSEvents stream
type stream struct {
	w         *watcher
	realRoots []string      // what FSEvents may call the root, without a trailing /
	stopping  chan struct{} // closed to stop the callback waiting to report a batch
	handle    cgo.Handle    // of the stream, which the callback finds it by
	ref       C.FSEventStreamRef
	queue     C.dispatch_queue_t // runs the callback
}

// Supported reports whether this build can watch for changes
func Supported() bool {
	return true
}

// Watch reports changes under root to fn until ctx is cancelled.
//
// It returns once watching is established. fn is called from a
// single goroutine.
//
// If root is deleted, renamed or replaced, an event with Rescan set
// is reported for it and watching is retried until root is a
// directory again, when another one is reported.
func Watch(ctx context.Context, root string, opt Options, fn func([]Event)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w := &watcher{
		root:    root,
		fn:      fn,
		retry:   retryDelay,
		batches: make(chan batch),
	}
	s, err := w.start()
	if err != nil {
		return err
	}
	go w.run(ctx, s)
	return nil
}

// start starts a stream watching the root.
//
// FIXME if the root is a symlink, FSEvents watches what it points to
// now and doesn't say if it is pointed somewhere else.
func (w *watcher) start() (*stream, error) {
	realRoot, err := realPath(w.root)
	if err != nil {
		return nil, fmt.Errorf("failed to watch %q: %w", w.root, err)
	}
	s := &stream{
		w:         w,
		realRoots: []string{strings.TrimSuffix(realRoot, "/")},
		stopping:  make(chan struct{}),
	}
	// A path on the data volume can be known by one starting with
	// where that volume is mounted too
	if systemRoot, ok := systemPath(realRoot); ok && systemRoot != realRoot {
		s.realRoots = append(s.realRoots, strings.TrimSuffix(systemRoot, "/"))
	}
	s.handle = cgo.NewHandle(s)
	cRoot := C.CString(realRoot)
	defer C.free(unsafe.Pointer(cRoot))
	started := C.fswatchStart(cRoot, C.uintptr_t(s.handle), latency)
	s.ref, s.queue = started.stream, started.queue
	if s.ref == nil {
		s.handle.Delete()
		return nil, fmt.Errorf("failed to watch %q: couldn't start FSEvents", w.root)
	}
	// FSEvents will watch a path with nothing at it. This is checked
	// once it is watching so that the root going at any time from
	// now on is reported.
	info, err := os.Stat(realRoot)
	if err == nil && !info.IsDir() {
		err = syscall.ENOTDIR
	}
	if err != nil {
		s.stop()
		return nil, fmt.Errorf("failed to watch %q: %w", w.root, err)
	}
	return s, nil
}

// realPath returns the path FSEvents reports changes to path under.
//
// That has symlinks resolved, and names in the case and Unicode form
// they have on disk rather than as they are in path.
func realPath(path string) (string, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cReal, err := C.realpath(cPath, nil)
	if cReal == nil {
		return "", err
	}
	defer C.free(unsafe.Pointer(cReal))
	return C.GoString(cReal), nil
}

// systemPath returns the path the system knows what is at path by
func systemPath(path string) (string, bool) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	buf := (*C.char)(C.malloc(C.MAXPATHLEN))
	defer C.free(unsafe.Pointer(buf))
	if C.fswatchPath(cPath, buf) != 0 {
		return "", false
	}
	return C.GoString(buf), true
}

// stop stops the stream and frees it.
//
// The callback is not running, and will not run again, once it returns.
func (s *stream) stop() {
	close(s.stopping)
	C.fswatchStop(s.ref, s.queue)
	s.handle.Delete()
}

// run reports what the stream s finds until ctx is cancelled
func (w *watcher) run(ctx context.Context, s *stream) {
	for {
		var b batch
		select {
		case <-ctx.Done():
		case b = <-w.batches:
		}
		// Checked here as select picks at random if there is a batch too
		if ctx.Err() != nil {
			s.stop()
			return
		}
		if len(b.events) > 0 {
			w.fn(b.events)
		}
		if !b.rootChanged {
			continue
		}
		// The stream carries on watching the path the root was at,
		// which may now be a different directory or nothing.
		s.stop()
		if s = w.restart(ctx); s == nil {
			return
		}
	}
}

// restart starts watching the root again, waiting for it to be a
// directory if need be.
//
// It returns nil if ctx was cancelled first.
func (w *watcher) restart(ctx context.Context) *stream {
	fs.Errorf(w.root, "Directory being watched was deleted, renamed or replaced: retrying every %v", w.retry)
	for {
		s, err := w.start()
		if err == nil {
			fs.Logf(w.root, "Watching for changes again")
			// Anything may have changed while nothing was watching
			w.fn([]Event{{Path: w.root, IsDir: true, Known: true, Rescan: true}})
			return s
		}
		timer := time.NewTimer(w.retry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

// fswatchCallback is called by FSEvents, on the queue of the stream
// which handle is for, with n changed paths and what happened to
// each of them.
//
//export fswatchCallback
func fswatchCallback(handle C.uintptr_t, n C.size_t, paths **C.char, flags *C.FSEventStreamEventFlags) {
	s := cgo.Handle(handle).Value().(*stream)
	var b batch
	cFlags := unsafe.Slice(flags, n)
	for i, cPath := range unsafe.Slice(paths, n) {
		f := uint32(cFlags[i])
		if f&flagRootChanged != 0 {
			b.rootChanged = true
		}
		path := C.GoString(cPath)
		if event, ok := convert(s.w.root, s.realRoots, path, f); ok {
			if event.Rescan && event.Path == s.w.root && f&(flagMustScanSubDirs|flagUserDropped|flagKernelDropped|flagRootChanged|flagMount|flagUnmount) == 0 {
				// convert didn't know what this was a change to
				s.w.outside.Do(func() {
					fs.Logf(s.w.root, "Change reported to %q which doesn't look to be in the directory being watched - checking everything instead", path)
				})
			}
			b.events = append(b.events, event)
		}
	}
	if len(b.events) == 0 && !b.rootChanged {
		return
	}
	// Waiting here when fn is slow stops FSEvents queueing without
	// limit: it merges or drops the events it can't deliver and says
	// that it has.
	select {
	case s.w.batches <- b:
	case <-s.stopping:
	}
}

// convert returns what to report for the FSEvents event for path
// with flags, or false if there is nothing to report.
//
// root is the root as passed to Watch and realRoots are what
// FSEvents may call it, without a trailing /.
//
// FSEvents merges the flags of everything that happened to a path
// recently, so they say what may have happened rather than what did.
func convert(root string, realRoots []string, path string, flags uint32) (event Event, ok bool) {
	// rel is "" for the root and starts with a / for anything in it
	var rel string
	inside := false
	for _, realRoot := range realRoots {
		rel, inside = strings.CutPrefix(strings.TrimSuffix(path, "/"), realRoot)
		if inside = inside && (rel == "" || rel[0] == '/'); inside {
			break
		}
	}
	rescanRoot := Event{Path: root, IsDir: true, Known: true, Rescan: true}
	switch {
	case flags&flagHistoryDone != 0:
		// Marks the end of events from before the stream started, and
		// its path means nothing
		return event, false
	case flags&(flagMustScanSubDirs|flagUserDropped|flagKernelDropped|flagRootChanged|flagMount|flagUnmount) != 0:
		// Changes were not reported individually below path, which is
		// above the root if events in different trees were merged, or
		// anywhere if events were dropped.
		if !inside || rel == "" || flags&(flagUserDropped|flagKernelDropped) != 0 {
			return rescanRoot, true
		}
		return Event{Path: strings.TrimSuffix(root, "/") + rel, IsDir: true, Known: true, Rescan: true}, true
	case !inside:
		// Only what is in the root is reported, so it is being called
		// something else and it isn't known what this is a change to
		return rescanRoot, true
	case rel == "":
		// A change to the root itself isn't a change to anything in it
		return event, false
	}
	event.Path = strings.TrimSuffix(root, "/") + rel
	switch {
	case flags&flagItemIsDir != 0:
		event.Known = true
		event.IsDir = true
		// What is in a directory which is renamed or cloned into
		// place is not reported
		event.Rescan = flags&(flagItemCreated|flagItemRenamed) != 0
	case flags&(flagItemIsFile|flagItemIsSymlink|flagItemIsHardlink|flagItemIsLastHardlink) != 0:
		event.Known = true
	}
	return event, true
}
