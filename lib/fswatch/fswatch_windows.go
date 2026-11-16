//go:build windows

package fswatch

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/rclone/rclone/fs"
	"golang.org/x/sys/windows"
)

const (
	// notifyFilter is the kinds of change asked for
	notifyFilter = windows.FILE_NOTIFY_CHANGE_FILE_NAME | windows.FILE_NOTIFY_CHANGE_DIR_NAME |
		windows.FILE_NOTIFY_CHANGE_SIZE | windows.FILE_NOTIFY_CHANGE_LAST_WRITE | windows.FILE_NOTIFY_CHANGE_ATTRIBUTES

	// bufferSize is the size of the buffer changes are read into.
	//
	// Reads with a bigger buffer fail on network shares.
	bufferSize = 64 * 1024

	// maxQueued is how many events are kept for fn before they are
	// replaced with a rescan of the root
	maxQueued = 16384

	// readDirectoryNotifyExtendedInformation asks ReadDirectoryChangesExW
	// for FILE_NOTIFY_EXTENDED_INFORMATION records
	readDirectoryNotifyExtendedInformation = 2

	// reparseTagNameSurrogate is set in the reparse tag of an entry which
	// stands for another, such as a symlink or a junction
	reparseTagNameSurrogate = 0x20000000
)

// Offsets of the fields used in the records read.
//
// The classic record is FILE_NOTIFY_INFORMATION and the extended one
// FILE_NOTIFY_EXTENDED_INFORMATION. Both start with the offset of the
// next record followed by the action.
const (
	actionOffset = 4

	classicNameLengthOffset = 8
	classicNameOffset       = 12

	extendedAttributesOffset = 56
	extendedReparseTagOffset = 60
	extendedNameLengthOffset = 80
	extendedNameOffset       = 84
)

var (
	// ReadDirectoryChangesExW isn't in golang.org/x/sys/windows
	procReadDirectoryChangesExW = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReadDirectoryChangesExW")

	errRootReplaced = errors.New("the directory has been moved or deleted")
)

// fileID identifies a file or directory on this machine.
//
// A directory made in place of a deleted one can be given the same
// index, on network shares at least, so its creation time is needed too.
type fileID struct {
	volume, indexHigh, indexLow uint32
	created                     windows.Filetime
}

// watcher watches a directory tree with ReadDirectoryChangesExW.
//
// One handle on the root reports changes anywhere below it, so nothing
// is kept for each directory.
type watcher struct {
	root     string
	fn       func([]Event)
	retry    time.Duration                     // how long to wait before retrying after a failure
	longPath func(string) (string, error)      // longPath unless testing
	lstat    func(string) (os.FileInfo, error) // os.Lstat unless testing
	done     windows.Handle                    // event set when a read completes

	mu      sync.Mutex     // protects the fields below
	stop    windows.Handle // event set to stop watching
	stopped bool           // set once stop has been closed
	queue   []Event        // read but not yet passed to fn
	wake    chan struct{}  // signalled when queue has been added to

	// The fields below are only used by the goroutine running run,
	// or before it has started.
	dir        windows.Handle // the root, windows.InvalidHandle if it isn't open
	id         fileID         // of the root when it was opened
	classic    bool           // set if using ReadDirectoryChangesW
	reading    bool           // set if a read is in progress
	overlapped windows.Overlapped
	buf        []byte
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
// If watching fails later on, a Rescan event is reported for root,
// and another when it is working again. The directory at root being
// replaced by another is noticed within a minute rather than at once.
func Watch(ctx context.Context, root string, opt Options, fn func([]Event)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w, err := newWatcher(root, fn)
	if err != nil {
		return err
	}
	return w.start(ctx)
}

// newWatcher makes a watcher for root which start sets going
func newWatcher(root string, fn func([]Event)) (w *watcher, err error) {
	w = &watcher{
		root:     root,
		fn:       fn,
		retry:    retryDelay,
		longPath: longPath,
		lstat:    os.Lstat,
		wake:     make(chan struct{}, 1),
		dir:      windows.InvalidHandle,
		buf:      make([]byte, bufferSize),
	}
	if w.stop, err = windows.CreateEvent(nil, 1, 0, nil); err != nil {
		return nil, err
	}
	if w.done, err = windows.CreateEvent(nil, 1, 0, nil); err != nil {
		_ = windows.CloseHandle(w.stop)
		return nil, err
	}
	return w, nil
}

// start watches until ctx is cancelled, returning once watching is
// established.
func (w *watcher) start(ctx context.Context) error {
	started := make(chan error)
	go w.run(ctx, started)
	if err := <-started; err != nil {
		return err
	}
	context.AfterFunc(ctx, func() {
		// run closes stop when it returns, which it can do without
		// being told to if watching fails as ctx is cancelled
		w.mu.Lock()
		defer w.mu.Unlock()
		if !w.stopped {
			_ = windows.SetEvent(w.stop)
		}
	})
	go w.report(ctx)
	return nil
}

// run reads changes and queues them for report until told to stop.
//
// It sends the result of starting to watch to started.
func (w *watcher) run(ctx context.Context, started chan<- error) {
	// Windows cancels a read when the thread which started it exits,
	// so start them all from a thread which lasts as long as this does.
	runtime.LockOSThread()
	defer func() {
		w.mu.Lock()
		w.stopped = true
		_ = windows.CloseHandle(w.stop)
		w.mu.Unlock()
		_ = windows.CloseHandle(w.done)
	}()
	err := w.open()
	started <- err
	if err != nil {
		return
	}
	for {
		wasClassic := w.classic
		err := w.watch()
		w.close()
		// ctx is checked as watching can fail before being told to
		// stop, if the root is deleted straight after ctx is cancelled.
		if err == nil || ctx.Err() != nil {
			return
		}
		w.send([]Event{w.rescanRoot()})
		if w.classic && !wasClassic && w.open() == nil {
			// It only failed to say that the kind of read which was
			// tried isn't supported here, so carry on with the other
			continue
		}
		fs.Errorf(w.root, "Failed to watch for changes, retrying every %v: %v", w.retry, err)
		for {
			if w.waitToRetry() {
				return
			}
			if w.open() == nil {
				break
			}
		}
		// Changes were missed while this wasn't watching
		w.send([]Event{w.rescanRoot()})
		fs.Logf(w.root, "Watching for changes again")
	}
}

// watch queues changes until told to stop, when it returns nil, or
// until watching fails.
func (w *watcher) watch() error {
	checked := time.Now()
	for {
		event, err := windows.WaitForMultipleObjects([]windows.Handle{w.stop, w.done}, false, w.timeout())
		switch {
		case err != nil:
			return err
		case event == windows.WAIT_OBJECT_0:
			return nil
		case event == windows.WAIT_OBJECT_0+1:
			var n uint32
			err := windows.GetOverlappedResult(w.dir, &w.overlapped, &n, false)
			if err == windows.ERROR_IO_INCOMPLETE {
				// The read is still going, so its buffer is still in use
				continue
			}
			w.reading = false
			switch err {
			case windows.ERROR_NOTIFY_ENUM_DIR, windows.ERROR_MORE_DATA:
				// More changed than was kept, which network shares
				// can report as ERROR_MORE_DATA.
				n, err = 0, nil
			}
			if err != nil {
				w.classic = w.classic || unsupported(err)
				return err
			}
			events := w.parse(w.buf[:n])
			if err := w.startRead(); err != nil {
				return err
			}
			w.send(events)
		}
		// Windows doesn't say when the root itself is renamed, and
		// carries on reporting changes to it under its new name.
		if time.Since(checked) >= w.retry {
			if !w.rootUnchanged() {
				return errRootReplaced
			}
			checked = time.Now()
		}
	}
}

// timeout is how long to wait before retrying, in milliseconds
func (w *watcher) timeout() uint32 {
	return uint32(max(w.retry.Milliseconds(), 1))
}

// waitToRetry waits until it is time to retry, returning true if told
// to stop first.
func (w *watcher) waitToRetry() bool {
	event, _ := windows.WaitForSingleObject(w.stop, w.timeout())
	return event != uint32(windows.WAIT_TIMEOUT)
}

// open opens the root and starts reading changes to it
func (w *watcher) open() (err error) {
	w.dir, w.id, err = openDir(w.root)
	if err != nil {
		return err
	}
	if err = w.startRead(); err != nil {
		w.close()
		return fmt.Errorf("failed to watch %q: %w", w.root, err)
	}
	return nil
}

// close stops reading changes and closes the root
func (w *watcher) close() {
	if w.reading {
		// The kernel may write to buf and overlapped until the read has
		// completed, which closing the handle doesn't wait for.
		_ = windows.CancelIoEx(w.dir, &w.overlapped)
		var n uint32
		_ = windows.GetOverlappedResult(w.dir, &w.overlapped, &n, true)
		w.reading = false
	}
	_ = windows.CloseHandle(w.dir)
	w.dir = windows.InvalidHandle
}

// openDir opens the directory at path for reading changes to it,
// returning what identifies it too.
func openDir(path string) (dir windows.Handle, id fileID, err error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return windows.InvalidHandle, id, &os.PathError{Op: "open", Path: path, Err: err}
	}
	dir, err = windows.CreateFile(name, windows.FILE_LIST_DIRECTORY,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OVERLAPPED, 0)
	if err != nil {
		return windows.InvalidHandle, id, &os.PathError{Op: "open", Path: path, Err: err}
	}
	var info windows.ByHandleFileInformation
	err = windows.GetFileInformationByHandle(dir, &info)
	if err == nil && info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 {
		// A file can be opened like this, but reads of it then fail
		// with an error which doesn't say why.
		err = syscall.ENOTDIR
	}
	if err != nil {
		_ = windows.CloseHandle(dir)
		return windows.InvalidHandle, id, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return dir, fileID{info.VolumeSerialNumber, info.FileIndexHigh, info.FileIndexLow, info.CreationTime}, nil
}

// rootUnchanged reports whether the root is still the directory that
// was opened.
func (w *watcher) rootUnchanged() bool {
	dir, id, err := openDir(w.root)
	if err != nil {
		return false
	}
	_ = windows.CloseHandle(dir)
	return id == w.id
}

// startRead asks for the next changes to be put in buf, setting done
// when they have been.
//
// It must only be called from the goroutine running run.
func (w *watcher) startRead() (err error) {
	w.overlapped = windows.Overlapped{HEvent: w.done}
	if !w.classic {
		err = readDirectoryChangesEx(w.dir, w.buf, &w.overlapped)
		w.classic = unsupported(err)
	}
	if w.classic {
		err = windows.ReadDirectoryChanges(w.dir, &w.buf[0], uint32(len(w.buf)), true, notifyFilter, nil, &w.overlapped, 0)
	}
	// Waiting for a read which didn't start would never finish
	w.reading = err == nil
	return err
}

// readDirectoryChangesEx is windows.ReadDirectoryChanges of a whole
// tree, except that the records read say what kind of entry changed.
func readDirectoryChangesEx(dir windows.Handle, buf []byte, overlapped *windows.Overlapped) error {
	if procReadDirectoryChangesExW.Find() != nil {
		// Added in Windows 10 version 1709
		return windows.ERROR_NOT_SUPPORTED
	}
	r, _, err := procReadDirectoryChangesExW.Call(uintptr(dir), uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf)),
		1, notifyFilter, 0, uintptr(unsafe.Pointer(overlapped)), 0, readDirectoryNotifyExtendedInformation)
	if r == 0 {
		return err
	}
	return nil
}

// unsupported reports whether err from readDirectoryChangesEx may mean
// that the file system can't supply its records.
//
// Network shares fail with ERROR_INVALID_FUNCTION.
func unsupported(err error) bool {
	switch err {
	case windows.ERROR_INVALID_FUNCTION, windows.ERROR_INVALID_PARAMETER, windows.ERROR_NOT_SUPPORTED:
		return true
	}
	return false
}

// send queues events for fn without waiting for it
func (w *watcher) send(events []Event) {
	if len(events) == 0 {
		return
	}
	w.mu.Lock()
	if len(w.queue)+len(events) > maxQueued {
		// fn isn't keeping up, so have it look at everything instead
		w.queue = []Event{w.rescanRoot()}
	} else {
		w.queue = append(w.queue, events...)
	}
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// report passes queued events to fn until ctx is cancelled.
//
// This is done apart from reading them so that a slow fn doesn't make
// Windows run out of room to keep changes in.
func (w *watcher) report(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
		}
		w.mu.Lock()
		events := w.queue
		w.queue = nil
		w.mu.Unlock()
		// ctx is checked again as select may choose wake when both are ready
		if len(events) > 0 && ctx.Err() == nil {
			w.fn(events)
		}
	}
}

// rescanRoot is reported when changes may have been missed anywhere
func (w *watcher) rescanRoot() Event {
	return Event{Path: w.root, IsDir: true, Known: true, Rescan: true}
}

// parse returns what to report for the records in buf.
//
// An empty buf is what is read when more changed than Windows kept.
func (w *watcher) parse(buf []byte) (events []Event) {
	if len(buf) == 0 {
		return []Event{w.rescanRoot()}
	}
	nameLengthOffset, nameOffset := extendedNameLengthOffset, extendedNameOffset
	if w.classic {
		nameLengthOffset, nameOffset = classicNameLengthOffset, classicNameOffset
	}
	for len(buf) >= nameOffset {
		next := binary.LittleEndian.Uint32(buf)
		action := binary.LittleEndian.Uint32(buf[actionOffset:])
		nameLength := binary.LittleEndian.Uint32(buf[nameLengthOffset:])
		if nameLength > uint32(len(buf)-nameOffset) {
			// The rest can't be read, so it isn't known what changed
			return append(events, w.rescanRoot())
		}
		isDir := false
		if !w.classic {
			attributes := binary.LittleEndian.Uint32(buf[extendedAttributesOffset:])
			reparseTag := binary.LittleEndian.Uint32(buf[extendedReparseTagOffset:])
			// As os.Lstat does, don't count links to directories as directories
			isLink := attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 && reparseTag&reparseTagNameSurrogate != 0
			isDir = attributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 && !isLink
		}
		event, ok := w.event(action, decodeName(buf[nameOffset:nameOffset+int(nameLength)]), isDir)
		// A change is often reported more than once
		if ok && (len(events) == 0 || events[len(events)-1] != event) {
			events = append(events, event)
		}
		if next == 0 {
			return events
		}
		if next > uint32(len(buf)) {
			break
		}
		buf = buf[next:]
	}
	// The rest can't be read, so it isn't known what changed
	return append(events, w.rescanRoot())
}

// decodeName returns the UTF-16 name in b as a string
func decodeName(b []byte) string {
	name := make([]uint16, len(b)/2)
	for i := range name {
		name[i] = binary.LittleEndian.Uint16(b[2*i:])
	}
	return windows.UTF16ToString(name)
}

// event returns what to report for action happening to name, a path
// relative to the root, or ok false if there is nothing to report.
//
// isDir is whether name is a directory, which isn't known if classic
// is set.
func (w *watcher) event(action uint32, name string, isDir bool) (event Event, ok bool) {
	name, exact := w.longName(name)
	if name != "" {
		event.Path = strings.TrimSuffix(w.root, `\`) + `\` + name
	} else {
		event.Path = w.root
	}
	if !exact {
		return Event{Path: event.Path, IsDir: true, Known: true, Rescan: true}, true
	}
	added := action == windows.FILE_ACTION_ADDED || action == windows.FILE_ACTION_RENAMED_NEW_NAME
	if w.classic {
		// What has gone can't be looked at to see what it was
		if action == windows.FILE_ACTION_REMOVED || action == windows.FILE_ACTION_RENAMED_OLD_NAME {
			return event, true
		}
		fi, err := w.lstat(event.Path)
		if err != nil {
			return event, true
		}
		isDir = fi.IsDir()
	}
	event.Known = true
	event.IsDir = isDir
	if isDir {
		if action == windows.FILE_ACTION_MODIFIED {
			// Reported whenever anything in the directory changes
			return event, false
		}
		// A directory which is moved in is reported without what is in it
		event.Rescan = added
	}
	return event, true
}

// longName returns name, a path relative to the root, with any short
// names in it replaced by the long ones.
//
// Changes made to a file opened by its short name are reported under
// it. If the long name can't be found, because the entry has gone,
// it returns the nearest directory above it which can be named with
// exact false. That is "" for the root.
func (w *watcher) longName(name string) (long string, exact bool) {
	if !strings.Contains(name, "~") {
		return name, true
	}
	elements := strings.Split(name, `\`)
	exact = true
	for n := len(elements); n > 0; n-- {
		if !mayBeShortName(elements[n-1]) {
			continue
		}
		// Only what a short name refers to has to exist to look it up
		full, err := w.longPath(strings.TrimSuffix(w.root, `\`) + `\` + strings.Join(elements[:n], `\`))
		if err == nil {
			// The names in the root must be left as they were given
			if long, ok := lastElements(full, n); ok {
				return strings.Join(append([]string{long}, elements[n:]...), `\`), exact
			}
		}
		elements = elements[:n-1]
		exact = false
	}
	return strings.Join(elements, `\`), exact
}

// mayBeShortName reports whether name looks like a short (8.3) name
// made for a long one, such as LONGFI~1.TXT.
//
// These are always upper case.
func mayBeShortName(name string) bool {
	base, extension, _ := strings.Cut(name, ".")
	tilde := strings.LastIndexByte(base, '~')
	if tilde < 0 || tilde == len(base)-1 || utf8.RuneCountInString(base) > 8 || utf8.RuneCountInString(extension) > 3 || name != strings.ToUpper(name) {
		return false
	}
	for _, c := range base[tilde+1:] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// lastElements returns the last n elements of path
func lastElements(path string, n int) (string, bool) {
	i := len(path)
	for ; n > 0; n-- {
		i = strings.LastIndexByte(path[:i], '\\')
		if i < 0 {
			return "", false
		}
	}
	return path[i+1:], true
}

// longPath returns path with any short names in it replaced by the
// long ones. It fails if path doesn't exist.
func longPath(path string) (string, error) {
	short, err := windows.UTF16FromString(path)
	if err != nil {
		return "", err
	}
	long := make([]uint16, len(short)+windows.MAX_PATH)
	for {
		n, err := windows.GetLongPathName(&short[0], &long[0], uint32(len(long)))
		if err != nil {
			return "", err
		}
		if int(n) <= len(long) {
			return windows.UTF16ToString(long[:n]), nil
		}
		long = make([]uint16, n)
	}
}
