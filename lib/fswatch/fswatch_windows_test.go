//go:build windows

package fswatch

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf16"
	"unsafe"

	"github.com/rclone/rclone/lib/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

const (
	fileAttributes = windows.FILE_ATTRIBUTE_ARCHIVE
	dirAttributes  = windows.FILE_ATTRIBUTE_DIRECTORY
	linkAttributes = windows.FILE_ATTRIBUTE_DIRECTORY | windows.FILE_ATTRIBUTE_REPARSE_POINT
)

// record returns a change to name as Windows writes it, in the
// extended layout if extended is set.
func record(extended bool, action uint32, name string, attributes, reparseTag uint32) []byte {
	encoded := utf16.Encode([]rune(name))
	nameLengthOffset, nameOffset := classicNameLengthOffset, classicNameOffset
	if extended {
		nameLengthOffset, nameOffset = extendedNameLengthOffset, extendedNameOffset
	}
	b := make([]byte, nameOffset+2*len(encoded))
	binary.LittleEndian.PutUint32(b[actionOffset:], action)
	if extended {
		binary.LittleEndian.PutUint32(b[extendedAttributesOffset:], attributes)
		binary.LittleEndian.PutUint32(b[extendedReparseTagOffset:], reparseTag)
	}
	binary.LittleEndian.PutUint32(b[nameLengthOffset:], uint32(2*len(encoded)))
	for i, c := range encoded {
		binary.LittleEndian.PutUint16(b[nameOffset+2*i:], c)
	}
	return b
}

// records joins records into a buffer as Windows fills it in
func records(recs ...[]byte) (buf []byte) {
	for i, rec := range recs {
		for len(rec)%4 != 0 {
			rec = append(rec, 0)
		}
		if i != len(recs)-1 {
			binary.LittleEndian.PutUint32(rec, uint32(len(rec)))
		}
		buf = append(buf, rec...)
	}
	return buf
}

// fakeLongPath expands the short names it knows about, and fails for
// any others, and for names starting with gone, as if what they named
// had gone.
func fakeLongPath(path string) (string, error) {
	known := map[string]string{
		"PROGRA~1":     "Program Files",
		"LONGFI~1.TXT": "long file name.txt",
		"LONGDI~1":     "long directory",
	}
	elements := strings.Split(path, `\`)
	for i, element := range elements {
		if long, ok := known[element]; ok {
			elements[i] = long
		} else if mayBeShortName(element) || strings.HasPrefix(element, "gone") {
			return "", os.ErrNotExist
		}
	}
	return strings.Join(elements, `\`), nil
}

func TestMayBeShortName(t *testing.T) {
	for name, want := range map[string]bool{
		"LONGFI~1.TXT":  true,
		"LONGDI~1":      true,
		"LO3F4A~1.TXT":  true,
		"LONGF~12.TXT":  true,
		"A~1":           true,
		"file.txt":      false,
		"longfi~1.txt":  false, // short names are upper case
		"~$REPORT.DOC":  false,
		"~WRL0001.TMP":  false,
		"BACKUP~":       false,
		"LONGFI~1.TEXT": false,
		"LONGFILE~1":    false,
		"LONG~1FI.TXT":  false,
		"":              false,
	} {
		assert.Equal(t, want, mayBeShortName(name), name)
	}
}

func TestLongName(t *testing.T) {
	// The root has a short name in it which must be left alone
	w := &watcher{root: `C:\PROGRA~1\root`, longPath: fakeLongPath}
	for _, test := range []struct {
		name  string
		want  string
		exact bool
	}{
		{`file.txt`, `file.txt`, true},
		{`dir\~$report.docx`, `dir\~$report.docx`, true},
		{`LONGFI~1.TXT`, `long file name.txt`, true},
		{`dir\LONGFI~1.TXT`, `dir\long file name.txt`, true},
		{`LONGDI~1\LONGFI~1.TXT`, `long directory\long file name.txt`, true},
		{`LONGDI~1\file.txt`, `long directory\file.txt`, true},
		{`LONGDI~1\gone.txt`, `long directory\gone.txt`, true},
		{`LONGDI~1\gone\LONGFI~1.TXT`, `long directory\gone`, false},
		{`GONE~1.TXT`, ``, false},
		{`dir\GONE~1.TXT`, `dir`, false},
		{`LONGDI~1\GONE~1.TXT`, `long directory`, false},
		{`GONEDI~1\sub\file.txt`, ``, false},
		{`dir\LONGDI~1\GONEDI~1\file.txt`, `dir\long directory`, false},
	} {
		got, exact := w.longName(test.name)
		assert.Equal(t, test.want, got, test.name)
		assert.Equal(t, test.exact, exact, test.name)
	}
}

func TestParse(t *testing.T) {
	// Real so that it can be looked at when the records don't say
	// what kind of entry changed
	root := tempRoot(t)
	require.NoError(t, os.Mkdir(filepath.Join(root, "dir"), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(root, "file"), nil, 0o666))

	const (
		added      = windows.FILE_ACTION_ADDED
		removed    = windows.FILE_ACTION_REMOVED
		modified   = windows.FILE_ACTION_MODIFIED
		renamedOld = windows.FILE_ACTION_RENAMED_OLD_NAME
		renamedNew = windows.FILE_ACTION_RENAMED_NEW_NAME
	)
	classic := func(action uint32, name string) []byte {
		return record(false, action, name, 0, 0)
	}
	extended := func(action uint32, name string, attributes uint32) []byte {
		return record(true, action, name, attributes, 0)
	}
	unknown := func(name string) Event { return Event{Path: root + `\` + name} }
	isFile := func(name string) Event { return Event{Path: root + `\` + name, Known: true} }
	isDir := func(name string) Event { return Event{Path: root + `\` + name, Known: true, IsDir: true} }
	rescan := func(name string) Event {
		return Event{Path: root + `\` + name, Known: true, IsDir: true, Rescan: true}
	}
	rescanRoot := Event{Path: root, Known: true, IsDir: true, Rescan: true}

	for _, test := range []struct {
		name    string
		classic bool
		buf     []byte
		want    []Event
	}{
		{
			name: "changes were dropped",
			want: []Event{rescanRoot},
		}, {
			name:    "changes were dropped classic",
			classic: true,
			want:    []Event{rescanRoot},
		}, {
			name: "file",
			buf: records(
				extended(added, `new.txt`, fileAttributes),
				extended(modified, `sub\deep\changed.txt`, fileAttributes),
				extended(renamedOld, `old.txt`, fileAttributes),
				extended(renamedNew, `renamed.txt`, fileAttributes),
				extended(removed, `gone.txt`, fileAttributes),
			),
			want: []Event{isFile(`new.txt`), isFile(`sub\deep\changed.txt`), isFile(`old.txt`), isFile(`renamed.txt`), isFile(`gone.txt`)},
		}, {
			name: "unusual names",
			buf: records(
				extended(added, `café 日本語.txt`, fileAttributes),
				extended(added, `😀`, fileAttributes),
			),
			want: []Event{isFile(`café 日本語.txt`), isFile(`😀`)},
		}, {
			name: "repeats are reported once",
			buf: records(
				extended(added, `a.txt`, fileAttributes),
				extended(modified, `a.txt`, fileAttributes),
				extended(modified, `a.txt`, fileAttributes),
				extended(modified, `b.txt`, fileAttributes),
				extended(modified, `a.txt`, fileAttributes),
			),
			want: []Event{isFile(`a.txt`), isFile(`b.txt`), isFile(`a.txt`)},
		}, {
			name: "new directory is rescanned",
			buf: records(
				extended(added, `made`, dirAttributes),
				extended(renamedNew, `sub\moved`, dirAttributes),
			),
			want: []Event{rescan(`made`), rescan(`sub\moved`)},
		}, {
			name: "directory gone",
			buf: records(
				extended(removed, `deleted`, dirAttributes),
				extended(renamedOld, `old`, dirAttributes),
			),
			want: []Event{isDir(`deleted`), isDir(`old`)},
		}, {
			name: "modified directory is dropped",
			buf: records(
				extended(modified, `sub`, dirAttributes),
				extended(added, `sub\new.txt`, fileAttributes),
				extended(modified, `sub`, dirAttributes),
			),
			want: []Event{isFile(`sub\new.txt`)},
		}, {
			name: "only modified directories",
			buf:  records(extended(modified, `sub`, dirAttributes)),
		}, {
			name: "link to a directory is not a directory",
			buf: records(
				record(true, modified, `junction`, linkAttributes, windows.IO_REPARSE_TAG_MOUNT_POINT),
				record(true, removed, `symlink`, linkAttributes, windows.IO_REPARSE_TAG_SYMLINK),
			),
			want: []Event{isFile(`junction`), isFile(`symlink`)},
		}, {
			name: "directory with another kind of reparse point",
			// IO_REPARSE_TAG_CLOUD, as used for files kept in the cloud
			buf:  records(record(true, added, `cloud`, linkAttributes, 0x9000001A)),
			want: []Event{rescan(`cloud`)},
		}, {
			name: "short names",
			buf: records(
				extended(modified, `LONGFI~1.TXT`, fileAttributes),
				extended(removed, `sub\GONE~1.TXT`, fileAttributes),
				extended(removed, `GONE~1.TXT`, fileAttributes),
				extended(added, `LONGDI~1\new.txt`, fileAttributes),
			),
			want: []Event{isFile(`long file name.txt`), rescan(`sub`), rescanRoot, isFile(`long directory\new.txt`)},
		}, {
			name: "truncated record",
			buf:  records(extended(added, `a.txt`, fileAttributes), extended(added, `b.txt`, fileAttributes))[:100+extendedNameOffset],
			// It isn't known what else changed
			want: []Event{isFile(`a.txt`), rescanRoot},
		}, {
			name: "next record outside the buffer",
			buf: func() []byte {
				buf := records(extended(added, `a.txt`, fileAttributes))
				binary.LittleEndian.PutUint32(buf, 4096)
				return buf
			}(),
			want: []Event{isFile(`a.txt`), rescanRoot},
		}, {
			name: "name outside the buffer",
			buf: func() []byte {
				buf := records(extended(added, `a.txt`, fileAttributes), extended(added, `b.txt`, fileAttributes))
				binary.LittleEndian.PutUint32(buf[binary.LittleEndian.Uint32(buf)+extendedNameLengthOffset:], 0xFFFFFFFF)
				return buf
			}(),
			want: []Event{isFile(`a.txt`), rescanRoot},
		}, {
			name:    "classic",
			classic: true,
			buf: records(
				classic(added, `file`),
				classic(modified, `file`),
				classic(added, `dir`),
				classic(modified, `dir`),
				classic(renamedNew, `dir`),
				classic(renamedNew, `file`),
				classic(added, `already gone`),
				classic(removed, `gone`),
				classic(renamedOld, `sub\old`),
			),
			want: []Event{
				isFile(`file`), rescan(`dir`), isFile(`file`),
				unknown(`already gone`), unknown(`gone`), unknown(`sub\old`),
			},
		}, {
			name:    "classic short names",
			classic: true,
			buf: records(
				classic(modified, `LONGFI~1.TXT`),
				classic(removed, `dir\GONE~1.TXT`),
			),
			want: []Event{unknown(`long file name.txt`), rescan(`dir`)},
		}, {
			name:    "classic truncated record",
			classic: true,
			buf:     records(classic(added, `file`), classic(added, `dir`))[:22],
			want:    []Event{isFile(`file`), rescanRoot},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := &watcher{root: root, classic: test.classic, longPath: fakeLongPath, lstat: os.Lstat}
			assert.Equal(t, test.want, w.parse(test.buf))
		})
	}
}

func TestWatchRootIsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(path, nil, 0o666))
	err := Watch(context.Background(), path, Options{}, func([]Event) {})
	assert.ErrorIs(t, err, syscall.ENOTDIR)
}

func TestWatchExtendedLengthRoot(t *testing.T) {
	// As the local backend makes its root
	root := file.UNCPath(tempRoot(t))
	require.True(t, strings.HasPrefix(root, `\\?\`), root)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := new(collector)
	require.NoError(t, Watch(ctx, root, Options{}, c.add))

	path := root + `\file.txt`
	require.NoError(t, os.WriteFile(path, nil, 0o666))
	c.requireEvent(t, "create", fileAt(path))
	dir := root + `\dir`
	require.NoError(t, os.Mkdir(dir, 0o777))
	c.requireEvent(t, "new directory", rescanAt(dir))
	path = dir + `\file.txt`
	require.NoError(t, os.WriteFile(path, nil, 0o666))
	c.requireEvent(t, "create in new directory", fileAt(path))
}

// shortPath returns path with its last element replaced by its short
// name, skipping the test if it doesn't have one.
func shortPath(t *testing.T, path string) string {
	long, err := windows.UTF16FromString(path)
	require.NoError(t, err)
	short := make([]uint16, len(long))
	n, err := windows.GetShortPathName(&long[0], &short[0], uint32(len(short)))
	require.NoError(t, err)
	require.LessOrEqual(t, int(n), len(short))
	name := filepath.Base(windows.UTF16ToString(short[:n]))
	if name == filepath.Base(path) {
		t.Skip("short names are turned off on this volume")
	}
	return filepath.Join(filepath.Dir(path), name)
}

func TestWatchShortNames(t *testing.T) {
	var long, dir string
	root, c := startWatch(t, func(root string) {
		long = filepath.Join(root, "a long file name.text")
		require.NoError(t, os.WriteFile(long, nil, 0o666))
		dir = filepath.Join(root, "a long directory name")
		require.NoError(t, os.Mkdir(dir, 0o777))
	})
	shortFile, shortDir := shortPath(t, long), shortPath(t, dir)
	short := func(ev Event) bool { return strings.Contains(ev.Path, "~") }

	// Changes made through a short name are reported under the long one
	require.NoError(t, os.WriteFile(shortFile, []byte("one"), 0o666))
	c.requireEvent(t, "write through short name", fileAt(long))
	inner := filepath.Join(dir, "inner.txt")
	require.NoError(t, os.WriteFile(filepath.Join(shortDir, "inner.txt"), nil, 0o666))
	c.requireEvent(t, "create through short directory name", fileAt(inner))
	assert.False(t, c.has(short), "short name reported")

	// The long name of what has gone can't be found, so the directory
	// it was in is looked at instead.
	c.reset()
	require.NoError(t, os.Remove(filepath.Join(shortDir, "inner.txt")))
	c.requireEvent(t, "delete through short directory name", fileAt(inner))
	require.NoError(t, os.Remove(shortFile))
	c.requireEvent(t, "delete through short name", rescanAt(root))
	requireReportedSoon(t, c, root)
	assert.False(t, c.has(short), "short name reported")
}

func TestWatchClassic(t *testing.T) {
	root := tempRoot(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := new(collector)
	w, err := newWatcher(root, c.add)
	require.NoError(t, err)
	w.classic = true
	require.NoError(t, w.start(ctx))

	path := filepath.Join(root, "file.txt")
	require.NoError(t, os.WriteFile(path, nil, 0o666))
	c.requireEvent(t, "create", func(ev Event) bool { return ev == Event{Path: path, Known: true} })
	dir := filepath.Join(root, "dir")
	require.NoError(t, os.Mkdir(dir, 0o777))
	c.requireEvent(t, "new directory", func(ev Event) bool {
		return ev == Event{Path: dir, Known: true, IsDir: true, Rescan: true}
	})
	inner := filepath.Join(dir, "inner.txt")
	require.NoError(t, os.WriteFile(inner, nil, 0o666))
	c.requireEvent(t, "create in new directory", fileAt(inner))

	// What kind of entry has gone isn't known
	c.reset()
	require.NoError(t, os.Remove(path))
	c.requireEvent(t, "delete", func(ev Event) bool { return ev == Event{Path: path} })
}

func TestWatchJunctionIsNotFollowed(t *testing.T) {
	outside := tempRoot(t)
	root, c := startWatch(t, nil)
	link := filepath.Join(root, "link")
	if out, err := exec.Command("cmd", "/c", "mklink", "/J", link, outside).CombinedOutput(); err != nil {
		t.Skipf("can't make a junction: %v: %s", err, out)
	}
	t.Cleanup(func() { _ = os.Remove(link) })

	require.NoError(t, os.WriteFile(filepath.Join(outside, "file.txt"), nil, 0o666))
	require.NoError(t, os.WriteFile(filepath.Join(link, "through.txt"), nil, 0o666))
	requireReportedSoon(t, c, root)
	assert.False(t, c.has(func(ev Event) bool { return filepath.Dir(ev.Path) == link }))
}

func TestWatchRecoversWhenRootIsDeleted(t *testing.T) {
	shortenRetryDelay(t)
	root := filepath.Join(tempRoot(t), "root")
	require.NoError(t, os.Mkdir(root, 0o777))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := new(collector)
	require.NoError(t, Watch(ctx, root, Options{}, c.add))
	requireReportedSoon(t, c, root)

	c.reset()
	require.NoError(t, os.RemoveAll(root))
	c.requireEvent(t, "root gone", rescanAt(root))
	c.reset()
	require.NoError(t, os.Mkdir(root, 0o777))
	c.requireEvent(t, "root back", rescanAt(root))
	requireReportedSoon(t, c, root)
}

// overflowFiles is enough new files to overflow the queue of events
const overflowFiles = maxQueued + 4096

func TestWatchSlowCallback(t *testing.T) {
	root := tempRoot(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := new(collector)
	release := make(chan struct{})
	w, err := newWatcher(root, func(events []Event) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		c.add(events)
	})
	require.NoError(t, err)
	require.NoError(t, w.start(ctx))

	for i := range overflowFiles {
		require.NoError(t, os.WriteFile(filepath.Join(root, fmt.Sprintf("file%d", i)), nil, 0o666))
	}
	// Reading carries on while the callback is stuck, until there are
	// too many events to keep.
	require.Eventually(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return len(w.queue) > 0 && w.queue[0] == w.rescanRoot()
	}, 30*time.Second, 10*time.Millisecond, "queue didn't overflow")
	close(release)
	c.requireEvent(t, "queue overflow", rescanAt(root))
	requireReportedSoon(t, c, root)
}

func TestWatchBufferOverflow(t *testing.T) {
	root := tempRoot(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := new(collector)
	w, err := newWatcher(root, c.add)
	require.NoError(t, err)
	// Hold up reading so that Windows runs out of room for changes
	release := make(chan struct{})
	w.longPath = func(path string) (string, error) {
		<-release
		return longPath(path)
	}
	require.NoError(t, w.start(ctx))

	require.NoError(t, os.WriteFile(filepath.Join(root, "A~1"), nil, 0o666))
	for i := range 2000 {
		require.NoError(t, os.WriteFile(filepath.Join(root, fmt.Sprintf("a file with quite a long name %d", i)), nil, 0o666))
	}
	close(release)
	c.requireEvent(t, "buffer overflow", rescanAt(root))
	requireReportedSoon(t, c, root)
}

// handleCount returns the number of handles this process has open
func handleCount(t *testing.T) uint32 {
	var count uint32
	r, _, err := windows.NewLazySystemDLL("kernel32.dll").NewProc("GetProcessHandleCount").Call(uintptr(windows.CurrentProcess()), uintptr(unsafe.Pointer(&count)))
	require.NotZero(t, r, err)
	return count
}

func TestWatchStartStopDoesNotLeak(t *testing.T) {
	const cycles = 200
	root := tempRoot(t)
	watch := func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := new(collector)
		require.NoError(t, Watch(ctx, root, Options{}, c.add))
		requireReportedSoon(t, c, root)
	}
	// Let anything made on first use be made before counting
	watch()
	time.Sleep(100 * time.Millisecond)
	goroutines, handles := runtime.NumGoroutine(), handleCount(t)
	for range cycles {
		watch()
	}
	// Stopping is asynchronous. This doesn't use assert.Eventually as
	// that runs the check in a goroutine of its own.
	deadline := time.Now().Add(10 * time.Second)
	for runtime.NumGoroutine() > goroutines && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(t, goroutines, runtime.NumGoroutine(), "goroutines left running")
	// Each watch uses three handles, so a leak would be far more than this
	assert.Less(t, handleCount(t), handles+cycles/4, "handles left open")
}
