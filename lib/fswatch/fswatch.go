// Package fswatch watches a directory tree on the local disk for changes
package fswatch

import (
	"errors"
	"time"
)

// ErrUnsupported is returned by Watch if this build can't watch for changes
var ErrUnsupported = errors.New("watching for changes is not supported on this platform")

// retryDelay is how long to wait before trying to watch again after
// failing to.
//
// Watch reads it once, so that tests can change it.
var retryDelay = time.Minute

// Event describes one change under a watched root.
//
// Events are hints. The path may have changed again, or have gone, by
// the time the event is delivered.
type Event struct {
	Path   string // absolute path of what changed, starting with the root passed to Watch
	IsDir  bool   // whether Path is a directory, valid if Known is set
	Known  bool   // whether the OS said what kind of entry Path is
	Rescan bool   // changes may have been missed at or below Path
}

// Options configures Watch
type Options struct {
	// OneFileSystem stops directories on other file systems than the
	// root from being watched where each directory is watched
	// individually. It has no effect elsewhere.
	OneFileSystem bool

	// Skip, if set, reports whether changes in the directory at path
	// and everything below it aren't wanted, so it needn't be
	// watched. It is only used where each directory is watched
	// individually: elsewhere changes in it are still reported.
	Skip func(path string) bool
}
