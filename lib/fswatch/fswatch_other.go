//go:build !linux

package fswatch

import "context"

// Supported reports whether this build can watch for changes
func Supported() bool {
	return false
}

// Watch reports changes under root to fn until ctx is cancelled.
//
// It returns once watching is established.
func Watch(ctx context.Context, root string, opt Options, fn func([]Event)) error {
	return ErrUnsupported
}
