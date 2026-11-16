//go:build !linux && !darwin && !windows

package local

// networkFileSystem returns the kind of file system path is on if it
// is one which doesn't report changes made to it by other computers,
// or "" if it isn't or that can't be found out.
func networkFileSystem(path string) string {
	return ""
}
