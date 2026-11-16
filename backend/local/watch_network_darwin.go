//go:build darwin

package local

import "golang.org/x/sys/unix"

// networkFileSystem returns the kind of file system path is on if it
// is one which doesn't report changes made to it by other computers,
// or "" if it isn't or that can't be found out.
func networkFileSystem(path string) string {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil || st.Flags&unix.MNT_LOCAL != 0 {
		return ""
	}
	return unix.ByteSliceToString(st.Fstypename[:])
}
