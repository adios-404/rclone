//go:build linux

package local

import "golang.org/x/sys/unix"

// networkFileSystem returns the kind of file system path is on if it
// is one which doesn't report changes made to it by other computers,
// or "" if it isn't or that can't be found out.
func networkFileSystem(path string) string {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return ""
	}
	switch uint32(st.Type) { //nolint:unconvert // the type differs between architectures
	case unix.NFS_SUPER_MAGIC:
		return "NFS"
	case unix.SMB_SUPER_MAGIC, unix.CIFS_SUPER_MAGIC, unix.SMB2_SUPER_MAGIC:
		return "SMB"
	case unix.FUSE_SUPER_MAGIC:
		return "FUSE"
	case unix.V9FS_MAGIC:
		return "9P"
	case unix.CEPH_SUPER_MAGIC:
		return "Ceph"
	case unix.AFS_SUPER_MAGIC:
		return "AFS"
	}
	return ""
}
