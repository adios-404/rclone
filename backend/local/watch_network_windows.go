//go:build windows

package local

import (
	"strings"

	"golang.org/x/sys/windows"
)

// networkFileSystem returns the kind of file system path is on if it
// is one which doesn't report changes made to it by other computers,
// or "" if it isn't or that can't be found out.
func networkFileSystem(path string) string {
	path, isLong := strings.CutPrefix(path, `\\?\`)
	if isLong && strings.HasPrefix(path, `UNC\`) || !isLong && strings.HasPrefix(path, `\\`) {
		return "network"
	}
	if len(path) < 3 || path[1] != ':' {
		return ""
	}
	drive, err := windows.UTF16PtrFromString(path[:2] + `\`)
	if err != nil || windows.GetDriveType(drive) != windows.DRIVE_REMOTE {
		return ""
	}
	return "network"
}
