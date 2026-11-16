// Package syncflags defines the flags used by rclone sync and copy.
// It is decoupled into a separate package so it can be replaced.
package syncflags

import (
	_ "embed"
	"strings"
	"time"

	"github.com/rclone/rclone/fs/config/flags"
	"github.com/rclone/rclone/fs/sync"
	"github.com/spf13/pflag"
)

//go:embed syncflags.md
var watchHelp string

// WatchHelp returns the help for the flags added by AddWatchFlags,
// cleaned up to simplify appending
func WatchHelp() string {
	return strings.TrimSpace(watchHelp)
}

// AddWatchFlags adds the flags for watching the source for changes
// to cmdFlags, which set watch and opt.
func AddWatchFlags(cmdFlags *pflag.FlagSet, watch *bool, opt *sync.WatchOptions) {
	opt.Settle = 2 * time.Second
	opt.MaxDelay = time.Minute
	opt.FullCheck = time.Hour
	flags.BoolVarP(cmdFlags, watch, "watch", "", *watch, "Keep running and sync changes to the source as they happen", "Sync")
	flags.DurationVarP(cmdFlags, &opt.Settle, "watch-settle", "", opt.Settle, "With --watch, sync a change once nothing more has changed there for this long", "Sync")
	flags.DurationVarP(cmdFlags, &opt.MaxDelay, "watch-max-delay", "", opt.MaxDelay, "With --watch, sync something which keeps changing after this long", "Sync")
	flags.DurationVarP(cmdFlags, &opt.FullCheck, "watch-full-check", "", opt.FullCheck, "With --watch, run a full sync this often (0 to disable)", "Sync")
}
