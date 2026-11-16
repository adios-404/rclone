// Package changenotify tests rclone's changenotify support
package changenotify

import (
	"context"
	"errors"
	"time"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/test"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/flags"
	"github.com/spf13/cobra"
)

var (
	pollInterval = fs.Duration(10 * time.Second)
)

func init() {
	test.Command.AddCommand(commandDefinition)
	cmdFlags := commandDefinition.Flags()
	flags.FVarP(cmdFlags, &pollInterval, "poll-interval", "", "Time to wait between polling for changes", "")
}

var commandDefinition = &cobra.Command{
	Use:   "changenotify remote:",
	Short: `Log any change notify requests for the remote passed in.`,
	Annotations: map[string]string{
		"versionIntroduced": "v1.56",
	},
	RunE: func(command *cobra.Command, args []string) error {
		cmd.CheckArgs(1, 1, command, args)
		f := cmd.NewFsSrc(args)
		ctx := context.Background()

		features := f.Features()
		watching := features.WatchChanges != nil
		if watching {
			if err := features.WatchChanges(ctx, watchChanges); err != nil {
				return err
			}
			fs.Logf(nil, "Waiting for changes to be reported")
		}

		// Start polling function
		if do := features.ChangeNotify; do != nil {
			pollChan := make(chan time.Duration)
			do(ctx, changeNotify, pollChan)
			pollChan <- time.Duration(pollInterval)
			fs.Logf(nil, "Waiting for changes, polling every %v", pollInterval)
		} else if !watching {
			return errors.New("poll-interval is not supported by this remote")
		}
		select {}
	},
}

// changeNotify invalidates the directory cache for the relativePath
// passed in.
//
// if entryType is a directory it invalidates the parent of the directory too.
func changeNotify(relativePath string, entryType fs.EntryType) {
	fs.Logf(nil, "%q: %v", relativePath, entryType)
}

// watchChanges logs the events reported by the WatchChanges feature
func watchChanges(events []fs.ChangeEvent) {
	for _, event := range events {
		recursive := ""
		if event.Recursive {
			recursive = " and everything below it"
		}
		fs.Logf(nil, "%q: %v%s", event.Path, event.EntryType, recursive)
	}
}
