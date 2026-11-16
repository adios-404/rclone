package sync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWatchTiming checks when Watch syncs with the default settings.
//
// It runs on a fake clock, so the weeks it covers take seconds.
func TestWatchTiming(t *testing.T) {
	r := fstest.NewRun(t)
	if !r.Fremote.Features().IsLocal {
		t.Skip("the fake clock only works if nothing waits on the network")
	}
	ctx := context.Background()
	opt := WatchOptions{
		Settle:     2 * time.Second,
		MaxDelay:   time.Minute,
		FullCheck:  time.Hour,
		DeleteMode: fs.DeleteModeDefault,
	}
	// Anything started the first time there is a transfer mustn't be
	// started on the fake clock as it would never finish.
	r.WriteFile("warm-up.txt", "warm-up", t1)
	require.NoError(t, Sync(ctx, r.Fremote, r.Flocal, false))

	// write changes a file in the source without reporting it.
	//
	// Each write of a file is longer than the last, as very little
	// real time passes between them and a file with the same size and
	// modification time as before hasn't changed as far as a sync can tell.
	writes := 0
	write := func(name, content string) {
		writes++
		content += strings.Repeat(".", writes)
		require.NoError(t, os.WriteFile(filepath.Join(r.LocalName, name), []byte(content), 0o666))
	}
	// synced reports whether the file in the destination has content
	synced := func(name, content string) bool {
		got, err := os.ReadFile(filepath.Join(r.FremoteName, name))
		return err == nil && strings.TrimRight(string(got), ".") == content
	}

	synctest.Test(t, func(t *testing.T) {
		watcher := new(fakeWatcher)
		ctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- watch(ctx, r.Fremote, r.Flocal, opt, watcher.watch)
		}()
		defer func() {
			cancel()
			assert.NoError(t, <-done)
		}()
		// sleep lets time pass, then waits for any syncing it caused to finish
		sleep := func(d time.Duration) {
			time.Sleep(d)
			synctest.Wait()
		}
		synctest.Wait()
		start := time.Now()

		// A change is synced once it has been quiet for the settle time
		write("settle.txt", "one")
		watcher.changed("settle.txt")
		sleep(opt.Settle - time.Millisecond)
		assert.False(t, synced("settle.txt", "one"), "synced before the settle time")
		sleep(2 * time.Millisecond)
		assert.True(t, synced("settle.txt", "one"), "not synced after the settle time")

		// Another change restarts the wait
		write("settle.txt", "two")
		watcher.changed("settle.txt")
		sleep(1500 * time.Millisecond)
		write("settle.txt", "three")
		watcher.changed("settle.txt")
		sleep(opt.Settle - time.Millisecond)
		assert.True(t, synced("settle.txt", "one"), "synced while still changing")
		sleep(2 * time.Millisecond)
		assert.True(t, synced("settle.txt", "three"))

		// Something which never goes quiet is synced every max delay
		syncs, last := 0, ""
		for second := range 170 {
			content := fmt.Sprintf("version %d", second)
			write("busy.txt", content)
			watcher.changed("busy.txt")
			sleep(time.Second)
			if got, err := os.ReadFile(filepath.Join(r.FremoteName, "busy.txt")); err == nil && string(got) != last {
				syncs++
				last = string(got)
			}
		}
		assert.Equal(t, 2, syncs, "busy file should be synced once a minute")
		sleep(opt.Settle)
		assert.True(t, synced("busy.txt", "version 169"))

		// A change which isn't reported is found by the next full
		// sync, which is run an hour after the last one finished
		write("unreported.txt", "found by the full sync")
		sleep(time.Until(start.Add(opt.FullCheck - time.Second)))
		assert.False(t, synced("unreported.txt", "found by the full sync"), "full sync ran early")
		sleep(2 * time.Second)
		assert.True(t, synced("unreported.txt", "found by the full sync"), "full sync didn't run after an hour")

		// A report that changes may have been missed anywhere runs a full sync
		write("unreported.txt", "found after events were lost")
		watcher.send(dirEvents("")...)
		sleep(opt.Settle + time.Millisecond)
		assert.True(t, synced("unreported.txt", "found after events were lost"))
		lastFull := time.Now()

		// A sync which fails is retried after 5s, then twice as long each time
		if runtime.GOOS != "windows" && os.Geteuid() != 0 {
			unreadable := filepath.Join(r.LocalName, "unreadable.txt")
			write("unreadable.txt", "can't be read at first")
			require.NoError(t, os.Chmod(unreadable, 0))
			failedAt := time.Now()
			watcher.changed("unreadable.txt")
			// Tried at 2s, 7s, 17s, 37s and 77s
			sleep(50 * time.Second)
			require.NoError(t, os.Chmod(unreadable, 0o666))
			sleep(time.Until(failedAt.Add(77*time.Second - time.Millisecond)))
			assert.False(t, synced("unreadable.txt", "can't be read at first"), "retried early")
			sleep(2 * time.Millisecond)
			assert.True(t, synced("unreadable.txt", "can't be read at first"), "not retried when due")

			// It starts again from 5s after working
			write("unreadable.txt", "changed")
			require.NoError(t, os.Chmod(unreadable, 0))
			failedAt = time.Now()
			watcher.changed("unreadable.txt")
			sleep(3 * time.Second)
			require.NoError(t, os.Chmod(unreadable, 0o666))
			sleep(time.Until(failedAt.Add(7*time.Second - time.Millisecond)))
			assert.False(t, synced("unreadable.txt", "changed"), "retried early")
			sleep(2 * time.Millisecond)
			assert.True(t, synced("unreadable.txt", "changed"), "not retried when due")

			// What is synced along with something which fails isn't
			// held up by it. The delete isn't done by the sync at 2s
			// as that had an error, so is tried again at 4s.
			write("unreadable.txt", "changed again")
			require.NoError(t, os.Chmod(unreadable, 0))
			require.NoError(t, os.Remove(filepath.Join(r.LocalName, "settle.txt")))
			failedAt = time.Now()
			watcher.changed("settle.txt", "unreadable.txt")
			sleep(2*opt.Settle - time.Millisecond)
			assert.FileExists(t, filepath.Join(r.FremoteName, "settle.txt"))
			sleep(2 * time.Millisecond)
			assert.NoFileExists(t, filepath.Join(r.FremoteName, "settle.txt"))
			require.NoError(t, os.Chmod(unreadable, 0o666))
			sleep(time.Until(failedAt.Add(7*time.Second + time.Millisecond)))
			assert.True(t, synced("unreadable.txt", "changed again"), "not retried when due")
		}

		// The full sync keeps running every hour, and reported
		// changes keep being synced, for a month
		for hour := 1; hour <= 30*24; hour++ {
			unreported := fmt.Sprintf("hour %d", hour)
			write("unreported.txt", unreported)
			reported := fmt.Sprintf("reported in hour %d", hour)
			write("reported.txt", reported)
			watcher.changed("reported.txt")
			sleep(opt.Settle + time.Millisecond)
			require.True(t, synced("reported.txt", reported), "hour %d", hour)
			require.False(t, synced("unreported.txt", unreported), "hour %d: full sync ran early", hour)

			lastFull = lastFull.Add(opt.FullCheck)
			sleep(time.Until(lastFull.Add(-time.Second)))
			require.False(t, synced("unreported.txt", unreported), "hour %d: full sync ran early", hour)
			sleep(2 * time.Second)
			require.True(t, synced("unreported.txt", unreported), "hour %d: full sync didn't run", hour)
			lastFull = time.Now().Add(-time.Second)
		}
		assert.Greater(t, time.Since(start), 30*24*time.Hour)
	})
}
