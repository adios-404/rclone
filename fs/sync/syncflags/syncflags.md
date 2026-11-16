## Watching for changes

With the `--watch` flag rclone keeps running after the first sync and
syncs changes to the source as they happen. It runs until it is
stopped, for example with Ctrl-C.

```sh
rclone sync --watch /path/to/src remote:dst
```

This needs a source which can report changes as they happen. At the
moment that is the local file system on Linux, macOS and Windows. With
any other source, or if watching can't be started, rclone falls back
to running a full sync every `--watch-full-check`.

Rclone waits until a file hasn't changed for `--watch-settle` (default
2s) before syncing it, so that a file which is being written isn't
copied half way through. A file which never stops changing is tried
anyway `--watch-max-delay` (default 1m) after it first changed. If
rclone finds a file is still changing while copying it, it doesn't
replace the copy in the destination and tries again later.

Only what changed is looked at, so the work done depends on the size
of the change rather than the size of the source:

- When a directory is created, renamed or deleted, rclone syncs that
  directory, listing it and the directories above it to find it.
- When files change, rclone lists the directories they are in and
  those above them, and syncs just those files and the modification
  times of the directories they are in.
- If the destination doesn't keep the modification times of
  directories, or `--no-update-dir-modtime` is used, rclone looks up
  just the files which changed without listing anything. This isn't
  done if the source or destination ignores case, or with
  `--track-renames`, `--ignore-case-sync`, `--fix-case`,
  `--name-transform` or `--exclude-if-present`, which need the
  listings.
- If more than 10,000 changes are waiting they are merged into the
  directories they are in, so a very large change costs listing the
  directories it touched rather than a full sync.

As a safety net rclone also runs a normal full sync every
`--watch-full-check` (default 1h, `0` to disable). This picks up
anything which wasn't reported, for example changes made on a network
file system by another computer. The time between full syncs doesn't
include time the computer spends asleep.

The other flags work as they do without `--watch`, including filters,
`--backup-dir`, `--dry-run` and `--track-renames`, with these
differences:

- Filters are applied to a file when a change to it is synced. A
  file which a filter on size or age comes to exclude or include
  without the file changing is dealt with by the next full sync.
- `--max-delete` and `--max-delete-size` apply to each sync rather
  than to the whole run. Rclone stops if they are exceeded.
- `--max-transfer` and `--max-duration` apply to the whole run, and
  rclone stops when they are reached.
- If syncing something fails it is retried after 5 seconds, then after
  twice as long each time it fails again, up to 5 minutes. Something
  which failed because it changed while it was being synced is retried
  as soon as it stops changing. Other changes are synced in the
  meantime, including changes to other things in the same directory.
  As usual, nothing is deleted by a sync in which there were errors.
- If the source directory can't be listed, or is found to be empty
  when the destination isn't without rclone having been told of each
  thing in it being deleted, rclone refuses to delete anything until
  the source has something in it again. That is what a drive which has
  been unplugged or unmounted usually looks like, and syncing it would
  delete everything in the destination. If you really have emptied the
  source and rclone refuses, restart rclone.

### Limits of watching the local file system

- On Linux each directory has to be watched individually, and the
  kernel limits how many directories a user can watch. If there are
  too many, rclone says so, watches as many as it can and checks the
  rest once a minute. To watch them all raise the limit, for example
  with `sysctl fs.inotify.max_user_watches=1048576`. Each watched
  directory uses about 1 KiB of kernel memory. Directories which the
  filters exclude aren't watched, so excluding large directories
  which don't need syncing, such as `node_modules`, keeps the number
  down.
- On Linux a file system being mounted or unmounted inside the source
  is noticed within a minute rather than at once. A directory which is
  mounted in two places inside the source is only watched in one of
  them. Changes made to a file through a memory mapping are not
  reported.
- On macOS watching needs a build of rclone with cgo, which the
  official releases are.
- Changes inside a directory which is reached through a symlink (with
  `--copy-links`) are not noticed until the next full sync.
- File systems which don't tell the operating system about changes
  made elsewhere, such as NFS, SMB and most FUSE mounts, only report
  changes made on this computer.
