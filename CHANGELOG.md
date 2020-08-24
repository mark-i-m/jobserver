# Changelog

## 0.7.0

- Matrices that have become empty because all of their jobs were forgotten will
  also be forgotten. This is different from prior behavior, so I'm bumping the
  major version.
- Added support for timing out jobs.
- Fix some bugs

## 0.6.1

- Minor backwards-compatible changes to client-server protocol and vast
  refactoring of client-side printing for `job ls`. These produce a major
  improvement in the format of job listings for matrices.

## 0.6

- Changes to client-side `j machine rm` arguments to allow removing classes of
  machines more easily. This allows removing expired reservations more easily.

## 0.5

- Add `j job results` subcommand.
- Major improvements to handling of failed/cloned jobs in matrices:
    - When a matrix job is cloned, the clone also ends up in the matrix.
    - Matrix jobs automatically repeat on failure.
- `j job matrix add` now supports the `-x` flag.
- `j job ls` now prints a summary of the printed jobs.
- Internal rearchitecting of the thread that copies results back to the host.
  This may allow future improvements to handling of failed/timed out/hanging
  copying tasks.

## 0.4

- Reimplemented the server state serialization for snapshots. This fixes weird
  errors where tasks would become corrutped after a server restart for no
  apparent reason. Unfortunately, this is breaking change to the format of the
  server snapshots, so tasks that were already in the snapshot will show as
  `Unknown` after restarting the server into version 0.4.

## 0.3

- This is the first version I published on crates.io.
