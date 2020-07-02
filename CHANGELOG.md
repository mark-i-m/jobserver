# Changelog

## 0.4

- Reimplemented the server state serialization for snapshots. This fixes weird
  errors where tasks would become corrutped after a server restart for no
  apparent reason. Unfortunately, this is breaking change to the format of the
  server snapshots, so tasks that were already in the snapshot will show as
  `Unknown` after restarting the server into version 0.4.

## 0.3

- This is the first version I published on crates.io.
