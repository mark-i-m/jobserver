# `jobserver`

This is a job server and client for running many experiments across many test
machines. In some sense, it is like a simple cluster manager.

The high-level idea is that you have a server (the `jobserver`) that runs on
your machine (some machine that is always on, like a desktop), perhaps in a
screen session or something. You have a bunch of experiments that you want to
run from some driver application or script. You also have a bunch of machines,
possibly of different types, where you want to run jobs. `jobserver` schedules
those jobs to run on those machines and copies the results back to the host
machine (the one running the server). One interracts with the server using the
stateless CLI client.

Additionally, `jobserver` supports the following:
- An awesome CLI client, with the ability to generate shell completion scripts.
- Machine classes: similar machines are added to the same class, and jobs are
  scheduled to run on any machine in the class.
- Machine setup: the `jobserver` can run a sequence of setup machines and
  automatically add machines to its class when finished.
- Job matrices: Run jobs with different combinations of parameters.
- Automatically copies results back to the host, as long as the experiment
  driver outputs a line to `stdout` with format `RESULTS: <OUTPUT FILENAME>`.
- Good server logging.
- Tries to be a bit resilient to failures, crashes, and mismatching
  server/client version by using protobufs to save server history and
  communicate with client"

# Building

Requires:
- `rust 1.37+`

```console
> cargo build
```

The debug build seems to be fast enough for ordinary use.

# Usage

Running the server:

```console
> cargo run --bin server -- /path/to/experiment/driver /path/to/log4rs/config.yaml
```

You may want to run this in a `screen` or `tmux` session or something. The
first time you run it, you will need to pass the `--allow_snap_fail` flag,
which allows overwriting server history. The default is to fail and exit if
loading history fails. It is intended to give a little bit of safety so that if
you restart in a weird configuration it won't wipe out your server's history,
which can be annoying.

Running the client:

```console
> cargo run --bin client -- <args>
```

There are a lot of commands. They are well-documented by the CLI usage message.

I recommend creating an alias for this client. I use the alias `j`, so that I
can just run commands like:

```console
> j job ls
```

# Server Logging

The server uses the [`log4rs`][l4rs] library for logging. It is highly configurable.

[l4rs]: https://crates.io/crates/log4rs

I use the following config:

```yaml
# Check this file for config changes every 30 seconds
refresh_rate: 30 seconds

# Write log records to stdout and to the logfile
appenders:
  stdout:
    kind: console
    # Format of the log entries
    encoder:
      pattern: "[{d} {h({l})}] {f}:{L} {m}{n}"

  logfile:
    kind: file
    path: "/nobackup/scratch/jobserver.log"
    encoder:
      pattern: "[{d} {h({l})}] {f}:{L} {m}{n}"
    # To keep logs compact, don't write debugging info
    filters:
      - compact:
        kind: threshold
        level: info

loggers:
  # All log entries from the `server` binary go to the stdout and logfile appenders
  server:
    level: debug
    appenders:
      - stdout
      - logfile
```

Save these contents to a file and point the server to it using the second
argument in the command.
