# Raft C Porting Baseline

Baseline captured on 2026-07-24 in the `Asia/Seoul` time zone, before any
Phase 1 implementation.

## Repository revision

- Commit: `030244736e0a9abed6b406d6667a522996cd5ee7`
- Commit subject: `add plan`
- The working tree was clean before these planning documents were created.

## Go environment

- `go version`: `go version go1.26.0 linux/amd64`
- `go env GOVERSION`: `go1.26.0`
- `go env GOTOOLCHAIN`: `local`
- `go.mod` language version: `go 1.26`
- `go.mod` suggested toolchain: `go1.26.4`

The effective compiler for this baseline was therefore Go 1.26.0, despite the
newer toolchain suggestion in `go.mod`.

## Test command and result

The repository-wide baseline command is:

```sh
TMPDIR="$PWD/.tmp" GOTMPDIR="$PWD/.tmp" GOCACHE="$PWD/.gocache" go test ./...
```

With the default temporary directory, the command exited with status 1 before
the tests could run. Compilation and vet failed with `no space left on device`
while writing Go build artifacts under `$WORK`. At the time of diagnosis,
`/tmp` was on the root filesystem, which had only 47 MiB available and was
reported as 100% used. The following packages consequently reported
`[build failed]`:

- `go.etcd.io/raft/v3`
- `go.etcd.io/raft/v3/confchange`
- `go.etcd.io/raft/v3/quorum`
- `go.etcd.io/raft/v3/raftpb`
- `go.etcd.io/raft/v3/rafttest`
- `go.etcd.io/raft/v3/tracker`

This was an environment failure, not a test assertion failure. To distinguish
it from a repository failure, the same test command was rerun with `TMPDIR`
pointing to a temporary directory on `/home`:

```sh
TMPDIR=/home/hilyun/raft-go-test.ScWEvD go test ./...
```

That run exited successfully:

```text
ok  	go.etcd.io/raft/v3	0.286s
ok  	go.etcd.io/raft/v3/confchange	0.058s
ok  	go.etcd.io/raft/v3/quorum	0.136s
ok  	go.etcd.io/raft/v3/raftpb	0.005s
ok  	go.etcd.io/raft/v3/rafttest	0.650s
ok  	go.etcd.io/raft/v3/tracker	0.005s
```

The temporary directory was removed after the run. Future comparisons should
continue to use `go test ./...`; if the root filesystem remains full, they
should set `TMPDIR` to a fresh temporary directory on a filesystem with
sufficient free space and record that fact alongside the result.
