# AGENTS.md

Instructions for agents working in this repository.

## What this project is

Coffer is a small distributed-systems playground written in Java 21 on top of
Netty. It implements a custom TCP transport, cluster discovery with gossip
membership, Raft leader election and log replication, and an in-memory
key-value store replicated through the Raft log. It is not a library;
`ClusterNode` runs a node and `docker compose` starts a three-node cluster.

## Layout

- `src/main/protobuf/` — wire, cluster and Raft schemas. Generated Java lands
  in `io.github.brunomvpcosta.coffer.protocol`; never edit generated sources.
- `src/main/java/io/github/brunomvpcosta/coffer/transport/` — `Transport`
  abstraction and `MessageKind`; the Netty implementation is in
  `transport/netty/`.
- `src/main/java/io/github/brunomvpcosta/coffer/cluster/` — seed discovery,
  join/leave, incarnation-based membership with tombstones.
- `src/main/java/io/github/brunomvpcosta/coffer/raft/` — `RaftService`
  (election, heartbeats, log replication), storage, and message helpers.
- `src/main/java/io/github/brunomvpcosta/coffer/kv/` — key-value state machine
  and `KvService`.
- `src/main/java/io/github/brunomvpcosta/coffer/ClusterNode.java` — wires
  transport, cluster, Raft and KV together and is the process entry point.
- `src/main/java/io/github/brunomvpcosta/coffer/KvClient.java` — interactive and
  one-shot KV client.
- `src/test/java/` — mirrors the main package structure; JUnit Jupiter tests.

## Build and test

```sh
mvn test
mvn -q -DskipTests package
```

If Maven is not installed, use the same Maven image the Dockerfile uses:

```sh
docker run --rm -v "$PWD":/app -w /app -v coffer-m2:/root/.m2 \
  maven:3.9-eclipse-temurin-21 mvn -q -B test
```

Run the three-node demo and check convergence:

```sh
docker compose up --build
```

Expected in the logs: every node prints the same `[RAFT] ... configuration`,
exactly one node wins each term, and all nodes converge to the same member list.
Stop it with `docker compose down`.

## Design constraints

- The Raft configuration is the static seed roster. Never derive quorum from
  live cluster membership; a shrinking view must not shrink the majority.
  Every node must be given the same peers.
- Membership merges are last-writer-wins by incarnation and then state
  (`DEAD` over `SUSPECT` over `ALIVE`). Dead nodes stay as tombstones.
- The transport pools one connection per address. Use `sendAndReceive` for RPC
  and `send` for fire-and-forget messages.
- Raft term, vote and log are persisted through `RaftStorage`.
  `FileRaftStorage` rewrites the whole log atomically; there is no separate
  write-ahead log yet. The commit index and the state machine are rebuilt from
  the log, so do not introduce a second source of truth.
- The leader appends a no-op entry when elected; state machines must ignore
  empty commands.
- KV writes are leader-only and followers forward them. Reads are served from
  the local replica and may be stale; do not describe them as linearizable.
- `RaftService` state is guarded by `synchronized`; network callbacks may run on
  Netty threads. Keep state mutations inside the lock.
- Prefer Java 21 features already in use (`record`, `var`, switch expressions)
  and match the surrounding style. No new frameworks without a clear need.
- Logging currently uses `System.out.println("[TAG] - ...")`; match that style
  unless the change is an explicit migration to a logger.

## Definition of done

- `mvn test` passes (or the Docker equivalent above).
- The Docker three-node demo still converges and elects a single leader.
- A KV write is visible on all nodes and survives a cluster restart.
- README is updated when behavior or commands change.
- New protocol fields or messages have a round-trip or state-machine test.

## Commit messages

Follow [How to Write a Git Commit Message](https://cbea.ms/git-commit/) by Chris
Beams. The seven rules:

1. Separate the subject from the body with a blank line.
2. Limit the subject line to 50 characters (72 is the hard limit).
3. Capitalize the subject line.
4. Do not end the subject line with a period.
5. Use the imperative mood in the subject. It must complete:
   "If applied, this commit will ...".
6. Wrap the body at 72 characters.
7. Use the body to explain **what** and **why**, not **how**; the diff already
   shows the how.

Example:

```text
Add Raft leader election and log replication

Introduce a RAFT message kind and RaftService with follower,
candidate and leader roles. Elections use randomized timeouts and
RequestVote; leaders replicate entries with AppendEntries.

The server routes RAFT envelopes alongside cluster and application
messages, and the node starts Raft after discovery so it has a peer
list before campaigning.
```

Avoid:

- `added raft stuff` — not capitalized, not imperative
- `Fix bug.` — trailing period
- `Add Raft leader election and log replication support for clustered nodes` —
  subject too long; split the change or shorten the subject
- One-line bodies hundreds of characters wide; wrap at 72

Operational rules:

- Make atomic commits; do not mix unrelated refactors.
- Keep generated code, build output and `target/` out of commits.
- Do not leave `WIP`, `fixup!`, or `squash!` messages in the final history.
- Write the message in a file or pre-wrapped `-m` body; do not rely on the shell
  to wrap text. Before committing, check the last message:

```sh
git log -1 --format=%s | awk 'length($0) > 50 { print "subject too long"; exit 1 }'
git log -1 --format=%B | awk 'NR > 1 && length($0) > 72 { print "body line too long"; exit 1 }'
```
