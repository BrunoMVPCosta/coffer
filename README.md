Not sure what is this yet.

Future work:
 - Add integration tests to the transport layer
 - Persist the Raft term, vote and log so a restart keeps consensus state
 - Build the key-value state machine on top of the Raft log
 - Add snapshots/compaction and linearizable reads (ReadIndex or leader lease)

## Discovery

Nodes start with a list of seed addresses and discover the rest of the cluster at runtime:

- a joining node sends `JOIN` to each seed until one answers with `WELCOME` containing the member list
- the node handling the join broadcasts `MEMBERS` to the peers it already knows
- every node pings its peers every 2 seconds and removes a peer after 3 missed heartbeats
- a node that shuts down cleanly broadcasts `LEAVE`

Each node advertises itself as `host:port`, where the host comes from the `NODE_HOST` environment variable (or the local address when unset) and the cluster name from `CLUSTER_NAME` (default `netty-cluster`).

## Membership

Every node advertises the same identity plus two pieces of membership metadata:

- an **incarnation** number, owned by the node itself and initialised from the
  wall clock so a restarted process outranks the tombstone of its previous run
- a **state**: `ALIVE`, `SUSPECT` or `DEAD`

Membership updates are merged with last-writer-wins semantics: a higher
incarnation always wins; at the same incarnation a higher state wins (`DEAD`
over `SUSPECT` over `ALIVE`). This makes concurrent updates converge
deterministically and stops a delayed message from resurrecting a dead node.

A dead node is kept as a **tombstone** for five minutes. If a node hears that it
is dead or suspected, it increments its own incarnation and gossips `ALIVE`
again. Nodes exchange their full membership view with every heartbeat
(anti-entropy), so a lost update is repaired on the next round.

## Connections

The transport keeps one pooled connection per peer address. A connection is
reused while it is active; after a close or a failed write the next message
reopens it. Concurrent callers racing to the same address share a single
in-flight connect attempt, and requests fail fast when the connect fails instead
of waiting for the request timeout.

## Raft

`RaftService` runs leader election, heartbeats and log replication over the
same transport:

- roles are `FOLLOWER`, `CANDIDATE` and `LEADER`; elections use randomized
  timeouts and a term-based `RequestVote` RPC
- the leader sends `AppendEntries` heartbeats every 300ms and also uses them to
  replicate log entries
- followers truncate conflicting entries, append new ones and advance their
  commit index from `leaderCommit`
- a committed entry is handed to the registered applier in log order, which is
  the hook for a state machine such as a key-value store
- membership comes from `ClusterService`; Raft assumes a stable configuration
  and does not change it itself

Raft state (term, vote, log) is in memory for now, and elections are delayed by
a short discovery grace period so a node has a chance to learn its peers before
campaigning.

## How to run with Docker

Builds the project and starts a 3-node cluster (no local Maven/Java needed):

```
docker compose up --build
```

Logs from all nodes are prefixed with the node name; `Ctrl+C` or `docker compose down` stops the cluster. Each node is also reachable from the host on `127.0.0.1:8080`, `127.0.0.1:8081` and `127.0.0.1:8082`.

## How to run locally

Requires JDK 21+ and Maven.

```
mvn exec:java -Dexec.mainClass=com.netty.echo.EchoClient -Dexec.args="8080 127.0.0.1:8081 127.0.0.1:8082"
```

```
mvn exec:java -Dexec.mainClass=com.netty.echo.EchoClient -Dexec.args="8081 127.0.0.1:8080 127.0.0.1:8082"
```

```
mvn exec:java -Dexec.mainClass=com.netty.echo.EchoClient -Dexec.args="8082 127.0.0.1:8080 127.0.0.1:8081"
```
