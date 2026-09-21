Not sure what is this yet.

Future work:
 - Add integration tests to the transport layer
 - Add pooled connections to the address and validate if the channel is still good or exists before sending new message
 - Add incarnation numbers to membership so concurrent updates converge deterministically

## Discovery

Nodes start with a list of seed addresses and discover the rest of the cluster at runtime:

- a joining node sends `JOIN` to each seed until one answers with `WELCOME` containing the member list
- the node handling the join broadcasts `MEMBERS` to the peers it already knows
- every node pings its peers every 2 seconds and removes a peer after 3 missed heartbeats
- a node that shuts down cleanly broadcasts `LEAVE`

Each node advertises itself as `host:port`, where the host comes from the `NODE_HOST` environment variable (or the local address when unset) and the cluster name from `CLUSTER_NAME` (default `netty-cluster`).

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
