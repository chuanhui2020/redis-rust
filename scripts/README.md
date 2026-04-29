# redis-rust Cluster Deployment (3-Master 3-Slave)

One-click deployment scripts for a redis-rust cluster.

## Prerequisites

- **redis-rust** built in release mode: `cargo build --release`
- **redis-cli.exe** from Redis for Windows (default path: `C:\Program Files\Redis\redis-cli.exe`)
- Windows with PowerShell 5.1+ (or Linux/macOS with equivalent shell)

## Architecture

| Node   | Data Port | Bus Port | Role  | Slot Range     |
|--------|-----------|----------|-------|----------------|
| node-1 | 7000      | 17000    | master| 0 - 5460       |
| node-2 | 7001      | 17001    | master| 5461 - 10922   |
| node-3 | 7002      | 17002    | master| 10923 - 16383  |
| node-4 | 7003      | 17003    | slave | replica of 7000|
| node-5 | 7004      | 17004    | slave | replica of 7001|
| node-6 | 7005      | 17005    | slave | replica of 7002|

Each node runs in its own working directory (`cluster-data/node-<port>`) to avoid `nodes.conf` conflicts.

## Quick Start (Local)

```powershell
cd scripts
.\deploy-cluster.ps1    # Deploy
.\check-cluster.ps1     # Verify health
.\stop-cluster.ps1      # Tear down
```

## Cross-Datacenter Deployment

The server now supports `--bind`, `--cluster-announce-ip`, `--cluster-announce-port`, and `--cluster-announce-bus-port`.

### Example: 3 machines across 2 datacenters

**Machine A (DC-1, IP: 10.0.1.10)**
```bash
redis-rust --port 7000 --bind 0.0.0.0 \
  --cluster-enabled yes \
  --cluster-announce-ip 10.0.1.10 \
  --cluster-announce-port 7000 \
  --cluster-announce-bus-port 17000

redis-rust --port 7003 --bind 0.0.0.0 \
  --cluster-enabled yes \
  --cluster-announce-ip 10.0.1.10 \
  --cluster-announce-port 7003 \
  --cluster-announce-bus-port 17003
```

**Machine B (DC-1, IP: 10.0.1.11)**
```bash
redis-rust --port 7001 --bind 0.0.0.0 \
  --cluster-enabled yes \
  --cluster-announce-ip 10.0.1.11 \
  --cluster-announce-port 7001 \
  --cluster-announce-bus-port 17001

redis-rust --port 7004 --bind 0.0.0.0 \
  --cluster-enabled yes \
  --cluster-announce-ip 10.0.1.11 \
  --cluster-announce-port 7004 \
  --cluster-announce-bus-port 17004
```

**Machine C (DC-2, IP: 10.0.2.20)**
```bash
redis-rust --port 7002 --bind 0.0.0.0 \
  --cluster-enabled yes \
  --cluster-announce-ip 10.0.2.20 \
  --cluster-announce-port 7002 \
  --cluster-announce-bus-port 17002

redis-rust --port 7005 --bind 0.0.0.0 \
  --cluster-enabled yes \
  --cluster-announce-ip 10.0.2.20 \
  --cluster-announce-port 7005 \
  --cluster-announce-bus-port 17005
```

Then form the cluster via redis-cli (run on any node):

```bash
# Full mesh MEET
redis-cli -p 7000 CLUSTER MEET 10.0.1.11 7001
redis-cli -p 7000 CLUSTER MEET 10.0.2.20 7002
redis-cli -p 7001 CLUSTER MEET 10.0.2.20 7002
redis-cli -p 7000 CLUSTER MEET 10.0.1.10 7003
redis-cli -p 7000 CLUSTER MEET 10.0.1.11 7004
redis-cli -p 7000 CLUSTER MEET 10.0.2.20 7005
# ... (meet all pairs, or just meet one seed per machine)

# Assign slots
redis-cli -p 7000 CLUSTER ADDSLOTS $(seq 0 5460)
redis-cli -p 7001 CLUSTER ADDSLOTS $(seq 5461 10922)
redis-cli -p 7002 CLUSTER ADDSLOTS $(seq 10923 16383)

# Attach slaves (get master IDs from CLUSTER NODES first)
redis-cli -p 7003 CLUSTER REPLICATE <master-1-id>
redis-cli -p 7004 CLUSTER REPLICATE <master-2-id>
redis-cli -p 7005 CLUSTER REPLICATE <master-3-id>
```

### PowerShell script with cross-DC params

```powershell
cd scripts
.\deploy-cluster.ps1 -Bind "0.0.0.0" -ClusterAnnounceIp "10.0.1.10"
```

## What the Deploy Script Does

1. **Cleanup** — kills any running redis-rust processes and removes old data
2. **Start 6 nodes** — each with `--cluster-enabled yes` on ports 7000-7005
3. **Full mesh MEET** — sends `CLUSTER MEET` for every pair of nodes (15 total)
4. **Wait for gossip sync** — 3 seconds for topology to propagate
5. **Assign slots** — batches `CLUSTER ADDSLOTS` in chunks of 200 per master
6. **Attach slaves** — parses `CLUSTER NODES` to get master IDs, then calls `CLUSTER REPLICATE`
7. **Verify** — prints `CLUSTER INFO`, `CLUSTER SLOTS`, and `CLUSTER NODES`

## Client Usage

Clients must handle `MOVED` / `ASK` redirections (standard Redis cluster behavior).

```bash
redis-cli -c -p 7000 SET foo bar      # use -c for cluster-aware mode
redis-cli -c -p 7000 GET foo
```

Or manually follow redirects:

```bash
redis-cli -p 7000 SET foo bar
# -> MOVED 12182 10.0.2.20:7002
redis-cli -p 7002 SET foo bar
```

## New CLI Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--bind <ip>` | IP to bind for client and bus sockets | `127.0.0.1` |
| `--cluster-announce-ip <ip>` | IP announced to other cluster nodes | same as `--bind` |
| `--cluster-announce-port <port>` | Data port announced to other nodes | same as `--port` |
| `--cluster-announce-bus-port <port>` | Bus port announced to other nodes | `port + 10000` |

## Cluster Capabilities (Current Implementation)

| Feature | Status |
|---------|--------|
| Gossip protocol (PING/PONG) | ✅ |
| Slot assignment (ADDSLOTS / DELSLOTS) | ✅ |
| Master-replica replication | ✅ |
| Automatic failover (PFail → Fail → promote replica) | ✅ |
| Manual failover (`CLUSTER FAILOVER`) | ✅ |
| MOVED / ASK redirections | ✅ |
| READONLY mode for replicas | ✅ |
| nodes.conf persistence | ✅ |
| Cross-datacenter (custom bind / announce IP) | ✅ |
| Slot migration metadata (`SETSLOT IMPORTING/MIGRATING/NODE`) | ✅ |
| **Actual key data migration** | ❌ Not implemented |

## Known Limitations

1. **No data migration** — `CLUSTER SETSLOT NODE` moves slot ownership in the topology table, but does not migrate actual keys.
2. **No cluster proxy** — clients must handle `MOVED`/`ASK` themselves (same as standard Redis cluster).
3. **Bus port known via gossip** — if a node uses a custom bus port, other nodes will learn it via `CLUSTER MEET` / `CLUSTER NODES`, but the initial MEET must target the correct bus port (currently MEET targets data port, and the server infers bus = data + 10000).
