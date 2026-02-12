![Current Release](https://img.shields.io/github/release/JohnSully/KeyDB.svg)
![CI](https://github.com/JohnSully/KeyDB/workflows/CI/badge.svg?branch=unstable)
[![StackShare](http://img.shields.io/badge/tech-stack-0690fa.svg?style=flat)](https://stackshare.io/eq-alpha-technology-inc/eq-alpha-technology-inc)

## KeyDB with Redis 8.2.3 Protocol Support

This fork adds full Redis 8.2.3 protocol compatibility to KeyDB while preserving all KeyDB advantages: multi-master active-active replication, multithreading, and Kubernetes-native scaling.

**Docker Hub:** [`vainkop/keydb8:8.2.3`](https://hub.docker.com/r/vainkop/keydb8) (linux/amd64, linux/arm64)

**Redis 8 upgrade by:** [Valerii Vainkop](https://github.com/vainkop)

---

What is KeyDB?
--------------

KeyDB is a high performance fork of Redis with a focus on multithreading, memory efficiency, and high throughput. In addition to performance improvements, KeyDB offers Active Replication, FLASH Storage, and Subkey Expires. KeyDB has an MVCC architecture that allows queries like KEYS and SCAN to run without blocking the database.

This fork extends KeyDB with full Redis 8.2.3 protocol support, making it the only solution that combines:

- **Redis 8.2.3 protocol** with all latest commands and Functions API
- **Master-master active replication** for true multi-master deployments
- **Multithreading** for higher throughput on modern hardware
- **Kubernetes-native** Helm chart with multi-master StatefulSet, health probes, and monitoring

KeyDB maintains full compatibility with the Redis protocol, modules, and scripts. This includes atomicity guarantees for scripts and transactions. Because KeyDB stays in sync with Redis development, it is a drop-in replacement for existing Redis deployments.

Redis 8 Commands
-----------------

All Redis 8.2.3 commands are implemented with thread-safe, production-ready code:

**List Operations:**
- `LMPOP`, `BLMPOP` -- Pop multiple elements from lists

**Sorted Set Operations:**
- `ZMPOP`, `BZMPOP` -- Pop multiple elements from sorted sets

**Set Operations:**
- `SINTERCARD` -- Intersection cardinality with LIMIT (without materializing)

**Hash Field Expiry (9 commands):**
- `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT` -- Set per-field expiration with NX/XX/GT/LT flags
- `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME` -- Get field TTL
- `HPERSIST` -- Remove field expiration (per-field, not key-level)

**String Operations:**
- `LCS` -- Longest common subsequence with LEN/IDX/MINMATCHLEN/WITHMATCHLEN

**Expiration:**
- `EXPIRETIME`, `PEXPIRETIME` -- Get absolute expiration timestamp

**Scripting:**
- `EVAL_RO`, `EVALSHA_RO` -- Read-only script execution (write operations are denied)

**Functions API:**
- `FUNCTION LOAD` -- Load Lua libraries with `redis.register_function` (simple and table form with flags)
- `FUNCTION DELETE`, `LIST`, `STATS`, `FLUSH`, `DUMP`, `RESTORE`, `KILL`
- `FCALL`, `FCALL_RO` -- Execute registered functions with return value support

All write commands replicate correctly via KeyDB's RREPLAY active-active replication mechanism. Read-only variants (`EVAL_RO`, `EVALSHA_RO`, `FCALL_RO`) properly deny write operations and skip replication.

Quick Start
-----------

### Docker

```bash
# Single node
docker run -d --name keydb -p 6379:6379 vainkop/keydb8:8.2.3

# Test Redis 8 commands
redis-cli PING
redis-cli RPUSH mylist a b c d e
redis-cli LMPOP 1 mylist LEFT COUNT 2
redis-cli HSET myhash f1 v1 f2 v2
redis-cli HEXPIRE myhash 60 FIELDS 1 f1
redis-cli HTTL myhash FIELDS 1 f1
```

### Kubernetes (Helm)

```bash
# Single node
helm install keydb ./pkg/helm

# Multi-master (3 nodes, active-active replication)
helm install keydb ./pkg/helm \
  --set nodes=3 \
  --set keydb.multiMaster=yes \
  --set keydb.activeReplicas=yes

# With monitoring
helm install keydb ./pkg/helm \
  --set nodes=3 \
  --set keydb.multiMaster=yes \
  --set keydb.activeReplicas=yes \
  --set exporter.enabled=true \
  --set serviceMonitor.enabled=true
```

See `pkg/helm/values.yaml` for all configuration options.

### Active-Active Replication

```bash
# Start two masters with mutual replication
./src/keydb-server --port 6379 --active-replica yes --replicaof 127.0.0.1 6380 &
./src/keydb-server --port 6380 --active-replica yes --replicaof 127.0.0.1 6379 &

# Write on either node, read from both
redis-cli -p 6379 SET key1 "written-on-6379"
redis-cli -p 6380 GET key1   # returns "written-on-6379"
```

Helm Chart
----------

The Helm chart in `pkg/helm/` supports:

- **Multi-master StatefulSet** with configurable replicas and active-active replication
- **Health probes** (liveness, readiness, startup) using ConfigMap scripts that handle LOADING state
- **Persistence** via PVC with configurable storage class and size
- **Redis exporter** sidecar (oliver006/redis_exporter v1.80.1) with ServiceMonitor for Prometheus/VictoriaMetrics
- **Pod Disruption Budget**, topology spread constraints, affinity, tolerations
- **Authentication** via inline password or existing Secret
- **Extra containers/volumes/init containers** for extensibility

Backward Compatibility
----------------------

Tested with clients using Redis protocol versions 6, 7, and 8:

| Client | RESP2 | RESP3 | Result |
|--------|-------|-------|--------|
| Python redis-py 5.1.1 | Pass | Pass | 126/128 (2 failures are client-side) |
| Node.js redis@4 | Pass | -- | 15/15 |
| Go go-redis/v9 | Pass | Pass | 26/26 |

All classic Redis commands (strings, lists, sets, sorted sets, hashes, HyperLogLog, streams, pub/sub, transactions, Lua scripting) work identically across both RESP2 and RESP3 protocols.

Building
--------

### From Source

```bash
# Install dependencies
sudo apt install build-essential nasm autotools-dev autoconf libjemalloc-dev \
  tcl tcl-dev uuid-dev libcurl4-openssl-dev libbz2-dev libzstd-dev \
  liblz4-dev libsnappy-dev libssl-dev pkg-config

# Build with TLS support
make BUILD_TLS=yes -j$(nproc)

# Run tests
./runtest --single unit/redis8
./runtest --single unit/hash-expiry
./runtest --single unit/functions
```

### Docker (Multi-Arch)

```bash
# Build and push multi-arch image (amd64 + arm64)
./build_push.sh 8.2.3

# Or build locally for a single platform
docker build -t keydb:local .
```

The Dockerfile handles arm64 cross-compilation via QEMU with automatic retry logic for GCC stability.

Testing
-------

| Test Suite | Count | Status |
|------------|-------|--------|
| Tcl unit tests (redis8, hash-expiry, functions) | 36 | Pass |
| K8s E2E test suite | 40 | Pass |
| Backward compatibility (Python/Node/Go) | 167 | Pass |
| Load testing (redis-benchmark) | -- | 199K rps peak |
| Multi-master chaos (3-node, pod kills) | -- | Pass |

Run the K8s test suite:
```bash
./deploy_and_test.sh
```

##### KeyDB is a part of Snap Inc! Original announcement [here](https://docs.keydb.dev/news/2022/05/12/keydb-joins-snap)

##### Need Help? Check out the extensive [documentation](https://docs.keydb.dev)

Why Fork Redis?
---------------

KeyDB has a different philosophy on how the codebase should evolve. We feel that ease of use, high performance, and a "batteries included" approach is the best way to create a good user experience.

This fork specifically addresses the need for Redis 8 compatibility while maintaining KeyDB's unique advantages that Redis 8 and Valkey don't offer:
- Master-master active-active replication
- True multithreading for better hardware utilization
- Kubernetes-native horizontal scaling
- FLASH storage support

Project Support
-------------------

The KeyDB team maintains this project as part of Snap Inc. KeyDB is used by Snap as part of its caching infrastructure and is fully open sourced. There is no separate commercial product and no paid support options available. We value collaborating with the open source community and welcome PRs, bug reports, and open discussion. For community support check out [docs.keydb.dev/docs/support](https://docs.keydb.dev/docs/support).

Additional Resources
--------------------

- [Docker Hub: vainkop/keydb8](https://hub.docker.com/r/vainkop/keydb8)
- [KeyDB Documentation](https://docs.keydb.dev)
- [Slack Community](https://docs.keydb.dev/slack/)

New Configuration Options
-------------------------

With new features comes new options. All other configuration options behave as you'd expect. Your existing configuration files should continue to work unchanged.

```
    server-threads N
    server-thread-affinity [true/false]
```
The number of threads used to serve requests. This should be related to the number of queues available in your network hardware, *not* the number of cores on your machine. Because KeyDB uses spinlocks to reduce latency; making this too high will reduce performance. We recommend using 4 here. By default this is set to two.

```
    active-replica yes
```
If you are using active-active replication set `active-replica` option to "yes". This will enable both instances to accept reads and writes while remaining synced. [See docs](https://docs.keydb.dev/docs/active-rep/).

```
    multi-master-no-forward no
```
Avoid forwarding RREPLAY messages to other masters. WARNING: This setting is dangerous! All masters must be connected in a true mesh topology or data loss will occur.

Multithreading Architecture
---------------------------

KeyDB works by running the normal Redis event loop on multiple threads. Network IO and query parsing are done concurrently. Each connection is assigned a thread on accept(). Access to the core hash table is guarded by spinlock. Because the hashtable access is extremely fast this lock has low contention. Transactions hold the lock for the duration of the EXEC command. Modules work in concert with the GIL which is only acquired when all server threads are paused.

Code contributions
-----------------

Note: by contributing code to the KeyDB project in any form, including sending
a pull request via Github, a code fragment or patch via private email or
public discussion groups, you agree to release your code under the terms
of the BSD license that you can find in the COPYING file included in the KeyDB
source distribution.

Please see the CONTRIBUTING file in this source distribution for more information.
