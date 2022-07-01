## HomeKV

### Basic
HomeKV is a Rust memory key-value store which support Atomicity.
Atomicity is implemented by a MVCC storage, which also make Reading
and Writing don't block each other so that they can run in parallel.

The core MVCC storage of HomeKV acts as `RWLock<T>`, but it's better.
Because writes and multiple reads can be performed simultaneously,
which means they are not block each other to reduce contention.

To make the implementation easier, HomeKV only use the BTreeMap in
Rust standard lib. The drawback of it is that MVCC needs to clone
the whole tree to maintain multiple versions. An improvement could
be change the underlying data structure to B+Tree. But it will be
more complicated and can not be done in a short time.

### Server Concurrency Model
HomeKV uses Rust Tonic as the GRPC package, which handles concurrent requests
in Asynchronous Programming way to achieve high performance.

ProtoBuf service definition is at `api/proto/homekv_service.proto`

### API
HomeKV provides a RPC service through GRPC, which includes the following APIs,
* `get`: get values for multiple keys
* `set`: set multiple key-value pairs **transactionally**
* `del`: del values for multiple keys **transactionally**
* `metrics`: show storage metrics, number of keys, size of total values, and number of commands

### Build

```bash
# homekv/
.
├── Cargo.lock
├── Cargo.toml
├── README.md
├── api
│   └── proto
│       └── homekv_service.proto
├── build.rs
└── src
    ├── bin
    │   ├── hkvctl.rs
    │   └── homekv.rs
    ├── common
    │   ├── error.rs
    │   └── mod.rs
    ├── lib.rs
    └── storage
        ├── btree_store.rs
        ├── mod.rs
        └── mvcc.rs
```

Rust has a versatile build tool, Cargo, which make our building is very
easy.

#### Rust ENV Preparation
Please follow the Official Rust Installation to use `rustup` tool to
install Rust and Cargo.

#### Cargo Build
HomeKV provides a command-line client tool called `hkvctl`, and a server
starter called `homekv`. They are both under the `src/bin` folder, which
will be built as binary executable files under `target/release` when we
build this repo.

We just need to run the following command under the root folder of this repo.

```bash
# switch pwd to homekv
cargo build --release
```

### Run
#### Start homekv server
```bash
# switch pwd to homekv
./target/release/homekv -h 127.0.0.1 -p 20001
```

#### Play with the cli
```bash
# switch pwd to homekv

# Set key/keys by specifying `cmd` as `set` and `kvs` as 1/n key-value pairs
./target/release/hkvctl -h 127.0.0.1 -p 20001  --cmd set --kvs dummy_key=🦫
./target/release/hkvctl -h 127.0.0.1 -p 20001  --cmd set --kvs trail=big_cedder people=5 start_time=12:00

# Get key/keys by specifying `cmd` as `get` and `keys` as 1/n keys
./target/release/hkvctl -h 127.0.0.1 -p 20001  --cmd get --keys dummy_key
./target/release/hkvctl -h 127.0.0.1 -p 20001  --cmd get --keys trail people
# Not existing keys
./target/release/hkvctl -h 127.0.0.1 -p 20001  --cmd get --keys end_time

# Delete key/keys by specifying `cmd` as `del` and `keys` as 1/n keys
./target/release/hkvctl -h 127.0.0.1 -p 20001  --cmd del --keys dummy_key
./target/release/hkvctl -h 127.0.0.1 -p 20001  --cmd del --keys people start_time
# Not existing keys
./target/release/hkvctl -h 127.0.0.1 -p 20001  --cmd del --keys end_time

# Observability
./target/release/hkvctl -h 127.0.0.1 -p 20001  --cmd metrics
```
