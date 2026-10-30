# Solana Substreams Indexer: Technical Guide & Project Post-Mortem

This document provides a detailed overview of the process used to build, debug, and execute a custom **Substreams Indexer on Solana Mainnet** to extract native SOL system transfers across blocks `385870151` to `385870156`.

---

## 1. Theoretical Background & Tool Overview

### What is Substreams?
**Substreams** is a high-performance blockchain indexing technology developed by StreamingFast and integrated with The Graph. Unlike traditional RPC polling—which fetches blocks one by one over HTTP—Substreams streams raw blockchain data directly using gRPC and executes user-defined WebAssembly (WASM) modules to filter, transform, and aggregate block data in parallel.

### Core Tools Used & Their Roles

* **Rust:** The primary programming language used to write the transformation logic (`src/lib.rs`). Rust ensures memory safety and compiles down into efficient WebAssembly binaries.
* **WebAssembly (WASM - `wasm32-unknown-unknown`):** The target binary platform. Substreams executes indexing logic inside a lightweight, sandboxed WASM virtual machine hosted on streaming servers (StreamingFast/Pinax).
* **Protocol Buffers (Protobufs - `.proto`):** A language-neutral serialization mechanism created by Google. Protobuf files define the structured schema for extracted data (e.g., `Transfer` and `Transfers` messages).
* **Substreams CLI (`substreams`):** The command-line utility used to package the WASM binary and schemas into a `.spkg` file, authenticate with streaming providers, and execute stream streaming jobs (`substreams run`).
* **Buf (`buf`):** A developer tool used by the Substreams CLI internally to inspect, lint, and compile Protobuf files.
* **Prost (`prost` / `prost-build`):** A Rust library that compiles Protobuf `.proto` schemas directly into native Rust data types at compile time (`build.rs`).
* **WSL2 (Windows Subsystem for Linux - Ubuntu):** The development shell environment providing a standard Unix toolchain (`cargo`, `protoc`, `gcc`) required for WebAssembly toolchains and gRPC execution.

---

## 2. Step-by-Step Procedure to Achieve the Task

### Step 1: Directory Setup & Structure Alignment
Ensure all configuration files, schemas, and source code reside directly at the project root to prevent mismatched compilation artifacts:

```text
solana-transfers-substream/
├── Cargo.toml
├── build.rs
├── substreams.yaml
├── proto/
│   └── transfers.proto
└── src/
    └── lib.rs
```

### Step 2: Protocol Buffer Definition
Define the extraction payload schema in `proto/transfers.proto`:

```protobuf
syntax = "proto3";

package solana.transfers.v1;

message Transfer {
  string from = 1;
  string to = 2;
  uint64 amount = 3;
}

message Transfers {
  repeated Transfer transfers = 1;
}
```

### Step 3: Substreams Manifest Configuration
Define the package inputs, outputs, and WASM binary output target in `substreams.yaml`:

```yaml
specVersion: v0.1.0
package:
  name: solana_transfers
  version: v0.1.0

imports:
  solana: https://spkg.io/streamingfast/solana-common-v0.4.0.spkg

protobuf:
  files:
    - transfers.proto
  importPaths:
    - ./proto

binaries:
  default:
    type: wasm/rust-v1
    file: ./target/wasm32-unknown-unknown/release/solana_transfers.wasm

modules:
  - name: map_transfers
    kind: map
    inputs:
      - source: sf.solana.type.v1.Block
    output:
      type: proto:solana.transfers.v1.Transfers
```

### Step 4: Memory-Safe Handler Implementation
Write the map handler in `src/lib.rs`. The logic iterates over raw transactions, filters for the System Program (`11111111111111111111111111111111`), checks for instruction type `2` (Native Transfer), and safely extracts sender/receiver public keys and amounts:

```rust
use substreams_solana::pb::sf::solana::type::v1::Block;
use bs58;

mod pb {
    pub mod solana {
        pub mod transfers {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/solana.transfers.v1.rs"));
            }
        }
    }
}

use pb::solana::transfers::v1::{Transfer, Transfers};

const SYSTEM_PROGRAM_ID: [u8; 32] = [0u8; 32];

#[substreams::handlers::map]
pub fn map_transfers(block: Block) -> Result<Transfers, substreams::errors::Error> {
    let mut transfers = Vec::new();

    for tx in &block.transactions {
        if let Some(meta) = &tx.meta {
            if meta.err.is_some() {
                continue;
            }
        }

        let transaction = match &tx.transaction {
            Some(t) => t,
            None => continue,
        };

        let message = match &transaction.message {
            Some(m) => m,
            None => continue,
        };

        let account_keys = &message.account_keys;
        if account_keys.is_empty() {
            continue;
        }

        for inst in &message.instructions {
            let program_id_idx = inst.program_id_index as usize;

            let program_id = match account_keys.get(program_id_idx) {
                Some(id) => id,
                None => continue,
            };

            if program_id.as_slice() != SYSTEM_PROGRAM_ID {
                continue;
            }

            if inst.data.len() < 12 {
                continue;
            }

            let type_bytes = match inst.data.get(0..4) {
                Some(slice) => match slice.try_into() {
                    Ok(arr) => arr,
                    Err(_) => continue,
                },
                None => continue,
            };

            let instruction_type = u32::from_le_bytes(type_bytes);
            if instruction_type != 2 {
                continue;
            }

            let amount_bytes = match inst.data.get(4..12) {
                Some(slice) => match slice.try_into() {
                    Ok(arr) => arr,
                    Err(_) => continue,
                },
                None => continue,
            };

            let amount = u64::from_le_bytes(amount_bytes);

            let from_idx = match inst.accounts.get(0) {
                Some(&idx) => idx as usize,
                None => continue,
            };

            let to_idx = match inst.accounts.get(1) {
                Some(&idx) => idx as usize,
                None => continue,
            };

            let from_account = match account_keys.get(from_idx) {
                Some(acc) => acc,
                None => continue,
            };

            let to_account = match account_keys.get(to_idx) {
                Some(acc) => acc,
                None => continue,
            };

            transfers.push(Transfer {
                from: bs58::encode(from_account).into_string(),
                to: bs58::encode(to_account).into_string(),
                amount,
            });
        }
    }

    Ok(Transfers { transfers })
}
```

### Step 5: Build, Package, and Execute
Run the compilation pipeline in WSL terminal:

```bash
cargo build --target wasm32-unknown-unknown --release
substreams pack
substreams run -e mainnet.sol.streamingfast.io:443 substreams.yaml map_transfers -s 385870151 -t 385870157
```

---

## 3. Problems Encountered & Resolutions

### Problem 1: Unauthenticated RPC / Invalid JWT Token
* **Issue:** `substreams run` failed with `Error: rpc error: code = Unauthenticated desc = invalid JWT token`.
* **Cause:** Passing a raw API key directly to `-e` or `--api-key` (which is an unrecognised CLI flag) instead of supplying an authenticated JWT token.
* **Fix:** Used `substreams auth` to exchange the market key for a valid JWT token stored in `SUBSTREAMS_API_TOKEN`.

### Problem 2: WASM Memory Panics (`invalid memory address or nil pointer dereference`)
* **Issue:** Execution aborted during block processing with `execute modules: panic at block #385870151`.
* **Cause:** 
  1. High-level helpers (`tx.resolved_accounts()`) in `substreams-solana` internal crates panicked when attempting to deserialize unresolved Address Lookup Tables (ALTs) in Versioned (v0) transactions.
  2. Direct vector indexing (e.g., `inst.data[0..4]`) caused slice boundary dereference failures inside the WASM sandbox when handling dynamic or malformed instruction buffers.
* **Fix:** Refactored the code to iterate directly over raw `&block.transactions` and protected every slice access using safe option matching (`.get()`, `try_into()`).

### Problem 3: Directory Structure & Code Mismatch
* **Issue:** Updates to `src/lib.rs` appeared to have no effect on remote WASM execution panics.
* **Cause:** The project had a nested subfolder containing an extra `Cargo.toml`. `substreams pack` was bundling an outdated or empty root binary instead of the active subfolder code.
* **Fix:** Consolidated all `proto/`, `src/`, `substreams.yaml`, and `Cargo.toml` files at the single root directory level (`~/solana-transfers-substream`).

---

## 4. Final Result Verification

Upon applying the fixes, the stream successfully processed block ranges `385870151` to `385870156`, reading **21.7 MB** of block payload and outputting JSON-formatted native SOL transfer arrays directly in the console before finishing with **`all done`**.