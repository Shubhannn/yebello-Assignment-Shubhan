Solana Native Transfers Substream Indexer
A high-performance, real-time Substreams indexer written in Rust for the Solana blockchain. This project processes raw mainnet blocks, filters for native SOL System Program transfers, and transforms raw instruction buffers into structured Protobuf and JSON payloads suitable for database ingestion or event-driven applications.

🌟 Architecture Overview
                      ┌───────────────────────────────┐
                      │    Solana Mainnet Network     │
                      └───────────────┬───────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │ StreamingFast gRPC Endpoint   │
                      └───────────────┬───────────────┘
                                      │ (sf.solana.type.v1.Block)
                                      ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                      Substreams WASM Sandbox                          │
  │                                                                       │
  │  ┌─────────────────────────────────────────────────────────────────┐  │
  │  │                     `map_transfers` Handler                     │  │
  │  │                                                                 │  │
  │  │  1. Iterates `block.transactions` safely                          │  │
  │  │  2. Filters for System Program (`111111111111111111111111111111`)│  │
  │  │  3. Validates Type 2 (Native Transfer) instruction data          │  │
  │  │  4. Decodes sender, receiver, and lamport amounts into Protobuf   │  │
  │  └─────────────────────────────────────────────────────────────────┘  │
  └───────────────────────────────────┬───────────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │ Structured Output Feed (JSON) │
                      │  or PostgreSQL (`sink-sql`)   │
                      └───────────────┬───────────────┘

🛠️ Key Features & Technical Design
Memory-Safe WASM Boundary: Built with defensive Rust bounds checking (.get(), .try_into()) to guarantee execution stability without memory address panics on Versioned (v0) transactions.
Zero-Copy Iteration: Iterates directly on raw transaction slices (&block.transactions) and instruction vectors, bypassing high-level lookup table helpers that can cause WASM dereference issues.
Structured Schema: Uses Protocol Buffers (proto/transfers.proto) compiled at build time via prost-build for type-safe data serialization.
Database Sink Integration: Fully compatible with substreams-sink-sql / substreams-sink-postgres for automated relational database insertion.
🚀 Prerequisites
Before building and running this project, ensure you have installed:

Rust & Cargo: wasm32-unknown-unknown target enabled (rustup target add wasm32-unknown-unknown)
Substreams CLI: v1.0.0+
Protobuf Compiler (protoc): Required for building Rust Protobuf bindings
Buf CLI: v1.30.0+ installed in system PATH
WSL2 / Linux Environment: Recommended for Linux binary builds and gRPC streaming
📦 Project Structure
solana-transfers-substream/
├── Cargo.toml               # Cargo package setup & WASM cdylib config
├── build.rs                 # Prost Protobuf build script
├── substreams.yaml          # Substreams manifest & module configuration
├── schema.sql               # PostgreSQL database schema setup
├── proto/
│   └── transfers.proto      # Protocol Buffer payload schema
└── src/
    └── lib.rs               # Core Rust map handler & parsing logic

⚙️ Building & Packaging
Compile the WASM Binary:

cargo build --target wasm32-unknown-unknown --release

Package the Substream Bundle (.spkg):

substreams pack

🔑 Authentication
Obtain an API key from StreamingFast or Pinax, and authenticate via the CLI to set your JWT token:

substreams auth

🏃 Running the Indexer
Run the stream against Solana Mainnet for a specific block range:

substreams run -e mainnet.sol.streamingfast.io:443 \
  substreams.yaml map_transfers \
  -s 385870151 -t 385870157

To output raw JSON directly into a file:

substreams run -e mainnet.sol.streamingfast.io:443 \
  substreams.yaml map_transfers \
  -s 385870151 -t 385870157 \
  -o json > transfers.json

🗄️ PostgreSQL Database Sink Integration
To persist streaming block data continuously into a local or remote PostgreSQL database:

1. Initialize PostgreSQL Database
CREATE DATABASE solana_indexer;
CREATE USER substreams WITH PASSWORD 'password123';
GRANT ALL PRIVILEGES ON DATABASE solana_indexer TO substreams;

2. Define Schema (schema.sql)
CREATE TABLE IF NOT EXISTS transfers (
    id VARCHAR(64) PRIMARY KEY,
    block_number BIGINT NOT NULL,
    from_address VARCHAR(44) NOT NULL,
    to_address VARCHAR(44) NOT NULL,
    amount_lamports BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

3. Run the SQL Sink
export DSN="psql://substreams:password123@localhost:5432/solana_indexer?sslmode=disable"

# Setup table cursor tracking
substreams-sink-sql setup "$DSN" substreams.yaml

# Run the live pipeline
substreams-sink-sql run "$DSN" substreams.yaml \
  -e mainnet.sol.streamingfast.io:443 \
  -s 385870151 -t 385870157

📊 Sample JSON Output
{
  "@module": "map_transfers",
  "@block": 385870151,
  "@data": {
    "transfers": [
      {
        "from": "FLiPtS5X5N8FxtWQrqYZASv7guoAsjyusLfv4ZbHqrdj",
        "to": "FLiPgesZ6ZkLgdXgQGaGwoNP2Pjvbma3NabMATa6wUvf",
        "amount": "100000000"
      },
      {
        "from": "5q4mrrpUW2spGJpTV7jv4TxqZCJYf8pJUit8Wxbhhjtz",
        "to": "CFY9cNrA4bYmvLtv3Fy2frnWdsS876EmVJiPAizXsdYJ",
        "amount": "1500000000"
      }
    ]
  }
}

🤝 License
Distributed under the MIT License.
