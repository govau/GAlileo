SET RUSTFLAGS=-C target-cpu=native 
SET RUST_BACKTRACE=1
SET RUST_LOG=warcraider=info
SET REPLICAS=4
SET WARC_NUMBER=4
SET OFFSET=0
cargo build --release
cargo run --release
