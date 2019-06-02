SET RUSTFLAGS=-C target-cpu=native -C link-args=/STACK:4194304
SET RUST_BACKTRACE=1
SET RUST_LOG=warcraider=info
SET REPLICAS=1
SET WARC_NUMBER=1
SET OFFSET=0
cargo build --release
cargo run --release
pause