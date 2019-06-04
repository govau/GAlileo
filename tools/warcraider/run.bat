SET RUSTFLAGS=-C target-cpu=native -C link-args=/STACK:4194304
SET RUST_BACKTRACE=1
SET RUST_LOG=warcraider=info
SET REPLICAS=8
SET OFFSET=1
cargo run --release %*
pause