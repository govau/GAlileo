#!/bin/bash

export RUST_LOG="warcraider=info"
export RUST_BACKTRACE=1 
export RUSTFLAGS="-C target-cpu=native -C link-args=-Wl,-zstack-size=4194304"
export REPLICAS=8
export OFFSET=3
for ((i=1;i<=8;i++)); 
do 
REPLICA=$i nohup ./target/debug/warcraider > $i.log 2>&1
done
