# select build image
FROM rust:1.35 as build

# create a new empty shell project
RUN USER=root cargo new --bin warcraider
WORKDIR /warcraider

# copy over your manifests
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# this build step will cache your dependencies
RUN cargo build --release
RUN rm src/*.rs

# copy your source tree
COPY ./src ./src

# build for release
RUN rm ./target/release/deps/warcraider*
RUN RUSTFLAGS="-C target-cpu=native -C link-args=-Wl,-zstack-size=4194304" cargo build --release

# our final base
FROM google/cloud-sdk:alpine

# copy the build artifact from the build stage
COPY --from=build /warcraider/target/release/warcraider .
ARG RUST_LOG=warcraider=info
ENV RUST_LOG $RUST_LOG
# set the startup command to run your binary
CMD ["./warcraider"]
