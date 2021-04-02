# mostly copied from https://alexbrand.dev/post/how-to-package-rust-applications-into-minimal-docker-containers/
FROM rust:1.51.0 AS build
WORKDIR /usr/src

RUN USER=root apt update && apt install -y musl-tools libssl-dev

# Create a dummy project and build the app's dependencies.
# If the Cargo.toml or Cargo.lock files have not changed,
# we can use the docker build cache and skip these (typically slow) steps.
RUN USER=root cargo new rust-kafka-consumer-example
WORKDIR /usr/src/rust-kafka-consumer-example
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release

# Copy the source and build the application.
COPY src ./src
RUN cargo install --path .

# Copy the statically-linked binary into a scratch container.
FROM scratch
COPY --from=build /usr/local/cargo/bin/rust-kafka-consumer-example .
USER 1000
CMD ["./rust-kafka-consumer-example"]