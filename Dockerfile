FROM rust:1.51.0 AS build
WORKDIR /usr/src/
RUN apt-get update; apt-get upgrade -y; apt-get install -y musl-tools
RUN rustup target add x86_64-unknown-linux-musl
WORKDIR /usr/src
COPY Cargo.toml Cargo.lock ./

COPY ./ca-certificate.crt .
COPY src ./src
RUN cargo install --target x86_64-unknown-linux-musl --path .

FROM alpine
COPY --from=build /usr/local/cargo/bin/rust-kafka-consumer-example .
CMD ["./rust-kafka-consumer-example"]
