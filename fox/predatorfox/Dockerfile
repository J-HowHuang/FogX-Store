# Use the official Rust image as the base image
FROM rust:1.81.0-slim-bookworm

# Install protobuf
RUN apt-get update && apt-get install -y protobuf-compiler g++ libssl-dev pkg-config wget
WORKDIR /
RUN wget https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.34/grpc_health_probe-linux-amd64 && chmod a+x /grpc_health_probe-linux-amd64
# Set the working directory inside the container
WORKDIR /app

# Copy the Cargo.toml and Cargo.lock files to the container
COPY Cargo.toml Cargo.lock ./

RUN mkdir src && touch src/lib.rs

COPY src/build.rs src/cmd.proto ./src/
RUN cargo build --release

# Copy the source code to the container
COPY src ./src

# Build the application
RUN cargo build --release

RUN mkdir -p /app/data

# Set the entrypoint command to run the application
CMD ["./target/release/predatorfox"]