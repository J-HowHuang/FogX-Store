# Use the official Rust image as the base image
FROM rust:1.81.0-slim-bullseye

# Install the C++ toolchain
RUN apt-get update && apt-get install -y build-essential
# Install protobuf
RUN apt-get install -y protobuf-compiler
# Set the working directory inside the container
WORKDIR /app

# Copy the Cargo.toml and Cargo.lock files to the container
COPY Cargo.toml Cargo.lock ./

RUN mkdir src && touch src/lib.rs
COPY src/catalog.proto src/build.rs src/cmd.proto ./src/

RUN cargo build --release

# Copy the source code to the container
COPY src ./src

# Build the application
RUN cargo build --release

RUN mkdir -p /app/db

# Set the entrypoint command to run the application
CMD ["./target/release/skulk"]