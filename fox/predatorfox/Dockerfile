# Use the official Rust image as the base image
FROM rust:1.81.0-slim-bullseye

# Install protobuf
RUN apt-get update && apt-get install -y protobuf-compiler
# Set the working directory inside the container
WORKDIR /app

# Copy the Cargo.toml and Cargo.lock files to the container
COPY Cargo.toml Cargo.lock ./

# Copy the source code to the container
COPY src ./src

# Build the application
RUN cargo build --release

RUN mkdir -p /app/db

# Set the entrypoint command to run the application
CMD ["./target/release/predatorfox"]