FROM rust:latest
WORKDIR /usr/src/log_aggregator
COPY . .
RUN cargo build --release
EXPOSE 8125/udp
ENTRYPOINT ["./target/release/log_aggregator"]