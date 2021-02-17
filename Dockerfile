FROM crystallang/crystal:0.36.1-alpine AS builder
WORKDIR /tmp

# Copying and install dependencies
COPY shard.yml shard.lock ./
RUN shards install --production

# Copying the rest of the code
COPY ./src ./src

# Build
RUN shards build --production --release --static
RUN strip bin/*

# start from scratch and only copy the built binary
FROM scratch
USER 2:2
COPY --from=builder /etc/ssl/cert.pem /etc/ssl/
COPY --from=builder /tmp/bin/ /
ENTRYPOINT ["/rmqrecover"]
