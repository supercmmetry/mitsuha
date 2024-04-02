# Builder
FROM rust:1.75.0-alpine3.19 as builder

RUN apk add --no-cache musl-dev openssl-libs-static openssl-dev protoc protobuf-dev bash make clang

RUN export CC=clang; \
    export CXX=clang++

ARG CARGO_REGISTRIES_MITSUHA_CENTRAL_FEED_TOKEN
ENV CARGO_REGISTRIES_MITSUHA_CENTRAL_FEED_TOKEN=${CARGO_REGISTRIES_MITSUHA_CENTRAL_FEED_TOKEN}

COPY . /src

WORKDIR /src

RUN make build

# Runner
FROM alpine:3.19 as runner

COPY --from=builder /src/target/release/mitsuha-runtime /bin/mitsuha-runtime

RUN apk add --no-cache openssl protobuf libc6-compat

RUN chmod +x /bin/mitsuha-runtime

CMD [ "/bin/mitsuha-runtime" ]