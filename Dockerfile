# Builder
FROM rust:1.71.1-alpine3.17 as builder

COPY . /src

WORKDIR /src

RUN apk add --no-cache musl-dev openssl-dev protoc protobuf-dev bash make
RUN make build

# Runner
FROM alpine:3.17 as runner

COPY --from=builder /src/target/release/mitsuha-runtime /bin/mitsuha-runtime

RUN apk add --no-cache openssl protobuf libc6-compat
RUN chmod +x /bin/mitsuha-runtime

CMD [ "/bin/mitsuha-runtime" ]