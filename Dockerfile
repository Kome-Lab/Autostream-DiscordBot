FROM golang:1.26-trixie AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -o /out/autostream-discord-bot ./cmd/discord-bot

FROM gcr.io/distroless/base-debian13
COPY --from=build /out/autostream-discord-bot /usr/local/bin/autostream-discord-bot
COPY --from=build /out/autostream-discord-bot /usr/local/bin/discord-bot
ENV AUTOSTREAM_NODE_CONFIG=/etc/autostream-node/config.yml
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/autostream-discord-bot"]
