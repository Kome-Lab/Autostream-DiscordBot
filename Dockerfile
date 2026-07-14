FROM golang:1.26.5-trixie AS build
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -o /out/autostream-discord-bot -ldflags="-s -w -X github.com/example/autostream-discord-bot/internal/version.Version=${VERSION} -X github.com/example/autostream-discord-bot/internal/version.Commit=${COMMIT} -X github.com/example/autostream-discord-bot/internal/version.BuildDate=${BUILD_DATE}" ./cmd/discord-bot

FROM gcr.io/distroless/base-debian13
COPY --from=build /out/autostream-discord-bot /usr/local/bin/autostream-discord-bot
COPY --from=build /out/autostream-discord-bot /usr/local/bin/discord-bot
ENV AUTOSTREAM_NODE_CONFIG=/etc/autostream-discord-bot/config.yml
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/autostream-discord-bot"]
