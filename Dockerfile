FROM golang:1.26-trixie AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -o /out/discord-bot ./cmd/discord-bot

FROM gcr.io/distroless/base-debian13
COPY --from=build /out/discord-bot /usr/local/bin/discord-bot
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/discord-bot"]
