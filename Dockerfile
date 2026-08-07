FROM golang:1.26.5-trixie AS build
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
WORKDIR /src
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates cmake ninja-build g++ make pkg-config autoconf automake \
    libtool zip unzip tar \
  && rm -rf /var/lib/apt/lists/*
COPY go.mod go.sum ./
COPY third_party ./third_party
RUN go mod download
COPY . .
RUN make -C third_party/discordgo/dave/libdave/cpp BUILD_TYPE=Release
RUN set -eu; \
  libdave_build="$PWD/third_party/discordgo/dave/libdave/cpp/build"; \
  vcpkg_libdir="$(find "$libdave_build/vcpkg_installed" -maxdepth 2 -type d -name lib -print -quit)"; \
  test -n "$vcpkg_libdir"; \
  vcpkg_libs="$(find "$vcpkg_libdir" -maxdepth 1 -type f -name '*.a' -print | sort | tr '\n' ' ')"; \
  CGO_ENABLED=1 \
  CGO_CFLAGS="-I$PWD/third_party/discordgo/dave/libdave/cpp/includes" \
  CGO_LDFLAGS="-L$vcpkg_libdir $libdave_build/libdave.a -Wl,--start-group $vcpkg_libs -Wl,--end-group -lstdc++ -lm -ldl -lpthread" \
    go build -trimpath -o /out/autostream-discord-bot \
      -ldflags="-s -w -X github.com/example/autostream-discord-bot/internal/version.Version=${VERSION} -X github.com/example/autostream-discord-bot/internal/version.Commit=${COMMIT} -X github.com/example/autostream-discord-bot/internal/version.BuildDate=${BUILD_DATE}" \
      ./cmd/discord-bot

FROM gcr.io/distroless/base-debian13
COPY --from=build /out/autostream-discord-bot /usr/local/bin/autostream-discord-bot
COPY --from=build /out/autostream-discord-bot /usr/local/bin/discord-bot
ENV AUTOSTREAM_NODE_CONFIG=/etc/autostream-discord-bot/config.yml
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/autostream-discord-bot"]
