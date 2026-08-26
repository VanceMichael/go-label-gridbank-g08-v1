FROM golang:1.26-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV GOTOOLCHAIN=local CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/gridbank ./cmd/server
RUN mkdir -p /out/data && chown 65532:65532 /out/data

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/gridbank /app/gridbank
COPY --chown=65532:65532 --from=build /out/data /data
ENV GRIDBANK_ADDRESS=:8080 GRIDBANK_DATABASE_PATH=/data/gridbank.db
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/app/gridbank"]
