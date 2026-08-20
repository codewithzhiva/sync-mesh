FROM golang:1.26-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /syncmesh ./cmd/server
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /syncmesh /syncmesh
EXPOSE 8080
ENTRYPOINT ["/syncmesh"]
