package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/codewithzhiva/sync-mesh/internal/syncengine"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	engine, err := syncengine.Open(env("JOURNAL_PATH", "data/syncmesh.wal"))
	if err != nil {
		log.Error("open engine", "error", err)
		os.Exit(1)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { respond(w, 200, map[string]string{"status": "ready"}) })
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "syncmesh_mutations_total %d\n", engine.Snapshot().Cursor)
	})
	mux.HandleFunc("/v1/mutations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		var mutation syncengine.Mutation
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&mutation); err != nil {
			respond(w, 400, map[string]string{"error": "invalid mutation"})
			return
		}
		applied, created, err := engine.Apply(mutation)
		if err != nil {
			respond(w, 422, map[string]string{"error": err.Error()})
			return
		}
		status := 201
		if !created {
			status = 200
		}
		respond(w, status, map[string]any{"created": created, "mutation": applied})
	})
	mux.HandleFunc("/v1/sync", func(w http.ResponseWriter, r *http.Request) {
		cursor, _ := strconv.ParseUint(r.URL.Query().Get("cursor"), 10, 64)
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit < 1 || limit > 1000 {
			limit = 200
		}
		changes, next := engine.ChangesSince(cursor, limit)
		respond(w, 200, map[string]any{"changes": changes, "next_cursor": next})
	})
	mux.HandleFunc("/v1/snapshot", func(w http.ResponseWriter, _ *http.Request) { respond(w, 200, engine.Snapshot()) })
	mux.HandleFunc("/v1/sync/ws", func(w http.ResponseWriter, r *http.Request) { serveWebSocket(w, r, engine) })
	server := &http.Server{Addr: env("LISTEN_ADDR", ":8080"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("server listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func serveWebSocket(w http.ResponseWriter, r *http.Request, engine *syncengine.Engine) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(w, "websocket upgrade required", 426)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "websocket unsupported", 500)
		return
	}
	connection, buffer, err := hijacker.Hijack()
	if err != nil {
		return
	}
	defer connection.Close()
	accept := websocketAccept(r.Header.Get("Sec-WebSocket-Key"))
	fmt.Fprintf(buffer, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept)
	_ = buffer.Flush()
	cursor, _ := strconv.ParseUint(r.URL.Query().Get("cursor"), 10, 64)
	changes, next := engine.ChangesSince(cursor, 1000)
	payload, _ := json.Marshal(map[string]any{"changes": changes, "next_cursor": next})
	_ = writeTextFrame(buffer, payload)
	_ = buffer.Flush()
}
func websocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}
func writeTextFrame(writer *bufio.ReadWriter, payload []byte) error {
	header := []byte{0x81}
	switch {
	case len(payload) < 126:
		header = append(header, byte(len(payload)))
	case len(payload) <= 65535:
		header = append(header, 126, 0, 0)
		binary.BigEndian.PutUint16(header[2:], uint16(len(payload)))
	default:
		header = append(header, 127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(header[2:], uint64(len(payload)))
	}
	if _, err := writer.Write(header); err != nil {
		return err
	}
	_, err := writer.Write(payload)
	return err
}
func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
