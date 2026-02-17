package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"
)

func run(ctx context.Context, stdout, stderr io.Writer) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	cfg := Config{
		Host:         "",
		Port:         envOr("JUSTDROP_SIGNALING_PORT", "9000"),
		TURNPort:     envOrInt("JUSTDROP_TURN_PORT", 3478),
		TURNRealm:    envOr("JUSTDROP_TURN_REALM", "justdrop"),
		TURNSecret:   envOr("JUSTDROP_TURN_SECRET", "change-me-in-production"),
		PublicIP:     resolvePublicIP(),
		RelayPortMin: envOrInt("JUSTDROP_TURN_RELAY_MIN", 49152),
		RelayPortMax: envOrInt("JUSTDROP_TURN_RELAY_MAX", 65535),
	}
	hub := NewHub()
	wsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		websocketHandler(&cfg, hub, w, r)
	})

	srv := NewServer(&cfg, wsHandler)

	httpServer := &http.Server{
		Addr:    net.JoinHostPort(cfg.Host, cfg.Port),
		Handler: srv,
	}

	fmt.Fprintf(stdout, "Signaling server starting on :%s\n", cfg.Port)
	fmt.Fprintf(stdout, "TURN server running on UDP+TCP :%d (publicIP=%s)\n", cfg.TURNPort, cfg.PublicIP)

	go func() {
		startTURN(&cfg)
	}()

	go func() {
		log.Printf("Listening on %s\n", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(stderr, "error listening and serving: %s\n", err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // waitgroup.go??? mozda im
		defer wg.Done()
		<-ctx.Done()

		shutdownCtx := context.Background()
		shutdownCtx, cancel := context.WithTimeout(shutdownCtx, 10*time.Second)
		defer cancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(stderr, "error shutting down http server: %s\n", err)
		}
	}()

	wg.Wait()

	return nil

}

func main() {
	ctx := context.Background()
	if err := run(ctx, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

}

func envOr(key string, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}

func envOrInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	val, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("Invalid %s=%q, using %d", key, raw, fallback)
		return fallback
	}
	return val
}

func resolvePublicIP() string {
	if value := os.Getenv("JUSTDROP_PUBLIC_IP"); value != "" {
		return value
	}

	// Best-effort outbound interface discovery for local development.
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Printf("Could not auto-detect public IP, falling back to 127.0.0.1. Set JUSTDROP_PUBLIC_IP in production.")
		return "127.0.0.1"
	}
	defer conn.Close()

	udpAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || udpAddr.IP == nil {
		log.Printf("Could not parse detected IP, falling back to 127.0.0.1. Set JUSTDROP_PUBLIC_IP in production.")
		return "127.0.0.1"
	}
	return udpAddr.IP.String()
}
