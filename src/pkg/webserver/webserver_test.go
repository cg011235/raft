package webserver

import (
	"net/http"
	"syscall"
	"testing"
	"time"
)

// Test case:
// 1. Start new webserver at port 8080
// 2. Confirm server started correctly
// 3. Send termination signal for graceful shutdown
// 4. Ensure webserver is stopped gracefully
func TestServerStartAndShutdown(t *testing.T) {
	server := NewServer(8080)

	go server.Start(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Make a test HTTP request to ensure the server has started
	resp, err := http.Get("http://localhost:8080")
	if err != nil {
		t.Fatalf("Failed to make test request: %v", err)
	}
	resp.Body.Close()

	// Simulate receiving a termination signal
	server.sigChan <- syscall.SIGTERM

	// Wait for the server to shutdown
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Server did not shut down in expected time")
	case <-func() chan struct{} {
		c := make(chan struct{})
		go func() {
			server.WaitTillSignaled()
			close(c)
		}()
		return c
	}():
	}
}
