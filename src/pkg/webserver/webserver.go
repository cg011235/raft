// Package webserver provides functionality of webserver with simple methods
// to start, gracefully stop, initialise instance.
package webserver

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type Server struct {
	port    int
	srv     *http.Server
	sigChan chan os.Signal
}

// NewServer creates a new Server instance with the given port.
func NewServer(port int) *Server {
	return &Server{
		port:    port,
		sigChan: make(chan os.Signal, 1),
	}
}

// Start initializes the server and begins listening on the specified port.
func (s *Server) Start(router http.Handler) {
	addr := ":" + strconv.Itoa(s.port)
	s.srv = &http.Server{
		Addr:    addr,
		Handler: router,
	}

	signal.Notify(s.sigChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()
	log.Printf("Server started on port %d", s.port)
}

// WaitTillSignaled waits for a termination signal and then gracefully shuts down the server.
func (s *Server) WaitTillSignaled() {
	<-s.sigChan
	log.Print("Stopping server")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.srv.Shutdown(ctx); err != nil {
		log.Printf("Server shutdown failed: %+v", err)
	} else {
		log.Print("Server exited gracefully")
	}
}
