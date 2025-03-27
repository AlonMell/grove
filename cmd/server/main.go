package main

import (
	"bufio"
	"cmp"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/grove/internal/store"
)

type Server[T cmp.Ordered, K store.Serializable] struct {
	db  *store.LSMTree[T, K]
	ctx context.Context
	ln  net.Listener
}

func NewServer[T cmp.Ordered, K store.Serializable](
	ctx context.Context, db *store.LSMTree[T, K], addr string,
) (*Server[T, K], error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Server[T, K]{
		db:  db,
		ctx: ctx,
		ln:  ln,
	}, nil
}

func (s *Server[T, K]) Run() {
	go func() {
		<-s.ctx.Done()
		s.ln.Close()
	}()

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			log.Printf("Accept error: %v", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server[T, K]) handleConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		cmd := scanner.Text()
		parts := strings.SplitN(cmd, " ", 3)

		switch parts[0] {
		case "SET":
			if len(parts) != 3 {
				fmt.Fprintln(conn, "ERR")
				continue
			}
			if err := s.db.Set(parts[1], []byte(parts[2])); err != nil {
				fmt.Fprintln(conn, "ERR")
			} else {
				fmt.Fprintln(conn, "OK")
			}
		case "GET":
			if len(parts) != 2 {
				fmt.Fprintln(conn, "ERR")
				continue
			}
			if val, found := s.db.Get(parts[1]); found {
				fmt.Fprintf(conn, "OK %d\n%s\n", len(val), val)
			} else {
				fmt.Fprintln(conn, "NOT_FOUND")
			}
		default:
			fmt.Fprintln(conn, "UNKNOWN_CMD")
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Инициализация БД
	db, err := store.NewLSMTree[int, string](ctx, "data")
	if err != nil {
		log.Fatal(err)
	}

	// Запуск сервера
	server, err := NewServer[int, string](ctx, db, ":9736")
	if err != nil {
		log.Fatal(err)
	}
	go server.Run()

	// Обработка сигналов
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("Received signal: %v", sig)
	case <-ctx.Done():
	}

	// Graceful shutdown
	log.Println("Initiating shutdown...")
	cancel()

	// Даем время на завершение активных соединений
	time.Sleep(1 * time.Second)

	// Принудительный сброс данных
	log.Println("Flushing remaining data...")
	if err := db.Flush(); err != nil {
		log.Printf("Flush error: %v", err)
	}

	log.Println("Server shutdown complete")
}
