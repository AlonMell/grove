package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/grove/internal/store"
)

type Server struct {
	db *store.LSMTree
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
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
	db, err := store.NewLSMTree("data")
	if err != nil {
		panic(err)
	}

	server := &Server{db: db}
	ln, _ := net.Listen("tcp", ":9736")
	defer ln.Close()

	for {
		conn, _ := ln.Accept()
		go server.handleConn(conn)
	}
}
