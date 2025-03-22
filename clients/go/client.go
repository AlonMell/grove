package client

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func Connect(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}, nil
}

func (c *Client) Set(key, value string) error {
	_, err := fmt.Fprintf(c.writer, "SET %s %s\n", key, value)
	if err != nil {
		return err
	}
	c.writer.Flush()

	response, _ := c.reader.ReadString('\n')
	if response != "OK\n" {
		return fmt.Errorf("operation failed")
	}
	return nil
}

func (c *Client) Get(key string) (string, error) {
	_, err := fmt.Fprintf(c.writer, "GET %s\n", key)
	if err != nil {
		return "", err
	}
	c.writer.Flush()

	response, _ := c.reader.ReadString('\n')
	return strings.TrimSpace(response), nil
}

func (c *Client) Close() {
	c.conn.Close()
}
