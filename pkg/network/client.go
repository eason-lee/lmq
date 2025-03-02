package network

import (
    "encoding/json"
    "net"
)

type Client struct {
    addr string
    conn net.Conn
}

func NewClient(addr string) (*Client, error) {
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        return nil, err
    }

    return &Client{
        addr: addr,
        conn: conn,
    }, nil
}

func (c *Client) Send(reqType string, payload interface{}) (*Response, error) {
    payloadBytes, err := json.Marshal(payload)
    if err != nil {
        return nil, err
    }

    req := Request{
        Type:    reqType,
        Payload: payloadBytes,
    }

    if err := json.NewEncoder(c.conn).Encode(&req); err != nil {
        return nil, err
    }

    var resp Response
    if err := json.NewDecoder(c.conn).Decode(&resp); err != nil {
        return nil, err
    }

    return &resp, nil
}

func (c *Client) Close() error {
    return c.conn.Close()
}