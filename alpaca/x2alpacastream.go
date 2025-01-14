package alpaca

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"time"
)

// StreamTradeUpdates streams the trade updates of the account. It blocks and keeps calling the handler
// function for each trade update until the context is cancelled.
func (c *Client) StreamTradeUpdates(ctx context.Context, handler func(TradeUpdate)) error {
	var proxyFunc func(*http.Request) (*url.URL, error)
	proxyUrlVal := ctx.Value("proxy")
	if proxyUrlVal != nil {
		proxyFunc = http.ProxyURL(proxyUrlVal.(*url.URL))
	}

	transport := http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, 5*time.Second)
		},
		Proxy: proxyFunc,
	}
	client := http.Client{
		Transport: &transport,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", c.base+"/events/trades", nil)
	if err != nil {
		return err
	}
	if c.credentials.OAuth != "" {
		req.Header.Set("Authorization", "Bearer "+c.credentials.OAuth)
	} else {
		req.Header.Set("APCA-API-KEY-ID", c.credentials.ID)
		req.Header.Set("APCA-API-SECRET-KEY", c.credentials.Secret)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("trade events returned HTTP %s, body: %s", resp.Status, string(body))
	}

	reader := bufio.NewReader(resp.Body)
	for {
		msg, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		const dataPrefix = "data: "
		if !bytes.HasPrefix(msg, []byte(dataPrefix)) {
			continue
		}
		msg = msg[(len(dataPrefix)):]
		var tu TradeUpdate
		if err := json.Unmarshal(msg, &tu); err != nil {
			return err
		}
		handler(tu)
	}
}

// StreamTradeUpdatesInBackground streams the trade updates of the account.
// It runs in the background and keeps calling the handler function for each trade update
// until the context is cancelled. If an error happens it logs it and retries immediately.
func (c *Client) StreamTradeUpdatesInBackground(ctx context.Context, handler func(TradeUpdate)) {
	go func() {
		for {
			err := c.StreamTradeUpdates(ctx, handler)
			if err == nil || errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("alpaca stream trade updates error: %v", err)
		}
	}()
}
