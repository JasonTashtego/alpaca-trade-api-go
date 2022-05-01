package alpaca

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/common"
	"github.com/gorilla/websocket"
)

const (
	TradeUpdates   = "trade_updates"
	AccountUpdates = "account_updates"
)

const (
	MaxConnectionAttempts = 3
)

//var (
//	once      sync.Once
//	str       *Stream
//	streamUrl = ""
//
//	baseStream = ""
//
//	dataOnce sync.Once
//	dataStr  *Stream
//)

type Stream struct {
	sync.Mutex
	sync.Once
	conn                  *websocket.Conn
	authenticated, closed atomic.Value
	handlers              sync.Map

	apiKey *common.APIKey

	baseUrl string
}

// Subscribe to the specified Alpaca stream channel.
func (s *Stream) Subscribe(apiKey *common.APIKey, channel string, handler func(msg interface{})) (err error) {
	s.apiKey = apiKey
	switch {
	case channel == TradeUpdates:
		fallthrough
	case channel == AccountUpdates:
		fallthrough
	case strings.HasPrefix(channel, "Q."):
		fallthrough
	case strings.HasPrefix(channel, "T."):
		fallthrough
	case strings.HasPrefix(channel, "AM."):
	default:
		err = fmt.Errorf("invalid stream (%s)", channel)
		return
	}
	if s.conn == nil {
		s.conn, err = s.openSocket()
		if err != nil {
			return
		}
	}

	if err = s.auth(); err != nil {
		return
	}
	s.Do(func() {
		go s.wrappedStartStream()
	})

	s.handlers.Store(channel, handler)

	if err = s.sub(channel); err != nil {
		s.handlers.Delete(channel)
		return
	}
	return
}

// Unsubscribe the specified Polygon stream channel.
func (s *Stream) Unsubscribe(channel string) (err error) {
	if s.conn == nil {
		err = errors.New("not yet subscribed to any channel")
		return
	}

	if err = s.auth(); err != nil {
		return
	}

	s.handlers.Delete(channel)

	err = s.unsub(channel)

	return
}

// Close gracefully closes the Alpaca stream.
func (s *Stream) Close() error {
	s.Lock()
	defer s.Unlock()

	if s.conn == nil {
		return nil
	}

	if err := s.conn.WriteMessage(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
	); err != nil {
		return err
	}

	// so we know it was gracefully closed
	s.closed.Store(true)

	return s.conn.Close()
}

func (s *Stream) reconnect() error {
	s.authenticated.Store(false)
	conn, err := s.openSocket()
	if err != nil {
		return err
	}
	s.conn = conn
	if err := s.auth(); err != nil {
		return err
	}
	s.handlers.Range(func(key, value interface{}) bool {
		// there should be no errors if we've previously successfully connected
		_ = s.sub(key.(string))
		return true
	})
	return nil
}

func (s *Stream) findHandler(stream string) func(interface{}) {
	if v, ok := s.handlers.Load(stream); ok {
		return v.(func(interface{}))
	}
	if strings.HasPrefix(stream, "Q.") ||
		strings.HasPrefix(stream, "T.") ||
		strings.HasPrefix(stream, "AM.") {
		c := stream[:strings.Index(stream, ".")]
		if v, ok := s.handlers.Load(c + ".*"); ok {
			return v.(func(interface{}))
		}
	}
	return nil
}

func (s *Stream) wrappedStartStream() {

	var retryBackoff = 5
	var ctr = 1
	for {
		log.Printf("alpaca stream read")
		reconn := s.startWithRecover()

		if s.closed.Load().(bool) {
			log.Printf("alpaca stream closed")
			break
		}

		if reconn {
			// reconnect loop
			for {
				time.Sleep( time.Duration(retryBackoff * ctr) * time.Second)

				log.Printf("alpaca stream reconnect")
				if err := s.reconnect(); err != nil {
					log.Printf(err.Error())
				} else {
					break
				}
				ctr++

				// 2-minute max
				if ctr > 24 {
					ctr = 24
				}
			}
		}
	}
}

func (s *Stream) startWithRecover() bool {

	defer func() {

		if s.conn != nil {
			_ = s.conn.Close()
			s.conn = nil
		}
		if err := recover(); err != nil {
			//stack := make([]byte, 8192)
			//stack = stack[:runtime.Stack(stack, false)]
			//
			//var strStack = string(stack)
			//var stackLines = strings.Split(strStack, "\n")
			//if len(stackLines) > 0 {
			//	for _, l := range stackLines {
			//		var l = strings.TrimSpace(l)
			//		if len(l) == 0 {
			//			break
			//		}
			//	}
			//}
			//
			//strStack = strings.ReplaceAll(strStack, "\n", "\r\n")
			//log.Printf(strStack)
			log.Printf("alpaca stream disconnected.")
		}
	}()

	return s.start()
}


func (s *Stream) start() bool {
	for {
		msg := ServerMsg{}

		if err := s.conn.ReadJSON(&msg); err == nil {
			handler := s.findHandler(msg.Stream)
			if handler != nil {
				msgBytes, _ := json.Marshal(msg.Data)
				switch {
				case msg.Stream == TradeUpdates:
					var tradeupdate TradeUpdate
					err := json.Unmarshal(msgBytes, &tradeupdate)
					if err != nil {
						continue
					}
					handler(tradeupdate)
				case strings.HasPrefix(msg.Stream, "Q."):
					var quote StreamQuote
					err := json.Unmarshal(msgBytes, &quote)
					if err != nil {
						continue
					}
					handler(quote)
				case strings.HasPrefix(msg.Stream, "T."):
					var trade StreamTrade
					err := json.Unmarshal(msgBytes, &trade)
					if err != nil {
						continue
					}
					handler(trade)
				case strings.HasPrefix(msg.Stream, "AM."):
					var agg StreamAgg
					err := json.Unmarshal(msgBytes, &agg)
					if err != nil {
						continue
					}
					handler(agg)

				default:
					handler(msg.Data)
				}
			}
		} else {
			if websocket.IsCloseError(err) {
				// if this was a graceful closure, don't reconnect
				if s.closed.Load().(bool) {
					return false
				}
			} else {
				log.Printf("alpaca stream read error (%v)", err)
			}

			if s.closed.Load().(bool) {
				return false
			}

			err := s.reconnect()
			if err != nil {
				return true
			} else {
				log.Printf("alpaca stream reconnected")
			}
		}
	}
}

func (s *Stream) sub(channel string) (err error) {
	s.Lock()
	defer s.Unlock()

	subReq := ClientMsg{
		Action: "listen",
		Data: map[string]interface{}{
			"streams": []interface{}{
				channel,
			},
		},
	}

	if err = s.conn.WriteJSON(subReq); err != nil {
		return
	}

	return
}

func (s *Stream) unsub(channel string) (err error) {
	s.Lock()
	defer s.Unlock()

	subReq := ClientMsg{
		Action: "unlisten",
		Data: map[string]interface{}{
			"streams": []interface{}{
				channel,
			},
		},
	}

	if err = s.conn.WriteJSON(subReq); err != nil {
		return
	}

	return
}

func (s *Stream) isAuthenticated() bool {
	return s.authenticated.Load().(bool)
}

func (s *Stream) auth() (err error) {
	s.Lock()
	defer s.Unlock()

	if s.isAuthenticated() {
		return
	}

	var authRequest ClientMsg
	if s.apiKey.OAuth != "" {
		authRequest = ClientMsg{
			Action: "authenticate",
			Data: map[string]interface{}{
				"oauth_token": s.apiKey.OAuth,
			},
		}
	} else {
		authRequest = ClientMsg{
			Action: "authenticate",
			Data: map[string]interface{}{
				"key_id":     s.apiKey.ID,
				"secret_key": s.apiKey.Secret,
			},
		}
	}

	if err = s.conn.WriteJSON(authRequest); err != nil {
		return
	}

	msg := ServerMsg{}

	// ensure the auth response comes in a timely manner
	_ = s.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer func() {
		_ = s.conn.SetReadDeadline(time.Time{})
	}()

	if err = s.conn.ReadJSON(&msg); err != nil {
		return
	}

	m := msg.Data.(map[string]interface{})

	if !strings.EqualFold(m["status"].(string), "authorized") {
		return fmt.Errorf("failed to authorize alpaca stream")
	}

	s.authenticated.Store(true)

	return
}

//// GetStream returns the singleton Alpaca stream structure.
//func GetStream() *Stream {
//	once.Do(func() {
//		str = &Stream{
//			authenticated: atomic.Value{},
//			handlers:      sync.Map{},
//			base:          baseStream,
//		}
//
//		str.authenticated.Store(false)
//		str.closed.Store(false)
//	})
//
//	return str
//}
//
//func GetDataStream() *Stream {
//	dataOnce.Do(func() {
//		if s := os.Getenv("DATA_PROXY_WS"); s != "" {
//			streamUrl = s
//		} else {
//			streamUrl = dataURL
//		}
//		dataStr = &Stream{
//			authenticated: atomic.Value{},
//			handlers:      sync.Map{},
//			base:          streamUrl,
//		}
//
//		dataStr.authenticated.Store(false)
//		dataStr.closed.Store(false)
//	})
//
//	return dataStr
//}

func (s *Stream) openSocket() (*websocket.Conn, error) {
	scheme := "wss"
	ub, _ := url.Parse(s.baseUrl)
	if ub.Scheme == "http" {
		scheme = "ws"
	}
	u := url.URL{Scheme: scheme, Host: ub.Host, Path: "/stream"}
	connectionAttempts := 0
	for connectionAttempts < MaxConnectionAttempts {
		connectionAttempts++
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err == nil {
			return c, nil
		}
		if connectionAttempts == MaxConnectionAttempts {
			return nil, err
		}
		time.Sleep(1 * time.Second)
	}
	return nil, fmt.Errorf("Error: Could not open Alpaca stream (max retries exceeded).")
}

func (s *Stream) SetBaseStream(sUrl string) {
	s.baseUrl = sUrl
}

// get new alpaca stream
func NewAlpacaStream(base string) *Stream {
	str := &Stream{
		authenticated: atomic.Value{},
		handlers:      sync.Map{},
		baseUrl:       base,
	}
	str.authenticated.Store(false)
	str.closed.Store(false)

	return str
}
