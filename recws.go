// Package recws provides websocket client based on gorilla/websocket
// that will automatically reconnect if the connection is dropped.
package recws

import (
	"crypto/tls"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jpillora/backoff"
)

// ErrNotConnected is returned when the application read/writes
// a message and the connection is closed
var ErrNotConnected = errors.New("websocket: not connected")

// The RecConn type represents a Reconnecting WebSocket connection.
type RecConn struct {
	// Configurable options

	// recIntervalMin specifies the initial reconnecting interval,
	// defaults to 2 seconds.
	recIntervalMin time.Duration
	// recIntervalMax specifies the maximum reconnecting interval,
	// defaults to 30 seconds.
	recIntervalMax time.Duration
	// recIntervalFactor specifies the rate of increase of the reconnection,
	// interval, default to 1.5.
	recIntervalFactor float64
	// handshakeTimeout specifies the duration for the handshake to complete,
	// defaults to 2 seconds.
	handshakeTimeout time.Duration
	// proxy specifies the proxy function for the dialer,
	// defaults to ProxyFromEnvironment.
	proxy func(*http.Request) (*url.URL, error)
	// Client TLS config to use.
	tlsClientConfig *tls.Config
	// subscribeHandler fires after the connection successfully establish.
	subscribeHandler func() error
	// keepAliveTimeout is an interval for sending ping/pong messages,
	// disabled if 0.
	keepAliveTimeout time.Duration
	// writeTimeout is a duration, after which write operations get canceled,
	// defaults to 3 seconds.
	writeTimeout time.Duration
	// verbose shows connecting/reconnecting messages,
	// defaults to false.
	verbose bool

	// Internal options
	isConnected  bool
	termChan     chan TerminationReason
	shutDownChan chan struct{}
	mu           sync.RWMutex
	url          string
	reqHeader    http.Header
	httpResp     *http.Response
	dialErr      error
	dialer       *websocket.Dialer

	*websocket.Conn
}

const (
	reasonClose TerminationReason = iota
	reasonCloseAndReconnect
)

type (
	TerminationReason int
	RecConnOption     func(conn *RecConn)
)

func New(opts ...RecConnOption) *RecConn {
	// Create RecConn with default values.
	rc := &RecConn{
		recIntervalMin:    2 * time.Second,
		recIntervalMax:    30 * time.Second,
		recIntervalFactor: 1.5,
		handshakeTimeout:  2 * time.Second,
		proxy:             http.ProxyFromEnvironment,
		tlsClientConfig:   nil,
		subscribeHandler:  nil,
		keepAliveTimeout:  0,
		writeTimeout:      3 * time.Second,
		verbose:           false,
	}

	// Apply all existing functional options.
	for _, opt := range opts {
		opt(rc)
	}

	// Configure the dialer
	rc.dialer = &websocket.Dialer{
		HandshakeTimeout: rc.handshakeTimeout,
		Proxy:            rc.proxy,
		TLSClientConfig:  rc.tlsClientConfig,
	}

	return rc
}

////////////////////////
// Functional Options //
////////////////////////

// WithReconnectInterval configures the reconnection interval.
func WithReconnectInterval(recIntervalMin, recIntervalMax time.Duration, recIntervalFactor float64) RecConnOption {
	return func(rc *RecConn) {
		rc.recIntervalMin = recIntervalMin
		rc.recIntervalMax = recIntervalMax
		rc.recIntervalFactor = recIntervalFactor
	}
}

// WithHandshakeTimeout configures the handshake timeout.
func WithHandshakeTimeout(timeout time.Duration) RecConnOption {
	return func(rc *RecConn) {
		rc.handshakeTimeout = timeout
	}
}

// WithKeepAliveTimeout configures the keep alive timeout.
func WithKeepAliveTimeout(timeout time.Duration) RecConnOption {
	return func(rc *RecConn) {
		rc.keepAliveTimeout = timeout
	}
}

// WithWriteTimeout configures the websocket write timeout.
func WithWriteTimeout(timeout time.Duration) RecConnOption {
	return func(rc *RecConn) {
		rc.writeTimeout = timeout
	}
}

// WithProxy configures the websocket proxy.
func WithProxy(proxy func(*http.Request) (*url.URL, error)) RecConnOption {
	return func(rc *RecConn) {
		rc.proxy = proxy
	}
}

// WithTLSConfig configures the TLSConfig to be used on the websocket connection.
func WithTLSConfig(tlsConfig *tls.Config) RecConnOption {
	return func(rc *RecConn) {
		rc.tlsClientConfig = tlsConfig
	}
}

// WithSubscribeHandler configures the subscribe-handler function to be called after successful websocket connection.
func WithSubscribeHandler(handler func() error) RecConnOption {
	return func(rc *RecConn) {
		rc.subscribeHandler = handler
	}
}

// WithVerbose configures the visibility of connecting/reconnecting messages.
func WithVerbose() RecConnOption {
	return func(rc *RecConn) {
		rc.verbose = true
	}
}

////////////////////
// Public methods //
////////////////////

// Dial creates a new client connection.
// The URL url specifies the host and request URI. Use requestHeader to specify
// the origin (Origin), subprotocols (Sec-WebSocket-Protocol) and cookies
// (Cookie). Use GetHTTPResponse() method for the response. Header to get
// the selected subprotocol (Sec-WebSocket-Protocol) and cookies (Set-Cookie).
func (rc *RecConn) Dial(urlStr string, reqHeader http.Header) {
	urlStr, err := rc.parseURL(urlStr)

	if err != nil {
		log.Fatalf("Dial: %v", err)
	}

	if rc.IsConnected() {
		log.Printf("Dial: already connected with %s", rc.GetURL())
		return
	}

	// Config
	rc.setURL(urlStr)
	rc.setReqHeader(reqHeader)

	// Connect
	go rc.connect()

	// Wait on first attempt
	time.Sleep(rc.getHandshakeTimeout())
}

// GetURL returns current connection url
func (rc *RecConn) GetURL() string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.url
}

// GetHTTPResponse returns the http response from the handshake.
// Useful when WebSocket handshake fails,
// so that callers can handle redirects, authentication, etc.
func (rc *RecConn) GetHTTPResponse() *http.Response {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.httpResp
}

// GetDialError returns the last dialer error.
// nil on successful connection.
func (rc *RecConn) GetDialError() error {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.dialErr
}

// IsConnected returns the WebSocket connection state
func (rc *RecConn) IsConnected() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.isConnected
}

// Close closes the underlying network connection without
// sending or waiting for a close frame.
func (rc *RecConn) Close() {
	rc.termChan <- reasonClose
}

// CloseAndReconnect will try to reconnect.
func (rc *RecConn) CloseAndReconnect() {
	rc.termChan <- reasonCloseAndReconnect
}

// Shutdown gracefully closes the connection by sending the websocket.CloseMessage.
// The global option writeTimeout defines the duration after which the write
// operation get canceled. (Can be set WithWriteTimeout())
func (rc *RecConn) Shutdown() {
	if !rc.IsConnected() {
		log.Println("Shutdown: connection is already closed")
		return
	}
	msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
	err := rc.WriteControl(websocket.CloseMessage, msg, time.Now().Add(rc.writeTimeout))
	if err != nil && err != websocket.ErrCloseSent {
		// If close message could not be sent, then close without the handshake.
		log.Printf("Shutdown: %v", err)
		rc.termChan <- reasonClose
		return
	}
	time.Sleep(time.Second)
}

// ReadMessage is a helper method for getting a reader
// using NextReader and reading from that reader to a buffer.
//
// If the connection is closed ErrNotConnected is returned
func (rc *RecConn) ReadMessage() (messageType int, message []byte, err error) {
	err = ErrNotConnected
	if rc.IsConnected() {
		messageType, message, err = rc.Conn.ReadMessage()
		if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
			rc.termChan <- reasonClose
			return messageType, message, nil
		}
		if err != nil {
			rc.termChan <- reasonCloseAndReconnect
		}
	}

	return
}

// WriteMessage is a helper method for getting a writer using NextWriter,
// writing the message and closing the writer.
//
// If the connection is closed ErrNotConnected is returned
func (rc *RecConn) WriteMessage(messageType int, data []byte) error {
	err := ErrNotConnected
	if rc.IsConnected() {
		rc.mu.Lock()
		err = rc.Conn.WriteMessage(messageType, data)
		rc.mu.Unlock()
		if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
			rc.termChan <- reasonClose
			return nil
		}
		if err != nil {
			rc.termChan <- reasonCloseAndReconnect
		}
	}

	return err
}

// WriteJSON writes the JSON encoding of v to the connection.
//
// See the documentation for encoding/json Marshal for details about the
// conversion of Go values to JSON.
//
// If the connection is closed ErrNotConnected is returned
func (rc *RecConn) WriteJSON(v interface{}) error {
	err := ErrNotConnected
	if rc.IsConnected() {
		rc.mu.Lock()
		err = rc.Conn.WriteJSON(v)
		rc.mu.Unlock()
		if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
			rc.termChan <- reasonClose
			return nil
		}
		if err != nil {
			rc.termChan <- reasonCloseAndReconnect
		}
	}

	return err
}

// ReadJSON reads the next JSON-encoded message from the connection and stores
// it in the value pointed to by v.
//
// See the documentation for the encoding/json Unmarshal function for details
// about the conversion of JSON to a Go value.
//
// If the connection is closed ErrNotConnected is returned
func (rc *RecConn) ReadJSON(v interface{}) error {
	err := ErrNotConnected
	if rc.IsConnected() {
		err = rc.Conn.ReadJSON(v)
		if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
			rc.termChan <- reasonClose
			return nil
		}
		if err != nil {
			rc.termChan <- reasonCloseAndReconnect
		}
	}

	return err
}

/////////////////////
// Private methods //
/////////////////////

// closeAndReconnect will try to reconnect.
func (rc *RecConn) closeAndReconnect() {
	rc.close()
	go rc.connect()
}

// setIsConnected sets state for isConnected
func (rc *RecConn) setIsConnected(state bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.isConnected = state
}

func (rc *RecConn) getConn() *websocket.Conn {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.Conn
}

func (rc *RecConn) close() {
	if rc.getConn() != nil && rc.IsConnected() {
		rc.mu.Lock()
		if err := rc.Conn.Close(); err != nil {
			log.Println(err)
		}
		rc.mu.Unlock()
	}

	rc.setIsConnected(false)
}

func (rc *RecConn) setURL(url string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.url = url
}

func (rc *RecConn) setReqHeader(reqHeader http.Header) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.reqHeader = reqHeader
}

// parseURL parses current url
func (rc *RecConn) parseURL(urlStr string) (string, error) {
	if urlStr == "" {
		return "", errors.New("dial: url cannot be empty")
	}

	u, err := url.Parse(urlStr)

	if err != nil {
		return "", errors.New("url: " + err.Error())
	}

	if u.Scheme != "ws" && u.Scheme != "wss" {
		return "", errors.New("url: websocket uris must start with ws or wss scheme")
	}

	if u.User != nil {
		return "", errors.New("url: user name and password are not allowed in websocket URIs")
	}

	return urlStr, nil
}

func (rc *RecConn) getHandshakeTimeout() time.Duration {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.handshakeTimeout
}

func (rc *RecConn) isVerbose() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.verbose
}

func (rc *RecConn) getBackoff() *backoff.Backoff {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return &backoff.Backoff{
		Min:    rc.recIntervalMin,
		Max:    rc.recIntervalMax,
		Factor: rc.recIntervalFactor,
		Jitter: true,
	}
}

func (rc *RecConn) hasSubscribeHandler() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.subscribeHandler != nil
}

func (rc *RecConn) getKeepAliveTimeout() time.Duration {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.keepAliveTimeout
}

func (rc *RecConn) writeControlPingMessage() error {
	err := ErrNotConnected
	if rc.IsConnected() {
		rc.mu.Lock()
		err = rc.Conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(rc.writeTimeout))
		rc.mu.Unlock()
		if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
			rc.termChan <- reasonClose
			return nil
		}
		if err != nil {
			rc.termChan <- reasonCloseAndReconnect
		}
	}
	return err
}

func (rc *RecConn) keepAlive() {
	var (
		lastResponse = time.Now()
		ticker       = time.NewTicker((rc.getKeepAliveTimeout() * 9) / 10)
		pongChan     = make(chan time.Time)
	)

	rc.mu.Lock()
	rc.Conn.SetPongHandler(func(msg string) error {
		if rc.getKeepAliveTimeout() > 0 {
			// Set read deadline in case the tcp connection failed.
			if err := rc.Conn.SetReadDeadline(time.Now().Add(rc.getKeepAliveTimeout())); err != nil {
				log.Printf("Dial: cannot set read deadline %q", err.Error())
			}
			if rc.isVerbose() {
				log.Printf("Dial: next read deadline %q", time.Now().Add(rc.getKeepAliveTimeout()))
			}
		}
		pongChan <- time.Now()
		return nil
	})
	rc.mu.Unlock()

	go func() {
		defer ticker.Stop()

		for {
			select {
			case lastResponse = <-pongChan:
				if time.Since(lastResponse) > rc.getKeepAliveTimeout() {
					rc.termChan <- reasonCloseAndReconnect
					return
				}
			case <-ticker.C:
				if !rc.IsConnected() {
					return
				}

				if err := rc.writeControlPingMessage(); err != nil {
					log.Printf("Dial: cannot write control ping message %q", err.Error())
					return
				}
			}
		}
	}()
}

func (rc *RecConn) connect() {
	b := rc.getBackoff()
	rand.Seed(time.Now().UTC().UnixNano())

	for {
		nextInterval := b.Duration()
		wsConn, httpResp, err := rc.dialer.Dial(rc.url, rc.reqHeader)

		rc.mu.Lock()
		rc.Conn = wsConn
		rc.dialErr = err
		rc.isConnected = err == nil
		rc.termChan = make(chan TerminationReason)
		rc.shutDownChan = make(chan struct{})
		rc.httpResp = httpResp
		rc.mu.Unlock()

		if err == nil {
			if rc.getKeepAliveTimeout() > 0 {
				// Set read deadline in case the tcp connection failed.
				if err := rc.Conn.SetReadDeadline(time.Now().Add(rc.getKeepAliveTimeout())); err != nil {
					log.Printf("Dial: cannot set read deadline %q", err.Error())
				}
			}

			if rc.isVerbose() {
				log.Printf("Dial: connection was successfully established with %s\n", rc.url)
			}

			if rc.hasSubscribeHandler() {
				if err := rc.subscribeHandler(); err != nil {
					log.Fatalf("Dial: connect handler failed with %s", err.Error())
				}
			}

			if rc.getKeepAliveTimeout() > 0 {
				rc.keepAlive()
			}

			go rc.terminationHandler()

			return
		}

		if rc.isVerbose() {
			log.Println(err)
			log.Println("Dial: will try again in", nextInterval, "seconds.")
		}

		time.Sleep(nextInterval)
	}
}

// terminationHandler handles the termination process
func (rc *RecConn) terminationHandler() {
	switch <-rc.termChan {
	case reasonClose:
		rc.close()
	case reasonCloseAndReconnect:
		rc.closeAndReconnect()
	}
}
