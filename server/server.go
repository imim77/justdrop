package main

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pion/turn/v4"
)

type Config struct {
	Host         string
	Port         string
	TURNPort     int
	TURNTLSPort  int
	TURNTLSCert  string
	TURNTLSKey   string
	TURNRealm    string
	TURNSecret   string
	PublicIP     string
	PublicHost   string
	RelayPortMin int
	RelayPortMax int
}

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 1 << 20
	sendBufferSize = 256
	enqueueWait    = 2 * time.Second
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func NewServer(cfg *Config, wsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	addRoutes(mux, wsHandler)
	return mux
}

func addRoutes(mux *http.ServeMux, wsHandler http.Handler) {
	mux.Handle("/ws", wsHandler)
}

func generateTURNCredentials(secret string, ttl time.Duration) (username, password string) {
	expiry := time.Now().Add(ttl).Unix()
	username = fmt.Sprintf("%d", expiry)
	mac := hmac.New(sha1.New, []byte(secret))
	mac.Write([]byte(username))
	password = base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return
}

func turnAuthKey(username, realm, password string) []byte {
	h := md5.New()
	h.Write([]byte(username + ":" + realm + ":" + password))
	return h.Sum(nil)
}

func startTURN(cfg *Config) {
	publicIp := cfg.PublicIP
	if publicIp == "" {
		publicIp = "127.0.0.1"
	}
	relayIP := net.ParseIP(publicIp)
	if relayIP == nil {
		resolved, err := net.ResolveIPAddr("ip4", publicIp)
		if err != nil || resolved == nil || resolved.IP == nil {
			log.Fatalf("Failed to resolve TURN relay IP from %q: %v", publicIp, err)
		}
		relayIP = resolved.IP
	}
	if isPrivateOrLoopbackIP(relayIP) {
		log.Printf(
			"TURN warning: relay IP %s is private/loopback; peers outside this network will fail unless JUSTDROP_PUBLIC_IP/PUBLIC_IP is a public address and NAT forwards TURN+relay UDP ports",
			relayIP.String(),
		)
	}
	addr := fmt.Sprintf("0.0.0.0:%d", cfg.TURNPort)

	udpListener, err := net.ListenPacket("udp4", addr)
	if err != nil {
		log.Fatalf("Failed to listen UDP for TURN: %v", err)
	}

	tcpListener, err := net.Listen("tcp4", addr)
	if err != nil {
		log.Fatalf("Failed to listen TCP for TURN: %v", err)
	}

	relayGen := &turn.RelayAddressGeneratorPortRange{
		RelayAddress: relayIP,
		Address:      "0.0.0.0",
		MinPort:      uint16(cfg.RelayPortMin),
		MaxPort:      uint16(cfg.RelayPortMax),
	}

	listenerConfigs := []turn.ListenerConfig{
		{
			Listener:              tcpListener,
			RelayAddressGenerator: relayGen,
		},
	}

	if cfg.turnTLSEnabled() {
		tlsCert, certErr := tls.LoadX509KeyPair(cfg.TURNTLSCert, cfg.TURNTLSKey)
		if certErr != nil {
			log.Fatalf("Failed to load TURN TLS certificate/key: %v", certErr)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{tlsCert},
			MinVersion:   tls.VersionTLS12,
		}
		tlsAddr := fmt.Sprintf("0.0.0.0:%d", cfg.TURNTLSPort)
		tlsTCPListener, tlsErr := net.Listen("tcp4", tlsAddr)
		if tlsErr != nil {
			log.Fatalf("Failed to listen TCP for TURN TLS: %v", tlsErr)
		}
		tlsListener := tls.NewListener(tlsTCPListener, tlsConfig)
		listenerConfigs = append(listenerConfigs, turn.ListenerConfig{
			Listener:              tlsListener,
			RelayAddressGenerator: relayGen,
		})
	}

	_, err = turn.NewServer(turn.ServerConfig{
		Realm: cfg.TURNRealm,
		AuthHandler: func(username string, realm string, srcAddr net.Addr) ([]byte, bool) {
			// Validate ephemeral credentials: username is the expiry timestamp
			// Check it hasn't expired, then compute the expected password
			t, parseErr := strconv.ParseInt(username, 10, 64)
			if parseErr != nil {
				log.Printf("TURN auth: invalid username %q", username)
				return nil, false
			}
			if time.Now().Unix() > t {
				log.Printf("TURN auth: expired credential for %q", username)
				return nil, false
			}
			mac := hmac.New(sha1.New, []byte(cfg.TURNSecret))
			mac.Write([]byte(username))
			password := base64.StdEncoding.EncodeToString(mac.Sum(nil))
			return turnAuthKey(username, realm, password), true
		},
		PacketConnConfigs: []turn.PacketConnConfig{
			{
				PacketConn:            udpListener,
				RelayAddressGenerator: relayGen,
			},
		},
		ListenerConfigs: listenerConfigs,
	})
	if err != nil {
		log.Fatalf("Failed to start TURN server: %v", err)
	}

	log.Printf("TURN server listening on UDP+TCP :%d (realm=%s, publicIP=%s, relay=%d-%d)",
		cfg.TURNPort, cfg.TURNRealm, publicIp, cfg.RelayPortMin, cfg.RelayPortMax)
	if cfg.turnTLSEnabled() {
		log.Printf("TURN TLS listener enabled on TCP :%d (cert=%s)", cfg.TURNTLSPort, cfg.TURNTLSCert)
	}
}

func buildIceServers(cfg *Config, r *http.Request) []IceServerInfo {
	hosts := gatherIceHosts(cfg, r)
	stunURLs := make([]string, 0, len(hosts)+2)
	turnURLs := make([]string, 0, len(hosts)*3)

	for _, host := range hosts {
		turnHostPort := net.JoinHostPort(host, strconv.Itoa(cfg.TURNPort))
		stunURLs = append(stunURLs, fmt.Sprintf("stun:%s", turnHostPort))
		turnURLs = append(turnURLs,
			fmt.Sprintf("turn:%s?transport=udp", turnHostPort),
			fmt.Sprintf("turn:%s?transport=tcp", turnHostPort),
		)
		if cfg.turnTLSEnabled() {
			turnsHostPort := net.JoinHostPort(host, strconv.Itoa(cfg.TURNTLSPort))
			turnURLs = append(turnURLs, fmt.Sprintf("turns:%s?transport=tcp", turnsHostPort))
		}
	}

	// Public STUN fallback helps diagnose routing problems when the TURN/STUN host itself is unreachable.
	//stunURLs = appendUniqueURL(stunURLs, "stun:stun.l.google.com:19302")
	//stunURLs = appendUniqueURL(stunURLs, "stun:stun.cloudflare.com:3478")

	servers := []IceServerInfo{{URLs: stunURLs}}

	if cfg.TURNSecret == "" || cfg.TURNRealm == "" {
		return servers
	}

	turnUser, turnPass := generateTURNCredentials(cfg.TURNSecret, 24*time.Hour)
	servers = append(servers, IceServerInfo{
		URLs:       dedupeURLs(turnURLs),
		Username:   turnUser,
		Credential: turnPass,
	})

	return servers
}

func gatherIceHosts(cfg *Config, r *http.Request) []string {
	forwardedHost := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Host"), ",")[0])
	candidates := []string{
		cfg.PublicHost,
		cfg.PublicIP,
		forwardedHost,
		r.Host,
		r.URL.Host,
	}

	seen := make(map[string]struct{}, len(candidates))
	hosts := make([]string, 0, len(candidates))

	for _, candidate := range candidates {
		host := normalizeIceHost(candidate)
		if host == "" {
			continue
		}
		if host == "localhost" {
			host = "127.0.0.1"
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		hosts = append(hosts, host)

		if ip := net.ParseIP(host); ip != nil && isPrivateOrLoopbackIP(ip) {
			log.Printf(
				"TURN warning: advertising private ICE host %s to client %s; set JUSTDROP_PUBLIC_HOST to your public TURN DNS name for internet peers",
				host,
				r.RemoteAddr,
			)
		}
	}

	if len(hosts) == 0 {
		return []string{"127.0.0.1"}
	}

	return hosts
}

func dedupeURLs(urls []string) []string {
	if len(urls) == 0 {
		return urls
	}

	seen := make(map[string]struct{}, len(urls))
	deduped := make([]string, 0, len(urls))
	for _, url := range urls {
		key := strings.ToLower(strings.TrimSpace(url))
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		deduped = append(deduped, strings.TrimSpace(url))
	}
	return deduped
}

func appendUniqueURL(urls []string, next string) []string {
	key := strings.ToLower(strings.TrimSpace(next))
	if key == "" {
		return urls
	}
	for _, existing := range urls {
		if strings.ToLower(strings.TrimSpace(existing)) == key {
			return urls
		}
	}
	return append(urls, strings.TrimSpace(next))
}

func (cfg *Config) turnTLSEnabled() bool {
	return cfg.TURNTLSCert != "" && cfg.TURNTLSKey != ""
}

func normalizeIceHost(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	raw = strings.TrimPrefix(raw, "stun:")
	raw = strings.TrimPrefix(raw, "turn:")
	raw = strings.TrimPrefix(raw, "turns:")

	if i := strings.Index(raw, "?"); i >= 0 {
		raw = raw[:i]
	}
	if i := strings.Index(raw, "/"); i >= 0 {
		raw = raw[:i]
	}

	if host, _, err := net.SplitHostPort(raw); err == nil {
		return strings.Trim(host, "[]")
	}

	return strings.Trim(raw, "[]")
}

func isPrivateOrLoopbackIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified()
}

func websocketHandler(cfg *Config, hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("error upgrading to websocket: %v", err)
		http.Error(w, "failed to upgrade to websocket", http.StatusBadRequest)
		return
	}

	client := NewClient(uuid.New(), conn, hub)
	log.Printf("client connected: %s (%s)", client.ID, conn.RemoteAddr())

	go client.writeMessages()

	peers, existing := hub.Register(client)
	iceServers := buildIceServers(cfg, r)
	log.Printf("HELLO ICE servers for %s (%s): %s", client.ID, r.RemoteAddr, summarizeIceServers(iceServers))
	client.enqueueJSON(HelloMessage{
		Type:       "HELLO",
		Client:     client.PublicInfo(),
		Peers:      peers,
		IceServers: iceServers,
	})

	join := JoinMessage{Type: "JOIN", Peer: client.PublicInfo()}
	for _, peer := range existing {
		peer.enqueueJSON(join)
	}

	client.readMessages()
}

type ClientInfoWithoutId struct {
	Alias       string `json:"alias,omitempty"`
	DeviceModel string `json:"deviceModel,omitempty"`
	DeviceType  string `json:"deviceType,omitempty"`
	Token       string `json:"token,omitempty"`
}

type ClientInfo struct {
	Id uuid.UUID `json:"id"`
	ClientInfoWithoutId
}

type Client struct {
	ID   uuid.UUID
	conn *websocket.Conn
	send chan []byte
	done chan struct{}

	hub *Hub

	infoMu sync.RWMutex
	info   ClientInfoWithoutId

	closeOnce sync.Once
}

func NewClient(id uuid.UUID, conn *websocket.Conn, hub *Hub) *Client {
	return &Client{
		ID:   id,
		conn: conn,
		send: make(chan []byte, sendBufferSize),
		done: make(chan struct{}),
		hub:  hub,
	}
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

func (c *Client) PublicInfo() ClientInfo {
	c.infoMu.RLock()
	info := c.info
	c.infoMu.RUnlock()

	return ClientInfo{
		Id:                  c.ID,
		ClientInfoWithoutId: info,
	}
}

func (c *Client) SetInfo(info ClientInfoWithoutId) {
	c.infoMu.Lock()
	c.info = info
	c.infoMu.Unlock()
}

func (c *Client) enqueueJSON(v any) bool {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("[%s] failed to encode outgoing message: %v", c.ID, err)
		return false
	}
	return c.enqueue(data)
}

func (c *Client) enqueue(payload []byte) bool {
	timer := time.NewTimer(enqueueWait)
	defer timer.Stop()

	select {
	case <-c.done:
		return false
	case c.send <- payload:
		return true
	case <-timer.C:
		log.Printf("[%s] send queue timeout, dropping message", c.ID)
		return false
	}
}

func (c *Client) sendError(code int) {
	c.enqueueJSON(ErrorMessage{Type: "ERROR", Code: code})
}

type WsClientMessage struct {
	Type      string               `json:"type"`
	SessionID string               `json:"sessionId,omitempty"`
	Target    string               `json:"target,omitempty"`
	SDP       string               `json:"sdp,omitempty"`
	Candidate json.RawMessage      `json:"candidate,omitempty"`
	Info      *ClientInfoWithoutId `json:"info,omitempty"`
}

type HelloMessage struct {
	Type       string          `json:"type"`
	Client     ClientInfo      `json:"client"`
	Peers      []ClientInfo    `json:"peers"`
	IceServers []IceServerInfo `json:"iceServers,omitempty"`
}

type IceServerInfo struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

type JoinMessage struct {
	Type string     `json:"type"`
	Peer ClientInfo `json:"peer"`
}

type UpdateMessage struct {
	Type string     `json:"type"`
	Peer ClientInfo `json:"peer"`
}

type LeftMessage struct {
	Type   string    `json:"type"`
	PeerId uuid.UUID `json:"peerId"`
}

type WsServerSdpMessage struct {
	Type      string     `json:"type"`
	Peer      ClientInfo `json:"peer"`
	SessionID string     `json:"sessionId"`
	SDP       string     `json:"sdp"`
}

type WsServerCandidateMessage struct {
	Type      string          `json:"type"`
	Peer      ClientInfo      `json:"peer"`
	SessionID string          `json:"sessionId"`
	Candidate json.RawMessage `json:"candidate"`
}

type ErrorMessage struct {
	Type string `json:"type"`
	Code int    `json:"code"`
}

func (c *Client) readMessages() {
	defer func() {
		recipients := c.hub.Unregister(c)
		left := LeftMessage{Type: "LEFT", PeerId: c.ID}
		for _, peer := range recipients {
			peer.enqueueJSON(left)
		}
		log.Printf("client disconnected: %s", c.ID)
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[%s] read error: %v", c.ID, err)
			}
			return
		}

		var msg WsClientMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Printf("[%s] invalid JSON: %v", c.ID, err)
			c.sendError(http.StatusBadRequest)
			continue
		}

		switch msg.Type {
		case "OFFER", "ANSWER", "CANDIDATE":
			c.forwardSignalingMessage(msg)
		case "UPDATE":
			c.handleUpdate(msg)
		default:
			log.Printf("[%s] unsupported message type %q", c.ID, msg.Type)
			c.sendError(http.StatusBadRequest)
		}
	}
}

func (c *Client) handleUpdate(msg WsClientMessage) {
	if msg.Info == nil {
		c.sendError(http.StatusBadRequest)
		return
	}

	c.SetInfo(*msg.Info)
	update := UpdateMessage{Type: "UPDATE", Peer: c.PublicInfo()}

	for _, peer := range c.hub.Others(c.ID) {
		peer.enqueueJSON(update)
	}
}

func (c *Client) forwardSignalingMessage(msg WsClientMessage) {
	if msg.Target == "" || msg.SessionID == "" {
		c.sendError(http.StatusBadRequest)
		return
	}

	switch msg.Type {
	case "OFFER", "ANSWER":
		if msg.SDP == "" {
			c.sendError(http.StatusBadRequest)
			return
		}
	case "CANDIDATE":
		if len(msg.Candidate) == 0 {
			c.sendError(http.StatusBadRequest)
			return
		}
	default:
		c.sendError(http.StatusBadRequest)
		return
	}

	targetID, err := uuid.Parse(msg.Target)
	if err != nil {
		c.sendError(http.StatusBadRequest)
		return
	}

	target, ok := c.hub.FindClient(targetID)
	if !ok {
		c.sendError(http.StatusNotFound)
		return
	}

	var outbound any
	switch msg.Type {
	case "OFFER", "ANSWER":
		outbound = WsServerSdpMessage{
			Type:      msg.Type,
			Peer:      c.PublicInfo(),
			SessionID: msg.SessionID,
			SDP:       msg.SDP,
		}
	case "CANDIDATE":
		outbound = WsServerCandidateMessage{
			Type:      msg.Type,
			Peer:      c.PublicInfo(),
			SessionID: msg.SessionID,
			Candidate: msg.Candidate,
		}
	}
	if !target.enqueueJSON(outbound) {
		log.Printf("[%s] -> %s dropped %s", c.ID, msg.Target, msg.Type)
	} else {
		log.Printf("[%s] -> %s %s", c.ID, msg.Target, msg.Type)
	}
}

func (c *Client) writeMessages() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.close()
	}()

	for {
		select {
		case <-c.done:
			return
		case message := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				log.Printf("[%s] write error: %v", c.ID, err)
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func summarizeIceServers(servers []IceServerInfo) string {
	if len(servers) == 0 {
		return "[]"
	}

	parts := make([]string, 0, len(servers))
	for _, server := range servers {
		label := "stun"
		if server.Username != "" || server.Credential != "" {
			label = "turn"
		}
		parts = append(parts, fmt.Sprintf("{type=%s urls=%v hasAuth=%t}", label, server.URLs, server.Username != "" && server.Credential != ""))
	}
	return strings.Join(parts, " ")
}
