package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// proxy.go — proxies com cache sobre a API pública REST da Binance
// (api.binance.com) usados pela UI: stats 24h do par e histórico de candles
// (klines). Nenhum endpoint exige autenticação — dados de mercado públicos.

type ttlCache struct {
	mu      sync.Mutex
	entries map[string]cachedValue
}

type cachedValue struct {
	body        []byte
	contentType string
	expires     time.Time
}

func newTTLCache() *ttlCache {
	return &ttlCache{entries: map[string]cachedValue{}}
}

func (c *ttlCache) get(key string) (cachedValue, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.entries[key]
	if !ok || time.Now().After(v.expires) {
		delete(c.entries, key)
		return cachedValue{}, false
	}
	return v, true
}

func (c *ttlCache) set(key string, v cachedValue, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v.expires = time.Now().Add(ttl)
	c.entries[key] = v
}

var binanceClient = &http.Client{Timeout: 4 * time.Second}

type proxyConfig struct {
	baseURL string
	cache   *ttlCache
	ttl     time.Duration
}

// proxy proxies upstream Binance requests with a small TTL cache. It always
// answers JSON; on failure it returns a JSON error body (502).
func (p *proxyConfig) proxy(w http.ResponseWriter, r *http.Request, path string, query url.Values) {
	cacheKey := path + "?" + query.Encode()
	if cached, ok := p.cache.get(cacheKey); ok {
		w.Header().Set("Content-Type", cached.contentType)
		w.Header().Set("X-Cache", "HIT")
		_, _ = w.Write(cached.body)
		return
	}

	upstream := p.baseURL + path + "?" + query.Encode()
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstream, nil)
	if err != nil {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		return
	}
	resp, err := binanceClient.Do(req)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "binance unreachable: "+err.Error())
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "binance read error: "+err.Error())
		return
	}
	if resp.StatusCode != http.StatusOK {
		writeJSONError(w, http.StatusBadGateway,
			fmt.Sprintf("binance returned %d", resp.StatusCode))
		return
	}

	p.cache.set(cacheKey, cachedValue{body: body, contentType: "application/json; charset=utf-8"}, p.ttl)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Cache", "MISS")
	_, _ = w.Write(body)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

var allowedIntervals = map[string]bool{
	"1m": true, "3m": true, "5m": true, "15m": true,
	"1h": true, "4h": true, "1d": true,
}

// handleTicker24h GET /api/ticker24h?symbol=BTCUSDT — proxy do
// /api/v3/ticker/24hr da Binance (preço, variação, high/low, volume).
func (p *proxyConfig) handleTicker24h(w http.ResponseWriter, r *http.Request) {
	symbol := strings.ToUpper(r.URL.Query().Get("symbol"))
	if symbol == "" {
		symbol = "BTCUSDT"
	}
	p.proxy(w, r, "/api/v3/ticker/24hr", url.Values{"symbol": {symbol}})
}

// handleKlines GET /api/klines?symbol=BTCUSDT&interval=1m&limit=300 — proxy
// do /api/v3/klines (candles históricos).
func (p *proxyConfig) handleKlines(w http.ResponseWriter, r *http.Request) {
	symbol := strings.ToUpper(r.URL.Query().Get("symbol"))
	if symbol == "" {
		symbol = "BTCUSDT"
	}
	interval := strings.ToLower(r.URL.Query().Get("interval"))
	if !allowedIntervals[interval] {
		interval = "1m"
	}
	var limit int
	if v := r.URL.Query().Get("limit"); v != "" {
		fmt.Sscanf(v, "%d", &limit)
	}
	if limit <= 0 || limit > 1000 {
		limit = 300
	}
	p.proxy(w, r, "/api/v3/klines", url.Values{
		"symbol":   {symbol},
		"interval": {interval},
		"limit":    {fmt.Sprintf("%d", limit)},
	})
}
