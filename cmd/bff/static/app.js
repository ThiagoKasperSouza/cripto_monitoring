/* RTD — Real-Time Dash. UI for the market-data pipeline.
   Data path: Binance WS → Go gateway → Redpanda → Java core (StreamTicker) → Go BFF → WebSocket here.
   Historical candles: BFF proxy /api/klines → Binance REST.
   24h stats: BFF proxy /api/ticker24h → Binance REST. */

'use strict';

/* ---------- formatting helpers ---------- */
const fmt1dp = new Intl.NumberFormat('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const fmt2dp = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmtCompact = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 });

function fmtPrice(p) {
  const n = parseFloat(p);
  if (!Number.isFinite(n)) return '—';
  if (n >= 1000) return fmt1dp.format(n);
  if (n >= 1) return fmt2dp.format(n);
  if (n >= 0.01) return n.toFixed(5);
  return n.toPrecision(4);
}

function splitSymbol(symbol) {
  const suffix = 'USDT';
  const base = symbol.endsWith(suffix) ? symbol.slice(0, symbol.length - suffix.length) : symbol.slice(0, 3);
  return { base, quote: symbol.slice(base.length) };
}

const INTERVAL_SECONDS = { '1m': 60, '3m': 180, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400 };
const UP = '#2ebd85';
const DOWN = '#f6465d';
const UP_VOL = 'rgba(46,189,133,0.45)';
const DOWN_VOL = 'rgba(246,70,93,0.45)';

const state = {
  symbol: null,
  interval: '1m',
  bucketSec: INTERVAL_SECONDS['1m'],
  currentCandle: null,
  volumeSum: 0,
  tradeCount: 0,
  lastTradePrice: null,
  ws: null,
  wsRetry: 0,
  reconnectTimer: null,
};

/* ---------- dom ---------- */
const $ = (id) => document.getElementById(id);
const pairButton = $('pairButton');
const pairMenu = $('pairMenu');
const pairFilter = $('pairFilter');
const pairList = $('pairList');
const pairTitle = $('pairTitle');
const coinBase = $('coinBase');
const lastPriceEl = $('lastPrice');
const lastPriceUsdEl = $('lastPriceUsd');
const statChange = $('statChange');
const statHigh = $('statHigh');
const statLow = $('statLow');
const statVolume = $('statVolume');
const statVolumeAsset = $('statVolumeAsset');
const tradeCountEl = $('tradeCount');
const lastEventTimeEl = $('lastEventTime');
const connBadge = $('connBadge');
const connText = connBadge.querySelector('.conn-text');
const toast = $('toast');

/* ---------- chart ---------- */
const chartEl = $('chart');
const chart = LightweightCharts.createChart(chartEl, {
  width: chartEl.clientWidth || 900,
  height: chartEl.clientHeight || 480,
  layout: {
    background: { color: 'transparent' },
    textColor: '#848e9c',
    fontSize: 11,
    fontFamily: 'Inter, system-ui, sans-serif',
  },
  grid: {
    vertLines: { color: 'rgba(31, 42, 55, 0.55)' },
    horzLines: { color: 'rgba(31, 42, 55, 0.55)' },
  },
  rightPriceScale: { borderColor: '#1f2a37' },
  timeScale: { borderColor: '#1f2a37', timeVisible: true, secondsVisible: false, rightOffset: 6 },
  watermark: {
    visible: true,
    text: 'BTC/USDT',
    color: 'rgba(234, 236, 239, 0.055)',
    fontSize: 96,
    fontStyle: 'bold',
    fontFamily: 'Inter, system-ui, sans-serif',
  },
  crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
});

const candleSeries = chart.addCandlestickSeries({
  upColor: UP, downColor: DOWN,
  borderUpColor: UP, borderDownColor: DOWN,
  wickUpColor: UP, wickDownColor: DOWN,
});

const volumeSeries = chart.addHistogramSeries({
  priceScaleId: '',
  priceFormat: { type: 'volume' },
  lastValueVisible: false,
  priceLineVisible: false,
});
chart.priceScale('').applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });

const lineSeries = chart.addAreaSeries({
  lineColor: '#f0b90b',
  topColor: 'rgba(240,185,11,0.32)',
  bottomColor: 'rgba(240,185,11,0.02)',
  lineWidth: 2,
  priceLineVisible: false,
});
lineSeries.applyOptions({ visible: false });

function resizeChart() {
  chart.applyOptions({ width: chartEl.clientWidth, height: chartEl.clientHeight });
}
window.addEventListener('resize', resizeChart);

/* ---------- data loading ---------- */
async function fetchJSON(url) {
  const res = await fetch(url, { headers: { accept: 'application/json' } });
  if (!res.ok) throw new Error(`${res.status} ${await res.text().catch(() => res.statusText)}`);
  return res.json();
}

function resetChartData() {
  state.currentCandle = null;
  state.volumeSum = 0;
  state.tradeCount = 0;
  chartEl && (tradeCountEl.textContent = '0');
  lastEventTimeEl.textContent = '—';
  candleSeries.setData([]);
  volumeSeries.setData([]);
  lineSeries.setData([]);
}

async function loadHistory() {
  try {
    const raw = await fetchJSON(
      `/api/klines?symbol=${encodeURIComponent(state.symbol)}&interval=${state.interval}&limit=300`
    );
    const candles = [];
    const vols = [];
    for (const k of raw) {
      const time = Math.floor(k[0] / 1000);
      const open = +k[1], high = +k[2], low = +k[3], close = +k[4], volume = +k[5];
      candles.push({ time, open, high, low, close });
      vols.push({ time, value: volume, color: close >= open ? UP_VOL : DOWN_VOL });
    }
    candleSeries.setData(candles);
    volumeSeries.setData(vols);
    lineSeries.setData(candles.map(c => ({ time: c.time, value: c.close })));
    state.currentCandle = candles.length ? { ...candles[candles.length - 1] } : null;
    state.volumeSum = candles.length ? vols[vols.length - 1].value : 0;
    chart.timeScale().fitContent();
  } catch (err) {
    showToast(`Failed to load klines: ${err.message}`);
  }
}

async function loadStats24h() {
  try {
    const d = await fetchJSON(`/api/ticker24h?symbol=${encodeURIComponent(state.symbol)}`);
    if (d.error) return;
    const pct = parseFloat(d.priceChangePercent);
    statChange.textContent = `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`;
    statChange.className = `stat-value ${pct >= 0 ? 'up' : 'down'}`;
    statHigh.textContent = fmtPrice(d.highPrice);
    statLow.textContent = fmtPrice(d.lowPrice);
    statVolumeAsset.textContent = splitSymbol(state.symbol).base;
    statVolume.textContent = fmtCompact.format(parseFloat(d.volume));
    const last = parseFloat(d.lastPrice);
    if (Number.isFinite(last) && state.lastTradePrice === null) {
      lastPriceEl.textContent = fmtPrice(last);
      lastPriceUsdEl.textContent = `≈ $${fmtPrice(last)}`;
    }
  } catch {
    /* stats are best-effort; the chart is the source of truth */
  }
}
setInterval(loadStats24h, 5000);

/* ---------- live trade application ---------- */
function updateLastPrice(px) {
  const prev = state.lastTradePrice;
  state.lastTradePrice = px;
  lastPriceEl.textContent = fmtPrice(px);
  lastPriceUsdEl.textContent = `≈ $${fmtPrice(px)}`;
  if (prev !== null && Number.isFinite(prev)) {
    lastPriceEl.classList.remove('flash-up', 'flash-down');
    if (px > prev) lastPriceEl.classList.add('flash-up');
    else if (px < prev) lastPriceEl.classList.add('flash-down');
    setTimeout(() => lastPriceEl.classList.remove('flash-up', 'flash-down'), 180);
  }
}

function applyTrade(t) {
  const bucket = Math.floor((t.event_time / 1000) / state.bucketSec) * state.bucketSec;
  const px = parseFloat(t.price);
  const qty = parseFloat(t.quantity);
  if (!Number.isFinite(px) || !Number.isFinite(qty)) return;

  let c = state.currentCandle;
  if (!c || c.time !== bucket) {
    c = { time: bucket, open: px, high: px, low: px, close: px };
    state.volumeSum = qty;
  } else {
    c = {
      ...c,
      high: Math.max(c.high, px),
      low: Math.min(c.low, px),
      close: px,
    };
    state.volumeSum += qty;
  }
  state.currentCandle = c;
  candleSeries.update(c);
  lineSeries.update({ time: bucket, value: px });
  volumeSeries.update({ time: bucket, value: state.volumeSum, color: c.close >= c.open ? UP_VOL : DOWN_VOL });

  updateLastPrice(px);
  state.tradeCount += 1;
  tradeCountEl.textContent = state.tradeCount.toLocaleString('en-US');
  lastEventTimeEl.textContent = new Date(t.event_time).toLocaleTimeString('en-US', { hour12: false });
}

/* ---------- websocket lifecycle ---------- */
function setConn(mode) {
  connBadge.classList.remove('live', 'down');
  if (mode === 'live') {
    connBadge.classList.add('live');
    connText.textContent = 'Live';
  } else if (mode === 'down') {
    connBadge.classList.add('down');
    connText.textContent = 'Reconnecting…';
  } else {
    connText.textContent = 'Connecting…';
  }
}

function showToast(msg) {
  if (!msg) {
    toast.hidden = true;
    return;
  }
  toast.textContent = msg;
  toast.hidden = false;
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => { toast.hidden = true; }, 4000);
}

function connectWs() {
  if (state.ws) {
    try { state.ws.close(); } catch { /* already closing */ }
    state.ws = null;
  }
  clearTimeout(state.reconnectTimer);

  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(`${proto}://${location.host}/ws?symbol=${encodeURIComponent(state.symbol)}`);
  state.ws = ws;
  setConn('connecting');

  ws.onopen = () => {
    state.wsRetry = 0;
    setConn('live');
    showToast('');
  };
  ws.onmessage = (ev) => {
    try {
      const msg = JSON.parse(ev.data);
      if (msg && msg.type === 'trade') applyTrade(msg);
    } catch {
      /* ignore malformed frames */
    }
  };
  ws.onclose = () => {
    setConn('down');
    scheduleReconnect();
  };
  ws.onerror = () => {
    try { ws.close(); } catch { /* noop */ }
  };
}

function scheduleReconnect() {
  const delay = Math.min(10_000, 1000 * 2 ** state.wsRetry);
  state.wsRetry = Math.min(state.wsRetry + 1, 5);
  state.reconnectTimer = setTimeout(() => {
    connectWs();
    loadHistory(); // heal any trades missed while disconnected
  }, delay);
}

/* ---------- symbol switching ---------- */
async function setSymbol(symbol) {
  if (!symbol || symbol === state.symbol) return;
  state.symbol = symbol.toUpperCase();
  const { base, quote } = splitSymbol(state.symbol);
  localStorage.setItem('rtd.symbol', state.symbol);

  pairTitle.textContent = `${base}/${quote}`;
  coinBase.textContent = base.charAt(0);
  statVolumeAsset.textContent = base;
  chart.applyOptions({ watermark: { text: `${base}/${quote}` } });

  state.lastTradePrice = null;
  resetChartData();

  closePairMenu();
  await Promise.all([loadHistory(), loadStats24h()]);
  connectWs();
}

function closePairMenu() {
  pairMenu.hidden = true;
  pairButton.setAttribute('aria-expanded', 'false');
}

function renderPairOptions(all) {
  const current = state.symbol;
  pairList.innerHTML = '';
  const needle = (pairFilter.value || '').trim().toUpperCase();
  const matches = all.filter(s => s.includes(needle));
  if (matches.length === 0) {
    pairList.innerHTML = '<li class="pair-option no-match">No pairs match</li>';
    return;
  }
  for (const symbol of matches) {
    const { base, quote } = splitSymbol(symbol);
    const li = document.createElement('li');
    const btn = document.createElement('button');
    btn.className = 'pair-option';
    btn.setAttribute('role', 'option');
    btn.setAttribute('aria-selected', String(symbol === current));
    btn.innerHTML = `<span>${base}/${quote}</span><small>Spot</small>`;
    btn.addEventListener('click', () => setSymbol(symbol));
    li.appendChild(btn);
    pairList.appendChild(li);
  }
}

function togglePairMenu() {
  const expanded = pairButton.getAttribute('aria-expanded') === 'true';
  pairMenu.hidden = expanded;
  pairButton.setAttribute('aria-expanded', String(!expanded));
  if (!expanded) {
    pairFilter.value = '';
    renderPairOptions(window._symbols || []);
    pairFilter.focus();
  }
}

pairButton.addEventListener('click', togglePairMenu);
pairFilter.addEventListener('input', () => renderPairOptions(window._symbols || []));
document.addEventListener('click', (ev) => {
  if (!pairMenu.hidden && !ev.target.closest('.pair-left')) closePairMenu();
});
document.addEventListener('keydown', (ev) => {
  if (ev.key === 'Escape') closePairMenu();
  if (ev.key === '/' && !ev.target.matches('input, textarea')) {
    ev.preventDefault();
    togglePairMenu();
  }
});

/* ---------- interval & chart type ---------- */
const intervalGroup = $('intervalGroup');
intervalGroup.addEventListener('click', (ev) => {
  const btn = ev.target.closest('.interval');
  if (!btn) return;
  intervalGroup.querySelectorAll('.interval').forEach(b => b.classList.toggle('active', b === btn));
  state.interval = btn.dataset.interval;
  state.bucketSec = INTERVAL_SECONDS[state.interval];
  resetChartData();
  loadHistory();
});

$('typeCandles').addEventListener('click', () => {
  $('typeCandles').classList.add('active');
  $('typeLine').classList.remove('active');
  candleSeries.applyOptions({ visible: true });
  lineSeries.applyOptions({ visible: false });
});
$('typeLine').addEventListener('click', () => {
  $('typeLine').classList.add('active');
  $('typeCandles').classList.remove('active');
  candleSeries.applyOptions({ visible: false });
  lineSeries.applyOptions({ visible: true });
});

/* ---------- boot ---------- */
(async function boot() {
  try {
    const { symbols } = await fetchJSON('/api/symbols');
    window._symbols = symbols;
    const saved = (localStorage.getItem('rtd.symbol') || '').toUpperCase();
    state.symbol = symbols.includes(saved) ? saved : 'BTCUSDT';
    const { base, quote } = splitSymbol(state.symbol);
    pairTitle.textContent = `${base}/${quote}`;
    coinBase.textContent = base.charAt(0);
    statVolumeAsset.textContent = base;
    chart.applyOptions({ watermark: { text: `${base}/${quote}` } });
    resizeChart();
    await Promise.all([loadHistory(), loadStats24h()]);
    connectWs();
  } catch (err) {
    showToast(`Failed to boot: ${err.message}`);
  }
})();
