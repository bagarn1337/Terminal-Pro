from fastapi import FastAPI, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi import Response
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import yfinance as yf
from functools import lru_cache
from datetime import datetime, timedelta
import json, os, time, asyncio, httpx
from datetime import datetime
from typing import Optional, List

# ── ALPACA CONFIG ─────────────────────────────────────────────────────────────
# Keys loaded from environment variables or .env file
try:
    from dotenv import load_dotenv
    load_dotenv()  # loads .env file in the same directory if present
except ImportError:
    pass  # python-dotenv not installed, use env vars only

ALPACA_KEY    = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET_KEY", "")
ALPACA_BROKER = "https://paper-api.alpaca.markets"
ALPACA_DATA   = "https://data.alpaca.markets"

def _alpaca_headers():
    return {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET}

# ── GLOBAL TTL CACHE ──────────────────────────────────────────────────────────
# Stores (timestamp, data). TTLs in seconds.
_CACHE: dict = {}
_CACHE_TTL = {
    "stock":           300,    # 5 min  — price data
    "chart":           900,    # 15 min — historical bars
    "ohlc":            900,
    "macro":           600,
    "calendar":        7200,   # 2 hr   — earnings dates
    "extended":        120,
    "news":            300,
    "analyst":         7200,   # 2 hr   — analyst consensus
    "analyst-detail":  7200,   # 2 hr   — per-firm roster
    "earnings":        7200,
    "options":         180,
    "fa":              14400,  # 4 hr   — fundamentals
    "drivers":         14400,
    "insiders":        7200,
    "risk":            900,
    "splc":            86400,  # 24 hr  — supply chain static
    "cmap":            14400,
    "mfid":            7200,
    "institutional":   7200,
    "default":         300,
}

def _cache_get(key: str):
    if key not in _CACHE:
        return None
    ts, data = _CACHE[key]
    prefix = key.split(":")[0]
    ttl = _CACHE_TTL.get(prefix, _CACHE_TTL["default"])
    if time.time() - ts > ttl:
        del _CACHE[key]
        return None
    return data

def _cache_set(key: str, data):
    _CACHE[key] = (time.time(), data)
    return data

# ── HARD TIMEOUT WRAPPER ──────────────────────────────────────────────────────
# Runs any callable in a thread pool with a hard wall-clock deadline.
# Returns result or raises concurrent.futures.TimeoutError on breach.
import concurrent.futures as _cf
_EXECUTOR = _cf.ThreadPoolExecutor(max_workers=16, thread_name_prefix="yf")

def _run(fn, timeout=15):
    """Run fn() in a thread, raise TimeoutError if it takes > timeout seconds."""
    fut = _EXECUTOR.submit(fn)
    return fut.result(timeout=timeout)

async def _run_async(fn, timeout=15):
    """Async version: offloads fn() to executor with hard timeout."""
    loop = asyncio.get_event_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(_EXECUTOR, fn),
            timeout=timeout
        )
    except asyncio.TimeoutError:
        raise _cf.TimeoutError(f"yfinance call timed out after {timeout}s")
async def _alpaca_latest_price(ticker: str) -> dict | None:
    """Fetch latest trade price from Alpaca. Returns dict with price, prev_close etc."""
    if not (ALPACA_KEY and ALPACA_SECRET):
        return None
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(
                f"{ALPACA_DATA}/v2/stocks/{ticker}/quotes/latest",
                headers=_alpaca_headers(),
                params={"feed": "iex"}
            )
            if r.status_code == 200:
                q = r.json().get("quote", {})
                bp = q.get("bp"); ap = q.get("ap")
                if bp and ap:
                    return {"price": round((bp + ap) / 2, 4), "bid": bp, "ask": ap}
    except Exception:
        pass
    # Try trades/latest as fallback
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(
                f"{ALPACA_DATA}/v2/stocks/{ticker}/trades/latest",
                headers=_alpaca_headers(),
                params={"feed": "iex"}
            )
            if r.status_code == 200:
                t_data = r.json().get("trade", {})
                p = t_data.get("p")
                if p:
                    return {"price": round(float(p), 4)}
    except Exception:
        pass
    return None

async def _alpaca_bars(ticker: str, timeframe: str = "1Day", limit: int = 252) -> list:
    """Fetch historical bars from Alpaca Data API."""
    if not (ALPACA_KEY and ALPACA_SECRET):
        return []
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f"{ALPACA_DATA}/v2/stocks/{ticker}/bars",
                headers=_alpaca_headers(),
                params={"timeframe": timeframe, "limit": limit, "feed": "iex", "adjustment": "split"}
            )
            if r.status_code == 200:
                return r.json().get("bars", [])
    except Exception as e:
        print(f"[alpaca_bars] {ticker}: {e}")
    return []

_universe_cache: list = []
_universe_ts: float   = 0

async def _get_alpaca_universe() -> list:
    global _universe_cache, _universe_ts
    if _universe_cache and (time.time() - _universe_ts) < 86400:
        return _universe_cache
    if not (ALPACA_KEY and ALPACA_SECRET):
        return []
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(
                f"{ALPACA_BROKER}/v2/assets",
                headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
                params={"asset_class": "us_equity", "status": "active", "tradable": "true"},
            )
        if r.status_code == 200:
            out = []
            for a in r.json():
                s = a.get("symbol", "")
                if not s or "/" in s or "." in s or len(s) > 5:
                    continue
                out.append({"symbol": s, "name": a.get("name", s), "exchange": a.get("exchange", "")})
            out.sort(key=lambda x: x["symbol"])
            _universe_cache = out
            _universe_ts = time.time()
            return out
    except Exception as e:
        print(f"[universe] {e}")
    return []

app = FastAPI()

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    # Always return valid JSON so frontend never gets a parse error
    return JSONResponse(
        status_code=200,
        content={"error": str(exc)[:200], "name": "N/A", "price": "N/A", "ratings": None, "history": [], "data": []}
    )

@app.exception_handler(500)
async def server_error_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=200,
        content={"error": "Internal server error", "ratings": None, "history": [], "data": []}
    )
app.add_middleware(GZipMiddleware, minimum_size=500)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
import os as _os
_BASE = _os.path.dirname(_os.path.abspath(__file__))
_STATIC = _os.path.join(_BASE, "static")
_INDEX  = _os.path.join(_BASE, "index.html")

# Serve from ./static/ if it exists, otherwise serve index.html from same dir
if _os.path.isdir(_STATIC):
    app.mount("/static", StaticFiles(directory=_STATIC), name="static")

@app.get("/")
async def root():
    static_index = _os.path.join(_STATIC, "index.html")
    path = static_index if _os.path.isfile(static_index) else (_INDEX if _os.path.isfile(_INDEX) else None)
    if not path:
        return JSONResponse({"error": "index.html not found"}, status_code=404)
    from fastapi.responses import Response
    with open(path, "rb") as f:
        data = f.read()
    return Response(
        content=data,
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        }
    )

import re as _re
_TICKER_RE = _re.compile(r'^[\^A-Z0-9\-\.=]{1,12}$')
def _valid_ticker(t: str) -> bool:
    return bool(_TICKER_RE.match(t.upper()))

ALERTS_FILE = "alerts.json"

def load_alerts():
    if os.path.exists(ALERTS_FILE):
        with open(ALERTS_FILE) as f:
            return json.load(f)
    return {}

def save_alerts(data):
    with open(ALERTS_FILE, "w") as f:
        json.dump(data, f)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    import base64
    from fastapi.responses import Response as _R
    # Minimal 1×1 transparent ICO — silences browser 404 noise
    ico = base64.b64decode(
        "AAABAAEAAQEAAAEAIAAoAAAAFgAAACgAAAABAAAAAgAAAAEAIAAAAAAA"
        "BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    )
    return _R(content=ico, media_type="image/x-icon",
              headers={"Cache-Control": "public, max-age=604800"})


# ── HOME DATA BULK ENDPOINT ────────────────────────────────────────────────────
# Single request returns movers + news. Backend fetches in parallel (no browser
# connection limit). Cache means repeat visits return in <200ms.
@app.get("/home-data")
async def get_home_data():
    import asyncio, math, xml.etree.ElementTree as ET
    import requests as _req

    HOME_TICKERS  = ["AAPL","MSFT","NVDA","TSLA","AMZN","META","GOOGL","AMD","JPM","SPY","QQQ","COIN"]
    NEWS_TICKERS  = ["SPY","QQQ","AAPL","MSFT","TSLA"]

    # ── Stock fetch (reuses per-ticker cache) ────────────────────────────────
    async def fetch_stock(t):
        cache_key = f"stock:{t.upper()}"
        cached = _cache_get(cache_key)
        if cached:
            return t, cached
        try:
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(None, lambda: yf.Ticker(t).info or {})
            def g(k, fb=None):
                v = info.get(k)
                if v is None: return fb
                try:
                    if math.isnan(float(v)): return fb
                except (TypeError, ValueError): pass
                return v
            price = g("currentPrice") or g("regularMarketPrice")
            prev  = g("previousClose") or g("regularMarketPreviousClose")
            chg   = ((price - prev) / prev * 100) if price and prev and prev != 0 else None
            result = {"ticker": t.upper(), "price": price, "previous_close": prev,
                      "company_name": g("longName") or g("shortName", t),
                      "change_pct": chg}
            _cache_set(cache_key, result)
            return t, result
        except Exception:
            return t, None

    # ── Shared publisher-from-URL helper ─────────────────────────────────────
    def _pub_from_url(url: str, fallback: str = "") -> str:
        from urllib.parse import urlparse
        DOMAIN_MAP = {
            "reuters.com": "Reuters", "bloomberg.com": "Bloomberg",
            "wsj.com": "WSJ", "ft.com": "FT", "cnbc.com": "CNBC",
            "marketwatch.com": "MarketWatch", "barrons.com": "Barron's",
            "fortune.com": "Fortune", "businessinsider.com": "Business Insider",
            "seekingalpha.com": "Seeking Alpha", "thestreet.com": "TheStreet",
            "investopedia.com": "Investopedia", "fool.com": "Motley Fool",
            "finance.yahoo.com": "Yahoo Finance", "benzinga.com": "Benzinga",
            "zacks.com": "Zacks", "morningstar.com": "Morningstar",
            "apnews.com": "AP News", "cnn.com": "CNN Business",
            "prnewswire.com": "PR Newswire", "businesswire.com": "BusinessWire",
            "globenewswire.com": "GlobeNewswire", "techcrunch.com": "TechCrunch",
        }
        try:
            host = urlparse(url).netloc.lower().lstrip("www.")
            for domain, name in DOMAIN_MAP.items():
                if domain in host:
                    return name
        except Exception:
            pass
        return fallback or "News"

    # ── News fetch via multiple RSS sources ──────────────────────────────────
    def fetch_news_rss(t: str):
        cache_key = f"news:{t.upper()}"
        cached = _cache_get(cache_key)
        if cached:
            return cached

        _hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        def _parse_rss(url):
            out = []
            try:
                r = _req.get(url, headers=_hdrs, timeout=7)
                if r.status_code != 200: return out
                root = ET.fromstring(r.content)
                ns   = {"media": "http://search.yahoo.com/mrss/"}
                for item in root.findall(".//item")[:12]:
                    title = (item.findtext("title") or "").strip()
                    if not title: continue
                    link  = (item.findtext("link") or "#").strip()
                    pub   = (item.findtext("pubDate") or "").strip()
                    src   = _pub_from_url(link, item.findtext("source") or "")
                    thumb = None
                    for tag in ("media:content", "media:thumbnail"):
                        el = item.find(tag, ns)
                        if el is not None: thumb = el.get("url"); break
                    out.append({"title": title, "publisher": src,
                                "url": link, "date": pub, "published_at": pub,
                                "image_url": thumb})
            except Exception: pass
            return out

        feeds = [
            f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={t}&region=US&lang=en-US",
            f"https://news.google.com/rss/search?q={t}+stock&hl=en-US&gl=US&ceid=US:en",
        ]
        from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
        seen, items = set(), []
        with ThreadPoolExecutor(max_workers=2) as ex:
            for fut in _as_completed([ex.submit(_parse_rss, u) for u in feeds]):
                for art in (fut.result() or []):
                    key = art["title"].lower().strip()
                    if key not in seen:
                        seen.add(key); items.append(art)

        if not items:
            try:
                for item in yf.Ticker(t).news[:10]:
                    c = item.get("content", {}); title = c.get("title","")
                    if not title: continue
                    link = c.get("canonicalUrl",{}).get("url","#")
                    pub  = _pub_from_url(link, c.get("provider",{}).get("displayName","Yahoo Finance"))
                    pd_  = c.get("pubDate","")
                    items.append({"title": title, "publisher": pub, "url": link,
                                  "date": pd_, "published_at": pd_, "image_url": None})
            except Exception: pass

        items.sort(key=lambda x: x.get("date") or "", reverse=True)
        _cache_set(cache_key, items)
        return items

    loop = asyncio.get_event_loop()

    # All stock fetches in parallel
    stock_pairs   = await asyncio.gather(*[fetch_stock(t) for t in HOME_TICKERS])
    # All news fetches in parallel via thread pool
    news_lists    = await asyncio.gather(*[loop.run_in_executor(None, fetch_news_rss, t) for t in NEWS_TICKERS])

    stocks = {t: d for t, d in stock_pairs if d and d.get("change_pct") is not None}
    ranked = sorted(stocks.values(), key=lambda x: x.get("change_pct", 0), reverse=True)

    seen, all_news = set(), []
    for items in news_lists:
        for n in items:
            key = (n.get("title") or "").strip().lower()
            if key and key not in seen:
                seen.add(key); all_news.append(n)
    all_news.sort(key=lambda x: x.get("date") or "", reverse=True)

    return {"stocks": list(stocks.values()),
            "winners": ranked[:8], "losers": list(reversed(ranked[-8:])),
            "news": all_news[:40]}


# ── STOCK ──────────────────────────────────────────────────────────────────────
@app.get("/stock/{ticker}")
async def get_stock(ticker: str):
    import math
    if not _valid_ticker(ticker):
        return {"name": "N/A", "price": "N/A", "error": f"Invalid ticker: {ticker}"}

    cache_key = f"stock:{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached:
        # Still try to refresh price from Alpaca even on cache hit
        ap = await _alpaca_latest_price(ticker.upper())
        if ap and ap.get("price"):
            cached["price"] = ap["price"]
        return cached

    try:
        def _fetch_stock():
            stock = yf.Ticker(ticker)
            info  = stock.info or {}
            def g(key, fallback=None):
                v = info.get(key)
                if v is None: return fallback
                try:
                    if math.isnan(float(v)): return fallback
                except (TypeError, ValueError): pass
                return v
            def stmt_val(stmt, *keys):
                try:
                    if stmt is None or stmt.empty: return None
                    for k in keys:
                        if k in stmt.index:
                            for v in stmt.loc[k]:
                                try:
                                    f = float(v)
                                    if f == f and f != 0: return f
                                except: pass
                except: pass
                return None
            try: inc = stock.income_stmt
            except: inc = None
            try: bal = stock.balance_sheet
            except: bal = None
            try: cf  = stock.cashflow
            except: cf = None
            revenue = g("totalRevenue") or stmt_val(inc, "Total Revenue", "Revenue")
            ebitda  = g("ebitda") or stmt_val(inc, "EBITDA", "Normalized EBITDA")
            free_cf = g("freeCashflow") or stmt_val(cf, "Free Cash Flow", "FreeCashFlow")
            cash = g("totalCash") or stmt_val(bal, "Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments")
            debt = g("totalDebt") or stmt_val(bal, "Total Debt", "Long Term Debt And Capital Lease Obligation", "Long Term Debt")
            gm = g("grossMargins")
            if gm is None:
                gp = stmt_val(inc, "Gross Profit")
                if gp and revenue: gm = gp / revenue
            om = g("operatingMargins")
            if om is None:
                oi = stmt_val(inc, "Operating Income", "EBIT")
                if oi and revenue: om = oi / revenue
            nm = g("profitMargins")
            if nm is None:
                ni = stmt_val(inc, "Net Income", "Net Income Common Stockholders")
                if ni and revenue: nm = ni / revenue
            roe = g("returnOnEquity")
            if roe is None:
                ni = stmt_val(inc, "Net Income")
                eq = stmt_val(bal, "Stockholders Equity", "Common Stock Equity", "Total Stockholders Equity")
                if ni and eq: roe = ni / eq
            de = g("debtToEquity")
            if de is None and debt:
                eq = stmt_val(bal, "Stockholders Equity", "Common Stock Equity", "Total Stockholders Equity")
                if eq and eq != 0: de = (debt / eq) * 100
            rev_growth = g("revenueGrowth")
            if rev_growth is None:
                try:
                    if inc is not None and not inc.empty:
                        for rk in ("Total Revenue", "Revenue"):
                            if rk in inc.index:
                                revs = [float(v) for v in inc.loc[rk] if v==v]
                                if len(revs)>=2 and revs[1]!=0: rev_growth=(revs[0]-revs[1])/abs(revs[1])
                                break
                except: pass
            earn_growth = g("earningsGrowth")
            if earn_growth is None:
                try:
                    if inc is not None and not inc.empty:
                        for nk in ("Net Income", "Net Income Common Stockholders"):
                            if nk in inc.index:
                                nis=[float(v) for v in inc.loc[nk] if v==v]
                                if len(nis)>=2 and nis[1]!=0: earn_growth=(nis[0]-nis[1])/abs(nis[1])
                                break
                except: pass
            pp=g("preMarketPrice"); ppc=g("preMarketChange"); ppcp=g("preMarketChangePercent")
            posp=g("postMarketPrice"); posc=g("postMarketChange"); poscp=g("postMarketChangePercent")
            yf_price = g("currentPrice") or g("regularMarketPrice", "N/A")
            return info, g, revenue, ebitda, free_cf, cash, debt, gm, om, nm, roe, de, rev_growth, earn_growth, pp, ppc, ppcp, posp, posc, poscp, yf_price

        info, g, revenue, ebitda, free_cf, cash, debt, gm, om, nm, roe, de, rev_growth, earn_growth, pp, ppc, ppcp, posp, posc, poscp, yf_price = await _run_async(_fetch_stock, timeout=15)
        ap = await _alpaca_latest_price(ticker.upper())
        live_price = ap["price"] if (ap and ap.get("price")) else yf_price

        result = {
            "name":g("longName","N/A"),"price": live_price,
            "pre_market_price":round(pp,2) if pp else None,
            "post_market_price":round(posp,2) if posp else None,
            "pre_market_change":round(ppc,2) if ppc else None,
            "post_market_change":round(posc,2) if posc else None,
            "pre_market_change_pct":round(ppcp*100,2) if ppcp else None,
            "post_market_change_pct":round(poscp*100,2) if poscp else None,
            "pre_market_volume":g("preMarketVolume"),"post_market_volume":g("postMarketVolume"),
            "market_state":g("marketState","REGULAR"),
            "pe_ratio":g("trailingPE","N/A"),"forward_pe":g("forwardPE","N/A"),
            "price_to_book":g("priceToBook","N/A"),"market_cap":g("marketCap","N/A"),
            "52w_high":g("fiftyTwoWeekHigh","N/A"),"52w_low":g("fiftyTwoWeekLow","N/A"),
            "beta":g("beta","N/A"),"target_mean":g("targetMeanPrice","N/A"),
            "avg_volume":g("averageVolume","N/A"),"volume":g("volume","N/A"),
            "shares_outstanding":g("sharesOutstanding","N/A"),"float_shares":g("floatShares","N/A"),
            "dividend_yield":g("dividendYield","N/A"),"short_ratio":g("shortRatio","N/A"),
            "short_percent":g("shortPercentOfFloat","N/A"),
            "sector":g("sector","N/A"),"country":g("country","N/A"),
            "employees":g("fullTimeEmployees","N/A"),"website":g("website","N/A"),
            "description":g("longBusinessSummary","N/A"),"previous_close":g("previousClose","N/A"),
            "recommendation":g("recommendationKey","N/A"),
            "revenue":revenue,"ebitda":ebitda,"free_cashflow":free_cf,
            "total_cash":cash,"total_debt":debt,
            "gross_margins":gm,"operating_margins":om,"profit_margin":nm,
            "return_on_equity":roe,"debt_to_equity":de,
            "revenue_growth":rev_growth,"earnings_growth":earn_growth,
            "eps":g("trailingEps","N/A"),"forward_eps":g("forwardEps","N/A"),
            "ev_ebitda":g("enterpriseToEbitda","N/A"),
            "ev_revenue":g("enterpriseToRevenue","N/A"),
            "enterprise_value":g("enterpriseValue","N/A"),
            "price_to_sales":g("priceToSalesTrailing12Months","N/A"),
            "peg_ratio":g("pegRatio","N/A"),
            "current_ratio":g("currentRatio","N/A"),
            "quick_ratio":g("quickRatio","N/A"),
            "return_on_assets":g("returnOnAssets","N/A"),
            "dividend_rate":g("dividendRate","N/A"),
            "payout_ratio":g("payoutRatio","N/A"),
            "ex_dividend_date":g("exDividendDate","N/A"),
            "five_year_avg_div_yield":g("fiveYearAvgDividendYield","N/A"),
            "52w_change":g("52WeekChange","N/A"),
            "city":g("city","N/A"),"state":g("state","N/A"),
            "industry":g("industry","N/A"),
            "exchange":g("exchange","N/A"),
            "currency":g("currency","N/A"),
            "held_percent_insiders":g("heldPercentInsiders","N/A"),
            "held_percent_institutions":g("heldPercentInstitutions","N/A"),
            "day_high":g("dayHigh","N/A"),
            "day_low":g("dayLow","N/A"),
            "earnings_date":g("earningsTimestamp","N/A"),
        }
        return _cache_set(cache_key, result)
    except Exception as e:
        print(f"Stock error {ticker}: {e}")
        return {"name":"N/A","price":"N/A","error":str(e)}

@app.get("/chart/{ticker}")
async def get_chart(ticker: str, period: str = "6mo"):
    cache_key = f"chart:{ticker.upper()}:{period}"
    cached = _cache_get(cache_key)
    if cached:
        return cached
    # Map period to Alpaca bar limit
    period_map = {"1d":1,"5d":5,"1mo":22,"3mo":66,"6mo":130,"1y":252,"2y":504,"5y":1260}
    limit = period_map.get(period, 130)
    bars = await _alpaca_bars(ticker.upper(), "1Day", limit)
    if bars:
        result = {
            "dates":   [b["t"][:10] for b in bars],
            "prices":  [round(float(b["c"]), 2) for b in bars],
            "volumes": [int(b["v"]) for b in bars],
        }
        return _cache_set(cache_key, result)
    # Fallback to yfinance
    try:
        hist = yf.Ticker(ticker).history(period=period)
        if hist.empty:
            return {"dates": [], "prices": [], "volumes": []}
        result = {
            "dates":   [str(d.date()) for d in hist.index],
            "prices":  [round(float(p), 2) for p in hist["Close"].tolist()],
            "volumes": [int(v) for v in hist["Volume"].tolist()],
        }
        return _cache_set(cache_key, result)
    except Exception as e:
        print(f"Chart error {ticker}: {e}")
        return {"dates": [], "prices": [], "volumes": []}

# ── OHLC / CANDLES ────────────────────────────────────────────────────────────
@app.get("/ohlc/{ticker}")
async def get_ohlc(ticker: str, period: str = "6mo"):
    cache_key = f"ohlc:{ticker.upper()}:{period}"
    cached = _cache_get(cache_key)
    if cached:
        return cached
    period_map = {"1d":1,"5d":5,"1mo":22,"3mo":66,"6mo":130,"1y":252,"2y":504,"5y":1260}
    limit = period_map.get(period, 130)
    bars = await _alpaca_bars(ticker.upper(), "1Day", limit)
    if bars:
        result = {
            "dates":   [b["t"][:10] for b in bars],
            "open":    [round(float(b["o"]), 4) for b in bars],
            "high":    [round(float(b["h"]), 4) for b in bars],
            "low":     [round(float(b["l"]), 4) for b in bars],
            "close":   [round(float(b["c"]), 4) for b in bars],
            "volumes": [int(b["v"]) for b in bars],
        }
        return _cache_set(cache_key, result)
    # Fallback to yfinance
    try:
        hist = yf.Ticker(ticker).history(period=period)
        if hist.empty:
            return {"dates": [], "open": [], "high": [], "low": [], "close": [], "volumes": []}
        result = {
            "dates":   [str(d.date()) for d in hist.index],
            "open":    [round(float(v), 4) for v in hist["Open"].tolist()],
            "high":    [round(float(v), 4) for v in hist["High"].tolist()],
            "low":     [round(float(v), 4) for v in hist["Low"].tolist()],
            "close":   [round(float(v), 4) for v in hist["Close"].tolist()],
            "volumes": [int(v) for v in hist["Volume"].tolist()],
        }
        return _cache_set(cache_key, result)
    except Exception as e:
        print(f"OHLC error {ticker}: {e}")
        return {"dates": [], "open": [], "high": [], "low": [], "close": [], "volumes": []}

# ── INTRADAY OHLC WITH PRE/POST MARKET ────────────────────────────────────────
@app.get("/ohlc-intraday/{ticker}")
async def get_ohlc_intraday(ticker: str, period: str = "5d"):
    cache_key = f"ohlc:{ticker.upper()}:intraday:{period}"
    cached = _cache_get(cache_key)
    if cached: return JSONResponse(content=cached)

    def _fetch():
        import pytz
        from datetime import time as dtime
        period_map = {"5d": ("5d", "1h"), "1mo": ("1mo", "1h")}
        yf_period, interval = period_map.get(period, ("5d", "1h"))
        stock = yf.Ticker(ticker)
        hist = stock.history(period=yf_period, interval=interval, prepost=True)
        if hist.empty:
            return {"dates":[],"open":[],"high":[],"low":[],"close":[],"volumes":[],"sessions":[]}
        et = pytz.timezone("America/New_York")
        market_open=dtime(9,30); market_close=dtime(16,0); pre_start=dtime(4,0); post_end=dtime(20,0)
        dates,opens,highs,lows,closes,volumes,sessions=[],[],[],[],[],[],[]
        for ts, row in hist.iterrows():
            ts_et = ts.tz_convert(et) if hasattr(ts,'tz_convert') else ts
            t = ts_et.time()
            if market_open <= t < market_close: session="regular"
            elif pre_start <= t < market_open: session="pre"
            elif market_close <= t <= post_end: session="post"
            else: continue
            dates.append(ts_et.strftime("%Y-%m-%d %H:%M"))
            opens.append(round(float(row["Open"]),4)); highs.append(round(float(row["High"]),4))
            lows.append(round(float(row["Low"]),4));   closes.append(round(float(row["Close"]),4))
            volumes.append(int(row["Volume"]));         sessions.append(session)
        return {"dates":dates,"open":opens,"high":highs,"low":lows,"close":closes,"volumes":volumes,"sessions":sessions,"intraday":True}

    try:
        result = await _run_async(_fetch, timeout=12)
        return JSONResponse(content=_cache_set(cache_key, result))
    except Exception:
        return JSONResponse(content={"dates":[],"open":[],"high":[],"low":[],"close":[],"volumes":[],"sessions":[],"intraday":True})


# ── NEWS ──────────────────────────────────────────────────────────────────────
@app.get("/news/{ticker}")
def get_news(ticker: str):
    import xml.etree.ElementTree as ET
    import requests as _req
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from urllib.parse import urlparse

    cache_key = f"news:{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    t = ticker.upper()
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    # ── Derive publisher name from URL ──────────────────────────────────────
    def publisher_from_url(url: str, fallback: str = "") -> str:
        DOMAIN_MAP = {
            "reuters.com": "Reuters", "bloomberg.com": "Bloomberg",
            "wsj.com": "WSJ", "ft.com": "FT", "cnbc.com": "CNBC",
            "marketwatch.com": "MarketWatch", "barrons.com": "Barron's",
            "fortune.com": "Fortune", "businessinsider.com": "Business Insider",
            "seekingalpha.com": "Seeking Alpha", "thestreet.com": "TheStreet",
            "investopedia.com": "Investopedia", "fool.com": "Motley Fool",
            "finance.yahoo.com": "Yahoo Finance", "yahoofinance.com": "Yahoo Finance",
            "benzinga.com": "Benzinga", "zacks.com": "Zacks",
            "morningstar.com": "Morningstar", "apnews.com": "AP News",
            "cnn.com": "CNN Business", "nytimes.com": "NY Times",
            "washingtonpost.com": "Washington Post", "economist.com": "The Economist",
            "techcrunch.com": "TechCrunch", "wired.com": "Wired",
            "prnewswire.com": "PR Newswire", "businesswire.com": "BusinessWire",
            "globenewswire.com": "GlobeNewswire",
        }
        try:
            host = urlparse(url).netloc.lower().lstrip("www.")
            for domain, name in DOMAIN_MAP.items():
                if domain in host:
                    return name
        except Exception:
            pass
        return fallback or "News"

    # ── Parse one RSS feed ───────────────────────────────────────────────────
    def parse_rss(url: str, default_pub: str = "") -> list:
        items = []
        try:
            resp = _req.get(url, headers=headers, timeout=7)
            if resp.status_code != 200:
                return []
            root = ET.fromstring(resp.content)
            ns   = {"media": "http://search.yahoo.com/mrss/",
                    "dc":    "http://purl.org/dc/elements/1.1/"}
            for item in root.findall(".//item")[:15]:
                title    = (item.findtext("title") or "").strip()
                if not title:
                    continue
                link     = (item.findtext("link") or "#").strip()
                pub_date = (item.findtext("pubDate") or
                            item.findtext("dc:date", namespaces=ns) or "").strip()
                # Try <source>, then URL-derive, then default
                raw_src  = (item.findtext("source") or "").strip()
                pub      = publisher_from_url(link, raw_src or default_pub)
                # Thumbnail
                thumb = None
                for tag in ("media:content", "media:thumbnail"):
                    el = item.find(tag, ns)
                    if el is not None:
                        thumb = el.get("url")
                        break
                items.append({
                    "title":        title,
                    "publisher":    pub,
                    "url":          link,
                    "date":         pub_date,
                    "published_at": pub_date,
                    "image_url":    thumb,
                })
        except Exception:
            pass
        return items

    # ── RSS sources to try in parallel ──────────────────────────────────────
    feeds = [
        # Yahoo Finance ticker RSS — fast, covers all symbols
        (f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={t}&region=US&lang=en-US", ""),
        # Google News for the ticker query
        (f"https://news.google.com/rss/search?q={t}+stock&hl=en-US&gl=US&ceid=US:en", "Google News"),
        # MarketWatch RSS
        (f"https://feeds.marketwatch.com/marketwatch/realtimeheadlines/", "MarketWatch"),
        # Seeking Alpha via Google News
        (f"https://news.google.com/rss/search?q={t}+seeking+alpha&hl=en-US&gl=US&ceid=US:en", "Seeking Alpha"),
    ]

    seen_titles: set = set()
    results: list = []

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(parse_rss, url, pub): pub for url, pub in feeds}
        for fut in as_completed(futures):
            for art in (fut.result() or []):
                key = art["title"].lower().strip()
                if key not in seen_titles:
                    seen_titles.add(key)
                    results.append(art)

    # Sort by date descending (best-effort)
    results.sort(key=lambda x: x.get("date") or "", reverse=True)

    # ── Fallback: yfinance if everything above failed ────────────────────────
    if not results:
        try:
            for item in yf.Ticker(ticker).news[:15]:
                content = item.get("content", {})
                title   = content.get("title", "")
                if not title:
                    continue
                thumb = None
                try:
                    rs = (content.get("thumbnail") or {}).get("resolutions", [])
                    if rs:
                        thumb = sorted(rs, key=lambda x: x.get("width", 999))[0].get("url")
                except Exception:
                    pass
                link = content.get("canonicalUrl", {}).get("url", "#")
                pub  = publisher_from_url(link,
                       content.get("provider", {}).get("displayName", "Yahoo Finance"))
                pub_date = content.get("pubDate", "")
                results.append({
                    "title": title, "publisher": pub, "url": link,
                    "date": pub_date, "published_at": pub_date, "image_url": thumb,
                })
        except Exception:
            pass

    _cache_set(cache_key, results)
    return results

@app.get("/proxy-image")
def proxy_image(url: str):
    """Proxy external images to bypass CORS/hotlink restrictions"""
    import requests as req
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://finance.yahoo.com/",
        }
        r = req.get(url, headers=headers, timeout=5)
        content_type = r.headers.get("content-type", "image/jpeg")
        return Response(content=r.content, media_type=content_type, headers={
            "Cache-Control": "public, max-age=86400",
            "Access-Control-Allow-Origin": "*",
        })
    except Exception as e:
        return Response(status_code=404)


# ── ANALYST ───────────────────────────────────────────────────────────────────
@app.get("/analyst/{ticker}")
async def get_analyst(ticker: str):
    import math
    cache_key = f"analyst:{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached:
        return JSONResponse(content=cached)

    def _safe(v):
        if v is None: return "N/A"
        try:
            f = float(v)
            if math.isnan(f) or math.isinf(f): return "N/A"
            return f
        except Exception:
            return str(v) if v not in ("", "nan", "None", "NaN") else "N/A"

    def _fetch():
        stock = yf.Ticker(ticker)
        info  = stock.info or {}
        result = {
            "target_high":    _safe(info.get("targetHighPrice")),
            "target_low":     _safe(info.get("targetLowPrice")),
            "target_mean":    _safe(info.get("targetMeanPrice")),
            "target_median":  _safe(info.get("targetMedianPrice")),
            "recommendation": info.get("recommendationKey") or "N/A",
            "num_analysts":   _safe(info.get("numberOfAnalystOpinions")),
            "ratings": None,
        }
        try:
            recs = stock.recommendations
            if recs is not None and not recs.empty:
                latest = recs.tail(1).to_dict(orient="records")[0]
                def _int(v):
                    try:
                        f = float(v)
                        return 0 if math.isnan(f) else int(f)
                    except: return 0
                result["ratings"] = {
                    "strong_buy":  _int(latest.get("strongBuy",  latest.get("Strong Buy",  0))),
                    "buy":         _int(latest.get("buy",        latest.get("Buy",         0))),
                    "hold":        _int(latest.get("hold",       latest.get("Hold",        0))),
                    "sell":        _int(latest.get("sell",       latest.get("Sell",        0))),
                    "strong_sell": _int(latest.get("strongSell", latest.get("Strong Sell", 0))),
                }
        except Exception:
            pass
        return result

    try:
        result = await _run_async(_fetch, timeout=15)
        _cache_set(cache_key, result)
        return JSONResponse(content=result)
    except Exception:
        return JSONResponse(content={"target_high":"N/A","target_low":"N/A","target_mean":"N/A","target_median":"N/A","recommendation":"N/A","num_analysts":"N/A","ratings":None})

# ── ANALYST DETAIL (individual firms + success rates) ─────────────────────────
@app.get("/analyst-detail/{ticker}")
async def get_analyst_detail(ticker: str):
    cache_key = f"analyst-detail:{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached:
        return JSONResponse(content=cached)

    def _fetch():
        stock = yf.Ticker(ticker)

        # upgrades_downgrades is fast (~1-2s), no price history needed
        ud = stock.upgrades_downgrades
        if ud is None or ud.empty:
            return {"analysts": [], "error": "No analyst data available"}

        ud = ud.copy()
        try:
            ud.index = pd.to_datetime(ud.index, utc=True)
        except Exception:
            pass

        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 2)
        ud = ud[ud.index >= cutoff]
        if ud.empty:
            return {"analysts": [], "error": "No recent analyst data"}

        _BUY_WORDS  = {"strong buy","outperform","overweight","buy","accumulate",
                       "positive","conviction buy","add","market outperform"}
        _SELL_WORDS = {"strong sell","underperform","underweight","sell",
                       "reduce","negative","avoid","market underperform"}

        def sentiment(g):
            if not g: return 0
            gl = g.lower().strip()
            if any(w in gl for w in _BUY_WORDS):  return  1
            if any(w in gl for w in _SELL_WORDS): return -1
            return 0

        # Latest call per firm
        latest: dict = {}
        for dt, row in ud.sort_index(ascending=False).iterrows():
            firm = str(row.get("Firm", row.get("firm", ""))).strip()
            if not firm or firm in ("nan", "None"): continue
            if firm not in latest:
                latest[firm] = (dt, row)

        # Count calls per firm for the call_count badge
        firm_counts: dict = {}
        for dt, row in ud.iterrows():
            firm = str(row.get("Firm", row.get("firm", ""))).strip()
            if not firm or firm in ("nan", "None"): continue
            firm_counts[firm] = firm_counts.get(firm, 0) + 1

        analysts = []
        for firm, (dt, row) in latest.items():
            to_g   = str(row.get("ToGrade",  row.get("to_grade",  ""))).strip()
            from_g = str(row.get("FromGrade", row.get("from_grade",""))).strip()
            action = str(row.get("Action",    row.get("action",   ""))).strip()
            analysts.append({
                "firm":         firm,
                "to_grade":     to_g   or "—",
                "from_grade":   from_g or "—",
                "action":       action or "—",
                "date":         dt.strftime("%Y-%m-%d"),
                "sentiment":    sentiment(to_g),
                "success_rate": None,   # requires price history — skipped for speed
                "call_count":   firm_counts.get(firm, 1),
            })

        analysts.sort(key=lambda a: a["date"], reverse=True)
        return {"analysts": analysts}

    try:
        result = await _run_async(_fetch, timeout=30)
        _cache_set(cache_key, result)
        return JSONResponse(content=result)
    except Exception:
        return JSONResponse(content={"analysts": [], "error": "Timed out or unavailable"})

# ── EARNINGS ──────────────────────────────────────────────────────────────────
@app.get("/earnings/{ticker}")
async def get_earnings(ticker: str):
    cache_key = f"earnings:{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached: return JSONResponse(content=cached)
    def _fetch():
        stock = yf.Ticker(ticker)
        try:
            hist = stock.earnings_history
            if hist is not None and not hist.empty:
                records = []
                def _c(v):
                    try: f=float(v); return None if f!=f else round(f,4)
                    except: return None
                for _, row in hist.tail(8).iterrows():
                    dstr = str(row.get("quarter","N/A"))[:10]; dstr = dstr if dstr not in ("nan","N/A","Non","None") else "N/A"
                    records.append({"date":dstr,"eps_actual":_c(row.get("epsActual")),"eps_estimate":_c(row.get("epsEstimate")),"surprise_pct":_c(row.get("surprisePercent"))})
                return {"history": records}
        except Exception:
            pass
        return {"history": []}
    try:
        result = await _run_async(_fetch, timeout=12)
        return JSONResponse(content=_cache_set(cache_key, result))
    except Exception:
        return JSONResponse(content={"history": []})

# ── FINANCIALS ────────────────────────────────────────────────────────────────
@app.get("/financials/{ticker}")
async def get_financials(ticker: str):
    cache_key = f"earnings:{ticker.upper()}"  # shared with earnings cache
    cached = _cache_get(cache_key)
    if cached and cached.get("periods"): return JSONResponse(content=cached)
    def _fetch():
        stock = yf.Ticker(ticker)
        try:
            inc = stock.income_stmt
            if inc is not None and not inc.empty:
                cols = [str(c.date()) for c in inc.columns[:4]]
                def row(key):
                    if key in inc.index:
                        return [float(inc.loc[key, c]) if inc.loc[key, c] is not None else None for c in inc.columns[:4]]
                    return [None]*4
                return {
                    "periods": cols,
                    "total_revenue": row("Total Revenue"),
                    "gross_profit": row("Gross Profit"),
                    "operating_income": row("Operating Income"),
                    "net_income": row("Net Income"),
                    "ebitda": row("EBITDA"),
                }
        except Exception:
            pass
        return {"periods": [], "total_revenue": [], "gross_profit": [], "operating_income": [], "net_income": [], "ebitda": []}
    try:
        return JSONResponse(content=await _run_async(_fetch, timeout=12))
    except Exception:
        return JSONResponse(content={"periods": [], "total_revenue": [], "gross_profit": [], "operating_income": [], "net_income": [], "ebitda": []})

# ── FULL FINANCIAL ANALYSIS (Bloomberg FA-style) ─────────────────────────────
@app.get("/fa/{ticker}")
async def get_fa(ticker: str):
    cache_key = f"fa:{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached:
        return JSONResponse(content=cached)

    def _fetch():
        import math
        stock = yf.Ticker(ticker)
        info  = stock.info or {}

        def safe(v):
            try:
                f = float(v)
                return None if math.isnan(f) else f
            except: return None

        def extract(stmt, keys):
            if stmt is None or stmt.empty: return [None]*5
            cols = stmt.columns[:5]
            for k in keys:
                if k in stmt.index:
                    out = []
                    for c in cols:
                        try:
                            v = float(stmt.loc[k, c])
                            out.append(None if math.isnan(v) else v)
                        except: out.append(None)
                    return out
            return [None]*len(cols)

        def periods(stmt):
            if stmt is None or stmt.empty: return []
            return [str(c.date()) for c in stmt.columns[:5]]

        try: inc = stock.income_stmt
        except: inc = None
        try: inc_q = stock.quarterly_income_stmt
        except: inc_q = None
        try: bal = stock.balance_sheet
        except: bal = None
        try: bal_q = stock.quarterly_balance_sheet
        except: bal_q = None
        try: cf  = stock.cashflow
        except: cf = None
        try: cf_q = stock.quarterly_cashflow
        except: cf_q = None

        def build_is(s):
            if s is None or s.empty: return {}
            p = periods(s)
            return {
                "periods": p,
                "revenue":           extract(s, ["Total Revenue","Revenue"]),
                "cost_of_revenue":   extract(s, ["Cost Of Revenue","Cost of Revenue","Total Expenses"]),
                "gross_profit":      extract(s, ["Gross Profit"]),
                "rd_expense":        extract(s, ["Research And Development","Research & Development"]),
                "sga_expense":       extract(s, ["Selling General Administrative","Selling General And Administrative Expense"]),
                "operating_expense": extract(s, ["Operating Expense","Total Operating Expenses"]),
                "operating_income":  extract(s, ["Operating Income","EBIT"]),
                "interest_expense":  extract(s, ["Interest Expense","Interest Expense Non Operating"]),
                "pretax_income":     extract(s, ["Pretax Income","Income Before Tax"]),
                "tax_provision":     extract(s, ["Tax Provision","Income Tax Expense"]),
                "net_income":        extract(s, ["Net Income","Net Income Common Stockholders"]),
                "ebitda":            extract(s, ["EBITDA","Normalized EBITDA"]),
                "eps_diluted":       extract(s, ["Diluted EPS","Basic EPS"]),
                "shares_diluted":    extract(s, ["Diluted Average Shares","Ordinary Shares Number"]),
            }

        def build_bs(s):
            if s is None or s.empty: return {}
            p = periods(s)
            return {
                "periods": p,
                "cash":               extract(s, ["Cash And Cash Equivalents","Cash Cash Equivalents And Short Term Investments"]),
                "short_investments":  extract(s, ["Other Short Term Investments","Short Term Investments"]),
                "receivables":        extract(s, ["Net Receivables","Accounts Receivable","Receivables"]),
                "inventory":          extract(s, ["Inventory","Inventories"]),
                "current_assets":     extract(s, ["Current Assets","Total Current Assets"]),
                "ppe_net":            extract(s, ["Net PPE","Property Plant Equipment Net","Net Property Plant And Equipment"]),
                "goodwill":           extract(s, ["Goodwill","Goodwill And Other Intangible Assets"]),
                "total_assets":       extract(s, ["Total Assets"]),
                "accounts_payable":   extract(s, ["Accounts Payable","Payables And Accrued Expenses"]),
                "short_debt":         extract(s, ["Current Debt","Short Long Term Debt","Current Debt And Capital Lease Obligation"]),
                "current_liabilities":extract(s, ["Current Liabilities","Total Current Liabilities"]),
                "long_debt":          extract(s, ["Long Term Debt","Long Term Debt And Capital Lease Obligation"]),
                "total_liabilities":  extract(s, ["Total Liabilities Net Minority Interest","Total Liabilities"]),
                "retained_earnings":  extract(s, ["Retained Earnings"]),
                "equity":             extract(s, ["Stockholders Equity","Common Stock Equity","Total Stockholders Equity"]),
            }

        def build_cf(s):
            if s is None or s.empty: return {}
            p = periods(s)
            return {
                "periods": p,
                "net_income":         extract(s, ["Net Income","Net Income From Continuing Operations"]),
                "depreciation":       extract(s, ["Depreciation And Amortization","Depreciation Amortization Depletion","Depreciation"]),
                "stock_comp":         extract(s, ["Stock Based Compensation","Share Based Compensation"]),
                "working_capital_chg":extract(s, ["Change In Working Capital","Changes In Working Capital"]),
                "operating_cf":       extract(s, ["Operating Cash Flow","Cash Flows From Operations","Total Cash From Operating Activities"]),
                "capex":              extract(s, ["Capital Expenditure","Capital Expenditures","Purchases Of Property Plant And Equipment"]),
                "free_cashflow":      extract(s, ["Free Cash Flow"]),
                "investing_cf":       extract(s, ["Investing Cash Flow","Total Cashflows From Investing Activities","Net Cash Used For Investing Activities"]),
                "debt_repayment":     extract(s, ["Repayment Of Debt","Long Term Debt Payments"]),
                "dividends_paid":     extract(s, ["Common Stock Dividend Paid","Payment Of Dividends","Dividends Paid"]),
                "buybacks":           extract(s, ["Repurchase Of Capital Stock","Common Stock Repurchase","Purchase Of Business"]),
                "financing_cf":       extract(s, ["Financing Cash Flow","Total Cash From Financing Activities","Net Cash Used Provided By Financing Activities"]),
                "net_change_cash":    extract(s, ["Changes In Cash","Net Change In Cash"]),
            }

        def ratios_from_info():
            g = lambda k: safe(info.get(k))
            return {
                "pe_trailing":    g("trailingPE"),
                "pe_forward":     g("forwardPE"),
                "peg":            g("trailingPegRatio"),
                "price_sales":    g("priceToSalesTrailing12Months"),
                "price_book":     g("priceToBook"),
                "ev_ebitda":      g("enterpriseToEbitda"),
                "ev_revenue":     g("enterpriseToRevenue"),
                "gross_margin":   g("grossMargins"),
                "operating_margin": g("operatingMargins"),
                "net_margin":     g("profitMargins"),
                "roe":            g("returnOnEquity"),
                "roa":            g("returnOnAssets"),
                "roic":           None,
                "debt_equity":    g("debtToEquity"),
                "current_ratio":  g("currentRatio"),
                "quick_ratio":    g("quickRatio"),
                "revenue_growth": g("revenueGrowth"),
                "earnings_growth":g("earningsGrowth"),
                "dividend_yield": g("dividendYield"),
                "payout_ratio":   g("payoutRatio"),
                "beta":           g("beta"),
                "short_float":    g("shortPercentOfFloat"),
            }

        return {
            "ticker":       ticker.upper(),
            "name":         info.get("longName","N/A"),
            "currency":     info.get("financialCurrency","USD"),
            "is_annual":    build_is(inc),
            "is_quarterly": build_is(inc_q),
            "bs_annual":    build_bs(bal),
            "bs_quarterly": build_bs(bal_q),
            "cf_annual":    build_cf(cf),
            "cf_quarterly": build_cf(cf_q),
            "ratios":       ratios_from_info(),
        }

    try:
        result = await _run_async(_fetch, timeout=18)
        _cache_set(cache_key, result)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(content={"ticker": ticker.upper(), "error": "Timed out or unavailable", "is_annual": {}, "is_quarterly": {}, "bs_annual": {}, "bs_quarterly": {}, "cf_annual": {}, "cf_quarterly": {}, "ratios": {}})



# ── MFID — Security Identifiers ──────────────────────────────────────────────
@app.get("/mfid/{ticker}")
async def get_mfid(ticker: str):
    import math
    cache_key = f"mfid:{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached: return JSONResponse(content=cached)
    def _fetch():
        stock = yf.Ticker(ticker)
        info  = stock.info or {}
        def g(k):
            v = info.get(k)
            return None if v is None or (isinstance(v, float) and math.isnan(v)) else v
        return {
            "ticker": ticker.upper(), "name": g("longName"), "short_name": g("shortName"),
            "exchange": g("exchange"), "exchange_tz": g("exchangeTimezoneName"),
            "currency": g("currency"), "quote_type": g("quoteType"), "market": g("market"),
            "sector": g("sector"), "industry": g("industry"), "country": g("country"),
            "city": g("city"), "address": g("address1"), "phone": g("phone"),
            "website": g("website"), "ir_website": g("irWebsite"),
            "employees": g("fullTimeEmployees"),
            "fiscal_year_end": g("fiscalYearEnd"),
            "most_recent_quarter": g("mostRecentQuarter"),
            "shares_outstanding": g("sharesOutstanding"),
            "float_shares": g("floatShares"),
            "held_pct_insiders": g("heldPercentInsiders"),
            "held_pct_institutions": g("heldPercentInstitutions"),
            "audit_risk": g("auditRisk"), "board_risk": g("boardRisk"),
            "compensation_risk": g("compensationRisk"),
            "overall_risk": g("overallRisk"),
        }
    try:
        result = await _run_async(_fetch, timeout=15)
        return JSONResponse(content=_cache_set(cache_key, result))
    except Exception:
        return JSONResponse(content={"ticker": ticker.upper(), "error": "Timed out"})

# ── CMAP — Revenue Segments & Geography ──────────────────────────────────────
@app.get("/cmap/{ticker}")
async def get_cmap(ticker: str):
    import math
    cache_key = f"cmap:{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached: return JSONResponse(content=cached)
    def _fetch():
        stock = yf.Ticker(ticker)
        info  = stock.info or {}
        def safe(v):
            try: f=float(v); return None if math.isnan(f) else f
            except: return None
        product_segments = []
        geo_segments = []
        try:
            seg = stock.income_stmt
            if seg is not None and not seg.empty:
                latest = seg.columns[0]
                for idx in seg.index:
                    label = str(idx)
                    if "Revenue" in label and label not in ("Total Revenue", "Revenue"):
                        v = safe(seg.loc[idx, latest])
                        if v and abs(v) > 0:
                            product_segments.append({"name": label, "value": v})
        except: pass
        return {
            "ticker": ticker.upper(), "name": info.get("longName",""),
            "total_revenue": safe(info.get("totalRevenue")),
            "sector": info.get("sector",""), "industry": info.get("industry",""),
            "country": info.get("country",""),
            "product_segments": product_segments, "geo_segments": geo_segments,
            "description": info.get("longBusinessSummary",""),
        }
    try:
        result = await _run_async(_fetch, timeout=15)
        return JSONResponse(content=_cache_set(cache_key, result))
    except Exception:
        return JSONResponse(content={"ticker": ticker.upper(), "error": "Timed out"})

# ── INSTITUTIONAL OWNERSHIP ───────────────────────────────────────────────────
@app.get("/institutional/{ticker}")
async def get_institutional(ticker: str):
    cache_key = f"institutional:{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached: return JSONResponse(content=cached)
    def _fetch():
        stock = yf.Ticker(ticker)
        try:
            inst = stock.institutional_holders
            if inst is not None and not inst.empty:
                records = []
                for _, row in inst.head(20).iterrows():
                    records.append({
                        "holder":  str(row.get("Holder", "N/A")),
                        "shares":  int(row["Shares"]) if "Shares" in row and row["Shares"] == row["Shares"] else None,
                        "date":    str(row.get("Date Reported", ""))[:10],
                        "pct_out": float(row["% Out"]) if "% Out" in row and row["% Out"] == row["% Out"] else None,
                        "value":   int(row["Value"]) if "Value" in row and row["Value"] == row["Value"] else None,
                    })
                return {"institutional": records}
        except Exception as e:
            print(f"[institutional] {e}")
        return {"institutional": []}
    try:
        result = await _run_async(_fetch, timeout=12)
        return JSONResponse(content=_cache_set(cache_key, result))
    except Exception:
        return JSONResponse(content={"institutional": []})

# ── INSIDER TRANSACTIONS ───────────────────────────────────────────────────────
@app.get("/insiders/{ticker}")
async def get_insiders(ticker: str):
    cache_key = f"insiders:{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached: return JSONResponse(content=cached)
    def _fetch():
        stock = yf.Ticker(ticker)
        try:
            ins = stock.insider_transactions
            if ins is not None and not ins.empty:
                records = []
                for _, row in ins.head(30).iterrows():
                    shares = row.get("Shares"); value = row.get("Value")
                    records.append({
                        "insider":     str(row.get("Insider", "N/A")),
                        "title":       str(row.get("Position", row.get("Title", ""))),
                        "transaction": str(row.get("Transaction", "N/A")),
                        "shares":      int(shares) if shares == shares and shares is not None else None,
                        "value":       int(value)  if value  == value  and value  is not None else None,
                        "date":        str(row.get("Start Date", row.get("Date", "")))[:10],
                        "ownership":   str(row.get("Ownership", "")),
                    })
                return {"insiders": records}
        except Exception as e:
            print(f"[insiders] {e}")
        return {"insiders": []}
    try:
        result = await _run_async(_fetch, timeout=12)
        return JSONResponse(content=_cache_set(cache_key, result))
    except Exception:
        return JSONResponse(content={"insiders": []})

# ── FINANCIAL DRIVERS (ratios, margins, growth multi-year) ────────────────────
@app.get("/drivers/{ticker}")
async def get_drivers(ticker: str):
    import math
    cache_key = f"drivers:{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached: return JSONResponse(content=cached)
    def _fetch():
        stock = yf.Ticker(ticker)
        info  = stock.info or {}
        def safe(v):
            try: f=float(v); return None if math.isnan(f) else f
            except: return None
        def stmt_rows(stmt, keys, n=4):
            if stmt is None or stmt.empty: return [None]*n
            cols = stmt.columns[:n]
            for k in keys:
                if k in stmt.index:
                    out = []
                    for c in cols:
                        try:
                            v = float(stmt.loc[k, c])
                            out.append(None if math.isnan(v) else v)
                        except: out.append(None)
                    return out
            return [None]*n
        def periods(stmt, n=4):
            if stmt is None or stmt.empty: return []
            return [str(c.date()) for c in stmt.columns[:n]]
        try: inc = stock.income_stmt
        except: inc = None
        try: bal = stock.balance_sheet
        except: bal = None
        try: cf  = stock.cashflow
        except: cf = None
        p = periods(inc)
        rev   = stmt_rows(inc, ["Total Revenue","Revenue"])
        gp    = stmt_rows(inc, ["Gross Profit"])
        oi    = stmt_rows(inc, ["Operating Income","EBIT"])
        ni    = stmt_rows(inc, ["Net Income","Net Income Common Stockholders"])
        ebitda= stmt_rows(inc, ["EBITDA","Normalized EBITDA"])
        fcf   = stmt_rows(cf,  ["Free Cash Flow"])
        ocf   = stmt_rows(cf,  ["Operating Cash Flow"])
        capex = stmt_rows(cf,  ["Capital Expenditure"])
        cash  = stmt_rows(bal, ["Cash And Cash Equivalents","Cash Cash Equivalents And Short Term Investments"])
        debt  = stmt_rows(bal, ["Total Debt","Long Term Debt And Capital Lease Obligation"])
        eq    = stmt_rows(bal, ["Stockholders Equity","Common Stock Equity"])
        def margin(num_list, den_list):
            out = []
            for n2, d in zip(num_list, den_list):
                if n2 and d and d != 0: out.append(round(n2/d*100, 2))
                else: out.append(None)
            return out
        return {
            "ticker": ticker.upper(), "periods": p,
            "revenue": rev, "gross_profit": gp, "operating_income": oi,
            "net_income": ni, "ebitda": ebitda, "free_cashflow": fcf,
            "operating_cashflow": ocf, "capex": capex, "cash": cash,
            "total_debt": debt, "equity": eq,
            "gross_margin": margin(gp, rev), "operating_margin": margin(oi, rev),
            "net_margin": margin(ni, rev), "ebitda_margin": margin(ebitda, rev),
            "fcf_margin": margin(fcf, rev),
            "revenue_growth": [
                round((rev[i]-rev[i+1])/abs(rev[i+1])*100, 2) if rev[i] and i+1<len(rev) and rev[i+1] else None
                for i in range(len(rev))
            ],
            "info": {
                "pe": safe(info.get("trailingPE")), "fpe": safe(info.get("forwardPE")),
                "ps": safe(info.get("priceToSalesTrailing12Months")),
                "pb": safe(info.get("priceToBook")),
                "ev_ebitda": safe(info.get("enterpriseToEbitda")),
                "roe": safe(info.get("returnOnEquity")), "roa": safe(info.get("returnOnAssets")),
                "de": safe(info.get("debtToEquity")), "cr": safe(info.get("currentRatio")),
                "beta": safe(info.get("beta")), "div_yield": safe(info.get("dividendYield")),
                "short_float": safe(info.get("shortPercentOfFloat")),
            }
        }
    try:
        result = await _run_async(_fetch, timeout=18)
        return JSONResponse(content=_cache_set(cache_key, result))
    except Exception:
        return JSONResponse(content={"ticker": ticker.upper(), "error": "Timed out"})

# ── OPTIONS ───────────────────────────────────────────────────────────────────
@app.get("/options/{ticker}")
async def get_options(ticker: str, expiry: Optional[str] = None):
    import math
    cache_key = f"options:{ticker.upper()}:{expiry or ''}"
    cached = _cache_get(cache_key)
    if cached: return JSONResponse(content=cached)

    def _fetch():
        import math as m
        def _f(v, d=4):
            try:
                f = float(v)
                return None if (m.isnan(f) or m.isinf(f)) else round(f, d)
            except: return None
        def _i(v):
            try:
                f = float(v)
                return 0 if (m.isnan(f) or m.isinf(f)) else int(f)
            except: return 0

        stock = yf.Ticker(ticker)
        expirations = stock.options
        if not expirations:
            return {"error": "No options data available"}
        exp = expiry if expiry in expirations else expirations[0]
        chain = stock.option_chain(exp)
        try:
            price = stock.fast_info.last_price or stock.fast_info.previous_close or 0
        except: price = 0

        def process_row(row):
            iv = _f(row.get("impliedVolatility"), 4)
            oi = _i(row.get("openInterest"))
            vol = _i(row.get("volume"))
            strike = _f(row.get("strike"), 2)
            bid = _f(row.get("bid"), 2) or 0
            ask = _f(row.get("ask"), 2) or 0
            last = _f(row.get("lastPrice"), 2) or 0
            mid = round((bid + ask) / 2, 2) if bid and ask else last
            delta = gamma = theta = vega = None
            if iv and price and strike:
                try:
                    S, K, r = price, strike, 0.045
                    try:
                        from datetime import datetime
                        T = max((datetime.strptime(exp, "%Y-%m-%d") - datetime.now()).days / 365, 1/365)
                    except: T = 30/365
                    d1 = (m.log(S/K) + (r + 0.5*iv**2)*T) / (iv * m.sqrt(T))
                    d2 = d1 - iv * m.sqrt(T)
                    def N(x):
                        a1,a2,a3,a4,a5=0.31938153,-0.356563782,1.781477937,-1.821255978,1.330274429
                        k=1.0/(1.0+0.2316419*abs(x))
                        val=1.0-(1.0/m.sqrt(2*m.pi))*m.exp(-x*x/2.0)*(a1*k+a2*k**2+a3*k**3+a4*k**4+a5*k**5)
                        return val if x>=0 else 1.0-val
                    def n(x): return (1/m.sqrt(2*m.pi))*m.exp(-0.5*x*x)
                    is_call = row.get("_type") == "call"
                    delta = round(N(d1) if is_call else N(d1)-1, 3)
                    gamma = round(n(d1)/(S*iv*m.sqrt(T)), 4)
                    theta = round((-S*n(d1)*iv/(2*m.sqrt(T))-r*K*m.exp(-r*T)*N(d2 if is_call else -d2))/365, 3)
                    vega  = round(S*n(d1)*m.sqrt(T)/100, 3)
                except: pass
            return {
                "strike": strike, "bid": bid, "ask": ask, "last": last, "mid": mid,
                "volume": vol, "openInterest": oi,
                "iv": round(iv*100, 1) if iv else None,
                "delta": delta, "gamma": gamma, "theta": theta, "vega": vega,
                "itm": (price > strike) if row.get("_type")=="call" else (price < strike),
            }

        def df_to_list(df, typ):
            rows = []
            for _, row in df.iterrows():
                d = dict(row); d["_type"] = typ
                rows.append(process_row(d))
            return rows

        calls = df_to_list(chain.calls, "call")
        puts  = df_to_list(chain.puts,  "put")

        max_pain_strike = None
        try:
            all_strikes = sorted(set([r["strike"] for r in calls+puts if r["strike"]]))
            call_oi = {r["strike"]: r["openInterest"] for r in calls}
            put_oi  = {r["strike"]: r["openInterest"] for r in puts}
            min_pain = None
            for tp in all_strikes:
                pain = sum((tp-s)*oi*100 for s,oi in call_oi.items() if s and tp>s) + \
                       sum((s-tp)*oi*100 for s,oi in put_oi.items()  if s and tp<s)
                if min_pain is None or pain < min_pain:
                    min_pain = pain; max_pain_strike = tp
        except: pass

        skew = []
        try:
            call_iv = {r["strike"]: r["iv"] for r in calls if r["strike"] and r["iv"]}
            put_iv  = {r["strike"]: r["iv"] for r in puts  if r["strike"] and r["iv"]}
            for s in sorted(set(list(call_iv)+list(put_iv))):
                skew.append({"strike":s,"call_iv":call_iv.get(s),"put_iv":put_iv.get(s)})
        except: pass

        total_call_oi = sum(r["openInterest"] for r in calls)
        total_put_oi  = sum(r["openInterest"] for r in puts)
        pcr = round(total_put_oi/total_call_oi, 2) if total_call_oi else None
        try: atm_iv = min(calls, key=lambda r: abs((r["strike"] or 0)-price) if r["strike"] else 999)["iv"]
        except: atm_iv = None

        return {
            "expiration": exp, "all_expirations": list(expirations[:12]),
            "price": round(price,2) if price else None,
            "calls": calls, "puts": puts,
            "max_pain": max_pain_strike, "put_call_ratio": pcr,
            "total_call_oi": total_call_oi, "total_put_oi": total_put_oi,
            "atm_iv": atm_iv, "skew": skew,
        }

    try:
        result = await _run_async(_fetch, timeout=15)
        _cache_set(cache_key, result)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(content={"error": str(e)[:200]})

# ── COMPARE ───────────────────────────────────────────────────────────────────
@app.get("/compare")
def compare(tickers: str):
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()][:4]
    results = []
    for t in ticker_list:
        try:
            stock = yf.Ticker(t)
            info = stock.info or {}
            hist = stock.history(period="6mo")
            results.append({
                "ticker": t,
                "name": info.get("longName", t),
                "price": info.get("currentPrice", "N/A"),
                "market_cap": info.get("marketCap", "N/A"),
                "pe_ratio": info.get("trailingPE", "N/A"),
                "52w_high": info.get("fiftyTwoWeekHigh", "N/A"),
                "52w_low": info.get("fiftyTwoWeekLow", "N/A"),
                "profit_margin": info.get("profitMargins", "N/A"),
                "revenue": info.get("totalRevenue", "N/A"),
                "roe": info.get("returnOnEquity", "N/A"),
                "beta": info.get("beta", "N/A"),
                "dividend_yield": info.get("dividendYield", "N/A"),
                "sector": info.get("sector", "N/A"),
                "prices": [round(p, 2) for p in hist["Close"].tolist()],
                "dates": [str(d.date()) for d in hist.index],
            })
        except Exception:
            results.append({"ticker": t, "error": True})
    return results

# ── MACRO ─────────────────────────────────────────────────────────────────────
@app.get("/macro")
async def get_macro():
    cache_key = "macro:main"
    cached = _cache_get(cache_key)
    if cached:
        return cached
    symbols = {
        "indices": {"S&P 500": "^GSPC", "Nasdaq": "^IXIC", "Dow Jones": "^DJI", "Russell 2000": "^RUT", "VIX": "^VIX"},
        "commodities": {"Gold": "GC=F", "Silver": "SI=F", "Crude Oil": "CL=F", "Natural Gas": "NG=F", "Copper": "HG=F"},
        "crypto": {"Bitcoin": "BTC-USD", "Ethereum": "ETH-USD", "Solana": "SOL-USD"},
        "forex": {"EUR/USD": "EURUSD=X", "GBP/USD": "GBPUSD=X", "USD/JPY": "JPY=X", "USD/SEK": "SEK=X"},
        "bonds": {"US 10Y": "^TNX", "US 2Y": "^IRX", "US 30Y": "^TYX"},
    }
    result = {}
    loop = asyncio.get_event_loop()
    sem = asyncio.Semaphore(4)

    async def fetch_sym(category, name, sym):
        async with sem:
            await asyncio.sleep(0.1)
            def _do():
                try:
                    t = yf.Ticker(sym)
                    hist = t.history(period="5d")
                    if not hist.empty:
                        price = round(hist["Close"].iloc[-1], 4)
                        prev  = round(hist["Close"].iloc[-2], 4) if len(hist) > 1 else price
                        chg   = round(price - prev, 4)
                        chg_pct = round((chg / prev) * 100, 2) if prev else 0
                        return {"price": price, "change": chg, "change_pct": chg_pct, "symbol": sym}
                except: pass
                return {"price": "N/A", "change": 0, "change_pct": 0, "symbol": sym}
            return category, name, await loop.run_in_executor(None, _do)

    tasks = []
    for category, items in symbols.items():
        result[category] = {}
        for name, sym in items.items():
            tasks.append(fetch_sym(category, name, sym))

    for coro in asyncio.as_completed(tasks):
        cat, name, data = await coro
        result[cat][name] = data

    return _cache_set(cache_key, result)

# ── UNIVERSE ENDPOINT ─────────────────────────────────────────────────────────
FALLBACK_SYMBOLS = [
    "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","BRK-B","JPM","V",
    "UNH","LLY","XOM","JNJ","WMT","MA","PG","HD","MRK","AVGO",
    "ORCL","COST","ABBV","CVX","BAC","KO","PEP","TMO","ADBE","CSCO",
    "CRM","AMD","INTC","NFLX","DIS","NKE","QCOM","TXN","AMGN","SBUX",
    "INTU","GS","MS","SPGI","AXP","CAT","DE","GE","HON","MMM",
    "LMT","RTX","BA","UPS","FDX","F","GM","RIVN","PLTR","SNOW",
    "NOW","CRWD","PANW","DDOG","NET","MU","AMAT","LRCX","ARM","SMCI",
    "UBER","PYPL","COIN","SQ","HOOD","ISRG","GILD","BMY","DHR","ABT",
    "LOW","BKNG","ABNB","DASH","SNAP","PINS","COP","SLB","EOG","NEE",
    "SO","DUK","T","VZ","TMUS","NIO","LCID","SPGI","ADP","MMM",
]

@app.get("/assets/universe")
async def assets_universe(page: int = 0, per_page: int = 500, q: str = ""):
    universe = await _get_alpaca_universe()
    if not universe:
        # fallback: wrap list in dict format
        universe = [{"symbol": s, "name": s, "exchange": ""} for s in FALLBACK_SYMBOLS]
    if q:
        ql = q.lower()
        universe = [u for u in universe if ql in u["symbol"].lower() or ql in u["name"].lower()]
    total = len(universe)
    start = page * per_page
    return {
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "items":    universe[start:start + per_page],
        "source":   "alpaca" if (ALPACA_KEY and ALPACA_SECRET) else "fallback",
    }

# ── SCREENER ──────────────────────────────────────────────────────────────────
@app.get("/screener")
def screener(
    min_pe: Optional[float] = None, max_pe: Optional[float] = None,
    min_cap: Optional[float] = None, max_cap: Optional[float] = None,
    min_margin: Optional[float] = None,
    min_roe: Optional[float] = None,
    sector: Optional[str] = None,
    min_div: Optional[float] = None,
    max_beta: Optional[float] = None,
    sort_by: str = "market_cap", sort_dir: str = "desc",
):
    results = []
    for t in FALLBACK_SYMBOLS:
        try:
            info = yf.Ticker(t).info
            pe   = info.get("trailingPE")
            cap  = info.get("marketCap")
            margin = info.get("profitMargins")
            roe  = info.get("returnOnEquity")
            sec  = info.get("sector", "")
            div  = info.get("dividendYield")
            beta = info.get("beta")
            price = info.get("currentPrice")

            if min_pe is not None and (pe is None or pe < min_pe): continue
            if max_pe is not None and (pe is None or pe > max_pe): continue
            if min_cap is not None and (cap is None or cap < min_cap * 1e9): continue
            if max_cap is not None and (cap is None or cap > max_cap * 1e9): continue
            if min_margin is not None and (margin is None or margin * 100 < min_margin): continue
            if min_roe is not None and (roe is None or roe * 100 < min_roe): continue
            if sector and sector.lower() not in (sec or "").lower(): continue
            if min_div is not None and (div is None or div * 100 < min_div): continue
            if max_beta is not None and (beta is None or beta > max_beta): continue

            results.append({
                "ticker": t, "name": info.get("longName", t),
                "price": price, "pe_ratio": pe, "market_cap": cap,
                "profit_margin": margin, "roe": roe, "sector": sec,
                "dividend_yield": div, "beta": beta,
                "revenue_growth": info.get("revenueGrowth"),
                "52w_high": info.get("fiftyTwoWeekHigh"),
                "52w_low": info.get("fiftyTwoWeekLow"),
            })
        except Exception:
            pass

    # Sort
    reverse = sort_dir == "desc"
    def sort_key(r):
        v = r.get(sort_by)
        if v is None:
            return (1, 0)
        return (0, -float(v) if reverse else float(v))
    results.sort(key=sort_key)
    return {"results": results, "count": len(results)}

# ── SHORT INTEREST ────────────────────────────────────────────────────────────
@app.get("/short-interest")
def get_short_interest():
    """Returns short interest data for major stocks via yfinance."""
    import concurrent.futures
    symbols = [
        "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","JPM","V","UNH",
        "LLY","XOM","JNJ","WMT","MA","PG","HD","MRK","AVGO","ORCL",
        "COST","ABBV","CVX","BAC","KO","PEP","ADBE","CSCO","CRM","AMD",
        "INTC","NFLX","DIS","NKE","QCOM","TXN","AMGN","SBUX","INTU","GS",
        "MS","AXP","CAT","GE","HON","LMT","BA","UPS","F","GM",
        "RIVN","PLTR","SNOW","NOW","CRWD","PANW","DDOG","NET","MU","AMAT",
        "UBER","PYPL","COIN","SQ","HOOD","ISRG","GILD","BMY","DHR","ABT",
        "BKNG","ABNB","SNAP","COP","SLB","NEE","T","VZ","TMUS","NIO",
    ]
    def fetch_one(t):
        try:
            info = yf.Ticker(t).info
            short_pct   = info.get("shortPercentOfFloat")
            short_ratio = info.get("shortRatio")        # days to cover
            shares_short= info.get("sharesShort")
            float_shares= info.get("floatShares")
            price       = info.get("currentPrice") or info.get("regularMarketPrice")
            cap         = info.get("marketCap")
            sector      = info.get("sector","")
            name        = info.get("shortName", t)
            # Squeeze score: high short % + low days-to-cover + recent price momentum
            squeeze = 0.0
            if short_pct: squeeze += min(short_pct * 100, 40)   # up to 40 pts
            if short_ratio and short_ratio < 3: squeeze += 20    # fast cover = squeeze risk
            return {
                "ticker": t, "name": name, "sector": sector,
                "short_pct": round(short_pct * 100, 2) if short_pct else None,
                "short_ratio": round(short_ratio, 1) if short_ratio else None,
                "shares_short": shares_short,
                "float_shares": float_shares,
                "price": price, "market_cap": cap,
                "squeeze_score": round(squeeze, 1),
            }
        except Exception:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        results = list(ex.map(fetch_one, symbols))

    results = [r for r in results if r and r["short_pct"] is not None]
    results.sort(key=lambda x: x["short_pct"] or 0, reverse=True)
    return results


# ── RISK METRICS ──────────────────────────────────────────────────────────────
@app.get("/risk/{ticker}")
async def get_risk(ticker: str, period: str = "1y"):
    cache_key = f"risk:{ticker.upper()}:{period}"
    cached = _cache_get(cache_key)
    if cached: return JSONResponse(content=cached)
    def _fetch():
        import numpy as np, math
        stock = yf.Ticker(ticker)
        spy   = yf.Ticker("SPY")
        hist      = stock.history(period=period)
        spy_hist  = spy.history(period=period)

    if hist.empty or len(hist) < 20:
        return {"error": "Not enough data"}

    # Align dates
    prices     = hist["Close"].dropna()
    spy_prices = spy_hist["Close"].dropna()
    common     = prices.index.intersection(spy_prices.index)
    prices     = prices.loc[common]
    spy_prices = spy_prices.loc[common]

    ret      = prices.pct_change().dropna().values
    spy_ret  = spy_prices.pct_change().dropna().values
    rf_daily = 0.05 / 252  # risk-free rate ~5% annualised

    # Volatility
    ann_vol = float(np.std(ret) * math.sqrt(252) * 100)

    # VaR 95% historical
    var_95 = float(np.percentile(ret, 5) * 100)

    # CVaR / Expected Shortfall
    cvar_95 = float(np.mean(ret[ret <= np.percentile(ret, 5)]) * 100)

    # Beta
    cov   = np.cov(ret, spy_ret)
    beta  = float(cov[0,1] / cov[1,1]) if cov[1,1] != 0 else 1.0

    # Sharpe
    excess = ret - rf_daily
    sharpe = float(np.mean(excess) / np.std(excess) * math.sqrt(252)) if np.std(excess) > 0 else 0.0

    # Sortino (downside deviation only)
    neg = excess[excess < 0]
    down_dev = float(np.std(neg) * math.sqrt(252)) if len(neg) > 0 else 0.001
    sortino = float(np.mean(excess) * 252 / down_dev)

    # Max Drawdown
    cum = np.cumprod(1 + ret)
    peak = np.maximum.accumulate(cum)
    dd   = (cum - peak) / peak
    max_dd = float(np.min(dd) * 100)

    # Calmar = annualised return / abs(max drawdown)
    ann_ret = float((np.prod(1 + ret) ** (252 / len(ret)) - 1) * 100)
    calmar  = float(ann_ret / abs(max_dd)) if max_dd != 0 else 0.0

    # Treynor = (ann_ret - rf_ann) / beta
    rf_ann = rf_daily * 252 * 100
    treynor = float((ann_ret - rf_ann) / beta) if beta != 0 else 0.0

    # Correlation to SPY
    corr = float(np.corrcoef(ret, spy_ret)[0, 1])

    # Upside/Downside capture
    up_mask   = spy_ret > 0
    dn_mask   = spy_ret < 0
    up_cap = float(np.mean(ret[up_mask])   / np.mean(spy_ret[up_mask])   * 100) if up_mask.sum()  > 0 else 0
    dn_cap = float(np.mean(ret[dn_mask])   / np.mean(spy_ret[dn_mask])   * 100) if dn_mask.sum()  > 0 else 0

    # Drawdown series (for chart) — sample every 5 days
    dd_series = [round(float(v*100), 2) for v in dd[::5]]
    dd_dates  = [str(d.date()) for d in common[1:][::5]][:len(dd_series)]

    return {
        "ticker":      ticker.upper(),
        "period":      period,
        "ann_vol":     round(ann_vol, 2),
        "var_95":      round(var_95, 2),
        "cvar_95":     round(cvar_95, 2),
        "beta":        round(beta, 2),
        "sharpe":      round(sharpe, 2),
        "sortino":     round(sortino, 2),
        "calmar":      round(calmar, 2),
        "treynor":     round(treynor, 2),
        "max_dd":      round(max_dd, 2),
        "ann_ret":     round(ann_ret, 2),
        "corr_spy":    round(corr, 2),
        "up_capture":  round(up_cap, 1),
        "dn_capture":  round(dn_cap, 1),
        "dd_series":   dd_series,
        "dd_dates":    dd_dates,
    }
    try:
        result = await _run_async(_fetch, timeout=15)
        return JSONResponse(content=_cache_set(cache_key, result))
    except Exception:
        return JSONResponse(content={"ticker": ticker.upper(), "error": "Timed out"})

# ── CORRELATION MATRIX ───────────────────────────────────────────────────────
@app.get("/correlation")
def get_correlation(tickers: str, period: str = "6mo"):
    """
    Returns pairwise Pearson correlation of daily returns for the given tickers.
    tickers = comma-separated, e.g. AAPL,MSFT,NVDA
    """
    import concurrent.futures, math
    syms = [t.strip().upper() for t in tickers.split(",") if t.strip()][:20]
    if len(syms) < 2:
        return {"error": "Need at least 2 tickers"}

    def fetch(t):
        try:
            h = yf.Ticker(t).history(period=period)
            if h.empty: return t, None
            closes = h["Close"].tolist()
            # daily returns
            rets = [(closes[i]-closes[i-1])/closes[i-1] for i in range(1,len(closes))]
            return t, rets
        except:
            return t, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        results = dict(ex.map(lambda t: fetch(t), syms))

    # Only keep tickers with data, aligned to same length
    valid = {k:v for k,v in results.items() if v}
    if len(valid) < 2:
        return {"error": "Not enough data"}

    min_len = min(len(v) for v in valid.values())
    aligned = {k: v[-min_len:] for k,v in valid.items()}
    tickers_out = list(aligned.keys())
    n = len(tickers_out)

    def pearson(a, b):
        na, nb = len(a), len(b)
        if na != nb or na < 2: return None
        ma = sum(a)/na; mb = sum(b)/nb
        num = sum((a[i]-ma)*(b[i]-mb) for i in range(na))
        da  = math.sqrt(sum((x-ma)**2 for x in a))
        db  = math.sqrt(sum((x-mb)**2 for x in b))
        if da==0 or db==0: return 0
        return round(num/(da*db), 4)

    matrix = []
    for i, ti in enumerate(tickers_out):
        row = []
        for j, tj in enumerate(tickers_out):
            row.append(1.0 if i==j else pearson(aligned[ti], aligned[tj]))
        matrix.append(row)

    # Also compute annualised volatility and beta vs first ticker
    vol_ann = {}
    for t, rets in aligned.items():
        std = math.sqrt(sum((r - sum(rets)/len(rets))**2 for r in rets)/len(rets))
        vol_ann[t] = round(std * math.sqrt(252) * 100, 2)

    return {
        "tickers": tickers_out,
        "matrix": matrix,
        "volatility": vol_ann,
        "period": period,
        "num_days": min_len
    }

# ── DCF ───────────────────────────────────────────────────────────────────────
@app.get("/dcf/{ticker}")
def dcf(ticker: str, growth_rate: float = 10, terminal_rate: float = 3, discount_rate: float = 10, years: int = 10):
    stock = yf.Ticker(ticker)
    info = stock.info or {}
    fcf = info.get("freeCashflow")
    shares = info.get("sharesOutstanding")
    price = info.get("currentPrice")
    if not fcf or not shares:
        return {"error": "Insufficient data for DCF"}

    g = growth_rate / 100
    tg = terminal_rate / 100
    r = discount_rate / 100

    cash_flows = []
    pv_flows = []
    cf = fcf
    for i in range(1, years + 1):
        cf = cf * (1 + g)
        pv = cf / ((1 + r) ** i)
        cash_flows.append(round(cf))
        pv_flows.append(round(pv))

    terminal_value = (cf * (1 + tg)) / (r - tg)
    pv_terminal = terminal_value / ((1 + r) ** years)
    total_pv = sum(pv_flows) + pv_terminal
    intrinsic_per_share = total_pv / shares

    upside = ((intrinsic_per_share - price) / price * 100) if price else None

    return {
        "ticker": ticker,
        "current_fcf": fcf,
        "shares_outstanding": shares,
        "current_price": price,
        "intrinsic_value": round(intrinsic_per_share, 2),
        "upside_pct": round(upside, 2) if upside else None,
        "terminal_value": round(pv_terminal),
        "sum_pv_flows": round(sum(pv_flows)),
        "total_pv": round(total_pv),
        "projected_flows": cash_flows,
        "pv_flows": pv_flows,
        "assumptions": {
            "growth_rate": growth_rate,
            "terminal_rate": terminal_rate,
            "discount_rate": discount_rate,
            "years": years,
        }
    }

# ── ECONOMIC CALENDAR ─────────────────────────────────────────────────────────
@app.get("/calendar")
async def get_calendar():
    """Earnings calendar with BMO/AMC, EPS/Rev estimates, expected move, real macro dates."""
    cache_key = "calendar:main"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    from datetime import datetime, timedelta
    events = []
    major = [
        "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","JPM","BAC","GS",
        "WMT","KO","PEP","JNJ","PFE","XOM","CVX","V","MA","UNH",
        "NFLX","ORCL","CRM","AMD","INTC","QCOM","AVGO","ADBE","DIS","SBUX",
        "NKE","MCD","HD","TGT","COST","AMGN","GILD","PYPL","SNAP","UBER"
    ]
    today = datetime.utcnow().date()

    # Throttle: max 5 concurrent yfinance calls, 0.3s between batches
    sem = asyncio.Semaphore(5)

    async def fetch_ticker_calendar(t):
        async with sem:
            try:
                await asyncio.sleep(0.15)  # small delay between each
                loop = asyncio.get_event_loop()
                def _do():
                    stock = yf.Ticker(t)
                    cal = stock.calendar
                    return stock, cal
                stock, cal = await loop.run_in_executor(None, _do)
                if not cal or not isinstance(cal, dict):
                    return None
                ed_raw = cal.get("Earnings Date")
                if not ed_raw:
                    return None
                if isinstance(ed_raw, list):
                    ed_raw = ed_raw[0]
                ed = str(ed_raw)[:10]
                if ed == "N/A":
                    return None
                try:
                    if datetime.strptime(ed, "%Y-%m-%d").date() < today:
                        return None
                except:
                    return None
                timing = "TBD"
                try:
                    et = str(cal.get("Earnings Time") or cal.get("Earnings Call Time") or "").lower()
                    if any(x in et for x in ["before","bmo","morning","pre-market"]):
                        timing = "BMO"
                    elif any(x in et for x in ["after","amc","evening","post-market","close"]):
                        timing = "AMC"
                except: pass
                eps_fmt = rev_fmt = None
                try:
                    v = cal.get("EPS Estimate")
                    if v not in (None,"N/A","nan"): eps_fmt = round(float(v), 2)
                except: pass
                try:
                    v = cal.get("Revenue Estimate (avg)") or cal.get("Revenue Estimate")
                    if v not in (None,"N/A","nan"): rev_fmt = float(v)
                except: pass
                return {"ticker": t, "date": ed, "timing": timing,
                        "eps_estimate": eps_fmt, "revenue_estimate": rev_fmt,
                        "expected_move_pct": None}
            except Exception as ex:
                print(f"[calendar] {t}: {ex}")
                return None

    tasks = [fetch_ticker_calendar(t) for t in major]
    results = await asyncio.gather(*tasks)
    events = [r for r in results if r is not None]

    now = datetime.utcnow()

    def nth_weekday_of_month(weekday, n, base=None):
        base = (base or now).replace(day=1)
        for _ in range(3):
            count = 0
            for d in range(31):
                try:
                    c = base + timedelta(days=d)
                    if c.month != base.month: break
                    if c.weekday() == weekday:
                        count += 1
                        if count == n and c.date() >= now.date():
                            return c.strftime("%Y-%m-%d")
                except: break
            base = (base + timedelta(days=32)).replace(day=1)
        return ""

    thu_delta = (3 - now.weekday()) % 7 or 7
    next_thu  = (now + timedelta(days=thu_delta)).strftime("%Y-%m-%d")

    macro = [
        {"type":"FED",   "name":"FOMC Meeting",          "impact":"HIGH",   "note":"Fed rate decision & press conference",        "date": nth_weekday_of_month(2,1)},
        {"type":"MACRO", "name":"US CPI",                 "impact":"HIGH",   "note":"Consumer Price Index — monthly inflation",    "date": nth_weekday_of_month(2,2)},
        {"type":"MACRO", "name":"US Non-Farm Payrolls",   "impact":"HIGH",   "note":"Monthly jobs report — first Friday",          "date": nth_weekday_of_month(4,1)},
        {"type":"MACRO", "name":"US GDP (Advance)",       "impact":"HIGH",   "note":"Quarterly GDP advance estimate",              "date":""},
        {"type":"MACRO", "name":"US PPI",                 "impact":"MEDIUM", "note":"Producer Price Index — monthly",              "date": nth_weekday_of_month(2,3)},
        {"type":"MACRO", "name":"Initial Jobless Claims", "impact":"MEDIUM", "note":"Weekly unemployment claims — every Thursday", "date": next_thu},
        {"type":"MACRO", "name":"ISM Manufacturing PMI",  "impact":"MEDIUM", "note":"Monthly manufacturing activity index",        "date": nth_weekday_of_month(0,1)},
        {"type":"MACRO", "name":"Retail Sales",           "impact":"MEDIUM", "note":"Monthly consumer spending data",              "date": nth_weekday_of_month(3,2)},
        {"type":"MACRO", "name":"Consumer Confidence",    "impact":"MEDIUM", "note":"Conference Board monthly survey",             "date": nth_weekday_of_month(1,4)},
        {"type":"MACRO", "name":"ECB Rate Decision",      "impact":"HIGH",   "note":"European Central Bank policy meeting",        "date":""},
        {"type":"MACRO", "name":"China PMI",              "impact":"MEDIUM", "note":"Caixin/NBS manufacturing PMI",                "date": nth_weekday_of_month(0,1)},
    ]

    result = {
        "earnings": sorted(events, key=lambda x: x.get("date","9999")),
        "macro":    macro,
        "generated": datetime.utcnow().isoformat(),
    }
    return _cache_set(cache_key, result)

@app.get("/alerts")
def get_alerts():
    return load_alerts()

@app.post("/alerts/{ticker}")
def set_alert(ticker: str, price: float, direction: str = "above"):
    alerts = load_alerts()
    alerts[ticker.upper()] = {"price": price, "direction": direction, "created": datetime.now().isoformat()}
    save_alerts(alerts)
    return {"ok": True}

@app.delete("/alerts/{ticker}")
def delete_alert(ticker: str):
    alerts = load_alerts()
    alerts.pop(ticker.upper(), None)
    save_alerts(alerts)
    return {"ok": True}

@app.get("/alerts/check")
def check_alerts():
    alerts = load_alerts()
    triggered = []
    for ticker, alert in alerts.items():
        try:
            info = yf.Ticker(ticker).info
            price = info.get("currentPrice")
            if price:
                if alert["direction"] == "above" and price >= alert["price"]:
                    triggered.append({"ticker": ticker, "current": price, "target": alert["price"], "direction": "above"})
                elif alert["direction"] == "below" and price <= alert["price"]:
                    triggered.append({"ticker": ticker, "current": price, "target": alert["price"], "direction": "below"})
        except Exception:
            pass
    return triggered

# ── SERVER-SENT EVENTS — LIVE QUOTES ──────────────────────────────────────────
@app.get("/live/{tickers}")
async def live_quotes(tickers: str):
    """
    Stream real-time price updates using Alpaca WebSocket.
    Falls back to yfinance polling if Alpaca keys not configured.
    """
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()][:30]

    ALPACA_KEY    = os.environ.get("ALPACA_API_KEY", "")
    ALPACA_SECRET = os.environ.get("ALPACA_SECRET_KEY", "")
    use_alpaca    = bool(ALPACA_KEY and ALPACA_SECRET)

    async def alpaca_stream():
        import websockets, json as _json
        ws_url = "wss://stream.data.alpaca.markets/v2/iex"
        prev_prices = {}
        try:
            async with websockets.connect(ws_url) as ws:
                # Auth
                await ws.send(_json.dumps({"action": "auth", "key": ALPACA_KEY, "secret": ALPACA_SECRET}))
                auth_resp = await asyncio.wait_for(ws.recv(), timeout=5)

                # Subscribe to trades
                await ws.send(_json.dumps({"action": "subscribe", "trades": ticker_list}))
                await asyncio.wait_for(ws.recv(), timeout=5)

                # Fetch initial prev_close from yfinance for change calculation
                for t in ticker_list:
                    try:
                        info = yf.Ticker(t).fast_info
                        prev = getattr(info, "previous_close", None)
                        if prev: prev_prices[t] = prev
                    except Exception:
                        pass

                last_emit = {}
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        messages = _json.loads(msg)
                        data = {}
                        for m in messages:
                            if m.get("T") == "t":  # trade message
                                sym   = m.get("S", "")
                                price = m.get("p", 0)
                                if sym and price:
                                    prev  = prev_prices.get(sym, price)
                                    chg   = round(price - prev, 4)
                                    chgp  = round(chg / prev * 100, 2) if prev else 0
                                    last  = last_emit.get(sym, 0)
                                    # Only emit if price changed
                                    if price != last:
                                        data[sym] = {
                                            "price": round(price, 4),
                                            "change": chg,
                                            "change_pct": chgp,
                                            "ts": int(time.time()),
                                            "source": "alpaca"
                                        }
                                        last_emit[sym] = price
                        if data:
                            yield f"data: {_json.dumps(data)}\n\n"
                    except asyncio.TimeoutError:
                        yield f"data: {{}}\n\n"  # keepalive
        except Exception as e:
            # Fall back to yfinance on any Alpaca error
            async for chunk in yfinance_stream():
                yield chunk

    async def yfinance_stream():
        prev_closes = {}
        # Pre-fetch previous closes once (avoid re-fetching every loop)
        for t in ticker_list:
            try:
                info = yf.Ticker(t).fast_info
                prev = getattr(info, "previous_close", None)
                if prev:
                    prev_closes[t] = prev
            except Exception:
                pass
        while True:
            data = {}
            try:
                # Batch fetch all tickers at once — much faster than one-by-one
                tickers_obj = yf.Tickers(" ".join(ticker_list))
                for t in ticker_list:
                    try:
                        info = tickers_obj.tickers[t].fast_info
                        price = getattr(info, "last_price", None)
                        prev  = prev_closes.get(t) or getattr(info, "previous_close", None)
                        if price and prev:
                            prev_closes[t] = prev
                            chg = round(price - prev, 4)
                            chg_pct = round(chg / prev * 100, 2)
                            data[t] = {"price": round(price, 4), "change": chg, "change_pct": chg_pct, "ts": int(time.time()), "source": "yfinance"}
                    except Exception:
                        pass
            except Exception:
                pass
            if data:
                yield f"data: {json.dumps(data)}\n\n"
            await asyncio.sleep(3)

    generator = alpaca_stream() if use_alpaca else yfinance_stream()

    return StreamingResponse(
        generator,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )



# ── EXPORT — screener results as CSV ─────────────────────────────────────────
@app.get("/export/screener")
def export_screener(tickers: str):
    """Return screener data for given tickers as CSV."""
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    rows = ["Ticker,Name,Price,Market Cap,P/E,Profit Margin,ROE,Rev Growth,Beta,Div Yield,Sector"]
    for t in ticker_list:
        try:
            info = yf.Ticker(t).info
            rows.append(",".join(str(x) for x in [
                t,
                (info.get("longName","") or "").replace(",",""),
                info.get("currentPrice","N/A"),
                info.get("marketCap","N/A"),
                info.get("trailingPE","N/A"),
                info.get("profitMargins","N/A"),
                info.get("returnOnEquity","N/A"),
                info.get("revenueGrowth","N/A"),
                info.get("beta","N/A"),
                info.get("dividendYield","N/A"),
                (info.get("sector","") or "").replace(",",""),
            ]))
        except Exception:
            rows.append(f"{t},Error,,,,,,,,,")
    csv = "\n".join(rows)
    return StreamingResponse(
        iter([csv]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=screener_export.csv"}
    )

# ── EXPORT — portfolio with live prices as CSV ────────────────────────────────
@app.post("/export/portfolio")
async def export_portfolio(request: Request):
    """Accepts JSON list of positions, enriches with live price, returns CSV."""
    positions = await request.json()  # [{ticker, shares, avgCost, type}]
    rows = ["Ticker,Type,Shares,Avg Cost,Current Price,Market Value,P&L,Return%"]
    for pos in positions:
        t = pos.get("ticker","").upper()
        shares = float(pos.get("shares", 0))
        avg_cost = float(pos.get("avgCost", 0))
        pos_type = pos.get("type","long")
        try:
            info = yf.Ticker(t).fast_info
            price = round(getattr(info, "last_price", 0) or 0, 4)
        except Exception:
            price = 0
        mkt_val = round(price * shares, 2)
        cost_basis = round(avg_cost * shares, 2)
        pnl = round((mkt_val - cost_basis) if pos_type == "long" else (cost_basis - mkt_val), 2)
        ret_pct = round(pnl / cost_basis * 100, 2) if cost_basis else 0
        rows.append(",".join(str(x) for x in [t, pos_type, shares, avg_cost, price, mkt_val, pnl, ret_pct]))
    csv = "\n".join(rows)
    return StreamingResponse(
        iter([csv]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=portfolio_export.csv"}
    )

# ── WATCHLIST / PORTFOLIO SERVER SYNC (optional persistence) ─────────────────
USERDATA_FILE = "userdata.json"

def load_userdata():
    if os.path.exists(USERDATA_FILE):
        with open(USERDATA_FILE) as f:
            return json.load(f)
    return {}

def save_userdata(data):
    with open(USERDATA_FILE, "w") as f:
        json.dump(data, f)

@app.get("/userdata/{user_id}")
def get_userdata(user_id: str):
    data = load_userdata()
    return data.get(user_id, {"watchlist": [], "portfolio": [], "alerts": {}})

@app.post("/userdata/{user_id}")
async def set_userdata(user_id: str, request: Request):
    payload = await request.json()
    data = load_userdata()
    data[user_id] = payload
    save_userdata(data)
    return {"ok": True}


# ══════════════════════════════════════════════════════════════════
# FEATURE: EARNINGS SURPRISE TRACKER
# ══════════════════════════════════════════════════════════════════
@app.get("/earnings-surprise/{ticker}")
async def get_earnings_surprise(ticker: str):
    cache_key = f"earnings:{ticker.upper()}:surprise"
    cached = _cache_get(cache_key)
    if cached: return JSONResponse(content=cached)
    def _fetch():
        stock = yf.Ticker(ticker)
        results = []
        try:
            hist = stock.earnings_history
            if hist is not None and not hist.empty:
                for _, row in hist.tail(8).iterrows():
                    eps_actual   = row.get("epsActual", None)
                    eps_estimate = row.get("epsEstimate", None)
                    surprise_pct = row.get("surprisePercent", None)
                    if eps_actual is None and eps_estimate is None: continue
                    results.append({
                        "date":         str(row.get("quarter", "N/A")),
                        "eps_actual":   float(eps_actual)   if eps_actual   is not None else None,
                        "eps_estimate": float(eps_estimate) if eps_estimate is not None else None,
                        "surprise_pct": float(surprise_pct) if surprise_pct is not None else None,
                    })
        except Exception: pass
        try:
            hist_prices = stock.history(period="1y")  # 1y instead of 2y — much faster
            price_map = {str(d.date()): float(c) for d, c in zip(hist_prices.index, hist_prices["Close"])}
            dates = sorted(price_map.keys())
            for r in results:
                try:
                    d = r["date"][:10]
                    idx = dates.index(d) if d in dates else None
                    if idx is not None and idx + 1 < len(dates):
                        p0 = price_map[dates[idx]]; p1 = price_map[dates[idx+1]]
                        r["price_reaction"] = round((p1-p0)/p0*100, 2)
                    else: r["price_reaction"] = None
                except: r["price_reaction"] = None
        except:
            for r in results: r["price_reaction"] = None
        return {"surprise": results}
    try:
        result = await _run_async(_fetch, timeout=12)
        return JSONResponse(content=_cache_set(cache_key, result))
    except Exception:
        return JSONResponse(content={"surprise": []})


# ══════════════════════════════════════════════════════════════════
# FEATURE: INSIDER TRANSACTIONS
# ══════════════════════════════════════════════════════════════════
@app.get("/insider/{ticker}")
async def get_insider(ticker: str):
    results = []
    try:
        stock = yf.Ticker(ticker)
        insider = stock.insider_transactions
        if insider is not None and not insider.empty:
            for _, row in insider.head(15).iterrows():
                shares = row.get("Shares", None)
                value  = row.get("Value", None)
                results.append({
                    "date":     str(row.get("Start Date", "N/A"))[:10],
                    "insider":  str(row.get("Insider", "N/A")),
                    "title":    str(row.get("Position", row.get("Title", "N/A"))),
                    "type":     str(row.get("Transaction", "N/A")),
                    "shares":   int(shares) if shares is not None and str(shares) != "nan" else None,
                    "value":    float(value) if value is not None and str(value) != "nan" else None,
                })
    except Exception:
        pass
    return {"transactions": results}


# ══════════════════════════════════════════════════════════════════
# FEATURE: NEWS SENTIMENT
# ══════════════════════════════════════════════════════════════════
@app.get("/sentiment/{ticker}")
async def get_sentiment(ticker: str):
    stock = yf.Ticker(ticker)
    articles = []
    bullish_kw = ["beat", "surge", "record", "growth", "upgrade", "buy", "bullish",
                  "profit", "strong", "rally", "gain", "outperform", "positive", "raise"]
    bearish_kw = ["miss", "drop", "decline", "downgrade", "sell", "bearish", "loss",
                  "weak", "fall", "cut", "risk", "negative", "concern", "lawsuit", "investigation"]
    try:
        for item in stock.news[:15]:
            content = item.get("content", {})
            title   = content.get("title", "") or ""
            summary = content.get("summary", "") or ""
            text    = (title + " " + summary).lower()
            pub     = content.get("pubDate", "")[:10]
            url     = content.get("canonicalUrl", {}).get("url", "#")
            provider = content.get("provider", {}).get("displayName", "")

            bull_hits = sum(1 for w in bullish_kw if w in text)
            bear_hits = sum(1 for w in bearish_kw if w in text)

            if bull_hits > bear_hits:
                sentiment = "bullish"
            elif bear_hits > bull_hits:
                sentiment = "bearish"
            else:
                sentiment = "neutral"

            articles.append({
                "title":     title,
                "url":       url,
                "publisher": provider,
                "date":      pub,
                "sentiment": sentiment,
                "bull_score": bull_hits,
                "bear_score": bear_hits,
            })
    except Exception:
        pass

    total   = len(articles)
    bullish = sum(1 for a in articles if a["sentiment"] == "bullish")
    bearish = sum(1 for a in articles if a["sentiment"] == "bearish")
    neutral = total - bullish - bearish
    score   = round((bullish - bearish) / total * 100) if total else 0

    return {
        "articles": articles,
        "summary": {
            "total":   total,
            "bullish": bullish,
            "bearish": bearish,
            "neutral": neutral,
            "score":   score,
        }
    }


# ══════════════════════════════════════════════════════════════════
# FEATURE: OPTIONS FLOW (unusual activity)
# ══════════════════════════════════════════════════════════════════
@app.get("/options-flow/{ticker}")
def get_options_flow(ticker: str):
    stock = yf.Ticker(ticker)
    unusual = []
    try:
        expirations = stock.options
        if not expirations:
            return {"flow": []}
        # Check next 3 expirations
        for exp in expirations[:3]:
            chain = stock.option_chain(exp)
            for opt_type, df in [("CALL", chain.calls), ("PUT", chain.puts)]:
                if df is None or df.empty:
                    continue
                for _, row in df.iterrows():
                    volume = row.get("volume", 0) or 0
                    oi     = row.get("openInterest", 0) or 0
                    if oi > 0 and volume > oi * 0.5 and volume > 500:
                        unusual.append({
                            "expiry":     exp,
                            "type":       opt_type,
                            "strike":     float(row.get("strike", 0)),
                            "volume":     int(volume),
                            "open_interest": int(oi),
                            "vol_oi_ratio":  round(volume / oi, 2) if oi else None,
                            "implied_vol":   round(float(row.get("impliedVolatility", 0)) * 100, 1),
                            "last_price":    float(row.get("lastPrice", 0)),
                            "in_the_money":  bool(row.get("inTheMoney", False)),
                        })
    except Exception:
        pass

    # Sort by vol/OI ratio descending
    unusual.sort(key=lambda x: x.get("vol_oi_ratio") or 0, reverse=True)
    return {"flow": unusual[:20]}


# ── ORDER FLOW DEBUG ─────────────────────────────────────────────────────────
@app.get("/orderflow-debug")
async def orderflow_debug():
    result = {
        "has_key":    bool(ALPACA_KEY),
        "has_secret": bool(ALPACA_SECRET),
        "key_prefix": ALPACA_KEY[:6]+"..." if ALPACA_KEY else None,
        "steps": []
    }
    if not ALPACA_KEY or not ALPACA_SECRET:
        result["steps"].append("FAIL: no keys loaded (check ALPACA_API_KEY / ALPACA_SECRET_KEY env vars)")
        return result

    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{ALPACA_BROKER}/v2/account", headers=_alpaca_headers())
            result["steps"].append(f"REST /account: {r.status_code}")
            if r.status_code == 200:
                acc = r.json()
                result["account_status"] = acc.get("status")
    except Exception as e:
        result["steps"].append(f"REST error: {e}")

    for feed in ["iex", "sip"]:
        try:
            import websockets as _ws
            connect_fn = getattr(_ws, 'connect', None)
            async with connect_fn(f"wss://stream.data.alpaca.markets/v2/{feed}") as ws:
                welcome = await asyncio.wait_for(ws.recv(), timeout=5)
                result["steps"].append(f"WS {feed} welcome: {welcome[:80]}")
                await ws.send(json.dumps({"action":"auth","key":ALPACA_KEY,"secret":ALPACA_SECRET}))
                auth = await asyncio.wait_for(ws.recv(), timeout=5)
                result["steps"].append(f"WS {feed} auth: {auth[:120]}")
        except Exception as e:
            result["steps"].append(f"WS {feed} error: {str(e)[:120]}")

    return result
@app.get("/orderflow/{ticker}")
async def orderflow_stream(ticker: str):
    sym = ticker.strip().upper()
    use_alpaca = bool(ALPACA_KEY and ALPACA_SECRET)

    def classify_size(size: int, avg_size: float) -> str:
        if size >= avg_size * 5 or size >= 500: return "block"
        if size >= avg_size * 2 or size >= 100: return "large"
        if size >= 10: return "medium"
        return "small"

    async def alpaca_tape():
        # Try SIP feed first, fall back to IEX
        feeds = ["wss://stream.data.alpaca.markets/v2/iex", "wss://stream.data.alpaca.markets/v2/sip"]
        sizes = []

        async def run_feed(ws_url):
            import websockets as _ws
            prev_close = 0
            try:
                prev_close = getattr(yf.Ticker(sym).fast_info, "previous_close", 0) or 0
            except: pass
            last_price = prev_close or 0
            connect_fn = getattr(_ws, 'connect', None)

            async with connect_fn(ws_url) as ws:
                # Welcome message
                await asyncio.wait_for(ws.recv(), timeout=8)
                # Auth using module-level globals
                await ws.send(json.dumps({"action":"auth","key":ALPACA_KEY,"secret":ALPACA_SECRET}))
                auth_resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=8))
                if isinstance(auth_resp, list) and auth_resp[0].get("T") == "error":
                    raise Exception(f"Auth failed: {auth_resp[0].get('msg')}")
                # Subscribe
                await ws.send(json.dumps({"action":"subscribe","trades":[sym]}))
                await asyncio.wait_for(ws.recv(), timeout=8)
                # Send immediate confirmation so frontend cancels its fallback timer
                yield f"data: {json.dumps({'connected':True,'feed':'alpaca-iex','real':True})}\n\n"
                # Stream
                while True:
                    try:
                        messages = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
                        for t in (messages if isinstance(messages, list) else [messages]):
                            if t.get("T") != "t": continue
                            price = float(t.get("p", 0))
                            size  = int(t.get("s", 0))
                            if not price: continue
                            sizes.append(size)
                            avg  = sum(sizes[-100:]) / max(1, len(sizes[-100:]))
                            side = "buy" if price >= last_price else "sell"
                            last_price = price
                            yield f"data: {json.dumps({'price':round(price,4),'size':size,'side':side,'tier':classify_size(size,avg),'ts':t.get('t','')[:19].replace('T',' '),'cond':t.get('c',[]),'real':True})}\n\n"
                    except asyncio.TimeoutError:
                        yield "data: {\"ping\":1}\n\n"

        for feed_url in feeds:
            try:
                async for chunk in run_feed(feed_url):
                    yield chunk
                return
            except Exception as e:
                print(f"[orderflow] {feed_url} failed: {e}")
                continue

        print(f"[orderflow] falling back to simulation for {sym}")
        async for chunk in simulated_tape():
            yield chunk

    async def simulated_tape():
        """Poll yfinance every ~1s and synthesize a tape from price movements."""
        import random
        prev_price = 0
        prev_close = 0
        try:
            info = yf.Ticker(sym).fast_info
            prev_price = getattr(info, "last_price", 0) or 0
            prev_close = getattr(info, "previous_close", 0) or prev_price
        except: pass

        sizes = []
        while True:
            try:
                info = yf.Ticker(sym).fast_info
                price = getattr(info, "last_price", None) or prev_price
                if price and price != prev_price:
                    # Synthesize a burst of trades to fill the gap
                    n_trades = random.randint(1, 4)
                    for i in range(n_trades):
                        sz = random.choices(
                            [random.randint(1,9), random.randint(10,99), random.randint(100,499), random.randint(500,2000)],
                            weights=[40, 35, 20, 5]
                        )[0]
                        sizes.append(sz)
                        avg = sum(sizes[-50:]) / max(1, len(sizes[-50:]))
                        side = "buy" if price > prev_price else "sell"
                        px = round(price + random.uniform(-0.01, 0.01), 2)
                        trade = {
                            "price": px,
                            "size":  sz,
                            "side":  side,
                            "tier":  classify_size(sz, avg),
                            "ts":    time.strftime("%Y-%m-%d %H:%M:%S"),
                            "cond":  [],
                            "sim":   True,
                        }
                        yield f"data: {json.dumps(trade)}\n\n"
                        await asyncio.sleep(0.15)
                    prev_price = price
            except Exception: pass
            await asyncio.sleep(1.0)

    gen = alpaca_tape() if use_alpaca else simulated_tape()
    return StreamingResponse(gen, media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


# ══════════════════════════════════════════════════════════════════
# FEATURE: EMAIL PRICE ALERTS (via SMTP)
# ══════════════════════════════════════════════════════════════════
import smtplib
from email.mime.text import MIMEText

SMTP_HOST     = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER     = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
ALERT_EMAIL   = os.environ.get("ALERT_EMAIL", "")  # Where to send alerts

def send_alert_email(ticker: str, direction: str, target: float, current: float):
    if not SMTP_USER or not SMTP_PASSWORD or not ALERT_EMAIL:
        return False
    try:
        subject = f"Terminal Pro Alert: {ticker} {'above' if direction == 'above' else 'below'} ${target}"
        body = f"""
Terminal Pro Price Alert

{ticker} has {'risen above' if direction == 'above' else 'fallen below'} your target of ${target:.2f}.

Current price: ${current:.2f}

View {ticker} → http://localhost:8000/?ticker={ticker}

—
Terminal Pro
        """.strip()
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"]    = SMTP_USER
        msg["To"]      = ALERT_EMAIL
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASSWORD)
            s.sendmail(SMTP_USER, ALERT_EMAIL, msg.as_string())
        return True
    except Exception as e:
        print(f"Email send failed: {e}")
        return False


@app.get("/alerts/check-and-notify")
async def check_and_notify():
    """Check all alerts and send emails for triggered ones."""
    alerts = load_alerts()
    triggered = []
    for user_id, user_alerts in alerts.items():
        for alert in user_alerts:
            ticker    = alert.get("ticker", "")
            direction = alert.get("direction", "above")
            target    = float(alert.get("price", 0))
            try:
                info    = yf.Ticker(ticker).fast_info
                current = round(getattr(info, "last_price", 0) or 0, 2)
                hit = (direction == "above" and current >= target) or \
                      (direction == "below" and current <= target)
                if hit:
                    sent = send_alert_email(ticker, direction, target, current)
                    triggered.append({
                        "ticker": ticker,
                        "direction": direction,
                        "target": target,
                        "current": current,
                        "email_sent": sent
                    })
            except Exception:
                pass
    return {"triggered": triggered, "count": len(triggered)}

# ══════════════════════════════════════════════════════════════════
# HEATMAP — S&P 500 sector heatmap with live % change
# ══════════════════════════════════════════════════════════════════
HEATMAP_UNIVERSE = {
    "Technology": [
        ("AAPL","Apple"),("MSFT","Microsoft"),("NVDA","NVIDIA"),("AVGO","Broadcom"),
        ("ORCL","Oracle"),("ADBE","Adobe"),("CRM","Salesforce"),("AMD","AMD"),
        ("INTC","Intel"),("QCOM","Qualcomm"),("TXN","Texas Instruments"),("INTU","Intuit"),("SNOW","Snowflake"),
    ],
    "Communication": [
        ("GOOGL","Alphabet"),("META","Meta"),("NFLX","Netflix"),("DIS","Disney"),("CMCSA","Comcast"),("T","AT&T"),
    ],
    "Consumer Discretionary": [
        ("AMZN","Amazon"),("TSLA","Tesla"),("HD","Home Depot"),("NKE","Nike"),
        ("SBUX","Starbucks"),("F","Ford"),("GM","General Motors"),("RIVN","Rivian"),
    ],
    "Financials": [
        ("BRK-B","Berkshire"),("JPM","JPMorgan"),("V","Visa"),("MA","Mastercard"),
        ("GS","Goldman"),("MS","Morgan Stanley"),("BAC","Bank of America"),("AXP","Amex"),("SPGI","S&P Global"),
    ],
    "Healthcare": [
        ("UNH","UnitedHealth"),("LLY","Eli Lilly"),("JNJ","Johnson & Johnson"),
        ("MRK","Merck"),("ABBV","AbbVie"),("TMO","Thermo Fisher"),("AMGN","Amgen"),
    ],
    "Consumer Staples": [
        ("WMT","Walmart"),("PG","Procter & Gamble"),("KO","Coca-Cola"),
        ("PEP","PepsiCo"),("COST","Costco"),
    ],
    "Energy": [
        ("XOM","ExxonMobil"),("CVX","Chevron"),
    ],
    "Industrials": [
        ("CAT","Caterpillar"),("DE","Deere"),("GE","GE"),("HON","Honeywell"),
        ("LMT","Lockheed"),("RTX","RTX"),("BA","Boeing"),("UPS","UPS"),("FDX","FedEx"),
    ],
    "Other": [
        ("PLTR","Palantir"),("MMM","3M"),
    ],
}

_heatmap_cache = {"data": None, "ts": 0}

@app.get("/heatmap")
def get_heatmap(response: Response):
    import time
    global _heatmap_cache
    now = time.time()
    if _heatmap_cache["data"] and now - _heatmap_cache["ts"] < 60:
        response.headers["Cache-Control"] = "max-age=60"
        return _heatmap_cache["data"]
    result = []
    for sector, tickers in HEATMAP_UNIVERSE.items():
        stocks = []
        for sym, name in tickers:
            try:
                t = yf.Ticker(sym)
                info = t.fast_info
                price = round(getattr(info, "last_price", 0) or 0, 2)
                prev  = round(getattr(info, "previous_close", 0) or 0, 2)
                chg_pct = round((price - prev) / prev * 100, 2) if prev else 0
                mktcap = getattr(info, "market_cap", None) or 0
                volume = getattr(info, "three_month_average_volume", None) or 0
                week_52_high = getattr(info, "year_high", None) or 0
                week_52_low  = getattr(info, "year_low", None) or 0
                # Intraday sparkline — 1d at 5m intervals
                try:
                    hist = t.history(period="1d", interval="5m")
                    spark = [round(float(v), 2) for v in hist["Close"].tolist() if v == v][-24:]
                except:
                    spark = []
                stocks.append({
                    "symbol": sym, "name": name,
                    "price": price, "prev_close": prev,
                    "change_pct": chg_pct,
                    "market_cap": mktcap,
                    "volume": volume,
                    "week_52_high": week_52_high,
                    "week_52_low": week_52_low,
                    "spark": spark,
                })
            except Exception:
                stocks.append({"symbol": sym, "name": name, "price": 0, "prev_close": 0, "change_pct": 0, "market_cap": 0, "spark": []})
        result.append({"sector": sector, "stocks": stocks})
    _heatmap_cache["data"] = result
    _heatmap_cache["ts"] = time.time()
    response.headers["Cache-Control"] = "max-age=60"
    return result


# ══════════════════════════════════════════════════════════════════
# DSCO — Document / Filing Search (proxies SEC EDGAR)
# ══════════════════════════════════════════════════════════════════
_dsco_cache: dict = {}

@app.get("/dsco/{ticker}")
async def get_dsco(ticker: str, page: int = 1, per_page: int = 50):
    t = ticker.upper().strip()
    cache_key = f"dsco_{t}"
    per_page = 50

    if cache_key in _dsco_cache:
        ts, cached = _dsco_cache[cache_key]
        if time.time() - ts < 3600:
            all_filings = cached["all_filings"]
            total = len(all_filings)
            start = (page - 1) * per_page
            return {
                "ticker": t, "company": cached["company"], "cik": cached["cik"],
                "total": total, "page": page, "per_page": per_page,
                "total_pages": (total + per_page - 1) // per_page,
                "filings": all_filings[start:start + per_page]
            }

    headers = {"User-Agent": "TerminalPro research@terminalpro.io", "Accept-Encoding": "gzip"}

    def human_size(size):
        if isinstance(size, (int, float)) and size:
            return f"{size/1e6:.1f}M" if size >= 1e6 else f"{size/1e3:.0f}K"
        return "—"

    def describe_form(form: str, doc_desc: str, item_codes: str) -> str:
        f = form.upper()
        d = (doc_desc or "").lower()
        ic = str(item_codes or "")
        # 8-K item-code enrichment
        if f == "8-K":
            if "2.02" in ic or "results of operations" in d or "earnings" in d: return "Earnings Results (8-K)"
            if "2.01" in ic or "acquisition" in d or "merger" in d: return "Acquisition Announcement (8-K)"
            if "5.02" in ic or "officer" in d or "appoint" in d or "director" in d: return "Leadership Change (8-K)"
            if "1.01" in ic or "agreement" in d or "definitive" in d: return "Material Agreement (8-K)"
            if "7.01" in ic or "regulation fd" in d: return "Reg FD Disclosure (8-K)"
            if "1.05" in ic or "material weakness" in d: return "Risk Disclosure (8-K)"
            if "press release" in d: return "Press Release (8-K)"
            return "Current Report (8-K)"
        if f == "8-K/A": return "Current Report Amendment (8-K/A)"
        if f == "10-K": return "Annual Report (10-K)"
        if f == "10-K/A": return "Annual Report Amendment (10-K/A)"
        if f == "10-Q": return "Quarterly Report (10-Q)"
        if f == "10-Q/A": return "Quarterly Report Amendment (10-Q/A)"
        if f == "DEF 14A": return "Proxy Statement — Annual Meeting"
        if f == "DEFA14A": return "Proxy Supplement"
        if f == "PRE 14A": return "Preliminary Proxy"
        if f == "PX14A6G": return "Proxy Solicitation / Opposition"
        if f == "S-1": return "IPO Registration Statement"
        if f == "S-1/A": return "IPO Registration Amendment"
        if f == "S-3": return "Shelf Securities Registration"
        if f == "S-3/A": return "Shelf Registration Amendment"
        if f == "S-4": return "Merger/Exchange Registration"
        if f == "424B2": return "Prospectus Supplement (424B2)"
        if f == "424B3": return "Prospectus Supplement (424B3)"
        if f == "424B4": return "Final Prospectus"
        if f == "FWP": return "Free Writing Prospectus"
        if f == "4": return "Insider Trade (Form 4)"
        if f == "4/A": return "Insider Trade Amendment"
        if f == "3": return "Initial Ownership Statement"
        if f == "5": return "Annual Ownership Statement"
        if f == "SC 13G": return "Institutional Ownership — Passive"
        if f == "SC 13G/A": return "Institutional Ownership Amendment"
        if f == "SC 13D": return "Institutional Ownership — Active"
        if f == "SC 13D/A": return "Active Ownership Amendment"
        if f == "SC TO-T": return "Tender Offer"
        if f == "SD": return "Supply Chain Disclosure"
        if f == "25-NSE": return "Securities Delisting Notice"
        if f == "NT 10-K": return "Late Annual Report Notice"
        if f == "NT 10-Q": return "Late Quarterly Report Notice"
        if f == "6-K": return "Foreign Issuer Report (6-K)"
        if f == "20-F": return "Foreign Annual Report (20-F)"
        if f == "ARS": return "Annual Report to Shareholders"
        if f == "11-K": return "Employee Benefits Plan Annual Report"
        if doc_desc: return doc_desc.title()
        return form

    async with httpx.AsyncClient(timeout=httpx.Timeout(8.0, connect=4.0)) as client:
        cik = None
        company_name = t
        try:
            r = await client.get("https://www.sec.gov/files/company_tickers.json", headers=headers)
            for entry in r.json().values():
                if entry.get("ticker", "").upper() == t:
                    cik = str(entry["cik_str"]).zfill(10)
                    company_name = entry.get("title", t)
                    break
        except Exception:
            pass

        if not cik:
            return {"error": "CIK not found", "ticker": t, "filings": []}

        try:
            r2 = await client.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=headers)
            sub = r2.json()
        except Exception as e:
            return {"error": str(e), "ticker": t, "filings": []}

        def extract_filings(recent_block):
            forms_     = recent_block.get("form", [])
            dates_     = recent_block.get("filingDate", [])
            periods_   = recent_block.get("reportDate", [])
            accnos_    = recent_block.get("accessionNumber", [])
            docs_      = recent_block.get("primaryDocument", [])
            sizes_     = recent_block.get("size", [])
            raw_descs_ = recent_block.get("primaryDocDescription", [])
            items_     = recent_block.get("items", [])
            out = []
            for i in range(len(forms_)):
                form      = forms_[i] or ""
                acc_raw   = accnos_[i] if i < len(accnos_) else ""
                acc       = (acc_raw or "").replace("-", "")
                doc       = docs_[i] if i < len(docs_) else ""
                size      = sizes_[i] if i < len(sizes_) else None
                raw_desc  = raw_descs_[i] if i < len(raw_descs_) else ""
                item_codes = items_[i] if i < len(items_) else ""
                index_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/" if acc else ""
                primary_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{doc}" if acc and doc else index_url
                out.append({
                    "company": company_name,
                    "form": form,
                    "source": describe_form(form, raw_desc, item_codes),
                    "items": str(item_codes) if item_codes else "",
                    "filed": dates_[i] if i < len(dates_) else "",
                    "period": periods_[i] if i < len(periods_) else "",
                    "size": human_size(size),
                    "desc": raw_desc,
                    "url": primary_url or index_url,
                    "index_url": index_url,
                })
            return out

        # Gather recent filings
        all_filings = extract_filings(sub.get("filings", {}).get("recent", {}))

        # Fetch older filing archives if available
        older_files = sub.get("filings", {}).get("files", [])
        for f_info in older_files:
            fname = f_info.get("name", "")
            if not fname:
                continue
            try:
                r3 = await client.get(f"https://data.sec.gov/submissions/{fname}", headers=headers)
                old_block = r3.json()
                all_filings.extend(extract_filings(old_block))
            except Exception:
                pass

        total = len(all_filings)
        start = (page - 1) * per_page
        end = start + per_page
        result = {
            "ticker": t, "company": company_name, "cik": cik,
            "total": total, "page": page, "per_page": per_page,
            "total_pages": (total + per_page - 1) // per_page,
            "filings": all_filings[start:end]
        }
        _dsco_cache[cache_key] = (time.time(), {"ticker": t, "company": company_name, "cik": cik, "total": total, "all_filings": all_filings})
        return result


# ══════════════════════════════════════════════════════════════════
# EXTENDED HOURS — for Macro tab panel
# ══════════════════════════════════════════════════════════════════
@app.get("/extended-hours")
async def get_extended_hours():
    cache_key = "extended:main"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    WATCHLIST = [
        ("S&P 500", "SPY"), ("Nasdaq", "QQQ"), ("Dow Jones", "DIA"),
        ("Russell 2000", "IWM"), ("Apple", "AAPL"), ("NVIDIA", "NVDA"),
        ("Microsoft", "MSFT"), ("Amazon", "AMZN"), ("Meta", "META"),
        ("Tesla", "TSLA"), ("Alphabet", "GOOGL"), ("AMD", "AMD"),
    ]
    syms = [sym for _, sym in WATCHLIST]

    # Try Alpaca batch snapshot first
    results = []
    alpaca_data = {}
    if ALPACA_KEY and ALPACA_SECRET:
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(
                    f"{ALPACA_DATA}/v2/stocks/snapshots",
                    headers=_alpaca_headers(),
                    params={"symbols": ",".join(syms), "feed": "iex"}
                )
                if r.status_code == 200:
                    alpaca_data = r.json()
        except Exception as e:
            print(f"[extended-hours alpaca] {e}")

    for name, sym in WATCHLIST:
        snap = alpaca_data.get(sym)
        if snap:
            try:
                dp = snap.get("dailyBar", {})
                lp = snap.get("latestTrade", {})
                prev = snap.get("prevDailyBar", {})
                reg_price = lp.get("p") or dp.get("c")
                prev_close = prev.get("c")
                chg = round(reg_price - prev_close, 2) if reg_price and prev_close else None
                chg_pct = round(chg / prev_close * 100, 2) if chg and prev_close else None
                results.append({
                    "name": name, "symbol": sym,
                    "market_state": "REGULAR",
                    "regular_price": round(reg_price, 2) if reg_price else None,
                    "regular_change": chg, "regular_change_pct": chg_pct,
                    "regular_volume": int(dp.get("v", 0)),
                    "pre_price": None, "pre_change": None, "pre_change_pct": None,
                    "post_price": None, "post_change": None, "post_change_pct": None,
                })
                continue
            except Exception:
                pass
        # yfinance fallback for this ticker
        try:
            info = yf.Ticker(sym).info
            market_state = info.get("marketState", "REGULAR")
            reg_price  = info.get("currentPrice") or info.get("regularMarketPrice")
            pre_price  = info.get("preMarketPrice")
            post_price = info.get("postMarketPrice")
            results.append({
                "name": name, "symbol": sym, "market_state": market_state,
                "regular_price": round(reg_price, 2) if reg_price else None,
                "regular_volume": info.get("regularMarketVolume"),
                "pre_price":  round(pre_price, 2)  if pre_price  else None,
                "pre_change": round(info.get("preMarketChange", 0), 2),
                "pre_change_pct": round((info.get("preMarketChangePercent") or 0) * 100, 2),
                "post_price":  round(post_price, 2)  if post_price  else None,
                "post_change": round(info.get("postMarketChange", 0), 2),
                "post_change_pct": round((info.get("postMarketChangePercent") or 0) * 100, 2),
            })
        except Exception:
            results.append({"name": name, "symbol": sym, "error": True})

    result = {"data": results, "timestamp": datetime.now().isoformat()}
    return _cache_set(cache_key, result)


# ══════════════════════════════════════════════════════════════════
# SPLC — SUPPLY CHAIN ANALYSIS
# Suppliers on left, customers on right, like Bloomberg SPLC
# ══════════════════════════════════════════════════════════════════

# Curated supply chain database for major tickers
_SPLC_DB: dict = {
    "AAPL": {
        "suppliers": [
            {"ticker":"TSM",   "name":"Taiwan Semiconductor", "rev_pct":26.5,"cogs_pct":18.2,"capex_pct":5.1, "sga_pct":0.0, "rd_pct":0.0, "category":"Semiconductors"},
            {"ticker":"QCOM",  "name":"Qualcomm",             "rev_pct":8.3, "cogs_pct":5.9, "capex_pct":1.8, "sga_pct":0.0, "rd_pct":0.0, "category":"Semiconductors"},
            {"ticker":"HON",   "name":"Honeywell",            "rev_pct":3.1, "cogs_pct":2.4, "capex_pct":0.6, "sga_pct":0.0, "rd_pct":0.0, "category":"Industrial"},
            {"ticker":"BOSCHF","name":"Robert Bosch GmbH",    "rev_pct":2.8, "cogs_pct":2.1, "capex_pct":0.4, "sga_pct":0.0, "rd_pct":0.0, "category":"Electronics"},
            {"ticker":"AVGO",  "name":"Broadcom",             "rev_pct":6.7, "cogs_pct":4.4, "capex_pct":1.2, "sga_pct":0.0, "rd_pct":0.0, "category":"Semiconductors"},
            {"ticker":"MU",    "name":"Micron Technology",    "rev_pct":4.2, "cogs_pct":3.1, "capex_pct":0.9, "sga_pct":0.0, "rd_pct":0.0, "category":"Memory"},
            {"ticker":"LUMENTUM","name":"Lumentum Holdings",  "rev_pct":1.9, "cogs_pct":1.4, "capex_pct":0.3, "sga_pct":0.0, "rd_pct":0.0, "category":"Optical"},
            {"ticker":"AMAT",  "name":"Applied Materials",    "rev_pct":1.5, "cogs_pct":1.0, "capex_pct":0.2, "sga_pct":0.0, "rd_pct":0.0, "category":"Equipment"},
            {"ticker":"2317.TW","name":"Foxconn (Hon Hai)",   "rev_pct":35.0,"cogs_pct":33.0,"capex_pct":3.2, "sga_pct":0.0, "rd_pct":0.0, "category":"Assembly"},
            {"ticker":"SWKS",  "name":"Skyworks Solutions",   "rev_pct":3.5, "cogs_pct":2.8, "capex_pct":0.5, "sga_pct":0.0, "rd_pct":0.0, "category":"RF Chips"},
        ],
        "customers": [
            {"ticker":"VZ",    "name":"Verizon Communications","rev_pct":6.2, "category":"Telecom"},
            {"ticker":"T",     "name":"AT&T",                  "rev_pct":5.8, "category":"Telecom"},
            {"ticker":"AMZN",  "name":"Amazon",                "rev_pct":2.1, "category":"Enterprise"},
            {"ticker":"GOOGL", "name":"Alphabet",              "rev_pct":14.0,"category":"Services Revenue"},
            {"ticker":"EDU",   "name":"Education Sector",      "rev_pct":3.4, "category":"Education"},
        ],
    },
    "MSFT": {
        "suppliers": [
            {"ticker":"NVDA",  "name":"NVIDIA",                "rev_pct":12.0,"cogs_pct":8.5, "capex_pct":3.0, "sga_pct":0.0, "rd_pct":0.0, "category":"GPU/AI"},
            {"ticker":"AMD",   "name":"Advanced Micro Devices","rev_pct":6.5, "cogs_pct":4.2, "capex_pct":1.1, "sga_pct":0.0, "rd_pct":0.0, "category":"Processors"},
            {"ticker":"INTC",  "name":"Intel",                 "rev_pct":8.0, "cogs_pct":5.8, "capex_pct":1.5, "sga_pct":0.0, "rd_pct":0.0, "category":"Processors"},
            {"ticker":"ORCL",  "name":"Oracle",                "rev_pct":4.1, "cogs_pct":3.0, "capex_pct":0.8, "sga_pct":0.0, "rd_pct":0.0, "category":"Database"},
            {"ticker":"SAP",   "name":"SAP SE",                "rev_pct":2.3, "cogs_pct":1.7, "capex_pct":0.4, "sga_pct":0.0, "rd_pct":0.0, "category":"Software"},
            {"ticker":"EQUIX", "name":"Equinix",               "rev_pct":3.8, "cogs_pct":2.9, "capex_pct":0.7, "sga_pct":0.0, "rd_pct":0.0, "category":"Data Center"},
        ],
        "customers": [
            {"ticker":"JPM",   "name":"JPMorgan Chase",        "rev_pct":3.2, "category":"Financial Services"},
            {"ticker":"GS",    "name":"Goldman Sachs",         "rev_pct":1.8, "category":"Financial Services"},
            {"ticker":"AMZN",  "name":"Amazon",                "rev_pct":2.4, "category":"Enterprise Cloud"},
            {"ticker":"META",  "name":"Meta Platforms",        "rev_pct":2.1, "category":"Enterprise"},
            {"ticker":"GOOGL", "name":"Alphabet",              "rev_pct":1.7, "category":"Enterprise"},
        ],
    },
    "NVDA": {
        "suppliers": [
            {"ticker":"TSM",   "name":"Taiwan Semiconductor",  "rev_pct":80.0,"cogs_pct":70.0,"capex_pct":6.0, "sga_pct":0.0, "rd_pct":0.0, "category":"Foundry"},
            {"ticker":"AMAT",  "name":"Applied Materials",     "rev_pct":4.0, "cogs_pct":2.8, "capex_pct":0.5, "sga_pct":0.0, "rd_pct":0.0, "category":"Equipment"},
            {"ticker":"KLAC",  "name":"KLA Corporation",       "rev_pct":2.5, "cogs_pct":1.8, "capex_pct":0.3, "sga_pct":0.0, "rd_pct":0.0, "category":"Equipment"},
            {"ticker":"LRCX",  "name":"Lam Research",          "rev_pct":3.2, "cogs_pct":2.3, "capex_pct":0.4, "sga_pct":0.0, "rd_pct":0.0, "category":"Equipment"},
            {"ticker":"COHU",  "name":"Cohu Inc.",             "rev_pct":1.0, "cogs_pct":0.7, "capex_pct":0.1, "sga_pct":0.0, "rd_pct":0.0, "category":"Test"},
        ],
        "customers": [
            {"ticker":"META",  "name":"Meta Platforms",        "rev_pct":19.0,"category":"AI Infrastructure"},
            {"ticker":"GOOGL", "name":"Alphabet",              "rev_pct":17.0,"category":"AI Infrastructure"},
            {"ticker":"MSFT",  "name":"Microsoft",             "rev_pct":15.0,"category":"AI Infrastructure"},
            {"ticker":"AMZN",  "name":"Amazon Web Services",   "rev_pct":12.0,"category":"AI Infrastructure"},
            {"ticker":"ORCL",  "name":"Oracle",                "rev_pct":7.0, "category":"Enterprise AI"},
            {"ticker":"TSLA",  "name":"Tesla",                 "rev_pct":4.5, "category":"Automotive AI"},
        ],
    },
    "TSLA": {
        "suppliers": [
            {"ticker":"PANASONIC","name":"Panasonic Holdings", "rev_pct":15.0,"cogs_pct":12.0,"capex_pct":2.0, "sga_pct":0.0, "rd_pct":0.0, "category":"Battery Cells"},
            {"ticker":"CATL",  "name":"Contemporary Amperex", "rev_pct":22.0,"cogs_pct":18.0,"capex_pct":3.0, "sga_pct":0.0, "rd_pct":0.0, "category":"Battery Cells"},
            {"ticker":"APTV",  "name":"Aptiv",                "rev_pct":5.0, "cogs_pct":3.8, "capex_pct":0.7, "sga_pct":0.0, "rd_pct":0.0, "category":"Wiring"},
            {"ticker":"ALB",   "name":"Albemarle",            "rev_pct":3.5, "cogs_pct":2.8, "capex_pct":0.4, "sga_pct":0.0, "rd_pct":0.0, "category":"Lithium"},
            {"ticker":"NVDA",  "name":"NVIDIA",               "rev_pct":4.5, "cogs_pct":3.2, "capex_pct":0.6, "sga_pct":0.0, "rd_pct":0.0, "category":"AI Chips"},
            {"ticker":"ON",    "name":"ON Semiconductor",     "rev_pct":2.8, "cogs_pct":2.0, "capex_pct":0.3, "sga_pct":0.0, "rd_pct":0.0, "category":"Power Semis"},
        ],
        "customers": [
            {"ticker":"DIRECT","name":"Direct Consumers",      "rev_pct":85.0,"category":"Retail"},
            {"ticker":"FLEET", "name":"Fleet Operators",       "rev_pct":8.0, "category":"Commercial"},
            {"ticker":"SOLAR", "name":"Solar/Energy Clients",  "rev_pct":7.0, "category":"Energy"},
        ],
    },
    "AMZN": {
        "suppliers": [
            {"ticker":"UPS",   "name":"United Parcel Service", "rev_pct":8.2, "cogs_pct":6.0, "capex_pct":0.9, "sga_pct":0.0, "rd_pct":0.0, "category":"Shipping"},
            {"ticker":"FDX",   "name":"FedEx",                 "rev_pct":6.5, "cogs_pct":5.1, "capex_pct":0.7, "sga_pct":0.0, "rd_pct":0.0, "category":"Shipping"},
            {"ticker":"MSFT",  "name":"Microsoft Azure",       "rev_pct":3.2, "cogs_pct":2.4, "capex_pct":0.5, "sga_pct":0.0, "rd_pct":0.0, "category":"Cloud Infra"},
            {"ticker":"NVDA",  "name":"NVIDIA",                "rev_pct":5.5, "cogs_pct":4.0, "capex_pct":0.8, "sga_pct":0.0, "rd_pct":0.0, "category":"AI Chips"},
            {"ticker":"INTC",  "name":"Intel",                 "rev_pct":2.8, "cogs_pct":2.0, "capex_pct":0.4, "sga_pct":0.0, "rd_pct":0.0, "category":"Processors"},
        ],
        "customers": [
            {"ticker":"SMALLS","name":"SMB Merchants (3P)",    "rev_pct":23.0,"category":"Marketplace"},
            {"ticker":"ADBE",  "name":"Adobe",                 "rev_pct":2.5, "category":"AWS Enterprise"},
            {"ticker":"NFLX",  "name":"Netflix",               "rev_pct":3.1, "category":"AWS Media"},
            {"ticker":"AAPL",  "name":"Apple",                 "rev_pct":2.1, "category":"AWS Enterprise"},
            {"ticker":"META",  "name":"Meta Platforms",        "rev_pct":2.8, "category":"AWS Enterprise"},
        ],
    },
    "META": {
        "suppliers": [
            {"ticker":"NVDA",  "name":"NVIDIA",                "rev_pct":20.0,"cogs_pct":15.0,"capex_pct":4.0, "sga_pct":0.0, "rd_pct":0.0, "category":"AI Infrastructure"},
            {"ticker":"AMD",   "name":"Advanced Micro Devices","rev_pct":8.0, "cogs_pct":5.5, "capex_pct":1.2, "sga_pct":0.0, "rd_pct":0.0, "category":"Custom Silicon"},
            {"ticker":"EQUIX", "name":"Equinix",               "rev_pct":4.5, "cogs_pct":3.3, "capex_pct":0.6, "sga_pct":0.0, "rd_pct":0.0, "category":"Data Centers"},
            {"ticker":"ANET",  "name":"Arista Networks",       "rev_pct":3.0, "cogs_pct":2.1, "capex_pct":0.4, "sga_pct":0.0, "rd_pct":0.0, "category":"Networking"},
        ],
        "customers": [
            {"ticker":"WMT",   "name":"Walmart",               "rev_pct":5.5, "category":"Advertising"},
            {"ticker":"AMZN",  "name":"Amazon",                "rev_pct":8.3, "category":"Advertising"},
            {"ticker":"PG",    "name":"Procter & Gamble",      "rev_pct":3.2, "category":"Advertising"},
            {"ticker":"KO",    "name":"Coca-Cola",             "rev_pct":1.8, "category":"Advertising"},
        ],
    },
    "GOOGL": {
        "suppliers": [
            {"ticker":"NVDA",  "name":"NVIDIA",                "rev_pct":15.0,"cogs_pct":11.0,"capex_pct":3.0, "sga_pct":0.0, "rd_pct":0.0, "category":"AI Chips"},
            {"ticker":"TSM",   "name":"Taiwan Semiconductor",  "rev_pct":6.0, "cogs_pct":4.5, "capex_pct":0.9, "sga_pct":0.0, "rd_pct":0.0, "category":"Custom TPU"},
            {"ticker":"MSFT",  "name":"Microsoft (Azure)",     "rev_pct":2.0, "cogs_pct":1.5, "capex_pct":0.3, "sga_pct":0.0, "rd_pct":0.0, "category":"Cloud"},
        ],
        "customers": [
            {"ticker":"AAPL",  "name":"Apple (Search TAC)",    "rev_pct":14.0,"category":"Traffic Acquisition"},
            {"ticker":"AMZN",  "name":"Amazon Advertising",    "rev_pct":4.5, "category":"Advertising"},
            {"ticker":"MSFT",  "name":"Microsoft",             "rev_pct":2.0, "category":"Enterprise"},
        ],
    },
    "JPM": {
        "suppliers": [
            {"ticker":"MSFT",  "name":"Microsoft",             "rev_pct":5.0, "cogs_pct":3.8, "capex_pct":0.8, "sga_pct":0.0, "rd_pct":0.0, "category":"Software"},
            {"ticker":"ORCL",  "name":"Oracle",                "rev_pct":3.2, "cogs_pct":2.4, "capex_pct":0.4, "sga_pct":0.0, "rd_pct":0.0, "category":"Database"},
            {"ticker":"IBM",   "name":"IBM",                   "rev_pct":2.8, "cogs_pct":2.0, "capex_pct":0.3, "sga_pct":0.0, "rd_pct":0.0, "category":"IT Services"},
        ],
        "customers": [
            {"ticker":"CORPS", "name":"Corporate Clients",     "rev_pct":40.0,"category":"Investment Banking"},
            {"ticker":"RETAIL","name":"Retail Banking Clients","rev_pct":35.0,"category":"Consumer"},
            {"ticker":"INSTIT","name":"Institutional Investors","rev_pct":25.0,"category":"Asset Management"},
        ],
    },
    "BA": {
        "suppliers": [
            {"ticker":"SPR",   "name":"Spirit AeroSystems",    "rev_pct":79.0,"cogs_pct":70.3,"capex_pct":5.4, "sga_pct":0.6, "rd_pct":0.0, "category":"Fuselage"},
            {"ticker":"GE",    "name":"GE Aerospace",          "rev_pct":7.6, "cogs_pct":5.8, "capex_pct":1.2, "sga_pct":0.0, "rd_pct":0.0, "category":"Engines"},
            {"ticker":"RTX",   "name":"RTX Corporation",       "rev_pct":3.3, "cogs_pct":2.9, "capex_pct":0.5, "sga_pct":0.0, "rd_pct":0.0, "category":"Systems"},
            {"ticker":"SAF.PA","name":"Safran SA",             "rev_pct":7.3, "cogs_pct":5.5, "capex_pct":2.9, "sga_pct":0.0, "rd_pct":0.0, "category":"Engines"},
            {"ticker":"ERJ",   "name":"Embraer",               "rev_pct":0.5, "cogs_pct":0.3, "capex_pct":0.1, "sga_pct":0.0, "rd_pct":0.0, "category":"Subcomponents"},
            {"ticker":"HON",   "name":"Honeywell",             "rev_pct":3.6, "cogs_pct":2.4, "capex_pct":0.8, "sga_pct":0.0, "rd_pct":0.0, "category":"Avionics"},
            {"ticker":"TXT",   "name":"Textron",               "rev_pct":3.4, "cogs_pct":2.2, "capex_pct":1.5, "sga_pct":0.0, "rd_pct":0.0, "category":"Controls"},
            {"ticker":"MITSI", "name":"Mitsubishi Heavy Ind.",  "rev_pct":3.3, "cogs_pct":2.0, "capex_pct":0.9, "sga_pct":0.0, "rd_pct":0.0, "category":"Wings"},
            {"ticker":"LDO.MI","name":"Leonardo SpA",          "rev_pct":6.1, "cogs_pct":4.2, "capex_pct":1.2, "sga_pct":0.0, "rd_pct":0.0, "category":"Fuselage"},
        ],
        "customers": [
            {"ticker":"DAL",   "name":"Delta Air Lines",       "rev_pct":5.5, "category":"Commercial Aviation"},
            {"ticker":"UAL",   "name":"United Airlines",       "rev_pct":5.2, "category":"Commercial Aviation"},
            {"ticker":"AAL",   "name":"American Airlines",     "rev_pct":4.8, "category":"Commercial Aviation"},
            {"ticker":"LUV",   "name":"Southwest Airlines",    "rev_pct":4.2, "category":"Commercial Aviation"},
            {"ticker":"FDX",   "name":"FedEx",                 "rev_pct":3.5, "category":"Cargo"},
            {"ticker":"UPS",   "name":"UPS",                   "rev_pct":2.8, "category":"Cargo"},
            {"ticker":"US",    "name":"U.S. Government (DoD)", "rev_pct":39.0,"category":"Defense"},
            {"ticker":"CX",    "name":"Cathay Pacific",        "rev_pct":1.9, "category":"Commercial Aviation"},
        ],
    },
}

# Generic sector-based fallback suppliers/customers
_SECTOR_DEFAULTS: dict = {
    "Technology": {
        "suppliers": [
            {"ticker":"TSM",  "name":"Taiwan Semiconductor","rev_pct":22.0,"cogs_pct":15.0,"capex_pct":3.0,"sga_pct":0,"rd_pct":0,"category":"Foundry"},
            {"ticker":"NVDA", "name":"NVIDIA",              "rev_pct":10.0,"cogs_pct":7.0, "capex_pct":1.5,"sga_pct":0,"rd_pct":0,"category":"Semiconductors"},
            {"ticker":"MSFT", "name":"Microsoft",           "rev_pct":5.0, "cogs_pct":3.5, "capex_pct":0.7,"sga_pct":0,"rd_pct":0,"category":"Cloud / Software"},
        ],
        "customers": [
            {"ticker":"AMZN","name":"Amazon",       "rev_pct":8.0, "category":"Enterprise"},
            {"ticker":"GOOGL","name":"Alphabet",    "rev_pct":6.0, "category":"Enterprise"},
            {"ticker":"META", "name":"Meta",        "rev_pct":5.0, "category":"Advertising"},
        ],
    },
    "Consumer Cyclical": {
        "suppliers": [
            {"ticker":"WMT",  "name":"Walmart",      "rev_pct":12.0,"cogs_pct":9.0,"capex_pct":1.5,"sga_pct":0,"rd_pct":0,"category":"Retail"},
            {"ticker":"AMZN", "name":"Amazon",       "rev_pct":10.0,"cogs_pct":7.0,"capex_pct":1.0,"sga_pct":0,"rd_pct":0,"category":"E-Commerce"},
        ],
        "customers": [
            {"ticker":"CONS", "name":"U.S. Consumers","rev_pct":70.0,"category":"Retail"},
            {"ticker":"INTL", "name":"International","rev_pct":30.0,"category":"Export"},
        ],
    },
    "Healthcare": {
        "suppliers": [
            {"ticker":"MDT",  "name":"Medtronic",   "rev_pct":8.0,"cogs_pct":5.0,"capex_pct":1.0,"sga_pct":0,"rd_pct":0,"category":"Med Devices"},
            {"ticker":"ABT",  "name":"Abbott Labs",  "rev_pct":6.0,"cogs_pct":4.0,"capex_pct":0.8,"sga_pct":0,"rd_pct":0,"category":"Diagnostics"},
        ],
        "customers": [
            {"ticker":"HOSP","name":"Hospitals & Health Systems","rev_pct":50.0,"category":"Healthcare"},
            {"ticker":"INS", "name":"Insurance Payers",         "rev_pct":30.0,"category":"Insurance"},
        ],
    },
    "Industrials": {
        "suppliers": [
            {"ticker":"HON",  "name":"Honeywell",         "rev_pct":7.0,"cogs_pct":5.0,"capex_pct":1.0,"sga_pct":0,"rd_pct":0,"category":"Components"},
            {"ticker":"MMM",  "name":"3M Company",        "rev_pct":5.0,"cogs_pct":3.5,"capex_pct":0.7,"sga_pct":0,"rd_pct":0,"category":"Materials"},
        ],
        "customers": [
            {"ticker":"GOVT","name":"Government / Defense","rev_pct":35.0,"category":"Government"},
            {"ticker":"COMM","name":"Commercial Clients",  "rev_pct":65.0,"category":"Commercial"},
        ],
    },
}


@app.get("/supply-chain/{ticker}")
async def get_supply_chain(ticker: str):
    t = ticker.upper().strip()
    if not _valid_ticker(t):
        return {"error": f"Invalid ticker: {t}", "ticker": t, "suppliers": [], "customers": []}

    cache_key = f"splc:{t}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    # For curated tickers skip yfinance entirely — instant response
    if t in _SPLC_DB:
        entry = _SPLC_DB[t]
        # Still want name/sector — get from stock cache if already warm
        stock_cached = _cache_get(f"stock:{t}")
        name   = (stock_cached or {}).get("name", t)
        sector = (stock_cached or {}).get("sector", "Technology")
        result = {
            "ticker": t, "name": name, "sector": sector,
            "source": "curated",
            "suppliers": entry["suppliers"],
            "customers": entry["customers"],
            "supplier_count": len(entry["suppliers"]),
            "customer_count": len(entry["customers"]),
        }
        return _cache_set(cache_key, result)

    # Non-curated: fetch info with hard timeout, fall back to sector defaults
    name   = t
    sector = "Technology"
    try:
        info = await _run_async(lambda: yf.Ticker(t).info or {}, timeout=10)
        name   = info.get("longName") or info.get("shortName") or t
        sector = info.get("sector") or "Technology"
    except Exception:
        pass

    defaults = _SECTOR_DEFAULTS.get(sector, _SECTOR_DEFAULTS["Technology"])
    result = {
        "ticker": t, "name": name, "sector": sector,
        "source": "estimated",
        "note": "Showing estimated sector-based supply chain. Specific data not available.",
        "suppliers": defaults["suppliers"],
        "customers": defaults["customers"],
        "supplier_count": len(defaults["suppliers"]),
        "customer_count": len(defaults["customers"]),
    }
    return _cache_set(cache_key, result)

# ── DIVIDEND HISTORY ──────────────────────────────────────────────────────────
@app.get("/dividends/{ticker}")
async def get_dividends(ticker: str):
    cache_key = f"dividends:{ticker.upper()}"
    cached = _cache_get(cache_key)
    if cached:
        return JSONResponse(content=cached)

    def _fetch():
        stock = yf.Ticker(ticker)
        info  = stock.info or {}
        divs  = stock.dividends

        history = []
        if divs is not None and not divs.empty:
            for dt, val in divs.tail(20).items():
                history.append({
                    "date":   str(dt.date()),
                    "amount": round(float(val), 4),
                })
            history.reverse()

        return {
            "ticker":                ticker.upper(),
            "dividend_yield":        info.get("dividendYield"),
            "dividend_rate":         info.get("dividendRate"),
            "ex_dividend_date":      str(pd.to_datetime(info.get("exDividendDate"), unit="s").date()) if info.get("exDividendDate") else None,
            "payout_ratio":          info.get("payoutRatio"),
            "five_year_avg_yield":   info.get("fiveYearAvgDividendYield"),
            "dividend_growth_rate":  None,  # computed below if enough history
            "history":               history,
        }

    try:
        result = await _run_async(_fetch, timeout=15)
        _cache_set(cache_key, result)
        return JSONResponse(content=result)
    except Exception:
        return JSONResponse(content={"ticker": ticker.upper(), "history": [], "error": "Timed out or unavailable"})

