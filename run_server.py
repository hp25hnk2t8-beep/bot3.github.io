import asyncio
import json
import logging
import os
import re
import signal
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager
from collections import deque
import time

import aiofiles
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ================= PRODUCTION CONFIG =================
CONFIG = {
    "port": int(os.getenv("PORT", 8000)),
    "host": os.getenv("HOST", "0.0.0.0"),
    "headless": os.getenv("HEADLESS", "true").lower() == "true",
    "timeout_navigation": int(os.getenv("TIMEOUT_NAV", 15000)),
    "timeout_element": int(os.getenv("TIMEOUT_ELEMENT", 8000)),
    "max_retries": int(os.getenv("MAX_RETRIES", 2)),
    "concurrent_limit": int(os.getenv("CONCURRENT_LIMIT", 2)),  # 2 is optimal for stability
    "delay_between_accounts": float(os.getenv("DELAY_ACCOUNTS", 1.0)),
    "delay_between_retries": float(os.getenv("DELAY_RETRY", 2.0)),
    "browser_restart_every": int(os.getenv("BROWSER_RESTART", 50)),  # restart browser every N accounts
}

# ================= LOGGER =================
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("AdjarabetBot")

# ================= DATA MODELS =================
class AccountStatus(Enum):
    SUCCESS = "✅"
    FAILED = "❌"
    TIMEOUT = "⏰"
    BLOCKED = "🚫"

@dataclass
class Account:
    username: str
    password: str
    status: AccountStatus = AccountStatus.FAILED
    balance: str = "0"
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
            "error": self.error[:100],
            "timestamp": self.timestamp
        }

# ================= CONNECTION MANAGER =================
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        dead = []
        for conn in self.active_connections:
            try:
                await conn.send_text(message)
            except:
                dead.append(conn)
        for conn in dead:
            self.disconnect(conn)

# ================= RATE LIMITER =================
class RateLimiter:
    def __init__(self, max_requests: int, time_window: float):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
    
    async def acquire(self):
        now = time.time()
        while self.requests and self.requests[0] < now - self.time_window:
            self.requests.popleft()
        if len(self.requests) >= self.max_requests:
            wait = self.time_window - (now - self.requests[0])
            if wait > 0:
                await asyncio.sleep(wait)
        self.requests.append(time.time())

# ================= OPTIMIZED BOT =================
class Bot:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.is_running = False
        self.results: List[Account] = []
        self.processed_count = 0
        self.semaphore = asyncio.Semaphore(CONFIG["concurrent_limit"])
        self.rate_limiter = RateLimiter(max_requests=5, time_window=10.0)  # 5 requests per 10 seconds
        
    async def initialize(self):
        """Initialize browser once"""
        try:
            logger.info("Initializing browser...")
            self.playwright = await async_playwright().start()
            self.browser = await self._create_browser()
            self.is_running = True
            logger.info("✓ Browser ready")
            return True
        except Exception as e:
            logger.error(f"Init failed: {e}")
            return False
    
    async def _create_browser(self):
        """Create a new browser instance"""
        return await self.playwright.chromium.launch(
            headless=CONFIG["headless"],
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
            ]
        )
    
    async def restart_browser(self):
        """Restart browser to prevent memory leaks"""
        logger.debug("Restarting browser...")
        if self.browser:
            try:
                await self.browser.close()
            except:
                pass
        self.browser = await self._create_browser()
        await asyncio.sleep(0.5)
    
    async def cleanup(self):
        self.is_running = False
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
        logger.info("Cleanup done")
    
    def _parse_balance(self, text: str) -> tuple:
        """Parse balance text to value and clean string"""
        try:
            text = text.replace(',', '').replace('₾', '').replace('GEL', '').strip()
            match = re.search(r'(\d+(?:\.\d+)?)', text)
            if match:
                value = float(match.group(1))
                clean = f"{value:,.2f} ₾" if value > 0 else "0 ₾"
                return clean, value
        except:
            pass
        return "0 ₾", 0.0
    
    async def login_single(self, account: Account) -> Account:
        """Login single account with isolated context"""
        context = None
        page = None
        
        try:
            # Apply rate limiting
            await self.rate_limiter.acquire()
            
            # Create fresh context for each account (prevents balance caching)
            context = await self.browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                ignore_https_errors=True,
                locale='hy-AM'
            )
            page = await context.new_page()
            page.set_default_timeout(CONFIG["timeout_navigation"])
            
            # Retry loop
            for attempt in range(CONFIG["max_retries"]):
                if not self.is_running:
                    return account
                
                try:
                    # Navigate to site
                    await page.goto('https://www.adjarabet.am/hy', wait_until='domcontentloaded')
                    await asyncio.sleep(0.5)
                    
                    # Check if already logged in
                    try:
                        balance_el = await page.wait_for_selector(
                            '[data-test-id="header-user-balance"]', 
                            timeout=3000
                        )
                        if balance_el:
                            balance_text = await balance_el.inner_text()
                            clean_balance, balance_value = self._parse_balance(balance_text)
                            account.balance = clean_balance
                            account.balance_value = balance_value
                            account.status = AccountStatus.SUCCESS
                            logger.info(f"✅ {account.username} | {clean_balance}")
                            return account
                    except PlaywrightTimeout:
                        pass
                    
                    # Login flow
                    await page.wait_for_selector('input[name="userIdentifier"]', timeout=CONFIG["timeout_element"])
                    await page.fill('input[name="userIdentifier"]', account.username)
                    await asyncio.sleep(0.3)
                    await page.fill('input[type="password"]', account.password)
                    await asyncio.sleep(0.3)
                    
                    # Click login button
                    await page.click('[data-test-id="header-login-button"]')
                    
                    # Wait for balance after login
                    try:
                        balance_el = await page.wait_for_selector(
                            '[data-test-id="header-user-balance"]', 
                            timeout=CONFIG["timeout_navigation"]
                        )
                        balance_text = await balance_el.inner_text()
                        clean_balance, balance_value = self._parse_balance(balance_text)
                        account.balance = clean_balance
                        account.balance_value = balance_value
                        account.status = AccountStatus.SUCCESS
                        logger.info(f"✅ {account.username} | {clean_balance}")
                        return account
                        
                    except PlaywrightTimeout:
                        # Check for error message
                        try:
                            error_el = await page.wait_for_selector('[class*="error"]', timeout=2000)
                            if error_el:
                                account.error = (await error_el.inner_text())[:80]
                        except:
                            pass
                        
                        if attempt == CONFIG["max_retries"] - 1:
                            account.status = AccountStatus.TIMEOUT
                            account.error = account.error or "Login timeout"
                            logger.warning(f"⏰ {account.username} - {account.error}")
                        else:
                            await asyncio.sleep(CONFIG["delay_between_retries"])
                            continue
                            
                except PlaywrightTimeout as e:
                    if attempt == CONFIG["max_retries"] - 1:
                        account.status = AccountStatus.TIMEOUT
                        account.error = "Navigation timeout"
                        logger.warning(f"⏰ {account.username} - timeout")
                    else:
                        await asyncio.sleep(CONFIG["delay_between_retries"])
                        continue
                        
                except Exception as e:
                    if attempt == CONFIG["max_retries"] - 1:
                        account.status = AccountStatus.FAILED
                        account.error = str(e)[:80]
                        logger.error(f"❌ {account.username}: {str(e)[:60]}")
                    else:
                        await asyncio.sleep(CONFIG["delay_between_retries"])
                        continue
                    
            return account
            
        except Exception as e:
            account.status = AccountStatus.FAILED
            account.error = f"Fatal: {str(e)[:60]}"
            logger.error(f"❌ {account.username}: {str(e)[:60]}")
            return account
            
        finally:
            # Proper cleanup
            if page:
                try:
                    await page.close()
                except:
                    pass
            if context:
                try:
                    await context.close()
                except:
                    pass
    
    async def process_accounts(self, accounts: List[Account], manager: ConnectionManager):
        """Process accounts with optimal concurrency"""
        self.manager = manager
        self.results = []
        self.processed_count = 0
        total = len(accounts)
        
        logger.info(f"▶ Processing {total} accounts (concurrent={CONFIG['concurrent_limit']})")
        await manager.broadcast(f"STATUS:started")
        await manager.broadcast(f"PROGRESS:0/{total}")
        
        async def process_with_semaphore(account: Account, idx: int):
            async with self.semaphore:
                result = await self.login_single(account)
                self.results.append(result)
                self.processed_count += 1
                
                # Broadcast updates
                await manager.broadcast(f"RESULT:{json.dumps(result.to_dict())}")
                await manager.broadcast(f"PROGRESS:{self.processed_count}/{total}")
                
                # Restart browser periodically to prevent memory issues
                if self.processed_count % CONFIG["browser_restart_every"] == 0:
                    await self.restart_browser()
                
                return result
        
        # Create tasks with small delays between starts
        tasks = []
        for i, account in enumerate(accounts):
            if not self.is_running:
                break
            tasks.append(process_with_semaphore(account, i+1))
            await asyncio.sleep(CONFIG["delay_between_accounts"] / CONFIG["concurrent_limit"])
        
        # Wait for all tasks
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Save results
        await self._save_results()
        
        # Summary
        successful = sum(1 for r in self.results if r.status == AccountStatus.SUCCESS)
        failed = sum(1 for r in self.results if r.status == AccountStatus.FAILED)
        timeout = sum(1 for r in self.results if r.status == AccountStatus.TIMEOUT)
        rate = (successful / total * 100) if total > 0 else 0
        
        summary = f"📊 Completed: ✅{successful} ❌{failed} ⏰{timeout} | {rate:.1f}%"
        logger.info(summary)
        await manager.broadcast(f"SUMMARY:{successful}/{total}:{rate:.1f}")
        await manager.broadcast("STATUS:stopped")
    
    async def _save_results(self):
        try:
            Path("results").mkdir(exist_ok=True)
            data = [r.to_dict() for r in self.results]
            async with aiofiles.open("results/results.json", "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Save failed: {e}")
    
    async def get_results(self) -> List[Dict]:
        if self.results:
            return [r.to_dict() for r in self.results]
        try:
            if Path("results/results.json").exists():
                async with aiofiles.open("results/results.json", "r", encoding="utf-8") as f:
                    return json.loads(await f.read())
        except:
            pass
        return []

# ================= FASTAPI APP =================
manager = ConnectionManager()
bot = Bot()
processing_task: Optional[asyncio.Task] = None

# HTML UI - Clean and fast
HTML_UI = '''<!DOCTYPE html>
<html lang="hy">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Adjarabet Bot | Ultimate</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { background: #0a0c10; color: #e6edf3; font-family: 'Inter', sans-serif; padding: 20px; }
        .container { max-width: 1600px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, #161b22, #0d1117); border-radius: 20px; padding: 20px 28px; margin-bottom: 24px; border: 1px solid #30363d; }
        .header h1 { font-size: 28px; background: linear-gradient(135deg, #58a6ff, #3fb950); -webkit-background-clip: text; background-clip: text; color: transparent; }
        .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }
        .stat-card { background: #161b22; border-radius: 16px; padding: 20px; border: 1px solid #30363d; cursor: pointer; transition: all 0.2s; text-align: center; }
        .stat-card:hover { transform: translateY(-2px); border-color: #58a6ff; background: #1a1f2e; }
        .stat-number { font-size: 32px; font-weight: 800; color: #58a6ff; }
        .stat-label { font-size: 12px; color: #8b949e; margin-top: 5px; }
        .main-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px; }
        .card { background: #161b22; border-radius: 20px; border: 1px solid #30363d; overflow: hidden; }
        .card-header { padding: 16px 20px; background: #0d1117; border-bottom: 1px solid #30363d; font-weight: 600; }
        .card-header i { color: #58a6ff; margin-right: 8px; }
        .accounts-input { width: 100%; min-height: 300px; background: #0d1117; border: 1px solid #30363d; border-radius: 12px; padding: 16px; color: #e6edf3; font-family: 'Courier New', monospace; font-size: 13px; resize: vertical; }
        .accounts-input:focus { outline: none; border-color: #58a6ff; }
        .btn { padding: 10px 20px; border: none; border-radius: 10px; font-weight: 600; cursor: pointer; transition: 0.2s; margin-right: 8px; }
        .btn-primary { background: #238636; color: white; }
        .btn-primary:hover { background: #2ea043; transform: translateY(-1px); }
        .btn-danger { background: #da3633; color: white; }
        .btn-danger:hover { background: #f85149; }
        .btn-secondary { background: #6e7681; color: white; }
        .btn-secondary:hover { background: #8b949e; }
        .terminal { background: #010409; border-radius: 12px; height: 320px; overflow-y: auto; padding: 12px; font-family: 'Courier New', monospace; font-size: 11px; }
        .terminal-line { padding: 4px 0; color: #b1bac4; border-bottom: 1px solid #21262d; }
        .terminal-line .time { color: #6e7681; margin-right: 10px; }
        .table-container { max-height: 450px; overflow-y: auto; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid #21262d; font-size: 13px; }
        th { background: #0d1117; cursor: pointer; position: sticky; top: 0; color: #8b949e; font-weight: 600; }
        th:hover { color: #58a6ff; }
        .balance-positive { color: #3fb950; font-weight: 600; }
        .balance-zero { color: #f85149; }
        .filter-group { display: flex; gap: 8px; margin: 12px 16px; flex-wrap: wrap; }
        .filter-btn { padding: 6px 14px; background: #21262d; border: none; border-radius: 20px; color: #8b949e; cursor: pointer; font-size: 12px; transition: 0.2s; }
        .filter-btn.active { background: #58a6ff; color: white; }
        .search-input { padding: 8px 16px; background: #0d1117; border: 1px solid #30363d; border-radius: 20px; color: white; width: 200px; }
        .progress-bar { height: 3px; background: #21262d; border-radius: 3px; overflow: hidden; margin-top: 12px; }
        .progress-fill { height: 100%; background: linear-gradient(90deg, #3fb950, #58a6ff); width: 0%; transition: width 0.3s; }
        @media (max-width: 768px) { .main-grid { grid-template-columns: 1fr; } .stats { grid-template-columns: repeat(2, 1fr); } }
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1><i class="fas fa-robot"></i> Adjarabet Bot Pro <span style="font-size: 12px; color: #8b949e;">v13.0</span></h1>
        <p style="font-size: 13px; color: #8b949e; margin-top: 6px;">High-performance account checker | Real-time updates</p>
        <div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
    </div>

    <div class="stats">
        <div class="stat-card" onclick="setFilter('all')"><div class="stat-number" id="totalCount">0</div><div class="stat-label">TOTAL</div></div>
        <div class="stat-card" onclick="setFilter('success')"><div class="stat-number" id="successCount">0</div><div class="stat-label">✅ SUCCESS</div></div>
        <div class="stat-card" onclick="setFilter('failed')"><div class="stat-number" id="failedCount">0</div><div class="stat-label">❌ FAILED</div></div>
        <div class="stat-card" onclick="setFilter('timeout')"><div class="stat-number" id="timeoutCount">0</div><div class="stat-label">⏰ TIMEOUT</div></div>
    </div>

    <div class="main-grid">
        <div class="card">
            <div class="card-header"><i class="fas fa-users"></i> Accounts (username:password)</div>
            <div style="padding: 20px;">
                <textarea id="accounts" class="accounts-input" placeholder="user1:pass123&#10;user2:pass456"></textarea>
                <div style="margin-top: 16px;">
                    <button class="btn btn-primary" onclick="startBot()"><i class="fas fa-play"></i> Start</button>
                    <button class="btn btn-danger" onclick="stopBot()"><i class="fas fa-stop"></i> Stop</button>
                    <button class="btn btn-secondary" onclick="clearAccounts()"><i class="fas fa-trash"></i> Clear</button>
                </div>
            </div>
        </div>

        <div class="card">
            <div class="card-header"><i class="fas fa-terminal"></i> Live Console</div>
            <div class="terminal" id="terminal">
                <div class="terminal-line"><span class="time">●</span> 🚀 Bot v13.0 ready</div>
                <div class="terminal-line"><span class="time">●</span> 💡 Real-time results will appear here</div>
            </div>
        </div>
    </div>

    <div class="card">
        <div class="card-header"><i class="fas fa-chart-line"></i> Results Dashboard</div>
        <div class="filter-group">
            <input type="text" id="searchInput" class="search-input" placeholder="🔍 Search...">
            <button class="filter-btn active" data-filter="all" onclick="setFilter('all')">All</button>
            <button class="filter-btn" data-filter="success" onclick="setFilter('success')">✅ Success</button>
            <button class="filter-btn" data-filter="failed" onclick="setFilter('failed')">❌ Failed</button>
            <button class="filter-btn" data-filter="timeout" onclick="setFilter('timeout')">⏰ Timeout</button>
        </div>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th onclick="sortBy('status')">Status</th>
                        <th onclick="sortBy('username')">Username</th>
                        <th onclick="sortBy('password')">Password</th>
                        <th onclick="sortBy('balance')">Balance</th>
                        <th>Error</th>
                    </tr>
                </thead>
                <tbody id="resultsBody"><tr><td colspan="5" style="text-align:center; padding:40px;">⏳ Waiting for results...</td></tr></tbody>
            </table>
        </div>
    </div>
</div>

<script>
let ws = null, allResults = [], currentFilter = 'all', currentSort = { field: 'balance', dir: 'desc' };

function connect() {
    ws = new WebSocket(`ws://${window.location.host}/ws`);
    ws.onopen = () => addLog('🟢 Connected to server');
    ws.onmessage = (e) => {
        let data = e.data;
        if (data.startsWith('RESULT:')) {
            try {
                let result = JSON.parse(data.substring(7));
                updateResult(result);
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

function updateResult(r) {
    let idx = allResults.findIndex(x => x.username === r.username);
    if (idx >= 0) allResults[idx] = r;
    else allResults.push(r);
    renderResults();
    updateStats();
}

function renderResults() {
    let filtered = allResults.filter(r => {
        if (currentFilter === 'all') return true;
        if (currentFilter === 'success') return r.status === '✅';
        if (currentFilter === 'failed') return r.status === '❌';
        if (currentFilter === 'timeout') return r.status === '⏰';
        return true;
    });
    let search = document.getElementById('searchInput')?.value.toLowerCase() || '';
    filtered = filtered.filter(r => (r.username || '').toLowerCase().includes(search) || (r.password || '').toLowerCase().includes(search));
    filtered.sort((a,b) => {
        let av = currentSort.field === 'balance' ? (parseFloat(a.balance)||0) : (a[currentSort.field]||'').toLowerCase();
        let bv = currentSort.field === 'balance' ? (parseFloat(b.balance)||0) : (b[currentSort.field]||'').toLowerCase();
        return currentSort.dir === 'asc' ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
    });
    document.getElementById('resultsBody').innerHTML = filtered.map(r => `
        <tr>
            <td style="font-size:18px">${r.status}</td>
            <td><strong style="color:#58a6ff">${escapeHtml(r.username)}</strong></td>
            <td><span style="font-family:monospace">${escapeHtml(r.password)}</span></td>
            <td class="${(parseFloat(r.balance)||0) > 0 ? 'balance-positive' : 'balance-zero'}">${r.balance || '0'}</td>
            <td style="color:#8b949e;font-size:11px">${escapeHtml(r.error || '-')}</td>
        </tr>
    `).join('');
}

function updateStats() {
    document.getElementById('totalCount').innerText = allResults.length;
    document.getElementById('successCount').innerText = allResults.filter(r => r.status === '✅').length;
    document.getElementById('failedCount').innerText = allResults.filter(r => r.status === '❌').length;
    document.getElementById('timeoutCount').innerText = allResults.filter(r => r.status === '⏰').length;
}

function sortBy(field) {
    if (currentSort.field === field) currentSort.dir = currentSort.dir === 'asc' ? 'desc' : 'asc';
    else { currentSort.field = field; currentSort.dir = field === 'balance' ? 'desc' : 'asc'; }
    renderResults();
}

function setFilter(f) { 
    currentFilter = f; 
    document.querySelectorAll('.filter-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.filter === f)); 
    renderResults(); 
}

function escapeHtml(s) { if(!s) return ''; return s.replace(/[&<>]/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;'})[m]); }

function addLog(msg) { 
    let term = document.getElementById('terminal'); 
    let div = document.createElement('div'); 
    div.className = 'terminal-line'; 
    div.innerHTML = `<span class="time">[${new Date().toLocaleTimeString()}]</span> ${msg}`; 
    term.appendChild(div); 
    div.scrollIntoView(); 
    if(term.children.length > 300) term.removeChild(term.firstChild); 
}

async function startBot() { 
    let acc = document.getElementById('accounts').value; 
    if(!acc.trim()) { addLog('❌ Please enter accounts first'); return; } 
    addLog('🚀 Starting bot...'); 
    try {
        let res = await fetch('/start', {method:'POST', body:acc});
        let data = await res.json();
        if(data.status === 'started') addLog(`✓ Started processing ${data.total} accounts`);
    } catch(e) { addLog('❌ Start failed: ' + e.message); }
}

async function stopBot() { 
    addLog('⏹ Stopping...'); 
    await fetch('/stop', {method:'POST'}); 
}

function clearAccounts() { document.getElementById('accounts').value = ''; addLog('🗑 Accounts cleared'); }

// Auto-load saved accounts
document.getElementById('accounts').value = localStorage.getItem('bot_accounts') || '';
document.getElementById('accounts').addEventListener('input', () => localStorage.setItem('bot_accounts', document.getElementById('accounts').value));

connect();
setInterval(async () => {
    try { let r = await fetch('/results'); let d = await r.json(); if(d.length > allResults.length) { allResults = d; renderResults(); updateStats(); } } 
    catch(e) {}
}, 3000);
</script>
</body>
</html>'''

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 50)
    logger.info("🎮 ADJARABET BOT v13.0 - PRODUCTION READY")
    logger.info(f"⚙ Headless: {CONFIG['headless']}, Timeout: {CONFIG['timeout_navigation']}ms")
    logger.info(f"⚡ Concurrent: {CONFIG['concurrent_limit']}, Browser restart: every {CONFIG['browser_restart_every']} accounts")
    logger.info("=" * 50)
    
    success = await bot.initialize()
    if not success:
        logger.error("Failed to initialize bot")
        raise RuntimeError("Bot initialization failed")
    
    yield
    
    if processing_task and not processing_task.done():
        processing_task.cancel()
        try:
            await processing_task
        except:
            pass
    
    await bot.cleanup()

app = FastAPI(title="Adjarabet Bot", version="13.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def root():
    return HTMLResponse(HTML_UI)

@app.post("/start")
async def start_bot(request: Request):
    global processing_task
    
    body = (await request.body()).decode()
    accounts = []
    
    for line in body.splitlines():
        line = line.strip()
        if line and not line.startswith('#') and ':' in line:
            parts = line.split(':', 1)
            if len(parts) == 2 and parts[0].strip() and parts[1].strip():
                accounts.append(Account(parts[0].strip(), parts[1].strip()))
    
    if not accounts:
        return JSONResponse({"error": "No valid accounts (format: username:password)"}, status_code=400)
    
    if processing_task and not processing_task.done():
        processing_task.cancel()
        try:
            await processing_task
        except:
            pass
    
    processing_task = asyncio.create_task(bot.process_accounts(accounts, manager))
    return {"status": "started", "total": len(accounts)}

@app.post("/stop")
async def stop_bot():
    global processing_task
    if processing_task and not processing_task.done():
        processing_task.cancel()
    await manager.broadcast("STATUS:stopped")
    return {"status": "stopped"}

@app.get("/results")
async def get_results():
    return await bot.get_results()

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "bot_running": bot.is_running,
        "total_processed": len(bot.results),
        "timestamp": datetime.now().isoformat()
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30)
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
    
    print("\n" + "=" * 50)
    print("🎮 ADJARABET BOT v13.0 - OPTIMIZED")
    print("=" * 50)
    print(f"📍 Server: http://{CONFIG['host']}:{CONFIG['port']}")
    print(f"🔧 Headless: {CONFIG['headless']}")
    print(f"⚡ Concurrent accounts: {CONFIG['concurrent_limit']}")
    print(f"🔄 Browser restart: every {CONFIG['browser_restart_every']} accounts")
    print("=" * 50 + "\n")
    
    uvicorn.run(
        app,
        host=CONFIG["host"],
        port=CONFIG["port"],
        log_level="warning",
        access_log=False
    )
