import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Dict, Any, Optional
from collections import deque
import time

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect  # ← Request-ը պետք է լինի
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ================= MINIMAL CONFIG =================
CONFIG = {
    "port": int(os.getenv("PORT", 8000)),
    "host": os.getenv("HOST", "0.0.0.0"),
    "headless": True,
    "timeout_navigation": 10000,
    "timeout_element": 5000,
    "max_retries": 1,
    "concurrent_limit": 1,
    "delay_between_accounts": 0.5,
}

# ================= SIMPLE LOGGER =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("AdjarabetBot")

# ================= DATA MODELS =================
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
            "error": self.error[:50]
        }

# ================= SIMPLE CONNECTION MANAGER =================
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
        for conn in self.active_connections[:]:
            try:
                await conn.send_text(message)
            except:
                self.disconnect(conn)

# ================= SIMPLE BOT =================
class SimpleBot:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.is_running = False
        self.results: List[Account] = []
        
    async def initialize(self):
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox']
            )
            self.context = await self.browser.new_context(
                viewport={'width': 1024, 'height': 600},
                ignore_https_errors=True
            )
            self.page = await self.context.new_page()
            self.is_running = True
            logger.info("Browser ready")
            return True
        except Exception as e:
            logger.error(f"Init failed: {e}")
            return False
    
    def parse_balance(self, text: str) -> str:
        try:
            match = re.search(r'(\d+(?:[.,]\d+)?)', text.replace(',', ''))
            if match:
                val = float(match.group(1))
                return f"{val:,.2f} ₾" if val > 0 else "0 ₾"
        except:
            pass
        return "0 ₾"
    
    async def login_single(self, account: Account) -> Account:
        try:
            await self.context.clear_cookies()
            await self.page.goto('https://www.adjarabet.am/hy', wait_until='domcontentloaded', timeout=10000)
            await asyncio.sleep(0.3)
            
            try:
                balance_el = await self.page.wait_for_selector('[data-test-id="header-user-balance"]', timeout=2000)
                if balance_el:
                    balance_text = await balance_el.inner_text()
                    account.balance = self.parse_balance(balance_text)
                    account.status = AccountStatus.SUCCESS
                    logger.info(f"✅ {account.username} | {account.balance}")
                    return account
            except:
                pass
            
            await self.page.fill('input[name="userIdentifier"]', account.username)
            await self.page.fill('input[type="password"]', account.password)
            await self.page.click('[data-test-id="header-login-button"]')
            
            try:
                balance_el = await self.page.wait_for_selector('[data-test-id="header-user-balance"]', timeout=8000)
                balance_text = await balance_el.inner_text()
                account.balance = self.parse_balance(balance_text)
                account.status = AccountStatus.SUCCESS
                logger.info(f"✅ {account.username} | {account.balance}")
            except:
                try:
                    error_el = await self.page.wait_for_selector('[class*="error"]', timeout=1000)
                    if error_el:
                        account.error = (await error_el.inner_text())[:50]
                except:
                    pass
                account.error = account.error or "Login timeout"
                logger.warning(f"❌ {account.username}: {account.error}")
                
        except Exception as e:
            account.error = str(e)[:50]
            logger.error(f"❌ {account.username}: {account.error}")
        
        return account
    
    async def process_accounts(self, accounts: List[Account], manager: ConnectionManager):
        self.results = []
        total = len(accounts)
        
        logger.info(f"Processing {total} accounts")
        await manager.broadcast(f"PROGRESS:0/{total}")
        
        for i, account in enumerate(accounts):
            if not self.is_running:
                break
            
            result = await self.login_single(account)
            self.results.append(result)
            
            await manager.broadcast(f"RESULT:{json.dumps(result.to_dict())}")
            await manager.broadcast(f"PROGRESS:{i+1}/{total}")
            
            await asyncio.sleep(CONFIG["delay_between_accounts"])
        
        self.save_results()
        
        success = sum(1 for r in self.results if r.status == AccountStatus.SUCCESS)
        logger.info(f"Done: ✅{success} ❌{total-success}")
        await manager.broadcast(f"SUMMARY:{success}/{total}")
    
    def save_results(self):
        try:
            Path("results").mkdir(exist_ok=True)
            with open("results/results.json", "w", encoding="utf-8") as f:
                json.dump([r.to_dict() for r in self.results], f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Save failed: {e}")
    
    async def get_results(self) -> List[Dict]:
        if self.results:
            return [r.to_dict() for r in self.results]
        try:
            with open("results/results.json", "r") as f:
                return json.load(f)
        except:
            return []
    
    async def cleanup(self):
        self.is_running = False
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

# ================= FASTAPI APP =================
manager = ConnectionManager()
bot = SimpleBot()
processing_task: Optional[asyncio.Task] = None

# HTML UI (կրճատված տարբերակ)
HTML_UI = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Adjarabet Bot</title>
    <style>
        * { margin:0; padding:0; box-sizing:border-box; }
        body { background:#0a0c10; color:#e6edf3; font-family:system-ui,sans-serif; padding:15px; }
        .container { max-width:1200px; margin:0 auto; }
        .header { background:#161b22; border-radius:12px; padding:15px 20px; margin-bottom:20px; border:1px solid #30363d; }
        .stats { display:flex; gap:12px; margin-bottom:20px; flex-wrap:wrap; }
        .stat-card { background:#161b22; border-radius:12px; padding:12px 24px; border:1px solid #30363d; text-align:center; cursor:pointer; }
        .stat-number { font-size:28px; font-weight:700; color:#58a6ff; }
        .stat-label { font-size:11px; color:#8b949e; }
        .grid { display:grid; grid-template-columns:1fr 1fr; gap:15px; margin-bottom:20px; }
        .card { background:#161b22; border-radius:12px; border:1px solid #30363d; overflow:hidden; }
        .card-header { padding:12px 15px; background:#0d1117; border-bottom:1px solid #30363d; font-weight:600; }
        textarea { width:100%; height:250px; background:#0d1117; border:1px solid #30363d; border-radius:8px; padding:12px; color:#e6edf3; font-family:monospace; font-size:12px; resize:vertical; }
        .btn { padding:8px 16px; border:none; border-radius:8px; font-weight:600; cursor:pointer; margin-right:8px; }
        .btn-primary { background:#238636; color:white; }
        .btn-primary:hover { background:#2ea043; }
        .btn-danger { background:#da3633; color:white; }
        .log { background:#010409; border-radius:8px; height:250px; overflow-y:auto; padding:10px; font-family:monospace; font-size:11px; }
        .log-line { padding:3px 0; border-bottom:1px solid #21262d; }
        .table-container { max-height:400px; overflow-y:auto; }
        table { width:100%; border-collapse:collapse; }
        th, td { padding:8px 10px; text-align:left; border-bottom:1px solid #21262d; font-size:12px; }
        th { background:#0d1117; cursor:pointer; }
        .balance-positive { color:#3fb950; }
        .balance-zero { color:#f85149; }
        .filter-group { display:flex; gap:8px; padding:10px 15px; flex-wrap:wrap; }
        .filter-btn { padding:4px 12px; background:#21262d; border:none; border-radius:16px; color:#8b949e; cursor:pointer; font-size:11px; }
        .filter-btn.active { background:#58a6ff; color:white; }
        .search { padding:6px 12px; background:#0d1117; border:1px solid #30363d; border-radius:20px; color:white; width:150px; }
        .progress-bar { height:2px; background:#21262d; margin-top:10px; border-radius:2px; }
        .progress-fill { height:100%; background:#3fb950; width:0%; transition:width 0.3s; }
        @media (max-width:700px){ .grid { grid-template-columns:1fr; } }
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h2>🎮 Adjarabet Bot</h2>
        <div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
    </div>
    <div class="stats">
        <div class="stat-card" onclick="setFilter('all')"><div class="stat-number" id="totalCount">0</div><div class="stat-label">TOTAL</div></div>
        <div class="stat-card" onclick="setFilter('success')"><div class="stat-number" id="successCount">0</div><div class="stat-label">✅ SUCCESS</div></div>
        <div class="stat-card" onclick="setFilter('failed')"><div class="stat-number" id="failedCount">0</div><div class="stat-label">❌ FAILED</div></div>
    </div>
    <div class="grid">
        <div class="card">
            <div class="card-header">📝 Accounts (user:pass)</div>
            <div style="padding:15px;">
                <textarea id="accounts" placeholder="user1:pass123&#10;user2:pass456"></textarea>
                <div style="margin-top:12px;">
                    <button class="btn btn-primary" onclick="startBot()">▶ Start</button>
                    <button class="btn btn-danger" onclick="stopBot()">⏹ Stop</button>
                </div>
            </div>
        </div>
        <div class="card">
            <div class="card-header">📡 Console</div>
            <div class="log" id="log"><div class="log-line">● Bot ready</div></div>
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
                <tbody id="results"><tr><td colspan="5" style="text-align:center">Waiting...</td></tr></tbody>
            </table>
        </div>
    </div>
</div>
<script>
let ws=null, results=[], filter='all', sortField='balance', sortDir='desc';

function connect(){
    ws=new WebSocket(`ws://${location.host}/ws`);
    ws.onopen=()=>addLog('🟢 Connected');
    ws.onmessage=e=>{
        let d=e.data;
        if(d.startsWith('RESULT:')){
            let r=JSON.parse(d.slice(7));
            let idx=results.findIndex(x=>x.username===r.username);
            if(idx>=0) results[idx]=r;
            else results.push(r);
            renderResults();
            updateStats();
            addLog(`${r.status} ${r.username} | ${r.balance}`);
        }else if(d.startsWith('PROGRESS:')){
            let p=d.slice(9).split('/');
            document.getElementById('progressFill').style.width=(p[0]/p[1]*100)+'%';
        }else if(!d.startsWith('STATUS:')) addLog(d);
    };
    ws.onclose=()=>setTimeout(connect,3000);
}

function renderResults(){
    let filtered=results.filter(r=>filter=='all'?true:filter=='success'?r.status=='✅':r.status=='❌');
    let search=document.getElementById('search')?.value.toLowerCase()||'';
    filtered=filtered.filter(r=>r.username.toLowerCase().includes(search));
    filtered.sort((a,b)=>{
        let av=sortField=='balance'?(parseFloat(a.balance)||0):(a[sortField]||'');
        let bv=sortField=='balance'?(parseFloat(b.balance)||0):(b[sortField]||'');
        return sortDir=='asc'?(av>bv?1:-1):(av<bv?1:-1);
    });
    document.getElementById('results').innerHTML=filtered.map(r=>`<tr><td>${r.status}</td><td><b>${escape(r.username)}</b></td><td>${escape(r.password)}</td><td class="${(parseFloat(r.balance)||0)>0?'balance-positive':'balance-zero'}">${r.balance}</td><td style="color:#8b949e;font-size:11px">${escape(r.error||'-')}</td></tr>`).join('');
}

function updateStats(){
    document.getElementById('totalCount').innerText=results.length;
    document.getElementById('successCount').innerText=results.filter(r=>r.status=='✅').length;
    document.getElementById('failedCount').innerText=results.filter(r=>r.status=='❌').length;
}

function addLog(msg){
    let log=document.getElementById('log');
    let div=document.createElement('div');
    div.className='log-line';
    div.innerHTML=`[${new Date().toLocaleTimeString()}] ${msg}`;
    log.appendChild(div);
    div.scrollIntoView();
    if(log.children.length>200) log.removeChild(log.firstChild);
}

function setFilter(f){filter=f;document.querySelectorAll('.filter-btn').forEach(b=>b.classList.toggle('active',b.dataset.filter===f));renderResults();}
function sortBy(f){if(sortField===f)sortDir=sortDir=='asc'?'desc':'asc';else{sortField=f;sortDir=f=='balance'?'desc':'asc';}renderResults();}
function escape(s){return s.replace(/[&<>]/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;'})[m]);}

async function startBot(){
    let acc=document.getElementById('accounts').value;
    if(!acc.trim()){addLog('❌ Enter accounts');return;}
    let res=await fetch('/start',{method:'POST',body:acc});
    let data=await res.json();
    addLog(data.status=='started'?`✓ Started ${data.total} accounts`:'❌ Failed');
}
async function stopBot(){await fetch('/stop',{method:'POST'});addLog('⏹ Stopping');}

document.getElementById('accounts').value=localStorage.getItem('accounts')||'';
document.getElementById('accounts').addEventListener('input',()=>localStorage.setItem('accounts',document.getElementById('accounts').value));
document.getElementById('search').addEventListener('input',()=>renderResults());
connect();
</script>
</body>
</html>'''

# ================= API ENDPOINTS =================
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def startup():
    await bot.initialize()

@app.on_event("shutdown")
async def shutdown():
    global processing_task
    if processing_task and not processing_task.done():
        processing_task.cancel()
    await bot.cleanup()

@app.get("/")
async def root():
    return HTMLResponse(HTML_UI)

@app.post("/start")
async def start_bot(request: Request):  # ← հիմա Request-ը սահմանված է
    global processing_task
    
    body = (await request.body()).decode()
    accounts = []
    
    for line in body.splitlines():
        line = line.strip()
        if line and ':' in line and not line.startswith('#'):
            parts = line.split(':', 1)
            if len(parts) == 2 and parts[0].strip() and parts[1].strip():
                accounts.append(Account(parts[0].strip(), parts[1].strip()))
    
    if not accounts:
        return JSONResponse({"error": "No valid accounts"}, status_code=400)
    
    if processing_task and not processing_task.done():
        processing_task.cancel()
        try:
            await processing_task
        except:
            pass
    
    bot.is_running = True
    processing_task = asyncio.create_task(bot.process_accounts(accounts, manager))
    return {"status": "started", "total": len(accounts)}

@app.post("/stop")
async def stop_bot():
    global processing_task
    bot.is_running = False
    if processing_task and not processing_task.done():
        processing_task.cancel()
    await manager.broadcast("STATUS:stopped")
    return {"status": "stopped"}

@app.get("/results")
async def get_results():
    return await bot.get_results()

@app.get("/health")
async def health():
    return {"status": "ok", "processed": len(bot.results), "running": bot.is_running}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.wait_for(websocket.receive_text(), timeout=60)
    except:
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    
    Path("results").mkdir(exist_ok=True)
    
    print("\n" + "=" * 40)
    print("🎮 ADJARABET BOT - LIGHTWEIGHT")
    print("=" * 40)
    print(f"📍 http://{CONFIG['host']}:{CONFIG['port']}")
    print(f"⚡ Single browser, no retries")
    print("=" * 40 + "\n")
    
    uvicorn.run(app, host=CONFIG["host"], port=CONFIG["port"], log_level="warning")
