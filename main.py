import asyncio
import json
import logging
import os
import re
import signal
import sys
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager

import aiofiles
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ================= PRODUCTION CONFIG =================
PRODUCTION_CONFIG = {
    "log_level": os.getenv("LOG_LEVEL", "INFO"),
    "port": int(os.getenv("PORT", 8000)),
    "host": os.getenv("HOST", "0.0.0.0"),
    "headless": os.getenv("HEADLESS", "true").lower() == "true",
    "timeout": int(os.getenv("TIMEOUT", 45000)),
    "results_dir": os.getenv("RESULTS_DIR", "results"),
    "log_file": os.getenv("LOG_FILE", "logs/bot.log"),
    "browser_type": os.getenv("BROWSER_TYPE", "chromium"),
}

# ================= PRODUCTION LOGGER =================
class ProductionLogger:
    def __init__(self):
        Path("logs").mkdir(exist_ok=True)
        
        self.logger = logging.getLogger("AdjarabetBot")
        self.logger.setLevel(getattr(logging, PRODUCTION_CONFIG["log_level"]))
        
        self.logger.handlers.clear()
        
        file_handler = logging.FileHandler(PRODUCTION_CONFIG["log_file"], encoding="utf-8")
        console_handler = logging.StreamHandler()
        
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def info(self, msg): self.logger.info(msg)
    def error(self, msg): self.logger.error(msg)
    def warning(self, msg): self.logger.warning(msg)
    def debug(self, msg): self.logger.debug(msg)

# ================= ENUMS =================
class AccountStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    BLOCKED = "blocked"

# ================= DATA =================
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
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "username": self.username,
            "password": self.password,
            "balance": self.balance,
            "balance_value": self.balance_value,
            "status": self.status.value,
            "error": self.error,
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
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                disconnected.append(connection)
        
        for conn in disconnected:
            self.disconnect(conn)

# ================= FIXED BOT - NEW CONTEXT PER ACCOUNT =================
class SimpleBot:
    def __init__(self):
        self.log = ProductionLogger()
        self.playwright = None
        self.is_running = False
        self.results: List[Account] = []
        self.total_accounts = 0
        self.processed = 0
        
    async def initialize(self):
        """Initialize Playwright (browser engine, not shared context)"""
        try:
            self.log.info("Initializing Playwright...")
            self.playwright = await async_playwright().start()
            self.is_running = True
            self.log.info("✅ Playwright initialized successfully")
            return True
        except Exception as e:
            self.log.error(f"Failed to initialize Playwright: {e}")
            return False
    
    async def cleanup(self):
        """Clean up resources"""
        self.is_running = False
        
        if self.playwright:
            try:
                await self.playwright.stop()
            except:
                pass
        
        self.log.info("🧹 Cleanup completed")
    
    async def login_account(self, account: Account) -> Account:
        """Login single account with FRESH browser context"""
        browser = None
        context = None
        page = None
        
        try:
            # Launch NEW browser for each account (or new context if you prefer)
            # For better performance, we use new context with isolated storage
            browser = await self.playwright.chromium.launch(
                headless=PRODUCTION_CONFIG["headless"],
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--window-size=1280,720'
                ]
            )
            
            # Create FRESH context with no cookies/localStorage from previous accounts
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                ignore_https_errors=True,
                # Important: no shared storage
                storage_state=None
            )
            
            page = await context.new_page()
            
            # Set timeouts
            page.set_default_timeout(PRODUCTION_CONFIG["timeout"])
            page.set_default_navigation_timeout(PRODUCTION_CONFIG["timeout"])
            
            # Navigate to site
            self.log.debug(f"Navigating to login page for {account.username}")
            await page.goto('https://www.adjarabet.am/hy', wait_until='domcontentloaded')
            
            await asyncio.sleep(1)  # Let page stabilize
            
            # Wait for login form
            try:
                await page.wait_for_selector('input[name="userIdentifier"]', timeout=10000)
            except PlaywrightTimeout:
                account.status = AccountStatus.TIMEOUT
                account.error = "Login form not found"
                self.log.warning(f"⏰ {account.username} - Login form not found")
                return account
            
            # Clear fields first
            await page.fill('input[name="userIdentifier"]', '')
            await asyncio.sleep(0.2)
            await page.fill('input[name="userIdentifier"]', account.username)
            await asyncio.sleep(0.3)
            
            await page.fill('input[type="password"]', '')
            await asyncio.sleep(0.2)
            await page.fill('input[type="password"]', account.password)
            await asyncio.sleep(0.3)
            
            # Click login button
            await page.click('[data-test-id="header-login-button"]')
            
            # Wait for balance or error
            try:
                # Wait for balance to appear
                balance_element = await page.wait_for_selector(
                    '[data-test-id="header-user-balance"]', 
                    timeout=15000
                )
                
                balance_text = await balance_element.inner_text()
                account.balance = balance_text.strip()
                account.balance_value = self._parse_balance(balance_text)
                account.status = AccountStatus.SUCCESS
                self.log.info(f"✅ {account.username} - Login successful | Balance: {balance_text}")
                
            except PlaywrightTimeout:
                # Check if login failed
                try:
                    # Try to find error message
                    error_selectors = [
                        '[class*="error"]',
                        '[class*="alert"]',
                        '.error-message',
                        '.alert-danger'
                    ]
                    error_text = ""
                    for selector in error_selectors:
                        try:
                            error_element = await page.wait_for_selector(selector, timeout=1000)
                            if error_element:
                                error_text = await error_element.inner_text()
                                break
                        except:
                            continue
                    
                    if error_text:
                        account.error = error_text[:100]
                    else:
                        account.error = "Login timeout - balance not found"
                except:
                    pass
                
                account.status = AccountStatus.FAILED
                account.error = account.error or "Login failed"
                self.log.warning(f"❌ {account.username} - Login failed: {account.error}")
            
            return account
            
        except Exception as e:
            account.status = AccountStatus.FAILED
            account.error = str(e)[:100]
            self.log.error(f"❌ {account.username} - Error: {str(e)[:80]}")
            return account
            
        finally:
            # Clean up - CLOSE everything for this account
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
            if browser:
                try:
                    await browser.close()
                except:
                    pass
    
    def _parse_balance(self, text: str) -> float:
        """Parse balance from text"""
        try:
            # Remove currency symbols and spaces
            clean_text = text.replace('₾', '').replace('GEL', '').replace(',', '').strip()
            match = re.search(r'([\d]+\.?[\d]*)', clean_text)
            if match:
                return float(match.group(1))
        except Exception as e:
            self.log.debug(f"Balance parse error: {e}")
        return 0.0
    
    async def process_accounts(self, accounts: List[Account], manager: ConnectionManager) -> Dict[str, Any]:
        """Process all accounts - each with fresh browser context"""
        if not self.is_running:
            await self.initialize()
        
        self.results = []
        self.total_accounts = len(accounts)
        self.processed = 0
        start_time = datetime.now()
        
        self.log.info(f"📊 Starting to process {self.total_accounts} accounts")
        await manager.broadcast(f"info:Processing {self.total_accounts} accounts")
        
        for i, account in enumerate(accounts, 1):
            if not self.is_running:
                break
            
            self.log.info(f"🔄 Processing account {i}/{self.total_accounts}: {account.username}")
            
            # Process account with FRESH browser
            result = await self.login_account(account)
            self.results.append(result)
            self.processed = i
            
            # Save results after each account
            await self._save_results()
            
            # Broadcast progress
            progress = (i / self.total_accounts) * 100
            await manager.broadcast(f"progress:{i}:{self.total_accounts}:{progress:.1f}")
            
            # Broadcast result
            await manager.broadcast(f"result:{json.dumps(result.to_dict())}")
            
            # Delay between accounts to avoid rate limiting
            await asyncio.sleep(2)
        
        # Calculate statistics
        duration = (datetime.now() - start_time).total_seconds()
        successful = sum(1 for r in self.results if r.status == AccountStatus.SUCCESS)
        
        stats = {
            "total": self.total_accounts,
            "successful": successful,
            "failed": self.total_accounts - successful,
            "success_rate": (successful / self.total_accounts * 100) if self.total_accounts > 0 else 0,
            "duration_seconds": duration,
            "accounts_per_minute": (self.total_accounts / (duration / 60)) if duration > 0 else 0
        }
        
        self.log.info(f"📈 Completed: {successful}/{self.total_accounts} ({stats['success_rate']:.1f}%) in {duration:.1f}s")
        
        # Send unique balances summary
        unique_balances = set(r.balance for r in self.results if r.status == AccountStatus.SUCCESS)
        self.log.info(f"💰 Unique balances found: {len(unique_balances)}")
        for balance in sorted(unique_balances):
            self.log.info(f"   - {balance}")
        
        await manager.broadcast(f"complete:{json.dumps(stats)}")
        
        return stats
    
    async def _save_results(self):
        """Save results to file"""
        try:
            results_file = Path(PRODUCTION_CONFIG["results_dir"]) / "results.json"
            results_file.parent.mkdir(exist_ok=True)
            
            data = [r.to_dict() for r in self.results]
            
            async with aiofiles.open(results_file, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
        except Exception as e:
            self.log.error(f"Failed to save results: {e}")
    
    async def get_results(self) -> List[Dict[str, Any]]:
        """Get current results"""
        if self.results:
            return [r.to_dict() for r in self.results]
        
        try:
            results_file = Path(PRODUCTION_CONFIG["results_dir"]) / "results.json"
            if results_file.exists():
                async with aiofiles.open(results_file, "r", encoding="utf-8") as f:
                    content = await f.read()
                    return json.loads(content) if content else []
        except:
            pass
        
        return []

# ================= ACCOUNT LOADER =================
def load_accounts(content: str) -> List[Account]:
    """Load accounts from text content"""
    accounts = []
    
    for line in content.splitlines():
        line = line.strip()
        if line and not line.startswith('#'):
            if ':' in line:
                parts = line.split(':', 1)
                username = parts[0].strip()
                password = parts[1].strip() if len(parts) > 1 else ""
                
                if username and password:
                    accounts.append(Account(username, password))
    
    return accounts

# ================= FASTAPI APP =================
manager = ConnectionManager()
bot = SimpleBot()
processing_task: Optional[asyncio.Task] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger = ProductionLogger()
    logger.info("=" * 50)
    logger.info("🎮 ADJARABET BOT PRO v13.0 - FIXED BALANCE ISSUE")
    logger.info("=" * 50)
    
    # Initialize bot
    await bot.initialize()
    
    yield
    
    # Shutdown
    logger.info("Shutting down...")
    if processing_task and not processing_task.done():
        processing_task.cancel()
        try:
            await processing_task
        except:
            pass
    
    await bot.cleanup()
    logger.info("Shutdown complete")

app = FastAPI(
    title="Adjarabet Bot API",
    version="13.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# HTML interface (same as before, but improved)
HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
    <title>Adjarabet Bot - Fixed Balance</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #eee; padding: 20px; }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { text-align: center; margin-bottom: 20px; color: #0f3460; }
        .card { background: #16213e; border-radius: 10px; padding: 20px; margin-bottom: 20px; }
        textarea { width: 100%; padding: 10px; border: 1px solid #0f3460; background: #0f3460; color: #eee; border-radius: 5px; font-family: monospace; font-size: 14px; }
        button { background: #e94560; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; font-size: 16px; margin-right: 10px; }
        button:hover { background: #c73d56; }
        button:disabled { opacity: 0.5; cursor: not-allowed; }
        .status { display: inline-block; padding: 5px 10px; border-radius: 5px; font-size: 12px; margin-top: 10px; }
        .status.running { background: #00ff00; color: #000; }
        .status.stopped { background: #ff0000; color: #fff; }
        .progress-bar { width: 100%; height: 30px; background: #0f3460; border-radius: 5px; overflow: hidden; margin-top: 10px; }
        .progress-fill { height: 100%; background: #e94560; transition: width 0.3s; display: flex; align-items: center; justify-content: center; color: white; font-size: 12px; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #0f3460; }
        th { background: #0f3460; }
        .success { color: #00ff00; font-weight: bold; }
        .failed { color: #ff0000; }
        .timeout { color: #ffaa00; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; margin-bottom: 20px; }
        .stat-card { background: #0f3460; padding: 15px; border-radius: 5px; text-align: center; }
        .stat-value { font-size: 24px; font-weight: bold; margin-top: 5px; }
        .warning { background: #ffaa0022; border-left: 3px solid #ffaa00; padding: 10px; margin-bottom: 10px; border-radius: 5px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🎮 Adjarabet Bot v13 - Fixed Balance</h1>
        
        <div class="warning">
            ⚡ Յուրաքանչյուր ակաունթ մշակվում է առանձին բրաուզերով - բալանսները ճիշտ են ցուցադրվում
        </div>
        
        <div class="card">
            <h3>Controls</h3>
            <button id="startBtn" onclick="startBot()">▶ Start</button>
            <button id="stopBtn" onclick="stopBot()" disabled>⏹ Stop</button>
            <span id="status" class="status stopped">Stopped</span>
        </div>
        
        <div class="card">
            <h3>Accounts (username:password)</h3>
            <textarea id="accounts" rows="5" placeholder="username1:password1&#10;username2:password2&#10;username3:password3"></textarea>
        </div>
        
        <div class="card">
            <h3>Progress</h3>
            <div class="progress-bar">
                <div id="progressFill" class="progress-fill" style="width: 0%">0%</div>
            </div>
        </div>
        
        <div id="stats" class="stats"></div>
        
        <div class="card">
            <h3>Results</h3>
            <div style="overflow-x: auto;">
                <table id="results">
                    <thead>
                        <tr><th>Username</th><th>Balance</th><th>Balance (numeric)</th><th>Status</th><th>Error</th></tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>
    </div>
    
    <script>
        let ws = null;
        
        function connectWebSocket() {
            ws = new WebSocket(`ws://${window.location.host}/ws`);
            
            ws.onopen = () => console.log('WebSocket connected');
            ws.onmessage = (event) => handleMessage(event.data);
            ws.onclose = () => setTimeout(connectWebSocket, 3000);
        }
        
        function handleMessage(data) {
            if (data.startsWith('progress:')) {
                const parts = data.split(':');
                const current = parseInt(parts[1]);
                const total = parseInt(parts[2]);
                const percent = parseFloat(parts[3]);
                document.getElementById('progressFill').style.width = percent + '%';
                document.getElementById('progressFill').textContent = current + '/' + total;
            } else if (data.startsWith('result:')) {
                const result = JSON.parse(data.substring(7));
                addResultToTable(result);
            } else if (data.startsWith('complete:')) {
                const stats = JSON.parse(data.substring(9));
                displayStats(stats);
                document.getElementById('startBtn').disabled = false;
                document.getElementById('stopBtn').disabled = true;
                document.getElementById('status').className = 'status stopped';
                document.getElementById('status').textContent = 'Stopped';
            } else if (data.startsWith('info:')) {
                console.log('Info:', data.substring(5));
            }
        }
        
        function addResultToTable(result) {
            const table = document.getElementById('results').getElementsByTagName('tbody')[0];
            const row = table.insertRow(0);
            row.innerHTML = `
                <td>${escapeHtml(result.username)}</td>
                <td>${escapeHtml(result.balance)}</td>
                <td>${result.balance_value.toFixed(2)}</td>
                <td class="${result.status}">${result.status}</td>
                <td>${escapeHtml(result.error || '-')}</td>
            `;
        }
        
        function displayStats(stats) {
            const statsDiv = document.getElementById('stats');
            statsDiv.innerHTML = `
                <div class="stat-card">Total<br><div class="stat-value">${stats.total}</div></div>
                <div class="stat-card">Successful<br><div class="stat-value success">${stats.successful}</div></div>
                <div class="stat-card">Failed<br><div class="stat-value failed">${stats.failed}</div></div>
                <div class="stat-card">Success Rate<br><div class="stat-value">${stats.success_rate.toFixed(1)}%</div></div>
                <div class="stat-card">Duration<br><div class="stat-value">${(stats.duration_seconds / 60).toFixed(1)} min</div></div>
                <div class="stat-card">Speed<br><div class="stat-value">${stats.accounts_per_minute.toFixed(1)}/min</div></div>
            `;
        }
        
        async function startBot() {
            const accounts = document.getElementById('accounts').value;
            if (!accounts.trim()) {
                alert('Please enter accounts');
                return;
            }
            
            document.getElementById('startBtn').disabled = true;
            document.getElementById(' true;
            document.getElementById(' true;
            document.getElementById('stopBtnstopBtn').disabledstopBtn').disabled').disabled = false;
            = false;
            = false;
            document.getElementById document.getElementById document.getElementById('status').className('status').className = '('status').className = ' = 'status runningstatus runningstatus running';
           ';
            document.getElementById';
            document.getElementById document.getElementById('status('status('status').textContent =').text').textContent = 'Running 'RunningContent = 'Running';
           ';
           ';
            document.getElementById document.getElementById document.getElementById('results('results('results').get').getElementsBy').getElementsByTagNameElementsByTagName('tbodyTagName('tbody('tbody')[0')[0')[0].innerHTML = '';
           ].innerHTML =].innerHTML = '';
            document.getElementById '';
            document.getElementById document.getElementById('progress('progressFill').('progressFill').Fill').style.widthstyle.widthstyle.width = '0% = ' = '0%';
           0%';
           ';
            document.getElementById(' document.getElementById('progress document.getElementByIdprogressFill').Fill').textContent('progressFill').textContenttextContent = ' = ' = '0%0%';
           0%';
           ';
            document.getElementById document.getElementById document.getElementById('stats('stats').inner('stats').innerHTML =').innerHTML =HTML = '';
            
            try '';
            
 '';
            
 {
                           try {
                const response            try {
                const response const response = await fetch('/ = await = awaitstart', fetch('/ fetch('/start', {
                    {
                   start', {
                    method: method: method: 'POST 'POST 'POST',
                    body: accounts
',
                    body: accounts
',
                    body: accounts
                });
                });
                const                });
                const                const data = data = data = await response.json();
 await response await response                if.json();
                if (data.json();
                if (data (data.status !==.status !==.status !== 'start 'start 'started')ed')ed') {
                    {
                    {
                    alert(' alert('Error: alert('Error: ' +Error: ' + data.message ' + data.message);
                   );
                    data.message);
                    document.getElementById document.getElementById document.getElementById('startBtn').('startBtn').('startBtn').disabled =disabled =disabled = false;
 false;
                    document false;
                    document.getElementById('                    document.getElementById('stopBtnstopBtn.getElementById('stopBtn').disabled = true').disabled = true').disabled = true;
                   ;
                   ;
                    document.getElementById document.getElementById('status document.getElementById('status').className('status').className = '').className = ' = 'status stopped';
                   status stopped';
                   status stopped';
                    document.getElementById('status document.getElementById('status document.getElementById('status').textContent = 'St').textContent = 'St').textContent = 'Stopped';
                }
           opped';
                }
           opped';
                }
            } catch } catch } catch (error) {
 (error) {
 (error) {
                alert                alert                alert('Error('Error: ' + error('Error: ' + error: ' + error.message);
.message);
.message);
                document.getElementById('                document.getElementById('                document.getElementById('startBtnstartBtnstartBtn').disabled').disabled = false;
               ').disabled = false;
                = false;
                document.getElementById document.getElementById document.getElementById('stop('stop('stopBtn').Btn').disabled =Btn').disabled =disabled = true;
            }
        }
 true;
            }
        }
 true;
            }
        }
        
               
        async function        
        async function stopBot async function stopBot() {
() {
 stopBot() {
            try            try            try {
                {
                await fetch {
                await fetch await fetch('/stop('/stop('/stop', {', { method: 'POST', { method: 'POST method: 'POST' });
' });
' });
                document                document.getElementById('                document.getElementById('startBtn.getElementById('startBtnstartBtn').disabled').disabled').disabled = false = false = false;
                document.getElementById;
                document.getElementById;
                document.getElementById('stop('stopBtn').('stopBtn').disabled = true;
disabled = true;
Btn').disabled = true;
                document                document.getElementById('status').                document.getElementById('status')..getElementById('status').className =className =className = 'status 'status 'status stopped';
                document.getElementById('status').textContent = 'Sto stopped';
                document.getElementById('status').textContent = 'Sto stopped';
                document.getElementById('status').textContent = 'Stopped';
            }pped';
            }pped';
 catch ( catch (            } catch (error)error)error) {
                console.error {
                console.error('Stop {
                console.error('Stop('Stop error:', error);
            }
 error:', error);
            }
 error:', error);
            }
        }
        }
        }
        
        function escapeHtml(text        
        function escapeHtml(text        
        function escapeHtml(text) {
) {
) {
            const            const div =            const div = div = document.createElement document.createElement document.createElement('div('div('div');
           ');
           ');
            div.text div.text div.textContent =Content = text;
            returnContent = text;
 text;
            return div.innerHTML            return div.innerHTML div.innerHTML;
       ;
        }
        
;
        }
        
 }
        
        async        async function load        async function loadResults() function loadResults() {
            {
           Results() try {
 try {
 {
            try {
                const                const response = await fetch                const response = await fetch response = await fetch('/results('/results');
               ('/results');
                const results');
                const results const results = await = await = await response.json response.json response.json();
               ();
                const tbody =();
                const tbody = const tbody = document.getElementById document.getElementById('results document.getElementById('results('results').getElementsBy').getElementsBy').getElementsByTagName('tbodyTagName('tbodyTagName')[0')[0('tbody')[0];
               ];
                tbody];
                tbody tbody.innerHTML =.innerHTML =.innerHTML = '';
                '';
                '';
                results.forEach(result => results.forEach(result => addResult results.forEach(result => addResult addResultToTableToTableToTable(result));
(result));
(result));
            }            } catch (            } catch ( catch (error) {
               error) {
               error) {
                console.error('Load results error:', error console.error('Load results error:', error console.error('Load results error:', error);
           );
            }
       );
            }
        }
        }
        
 }
        
 }
        
        connectWebSocket();
               connectWebSocket        connectWebSocket loadResults();
       ();
        loadResults loadResults();
    </script>
</();
    </script();
    </script>
</body>
>
</body>
</htmlbody>
</html>
"""

>
"""

@app.get</html@app.get("/")
>
"""

@app.get("/")
("/")
async defasync def root():
async def root():
    return root():
    return HTMLResponse    return HTMLResponse(HTML(HTML HTMLResponse(HTML_PAGE_PAGE_PAGE)

@app)

@app)

@app.get("/.get("/api")
.get("/api")
api")
async def api_infoasync def api_infoasync def api_info():
   ():
   ():
    return {
 return {
 return {
        "        "        "name": "Adjarabet Bot APIname": "Adjarabet Bot APIname": "Adjarabet Bot API",
       ",
        "",
        "version "version": "version": "": "13.13.0",
       13.0",
        " "status":0",
        "status":status": "running "running "running",
       ",
        "fix",
        "fix "fix":": "": " "Each account uses freshEach account uses freshEach account uses fresh browser context browser context browser context - balances are - balances are - balances are now now now correct",
 correct",
 correct",
        "endpoints        "endpoints": {
        "endpoints": {
": {
            "            "            "POST /POST /start":POST /start": "Startstart": "Start "Start bot with accounts",
            " bot with accounts",
            " bot with accounts",
POST /POST /            "POST /stopstop": "Stopstop": "Stop": "Stop bot",
 bot",
 bot",
            "GET /            "GET /results":            "GET /results":results": "Get "Get "Get results",
 results",
            " results",
            "GET /            "GET /GET /health": "Health check",
health": "Health check",
health": "Health check",
            "            "            "WS /ws":WS /WS / "Webws": "Webws": "WebSocket forSocket forSocket for updates"
        }
 updates"
        }
 updates"
        }
    }

    }

    }

@app.post@app.post@app.post("/start("/start")
async("/start")
async")
async def start def start_bot def start_bot(request_bot(request:(request: Request):
: Request):
 Request):
    global    global    global processing_task
    
    processing_task
    
    try:
 processing_task
    
    try:
 try:
        body        body = await        body = await = await request.body request.body()
        request.body()
       ()
        content = content = content = body.decode("utf body.decode("utf body.decode("utf-8-8-8")
        
")
        
        if")
        
        if not content        if not content not content.strip():
.strip():
.strip():
            return            return JSONResponse            return JSONResponse JSONResponse({"status({"status({"status": "": "error", "message": "error",error",": " "message": " "message": "No accountsNo accountsNo accounts provided"}, provided"}, provided"}, status_code status_code status_code=400=400=400)
        
        accounts)
        
        accounts)
        
        accounts = load = load_accounts = load_accounts_accounts(content)
(content)
(content)
        
        if not        
        if not        
        if not accounts:
 accounts:
 accounts:
            return JSONResponse            return JSONResponse            return JSONResponse({"status({"status": "error",({"status": "": "error", "messageerror", "message "message": "": "": "No valid accounts foundNo valid accounts foundNo valid"}, status"}, status accounts found"}, status_code=400)
        
        if processing_task and_code=400)
        
        if processing_task and not processing_task_code=400)
        
        if processing_task and not processing not processing_task.d.done():
_task.done():
one():
            processing            processing_task.c            processing_task.cancel()
_task.cancel()
ancel()
            try            try            try:
               :
               :
                await processing_task
            except await processing_task
            except:
                await processing_task
            except:
                pass
        
:
                pass
        
        async        async pass
        
 def run def run():
           ():
            try:
                await try:
                await        async def run():
            try:
                await manager.b manager.broadcast manager.broadcast("info:Botroadcast("info:Bot("info:Bot started - started - started - each account uses fresh each account uses fresh each account uses fresh browser")
 browser")
 browser")
                stats = await bot.process                stats = await bot.process_accounts                stats = await bot.process_accounts_accounts(accounts(accounts(accounts, manager)
           , manager)
           , manager)
            except as except as except asyncio.Cancyncioyncio.CancelledError.CancelledErrorelledError:
               :
               :
                await manager.broad await manager.broadcast(" await manager.broadcast("cast("info:info:info:Bot stopped by user")
           Bot stopped by user")
           Bot stopped by user")
            except Exception except Exception except Exception as e:
                await manager as e:
                await manager as e:
                await manager.broad.broadcast(f.broadcast(f"info"info:Error:Errorcast(f"info:Error: {str(e: {str(e: {str(e)}")
        
       )}")
        
        processing_task)}")
        
        processing_task = as = as processing_task = asyncioyncioyncio.create_task.create_task.create_task(run(run())
        
(run())
        
())
        
        return JSONResponse        return        return({
            JSONResponse({
            JSONResponse({
            "status": " "status": " "status": "startedstarted",
           started",
           ",
            "message "message": f"Processing "message": f"Processing": f"Processing {len {len(accounts {len(accounts(accounts)} accounts)} accounts with fresh)} accounts with fresh with fresh browser per account",
 browser per account",
 browser per account",
            "total":            "total":            "total": len( len( len(accounts)
accounts)
        })
accounts)
        })
        })
        
           
           
    except Exception except Exception except Exception as e:
        as e:
        return JSON as e:
        return JSON return JSONResponse({"status":Response({"status":Response({"status": "error", " "error", " "error", "message":message": str(emessage": str(e)}, status)}, status_code= str(e)}, status_code=_code=500)

500)

@app.post500)

@app.post("/stop@app.post("/stop("/stop")
async")
async def stop_bot")
async def stop_bot def stop_bot():
   ():
   ():
    global processing global processing global processing_task
    
_task
    
    if_task
    
    if    if processing_task processing_task processing_task and not and not processing_task and not processing_task.done.done processing_task.done():
       ():
       ():
        processing_task processing_task.cancel processing_task.cancel()
       .cancel()
        try:
()
        try:
            await try:
            await processing_task            await processing_task processing_task
       
       
        except:
 except:
            pass
        except:
            pass            pass
        processing_task
        processing_task processing_task = None = None
    
    await manager = None
    
   
    
    await manager await manager.broadcast("info:Bot stopped")
    
    return JSONResponse({"status": "stopped", ".broadcast("info:Bot stopped")
    
    return JSONResponse({"status": "stopped", "message":.broadcast("info:Bot stopped")
    
    return JSONResponse({"status": "stopped", "message":message": "Bot "Bot "Bot stopped"})

@app stopped" stopped".get("/})

@app.get("/})

@app.get("/results")
results")
results")
async def get_resultsasync defasync def get_results():
   ():
    return await bot.get get_results():
    return await return await_results()

 bot.get_results()

@app.get bot.get@app.get("/health_results()

@app.get("/health("/health")
async def health")
async")
async def health_check():
    return def health_check():
    return_check():
 {
        "status {
           return {
        "status": " "status": "healthy",
": "healthy",
healthy",
        "        "bot_r        "bot_running":bot_running":unning": bot.is bot.is_running bot.is_running_running,
       ,
       ,
        " "timestamp": datetime "timestamp": datetime.now()..now().isoformattimestamp": datetime.now().isoformatisoformat(),
       (),
       (),
        "version "version": " "version": "13.": "13.0-f13.0-fixed-bal0-fixed-balixed-balance"
ance"
ance"
    }

    }

    }

@app.websocket@app.@app.websocket("/wswebsocket("/ws")
async("/ws")
async def websocket_end")
async def websocket_end def websocket_endpoint(point(point(websocket: Webwebsocket: WebSocket):
websocket: WebSocket):
    awaitSocket):
    await    await manager.connect manager.connect(webs manager.connect(webs(websocket)
ocket)
ocket)
    try    try:
        while True    try:
       :
        while True:
            while True:
           :
            try:
                data try:
                data try:
                data = await asyncio.wait_for( = await asyncio.wait_for( = await asyncio.wait_for(websocketwebsocketwebsocket.receive.receive.receive_text(), timeout=_text(), timeout=_text(), timeout=30)
30)
30)
                if data ==                if                if "ping data == "ping":
                    data == "ping":
                   ":
                    await webs await websocket.send_text(" await websocket.send_text("ocket.send_text("pong")
           pongpong")
            except as except as")
            except asyncioyncioyncio.TimeoutError:
.TimeoutError.TimeoutError:
                try:
                try                try:
                   :
                   :
                    await websocket.send await websocket.send await websocket.send_text("_text("_text("ping")
               ping")
ping")
                except:
                    except:
                                   except:
                    break
 break
 break
    except    except WebSocket    except WebSocket WebSocketDisconnectDisconnect:
       Disconnect:
       :
        manager.disconnect( manager.disconnect( manager.disconnect(websocketwebsocketwebsocket)
    except Exception)
    except Exception:
       )
    except Exception:
       :
        manager.dis manager.disconnect( manager.disconnect(connect(websocketwebsocketwebsocket)

if)

if __name)

if __name __name__ == "__main__ == "__main__ == "__main__":
   __":
   __":
    import u import uvicorn
    
    import uvicorn
    
   vicorn
    
    Path(P Path(PRODUCTION_CONFIG Path(PRODUCTION_CONFIGRODUCTION_CONFIG["["results["resultsresults_dir"]_dir"]_dir"]).mkdir).mkdir(exist_ok).mkdir(exist_ok(exist_ok=True)
=True)
    Path=True)
    Path    Path("logs").mkdir("logs").mkdir("logs").mkdir(ex(exist_ok(existist=True)
_ok_ok=True)
    
       
   =True)
    
    print(" print("=" * 50 print("=" *=" * 50)
    50)
    print(")
    print("🎮 print("🎮 ADJ🎮 ADJ ADJARABET BARABET BARABOT vOT vET BOT v13.13.0 -13.0 -0 - FIX FIX FIXED BALANCE ISSUED BALED BALANCE ISSUE")
E")
ANCE ISSUE")
    print    print    print("="("="("=" * 50)
 *  * 50)
    print50)
    print(f"(f"    print(f"📍 Server📍 Server: http📍 Server: http: http://{://{PRODUCTION://{PRODUPRODU_CONFIG['hostCTION_CONFIGCTION_CONFIG['host']}:['host']}:{PRODUCTION{PRODUCTION']}:{PRODUCTION_CONFIG['_CONFIG['_CONFIG['port']port']port']}")
    print(f"🔧 Head}")
    print(f"🔧 Head}")
    print(f"🔧 Headless:less: {PROless: {PRO {PRODUCTIONDUCTIONDUCTION_CONFIG['headless_CONFIG['headless']}")
_CONFIG['headless']}")
']}")
    print    print    print(f"⏱ Timeout:(f"⏱ Timeout:(f"⏱ Timeout: {PRO {PRO {PRODUCTIONDUCTION_CONFIG['DUCTION_CONFIG['_CONFIG['timeout']timeout']}ms")
    printtimeout']}ms")
    print}ms")
    print("✅("✅("✅ FIX FIX FIX: Each: Each: Each account uses account uses account uses fresh fresh browser context fresh browser context")
    browser context")
   ")
    print(" print(" print("=" *=" * 50=" * 50)
    
)
    
    u 50)
    
    u    uvicornvicornvicorn.run(
.run(
        app.run(
        app        app,
       ,
       ,
        host=PRODU host=PRODUCTION_CONFIG host=PRODUCTION_CONFIG["hostCTION_CONFIG["host["host"],
       "],
        port="],
        port= port=PRODUPRODUPRODUCTION_CONFIG["port"],
        log_level=PROCTION_CONFIG["port"],
        log_level=PROCTION_CONFIG["port"],
        log_level=PRODUCTION_CONFIG["DUCTIONDUCTION_CONFIG["log_levellog_level_CONFIG["log_level"].lower"].lower(),
       "].lower(),
       (),
        access_log access_log access_log=False
=False
=False
    )
    )
