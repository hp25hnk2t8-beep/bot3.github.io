import asyncio
import json
import logging
import os
import re
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
    "headless": os.getenv("HEADLESS", "false").lower() == "true",  # false for debugging
    "timeout": int(os.getenv("TIMEOUT", 45000)),
    "max_retries": int(os.getenv("MAX_RETRIES", 2)),
    "results_dir": os.getenv("RESULTS_DIR", "results"),
    "log_file": os.getenv("LOG_FILE", "logs/bot.log"),
}

# ================= LOGGER =================
class Logger:
    def __init__(self):
        Path("logs").mkdir(exist_ok=True)
        self.logger = logging.getLogger("AdjarabetBot")
        self.logger.setLevel(getattr(logging, PRODUCTION_CONFIG["log_level"]))
        self.logger.handlers.clear()
        
        file_handler = logging.FileHandler(PRODUCTION_CONFIG["log_file"], encoding="utf-8")
        console_handler = logging.StreamHandler()
        
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def info(self, msg): self.logger.info(msg)
    def error(self, msg): self.logger.error(msg)
    def warning(self, msg): self.logger.warning(msg)
    def debug(self, msg): self.logger.debug(msg)

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
        for connection in self.active_connections[:]:
            try:
                await connection.send_text(message)
            except:
                self.disconnect(connection)

# ================= BOT =================
class Bot:
    def __init__(self):
        self.log = Logger()
        self.playwright = None
        self.browser = None
        self.is_running = False
        self.results: List[Account] = []
        self.total_accounts = 0
        self.processed = 0
        self.manager: Optional[ConnectionManager] = None
    
    async def initialize(self):
        """Initialize browser once"""
        try:
            self.log.info("Initializing Playwright...")
            self.playwright = await async_playwright().start()
            
            self.log.info(f"Launching browser (headless={PRODUCTION_CONFIG['headless']})...")
            
            self.browser = await self.playwright.chromium.launch(
                headless=PRODUCTION_CONFIG["headless"],
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process',
                ]
            )
            
            self.is_running = True
            self.log.info("Browser initialized successfully")
            return True
            
        except Exception as e:
            self.log.error(f"Failed to initialize browser: {e}")
            return False
    
    async def cleanup(self):
        """Clean up resources"""
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
        
        self.log.info("Cleanup completed")
    
    async def login_account(self, account: Account, retry_count: int = 0) -> Account:
        """Login single account with retry logic"""
        context = None
        page = None
        
        try:
            # New context for each account
            context = await self.browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                ignore_https_errors=True,
                locale='hy-AM'
            )
            
            page = await context.new_page()
            page.set_default_timeout(PRODUCTION_CONFIG["timeout"])
            
            # Go to login page
            self.log.debug(f"Navigating to site for {account.username}")
            await page.goto('https://www.adjarabet.am/hy', wait_until='networkidle')
            await asyncio.sleep(2)
            
            # Try different selectors for login form
            login_selectors = [
                'input[name="userIdentifier"]',
                'input[placeholder*="Username"]',
                'input[placeholder*="Email"]',
                'input[type="text"][name*="user"]',
                '#login-username',
                '.login-input'
            ]
            
            password_selectors = [
                'input[type="password"]',
                'input[name="password"]',
                '#login-password',
                '.password-input'
            ]
            
            login_button_selectors = [
                '[data-test-id="header-login-button"]',
                'button[type="submit"]',
                '.login-button',
                '#login-btn',
                'button:has-text("Login")',
                'button:has-text("Մուտք")'
            ]
            
            # Find and fill username
            username_field = None
            for selector in login_selectors:
                try:
                    username_field = await page.wait_for_selector(selector, timeout=3000)
                    if username_field:
                        self.log.debug(f"Found username field with selector: {selector}")
                        break
                except:
                    continue
            
            if not username_field:
                # Take screenshot for debugging
                screenshot_path = f"logs/debug_{account.username}.png"
                await page.screenshot(path=screenshot_path)
                raise Exception(f"Could not find username field. Screenshot saved to {screenshot_path}")
            
            await username_field.click()
            await username_field.fill(account.username)
            await asyncio.sleep(0.5)
            
            # Find and fill password
            password_field = None
            for selector in password_selectors:
                try:
                    password_field = await page.wait_for_selector(selector, timeout=3000)
                    if password_field:
                        self.log.debug(f"Found password field with selector: {selector}")
                        break
                except:
                    continue
            
            if not password_field:
                raise Exception("Could not find password field")
            
            await password_field.click()
            await password_field.fill(account.password)
            await asyncio.sleep(0.5)
            
            # Click login button
            login_button = None
            for selector in login_button_selectors:
                try:
                    login_button = await page.wait_for_selector(selector, timeout=3000)
                    if login_button:
                        self.log.debug(f"Found login button with selector: {selector}")
                        break
                except:
                    continue
            
            if not login_button:
                raise Exception("Could not find login button")
            
            await login_button.click()
            
            # Wait for navigation and check for balance
            await page.wait_for_load_state("networkidle", timeout=15000)
            await asyncio.sleep(2)
            
            # Check if login was successful by looking for balance element
            balance_selectors = [
                '[data-test-id="header-user-balance"]',
                '.user-balance',
                '.balance',
                '.header-balance',
                '[class*="balance"]'
            ]
            
            balance_text = None
            for selector in balance_selectors:
                try:
                    balance_el = await page.wait_for_selector(selector, timeout=5000)
                    if balance_el:
                        balance_text = await balance_el.inner_text()
                        self.log.debug(f"Found balance with selector: {selector}")
                        break
                except:
                    continue
            
            # Also check for error messages
            error_selectors = [
                '.error-message',
                '.alert-danger',
                '[class*="error"]',
                'text=Invalid',
                'text=Suspend'
            ]
            
            for selector in error_selectors:
                try:
                    error_el = await page.query_selector(selector)
                    if error_el:
                        error_text = await error_el.inner_text()
                        if error_text and len(error_text) > 0:
                            raise Exception(f"Login error: {error_text[:100]}")
                except:
                    pass
            
            if balance_text:
                account.balance = balance_text.strip()
                account.balance_value = self._parse_balance(balance_text)
                account.status = AccountStatus.SUCCESS
                self.log.info(f"✅ {account.username} | Balance: {balance_text}")
            else:
                # Check if we're still on login page
                current_url = page.url
                if 'login' in current_url or 'signin' in current_url:
                    raise Exception("Still on login page - authentication failed")
                else:
                    account.balance = "0"
                    account.balance_value = 0.0
                    account.status = AccountStatus.SUCCESS
                    self.log.warning(f"⚠️ {account.username} | Logged in but balance not found")
            
            return account
            
        except PlaywrightTimeout as e:
            if retry_count < PRODUCTION_CONFIG["max_retries"]:
                self.log.warning(f"🔄 Retry {retry_count + 1}/{PRODUCTION_CONFIG['max_retries']} for {account.username}")
                await asyncio.sleep(2)
                return await self.login_account(account, retry_count + 1)
            
            account.status = AccountStatus.TIMEOUT
            account.error = f"Timeout: {str(e)[:80]}"
            self.log.warning(f"⏰ {account.username} - Timeout after {retry_count + 1} attempts")
            return account
            
        except Exception as e:
            if retry_count < PRODUCTION_CONFIG["max_retries"]:
                self.log.warning(f"🔄 Retry {retry_count + 1}/{PRODUCTION_CONFIG['max_retries']} for {account.username}: {str(e)[:50]}")
                await asyncio.sleep(2)
                return await self.login_account(account, retry_count + 1)
            
            account.status = AccountStatus.FAILED
            account.error = str(e)[:100]
            self.log.error(f"❌ {account.username}: {str(e)[:80]}")
            return account
            
        finally:
            try:
                if page:
                    await page.close()
            except:
                pass
            
            try:
                if context:
                    await context.close()
            except:
                pass
    
    def _parse_balance(self, text: str) -> float:
        try:
            # Extract numbers with commas and dots
            clean_text = re.sub(r'[^\d,.]', '', text)
            # Handle Armenian currency format
            clean_text = clean_text.replace(',', '')
            match = re.search(r'(\d+(?:\.\d+)?)', clean_text)
            if match:
                return float(match.group(1))
        except Exception as e:
            self.log.debug(f"Balance parsing error: {e}")
        return 0.0
    
    async def process_accounts(self, accounts: List[Account], manager: ConnectionManager):
        """Process all accounts"""
        self.manager = manager
        self.results = []
        self.total_accounts = len(accounts)
        self.processed = 0
        start_time = datetime.now()
        
        self.log.info(f"Processing {self.total_accounts} accounts")
        
        for i, account in enumerate(accounts, 1):
            if not self.is_running:
                self.log.info("Bot stopped by user")
                break
            
            self.log.info(f"Processing {i}/{self.total_accounts}: {account.username}")
            result = await self.login_account(account)
            self.results.append(result)
            self.processed = i
            
            # Save results after each account
            await self._save_results()
            
            # Send real-time updates
            await manager.broadcast(f"RESULT:{json.dumps(result.to_dict())}")
            await manager.broadcast(f"PROGRESS:{i}/{self.total_accounts}")
            
            # Small delay between accounts
            await asyncio.sleep(1)
        
        # Send summary
        successful = sum(1 for r in self.results if r.status == AccountStatus.SUCCESS)
        rate = (successful / self.total_accounts * 100) if self.total_accounts > 0 else 0
        
        summary_msg = f"SUMMARY:✅ {successful} success, ❌ {self.total_accounts - successful} failed, 📊 {rate:.1f}% success rate"
        await manager.broadcast(summary_msg)
        await manager.broadcast("STATUS:stopped")
        
        duration = (datetime.now() - start_time).total_seconds()
        self.log.info(f"Completed: {successful}/{self.total_accounts} ({rate:.1f}%) in {duration:.1f}s")
    
    async def _save_results(self):
        try:
            results_file = Path(PRODUCTION_CONFIG["results_dir"]) / "results.json"
            results_file.parent.mkdir(exist_ok=True)
            
            data = [r.to_dict() for r in self.results]
            async with aiofiles.open(results_file, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
        except Exception as e:
            self.log.error(f"Failed to save results: {e}")
    
    async def get_results(self) -> List[Dict]:
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

# ================= FASTAPI APP =================
manager = ConnectionManager()
bot = Bot()
processing_task: Optional[asyncio.Task] = None

# Simple HTML UI (minimal version for testing)
HTML_UI = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Adjarabet Bot</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #1a1a2e; color: #eee; }
        .container { max-width: 1200px; margin: 0 auto; }
        .card { background: #16213e; border-radius: 10px; padding: 20px; margin-bottom: 20px; }
        textarea { width: 100%; min-height: 200px; background: #0f0f1a; color: #fff; border: 1px solid #2c3e50; padding: 10px; font-family: monospace; }
        button { background: #e94560; color: white; border: none; padding: 10px 20px; margin: 5px; cursor: pointer; border-radius: 5px; }
        button:hover { background: #ff6b6b; }
        .status { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 5px; }
        .status.running { background: #00ff00; box-shadow: 0 0 5px #00ff00; }
        .status.stopped { background: #ff0000; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #2c3e50; }
        th { background: #0f3460; cursor: pointer; }
        tr:hover { background: #1a1a2e; }
        .success { color: #00ff00; }
        .error { color: #ff4444; }
        .timeout { color: #ffaa00; }
        .progress-bar { width: 100%; height: 20px; background: #2c3e50; border-radius: 10px; overflow: hidden; margin: 10px 0; }
        .progress-fill { height: 100%; background: #00ff00; transition: width 0.3s; }
        .terminal { background: #0f0f1a; padding: 10px; border-radius: 5px; height: 200px; overflow-y: auto; font-family: monospace; font-size: 12px; }
        .stats { display: flex; gap: 20px; margin-bottom: 20px; }
        .stat { flex: 1; background: #0f3460; padding: 15px; border-radius: 10px; text-align: center; }
        .stat-value { font-size: 32px; font-weight: bold; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Adjarabet Bot v13.0</h1>
        
        <div class="stats">
            <div class="stat">
                <div>Total Accounts</div>
                <div class="stat-value" id="totalCount">0</div>
            </div>
            <div class="stat">
                <div>Success</div>
                <div class="stat-value" id="successCount" style="color: #00ff00;">0</div>
            </div>
            <div class="stat">
                <div>Failed</div>
                <div class="stat-value" id="failedCount" style="color: #ff4444;">0</div>
            </div>
            <div class="stat">
                <div>Timeout</div>
                <div class="stat-value" id="timeoutCount" style="color: #ffaa00;">0</div>
            </div>
        </div>
        
        <div class="card">
            <h3>Account Management</h3>
            <textarea id="accounts" placeholder="username1:password1&#10;username2:password2"></textarea>
            <div>
                <button onclick="startBot()">▶ Start Bot</button>
                <button onclick="stopBot()">⏹ Stop</button>
                <button onclick="clearAccounts()">🗑 Clear</button>
                <button onclick="loadSample()">📝 Sample</button>
                <button onclick="clearResults()">🗑 Clear Results</button>
            </div>
        </div>
        
        <div class="card">
            <h3>Progress</h3>
            <div class="progress-bar">
                <div class="progress-fill" id="progressBar"></div>
            </div>
            <div id="progressText">Waiting to start...</div>
        </div>
        
        <div class="card">
            <h3>Live Console</h3>
            <div class="terminal" id="terminal">
                <div>🎮 Bot initialized - Waiting for commands...</div>
            </div>
        </div>
        
        <div class="card">
            <h3>Results</h3>
            <div style="overflow-x: auto;">
                <table id="resultsTable">
                    <thead>
                        <tr>
                            <th>Status</th>
                            <th>Username</th>
                            <th>Password</th>
                            <th>Balance</th>
                            <th>Error</th>
                        </tr>
                    </thead>
                    <tbody id="resultsBody">
                        <tr><td colspan="5">No results yet...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    
    <script>
        let ws = null;
        let allResults = [];
        
        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
            
            ws.onopen = () => {
                addLog('🟢 Connected to server');
                loadResults();
            };
            
            ws.onmessage = (event) => {
                const data = event.data;
                if (data.startsWith('RESULT:')) {
                    const result = JSON.parse(data.substring(7));
                    addOrUpdateResult(result);
                    addLog(`${result.status} ${result.username} | Balance: ${result.balance || '0'}`);
                } else if (data.startsWith('PROGRESS:')) {
                    const parts = data.substring(9).split('/');
                    const processed = parseInt(parts[0]);
                    const total = parseInt(parts[1]);
                    const percent = (processed / total * 100);
                    document.getElementById('progressBar').style.width = percent + '%';
                    document.getElementById('progressText').innerHTML = `Processing: ${processed}/${total} (${percent.toFixed(1)}%)`;
                } else if (data.startsWith('SUMMARY:')) {
                    addLog('📊 ' + data.substring(8));
                } else if (data.startsWith('STATUS:')) {
                    const status = data.substring(7);
                    addLog(`Bot status: ${status}`);
                } else {
                    addLog(data);
                }
            };
            
            ws.onclose = () => {
                addLog('🔴 Disconnected - Reconnecting...');
                setTimeout(connectWebSocket, 3000);
            };
        }
        
        function addOrUpdateResult(newResult) {
            const index = allResults.findIndex(r => r.username === newResult.username);
            if (index >= 0) {
                allResults[index] = newResult;
            } else {
                allResults.push(newResult);
            }
            renderResults();
            updateStats();
        }
        
        function renderResults() {
            const tbody = document.getElementById('resultsBody');
            if (allResults.length === 0) {
                tbody.innerHTML = '<tr><td colspan="5">No results yet...</td></tr>';
                return;
            }
            
            tbody.innerHTML = allResults.map(r => `
                <tr>
                    <td class="${r.status === '✅' ? 'success' : r.status === '❌' ? 'error' : 'timeout'}">${r.status}</td>
                    <td>${escapeHtml(r.username)}</td>
                    <td>${escapeHtml(r.password)}</td>
                    <td>${r.balance || '0'}</td>
                    <td>${escapeHtml(r.error || '-')}</td>
                </tr>
            `).join('');
        }
        
        function updateStats() {
            document.getElementById('totalCount').innerHTML = allResults.length;
            document.getElementById('successCount').innerHTML = allResults.filter(r => r.status === '✅').length;
            document.getElementById('failedCount').innerHTML = allResults.filter(r => r.status === '❌').length;
            document.getElementById('timeoutCount').innerHTML = allResults.filter(r => r.status === '⏰').length;
        }
        
        function addLog(message) {
            const terminal = document.getElementById('terminal');
            const time = new Date().toLocaleTimeString();
            const div = document.createElement('div');
            div.innerHTML = `[${time}] ${message}`;
            terminal.appendChild(div);
            div.scrollIntoView();
            while (terminal.children.length > 100) terminal.removeChild(terminal.firstChild);
        }
        
        function escapeHtml(str) {
            if (!str) return '';
            return str.replace(/[&<>]/g, m => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' })[m]);
        }
        
        async function startBot() {
            const accounts = document.getElementById('accounts').value;
            if (!accounts.trim()) {
                addLog('❌ Please enter accounts first!');
                return;
            }
            addLog('🚀 Starting bot...');
            try {
                const response = await fetch('/start', { method: 'POST', body: accounts });
                const data = await response.json();
                if (data.status === 'started') {
                    allResults = [];
                    renderResults();
                    updateStats();
                    document.getElementById('progressBar').style.width = '0%';
                    addLog(`✅ Started processing ${data.total} accounts`);
                }
            } catch(e) {
                addLog(`❌ Error: ${e.message}`);
            }
        }
        
        async function stopBot() {
            addLog('⏹ Stopping bot...');
            try {
                await fetch('/stop', { method: 'POST' });
                addLog('✅ Bot stopped');
            } catch(e) {
                addLog(`❌ Error: ${e.message}`);
            }
        }
        
        async function clearAccounts() {
            document.getElementById('accounts').value = '';
            addLog('🗑 All accounts cleared');
        }
        
        function clearResults() {
            allResults = [];
            renderResults();
            updateStats();
            addLog('🗑 Results cleared');
        }
        
        function loadSample() {
            document.getElementById('accounts').value = 'testuser1:password123\\ntestuser2:password456\\ntestuser3:password789';
            addLog('📝 Sample accounts loaded');
        }
        
        async function loadResults() {
            try {
                const response = await fetch('/results');
                const data = await response.json();
                if (Array.isArray(data) && data.length > 0) {
                    allResults = data;
                    renderResults();
                    updateStats();
                }
            } catch(e) {}
        }
        
        // Auto-refresh results every 3 seconds
        setInterval(loadResults, 3000);
        
        // Initialize
        connectWebSocket();
        loadResults();
    </script>
</body>
</html>'''

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger = Logger()
    logger.info("=" * 50)
    logger.info("ADJARABET BOT v13.0 - ENHANCED")
    logger.info("=" * 50)
    
    await bot.initialize()
    
    yield
    
    logger.info("Shutting down...")
    if processing_task and not processing_task.done():
        processing_task.cancel()
        try:
            await processing_task
        except:
            pass
    
    await bot.cleanup()
    logger.info("Shutdown complete")

app = FastAPI(title="Adjarabet Bot API", version="13.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return HTMLResponse(HTML_UI)

@app.post("/start")
async def start_bot(request: Request):
    global processing_task
    
    try:
        body = await request.body()
        content = body.decode("utf-8")
        
        if not content.strip():
            return JSONResponse({"status": "error", "message": "No accounts provided"}, status_code=400)
        
        accounts = []
        for line in content.splitlines():
            line = line.strip()
            if line and not line.startswith('#') and ':' in line:
                parts = line.split(':', 1)
                username = parts[0].strip()
                password = parts[1].strip() if len(parts) > 1 else ""
                if username and password:
                    accounts.append(Account(username, password))
        
        if not accounts:
            return JSONResponse({"status": "error", "message": "No valid accounts found"}, status_code=400)
        
        if processing_task and not processing_task.done():
            processing_task.cancel()
            try:
                await processing_task
            except:
                pass
        
        async def run():
            try:
                await manager.broadcast("STATUS:started")
                await bot.process_accounts(accounts, manager)
            except asyncio.CancelledError:
                await manager.broadcast("STATUS:stopped")
            except Exception as e:
                bot.log.error(f"Processing error: {e}")
                await manager.broadcast(f"ERROR:{str(e)}")
                await manager.broadcast("STATUS:stopped")
        
        processing_task = asyncio.create_task(run())
        
        return JSONResponse({"status": "started", "message": f"Processing {len(accounts)} accounts", "total": len(accounts)})
        
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.post("/stop")
async def stop_bot():
    global processing_task
    
    if processing_task and not processing_task.done():
        processing_task.cancel()
        try:
            await processing_task
        except:
            pass
        processing_task = None
    
    await manager.broadcast("STATUS:stopped")
    
    return JSONResponse({"status": "stopped", "message": "Bot stopped"})

@app.get("/results")
async def get_results():
    return await bot.get_results()

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "bot_running": bot.is_running,
        "timestamp": datetime.now().isoformat()
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                if data == "ping":
                    await websocket.send_text("pong")
            except asyncio.TimeoutError:
                try:
                    await websocket.send_text("ping")
                except:
                    break
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    
    Path(PRODUCTION_CONFIG["results_dir"]).mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    print("=" * 50)
    print("🎮 ADJARABET BOT v13.0 - ENHANCED")
    print("=" * 50)
    print(f"📍 Server: http://{PRODUCTION_CONFIG['host']}:{PRODUCTION_CONFIG['port']}")
    print(f"🔧 Headless: {PRODUCTION_CONFIG['headless']}")
    print(f"⏱ Timeout: {PRODUCTION_CONFIG['timeout']}ms")
    print(f"🔄 Max Retries: {PRODUCTION_CONFIG['max_retries']}")
    print("=" * 50)
    print("⚠️  If login fails, check if selectors need updating")
    print("💡 Debug screenshots saved to logs/debug_*.png")
    print("=" * 50)
    
    uvicorn.run(
        app,
        host=PRODUCTION_CONFIG["host"],
        port=PRODUCTION_CONFIG["port"],
        log_level=PRODUCTION_CONFIG["log_level"].lower(),
        access_log=False
    )
