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
    "user_data_dir": os.getenv("USER_DATA_DIR", "/tmp/playwright_profile"),
}

# ================= PRODUCTION LOGGER =================
class ProductionLogger:
    def __init__(self):
        Path("logs").mkdir(exist_ok=True)
        
        self.logger = logging.getLogger("AdjarabetBot")
        self.logger.setLevel(getattr(logging, PRODUCTION_CONFIG["log_level"]))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # File handler
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

# ================= SIMPLE BOT WITH BETTER ERROR HANDLING =================
class SimpleBot:
    def __init__(self):
        self.log = ProductionLogger()
        self.playwright = None
        self.browser = None
        self.context = None
        self.is_running = False
        self.results: List[Account] = []
        self.total_accounts = 0
        self.processed = 0
        
    async def initialize(self):
        """Initialize browser once"""
        try:
            self.log.info("Initializing Playwright...")
            self.playwright = await async_playwright().start()
            
            self.log.info(f"Launching browser (headless={PRODUCTION_CONFIG['headless']})...")
            
            # Clean up old user data dir
            import shutil
            user_data_dir = Path(PRODUCTION_CONFIG["user_data_dir"])
            if user_data_dir.exists():
                try:
                    shutil.rmtree(user_data_dir)
                except:
                    pass
            user_data_dir.mkdir(parents=True, exist_ok=True)
            
            # Launch browser with persistent context
            self.browser = await self.playwright.chromium.launch(
                headless=PRODUCTION_CONFIG["headless"],
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-accelerated-2d-canvas',
                    '--disable-infobars',
                    '--window-size=1280,720'
                ]
            )
            
            # Create context
            self.context = await self.browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                ignore_https_errors=True
            )
            
            self.is_running = True
            self.log.info("✅ Browser initialized successfully")
            return True
            
        except Exception as e:
            self.log.error(f"Failed to initialize browser: {e}")
            return False
    
    async def cleanup(self):
        """Clean up resources"""
        self.is_running = False
        
        if self.context:
            try:
                await self.context.close()
            except:
                pass
        
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
        
        self.log.info("🧹 Cleanup completed")
    
    async def login_account(self, account: Account) -> Account:
        """Login single account with better error handling"""
        page = None
        
        try:
            # Create new page
            page = await self.context.new_page()
            
            # Set timeouts
            page.set_default_timeout(PRODUCTION_CONFIG["timeout"])
            page.set_default_navigation_timeout(PRODUCTION_CONFIG["timeout"])
            
            # Navigate to site
            self.log.debug(f"Navigating to login page for {account.username}")
            await page.goto('https://www.adjarabet.am/hy', wait_until='domcontentloaded')
            
            # Small random delay
            await asyncio.sleep(0.5)
            
            # Check if already logged in
            try:
                balance_element = await page.wait_for_selector('[data-test-id="header-user-balance"]', timeout=5000)
                if balance_element:
                    balance_text = await balance_element.inner_text()
                    account.balance = balance_text
                    account.balance_value = self._parse_balance(balance_text)
                    account.status = AccountStatus.SUCCESS
                    self.log.info(f"✅ {account.username} - Already logged in | Balance: {balance_text}")
                    return account
            except:
                pass
            
            # Wait for login form
            try:
                await page.wait_for_selector('input[name="userIdentifier"]', timeout=10000)
            except PlaywrightTimeout:
                account.status = AccountStatus.TIMEOUT
                account.error = "Login form not found"
                self.log.warning(f"⏰ {account.username} - Login form not found")
                return account
            
            # Fill username
            await page.fill('input[name="userIdentifier"]', account.username)
            await asyncio.sleep(0.3)
            
            # Fill password
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
                account.balance = balance_text
                account.balance_value = self._parse_balance(balance_text)
                account.status = AccountStatus.SUCCESS
                self.log.info(f"✅ {account.username} - Login successful | Balance: {balance_text}")
                
            except PlaywrightTimeout:
                # Check if login failed
                try:
                    error_element = await page.wait_for_selector('[class*="error"]', timeout=2000)
                    if error_element:
                        error_text = await error_element.inner_text()
                        account.error = error_text[:100]
                except:
                    pass
                
                account.status = AccountStatus.FAILED
                account.error = account.error or "Login timeout - balance not found"
                self.log.warning(f"❌ {account.username} - Login failed: {account.error}")
            
            return account
            
        except Exception as e:
            account.status = AccountStatus.FAILED
            account.error = str(e)[:100]
            self.log.error(f"❌ {account.username} - Error: {str(e)[:80]}")
            return account
            
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass
    
    def _parse_balance(self, text: str) -> float:
        """Parse balance from text"""
        try:
            # Extract numbers from balance text
            match = re.search(r'([\d,]+\.?\d*)', text.replace(',', ''))
            if match:
                return float(match.group(1))
        except:
            pass
        return 0.0
    
    async def process_accounts(self, accounts: List[Account], manager: ConnectionManager) -> Dict[str, Any]:
        """Process all accounts"""
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
            
            # Process account
            result = await self.login_account(account)
            self.results.append(result)
            self.processed = i
            
            # Save results
            await self._save_results()
            
            # Broadcast progress
            progress = (i / self.total_accounts) * 100
            await manager.broadcast(f"progress:{i}:{self.total_accounts}:{progress:.1f}")
            
            # Broadcast result
            await manager.broadcast(f"result:{json.dumps(result.to_dict())}")
            
            # Small delay between accounts
            await asyncio.sleep(1)
        
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
    logger.info("🎮 ADJARABET BOT PRO v12.0 - OPTIMIZED")
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
    version="12.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# HTML interface
HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
    <title>Adjarabet Bot</title>
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
        .success { color: #00ff00; }
        .failed { color: #ff0000; }
        .timeout { color: #ffaa00; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; margin-bottom: 20px; }
        .stat-card { background: #0f3460; padding: 15px; border-radius: 5px; text-align: center; }
        .stat-value { font-size: 24px; font-weight: bold; margin-top: 5px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🎮 Adjarabet Bot</h1>
        
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
                        <tr><th>Username</th><th>Balance</th><th>Status</th><th>Error</th></tr>
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
                <td class="${result.status}">${result.status}</td>
                <td>${escapeHtml(result.error || '')}</td>
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
            document.getElementById('stopBtn').disabled = false;
            document.getElementById('status').className = 'status running';
            document.getElementById('status').textContent = 'Running';
            document.getElementById('results').getElementsByTagName('tbody')[0].innerHTML = '';
            document.getElementById('progressFill').style.width = '0%';
            document.getElementById('progressFill').textContent = '0%';
            document.getElementById('stats').innerHTML = '';
            
            try {
                const response = await fetch('/start', {
                    method: 'POST',
                    body: accounts
                });
                const data = await response.json();
                if (data.status !== 'started') {
                    alert('Error: ' + data.message);
                    document.getElementById('startBtn').disabled = false;
                    document.getElementById('stopBtn').disabled = true;
                    document.getElementById('status').className = 'status stopped';
                    document.getElementById('status').textContent = 'Stopped';
                }
            } catch (error) {
                alert('Error: ' + error.message);
                document.getElementById('startBtn').disabled = false;
                document.getElementById('stopBtn').disabled = true;
            }
        }
        
        async function stopBot() {
            try {
                await fetch('/stop', { method: 'POST' });
                document.getElementById('startBtn').disabled = false;
                document.getElementById('stopBtn').disabled = true;
                document.getElementById('status').className = 'status stopped';
                document.getElementById('status').textContent = 'Stopped';
            } catch (error) {
                console.error('Stop error:', error);
            }
        }
        
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        async function loadResults() {
            try {
                const response = await fetch('/results');
                const results = await response.json();
                const tbody = document.getElementById('results').getElementsByTagName('tbody')[0];
                tbody.innerHTML = '';
                results.forEach(result => addResultToTable(result));
            } catch (error) {
                console.error('Load results error:', error);
            }
        }
        
        connectWebSocket();
        loadResults();
    </script>
</body>
</html>
"""

@app.get("/")
async def root():
    return HTMLResponse(HTML_PAGE)

@app.get("/api")
async def api_info():
    return {
        "name": "Adjarabet Bot API",
        "version": "12.0",
        "status": "running",
        "endpoints": {
            "POST /start": "Start bot with accounts",
            "POST /stop": "Stop bot",
            "GET /results": "Get results",
            "GET /health": "Health check",
            "WS /ws": "WebSocket for updates"
        }
    }

@app.post("/start")
async def start_bot(request: Request):
    global processing_task
    
    try:
        # Get accounts
        body = await request.body()
        content = body.decode("utf-8")
        
        if not content.strip():
            return JSONResponse({"status": "error", "message": "No accounts provided"}, status_code=400)
        
        # Load accounts
        accounts = load_accounts(content)
        
        if not accounts:
            return JSONResponse({"status": "error", "message": "No valid accounts found"}, status_code=400)
        
        # Stop existing task
        if processing_task and not processing_task.done():
            processing_task.cancel()
            try:
                await processing_task
            except:
                pass
        
        # Start new processing
        async def run():
            try:
                await manager.broadcast("info:Bot started")
                stats = await bot.process_accounts(accounts, manager)
            except asyncio.CancelledError:
                await manager.broadcast("info:Bot stopped by user")
            except Exception as e:
                await manager.broadcast(f"info:Error: {str(e)}")
        
        processing_task = asyncio.create_task(run())
        
        return JSONResponse({
            "status": "started",
            "message": f"Processing {len(accounts)} accounts",
            "total": len(accounts)
        })
        
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
    
    await manager.broadcast("info:Bot stopped")
    
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
    
    # Create directories
    Path(PRODUCTION_CONFIG["results_dir"]).mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    print("=" * 50)
    print("🎮 ADJARABET BOT v12.0 - OPTIMIZED")
    print("=" * 50)
    print(f"📍 Server: http://{PRODUCTION_CONFIG['host']}:{PRODUCTION_CONFIG['port']}")
    print(f"🔧 Headless: {PRODUCTION_CONFIG['headless']}")
    print(f"⏱ Timeout: {PRODUCTION_CONFIG['timeout']}ms")
    print("=" * 50)
    
    uvicorn.run(
        app,
        host=PRODUCTION_CONFIG["host"],
        port=PRODUCTION_CONFIG["port"],
        log_level=PRODUCTION_CONFIG["log_level"].lower(),
        access_log=False
    )
