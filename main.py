import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import List, Dict, Optional
from contextlib import asynccontextmanager
from collections import deque
import time
import secrets

import aiofiles
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from dotenv import load_dotenv
load_dotenv()

# ================= AUTH CONFIG =================
AUTH_USERNAME = os.getenv("BOT_USERNAME")
AUTH_PASSWORD = os.getenv("BOT_PASSWORD")
MOBILE_PIN = os.getenv("MOBILE_PIN", "1111")

if not AUTH_USERNAME or not AUTH_PASSWORD:
    raise ValueError("❌ BOT_USERNAME and BOT_PASSWORD must be set in .env file")

security = HTTPBasic()

def verify_auth(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, AUTH_USERNAME)
    correct_password = secrets.compare_digest(credentials.password, AUTH_PASSWORD)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# ================= TOKEN MANAGER =================
class TokenManager:
    def __init__(self):
        self.tokens: Dict[str, datetime] = {}
    
    def create_token(self) -> str:
        token = secrets.token_urlsafe(32)
        self.tokens[token] = datetime.now() + timedelta(minutes=5)
        return token
    
    def verify_token(self, token: str) -> bool:
        if token not in self.tokens:
            return False
        if self.tokens[token] < datetime.now():
            del self.tokens[token]
            return False
        return True
    
    def remove_token(self, token: str):
        self.tokens.pop(token, None)

token_manager = TokenManager()

# ================= CONFIG =================
CONFIG = {
    "port": int(os.getenv("PORT", 8000)),
    "host": os.getenv("HOST", "0.0.0.0"),
    "headless": os.getenv("HEADLESS", "True").lower() == "true",
    "timeout_nav": int(os.getenv("TIMEOUT_NAV", 15000)),
    "timeout_element": int(os.getenv("TIMEOUT_ELEMENT", 5000)),
    "max_retries": int(os.getenv("MAX_RETRIES", 1)),
    "concurrent_limit": int(os.getenv("CONCURRENT_LIMIT", 4)),
    "delay_between": float(os.getenv("DELAY_BETWEEN", 0.3)),
    "loop_mode": os.getenv("LOOP_MODE", "True").lower() == "true",
    "loop_delay": float(os.getenv("LOOP_DELAY", 5.0)),
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
    def __init__(self, max_req: int = 5, window: float = 10.0):
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
        self.all_accounts: List[Account] = []
        self.current_index = 0
        self.semaphore = None
        self.rate_limiter = RateLimiter()
        self._running = False
        self._stop_flag = False
        self._processing_task: Optional[asyncio.Task] = None
        self._loop_task: Optional[asyncio.Task] = None

    async def init(self):
        logger.info("Starting browser...")
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=CONFIG["headless"],
            args=[
                '--no-sandbox', 
                '--disable-setuid-sandbox', 
                '--disable-dev-shm-usage', 
                '--disable-gpu',
                '--disable-blink-features=AutomationControlled'
            ]
        )
        self.semaphore = asyncio.Semaphore(CONFIG["concurrent_limit"])
        self._running = True
        logger.info(f"✓ Browser ready | Concurrent tabs: {CONFIG['concurrent_limit']}")
        logger.info(f"🔄 LOOP MODE: {'ON' if CONFIG['loop_mode'] else 'OFF'} (will restart from beginning after completion)")
        return True

    async def cleanup(self):
        self._running = False
        if self._processing_task and not self._processing_task.done():
            self._processing_task.cancel()
        if self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
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

    async def _login_one(self, acc: Account, worker_id: int) -> Account:
        ctx = None
        page = None
        try:
            await self.rate_limiter.acquire()
            
            logger.info(f"[Worker {worker_id}] 🔄 Processing: {acc.username}")
            
            ctx = await self.browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                ignore_https_errors=True
            )
            page = await ctx.new_page()
            page.set_default_timeout(CONFIG["timeout_nav"])
            
            for attempt in range(CONFIG["max_retries"] + 1):
                if not self._running or self._stop_flag:
                    return acc
                try:
                    await page.goto('https://www.adjarabet.am/hy', wait_until='domcontentloaded')
                    await asyncio.sleep(0.5)
                    
                    try:
                        bal_el = await page.wait_for_selector('[data-test-id="header-user-balance"]', timeout=2000)
                        if bal_el:
                            txt = await bal_el.inner_text()
                            acc.balance, acc.balance_value = self._parse_balance(txt)
                            acc.status = AccountStatus.SUCCESS
                            logger.info(f"[Worker {worker_id}] ✅ Already logged in: {acc.username} | Balance: {acc.balance}")
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
                    logger.info(f"[Worker {worker_id}] ✅ Login success: {acc.username} | Balance: {acc.balance}")
                    return acc
                    
                except PlaywrightTimeout:
                    if attempt == CONFIG["max_retries"]:
                        acc.status = AccountStatus.TIMEOUT
                        acc.error = "Response timeout"
                        logger.warning(f"[Worker {worker_id}] ⏰ Timeout: {acc.username}")
                    else:
                        await asyncio.sleep(1.5)
                        continue
                except Exception as e:
                    if attempt == CONFIG["max_retries"]:
                        acc.status = AccountStatus.FAILED
                        acc.error = str(e)[:60]
                        logger.warning(f"[Worker {worker_id}] ❌ Failed: {acc.username} | {str(e)[:50]}")
                    else:
                        await asyncio.sleep(1.5)
                        continue
            return acc
            
        except Exception as e:
            acc.status = AccountStatus.FAILED
            acc.error = str(e)[:60]
            logger.error(f"[Worker {worker_id}] ❌ Exception: {acc.username} | {str(e)[:50]}")
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

    async def _worker(self, acc: Account, worker_id: int, manager: ConnectionManager):
        async with self.semaphore:
            if self._stop_flag:
                return acc
            result = await self._login_one(acc, worker_id)
            self.results.append(result)
            self.current_index += 1
            await manager.broadcast(f"RESULT:{json.dumps(result.to_dict())}")
            await manager.broadcast(f"PROGRESS:{self.current_index}/{len(self.all_accounts)}")
            return result

    async def _run_one_cycle(self, accounts: List[Account], manager: ConnectionManager) -> bool:
        self.all_accounts = accounts.copy()
        self.results = []
        self.current_index = 0
        self._stop_flag = False
        
        total = len(self.all_accounts)
        start_time = time.time()
        
        logger.info(f"🔄 Starting NEW CYCLE: {total} accounts")
        await manager.broadcast(f"STATUS:started_cycle")
        await manager.broadcast(f"PROGRESS:{self.current_index}/{total}")
        
        remaining_accounts = self.all_accounts[self.current_index:]
        
        if not remaining_accounts:
            logger.info("No accounts to process")
            await manager.broadcast("STATUS:cycle_empty")
            return True
        
        logger.info(f"🚀 Starting concurrent processing with {CONFIG['concurrent_limit']} tabs")
        
        tasks = []
        for i, acc in enumerate(remaining_accounts):
            if self._stop_flag:
                break
            worker_id = i % CONFIG["concurrent_limit"]
            task = asyncio.create_task(self._worker(acc, worker_id, manager))
            tasks.append(task)
            await asyncio.sleep(CONFIG["delay_between"])
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        if not self._stop_flag:
            await self._save_results()
            
            elapsed = time.time() - start_time
            success = sum(1 for r in self.results if r.status == AccountStatus.SUCCESS)
            fail = sum(1 for r in self.results if r.status == AccountStatus.FAILED)
            to = sum(1 for r in self.results if r.status == AccountStatus.TIMEOUT)
            
            rate_per_sec = len(self.results) / elapsed if elapsed > 0 else 0
            logger.info(f"📊 Cycle complete: {elapsed:.1f}s | {rate_per_sec:.2f} acc/sec | ✅{success} ❌{fail} ⏰{to}")
            await manager.broadcast(f"SUMMARY:{success}/{len(self.results)}:{success/len(self.results)*100:.1f}")
            await manager.broadcast("STATUS:cycle_completed")
            return True
        
        return False

    async def start(self, accounts: List[Account], manager: ConnectionManager):
        if self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
            await asyncio.sleep(0.5)
        
        if CONFIG["loop_mode"]:
            self._loop_task = asyncio.create_task(self._run_loop(accounts, manager))
            logger.info(f"▶ LOOP MODE STARTED - will run continuously")
            await manager.broadcast("STATUS:loop_mode_started")
        else:
            self._processing_task = asyncio.create_task(self._run_one_cycle(accounts, manager))
            logger.info(f"▶ SINGLE RUN MODE - will stop after one cycle")
            await manager.broadcast("STATUS:started")

    async def _run_loop(self, original_accounts: List[Account], manager: ConnectionManager):
        cycle_number = 0
        self._running = True
        
        while self._running and not self._stop_flag:
            cycle_number += 1
            logger.info(f"🔄 ===== LOOP CYCLE #{cycle_number} STARTING =====")
            await manager.broadcast(f"STATUS:loop_cycle_{cycle_number}")
            
            accounts_copy = [Account(acc.username, acc.password) for acc in original_accounts]
            
            completed = await self._run_one_cycle(accounts_copy, manager)
            
            if not self._running or self._stop_flag:
                logger.info(f"⏹ Loop stopped at cycle #{cycle_number}")
                await manager.broadcast("STATUS:loop_stopped")
                break
            
            if completed and CONFIG["loop_mode"]:
                logger.info(f"✅ Cycle #{cycle_number} completed. Waiting {CONFIG['loop_delay']}s before next cycle...")
                await manager.broadcast(f"STATUS:waiting_next_cycle:{CONFIG['loop_delay']}")
                
                for _ in range(int(CONFIG['loop_delay'])):
                    if self._stop_flag or not self._running:
                        break
                    await asyncio.sleep(1)
                
                if not self._stop_flag and self._running:
                    logger.info(f"🔄 Starting CYCLE #{cycle_number + 1}")
                    await manager.broadcast("STATUS:starting_next_cycle")
        
        self._running = False
        logger.info("Loop finished")

    async def stop(self):
        self._stop_flag = True
        logger.info(f"⏹ Stop signal sent")

    async def reset(self):
        self.all_accounts = []
        self.results = []
        self.current_index = 0
        self._stop_flag = False
        self._running = False
        if self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
        if self._processing_task and not self._processing_task.done():
            self._processing_task.cancel()
        Path("results/results.json").unlink(missing_ok=True)
        logger.info("🔄 Bot reset - fresh start ready")

    async def clear_results(self):
        self.results = []
        self.current_index = 0
        Path("results/results.json").unlink(missing_ok=True)
        logger.info("🗑 Results cleared")

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

manager = ConnectionManager()
bot = Bot()
processing_task: Optional[asyncio.Task] = None

async def retry_single_account(acc: Account):
    try:
        result = await bot._login_one(acc, 99)
        
        existing_index = None
        for i, r in enumerate(bot.results):
            if r.username == result.username:
                existing_index = i
                break
        
        if existing_index is not None:
            bot.results[existing_index] = result
        else:
            bot.results.append(result)
        
        for i, a in enumerate(bot.all_accounts):
            if a.username == result.username:
                bot.all_accounts[i] = result
                break
        
        await bot._save_results()
        await manager.broadcast(f"RESULT:{json.dumps(result.to_dict())}")
        logger.info(f"🔄 Retry completed for {result.username}: {result.status.value}")
        return result
    except Exception as e:
        logger.error(f"Retry failed for {acc.username}: {e}")
        return None

# ================= MAIN UI =================
HTML_UI = '''<!DOCTYPE html>
<html lang="hy">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Adjarabet Bot | LOOP MODE v27.0</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * { margin:0; padding:0; box-sizing:border-box; }
        body { background: linear-gradient(135deg, #0a0c10 0%, #0d1117 100%); color: #e6edf3; font-family: 'Inter', sans-serif; padding: 20px; min-height: 100vh; }
        .container { max-width: 1600px; margin: 0 auto; }
        .login-overlay { position: fixed; top:0; left:0; width:100%; height:100%; background: rgba(0,0,0,0.9); backdrop-filter: blur(12px); display: flex; align-items: center; justify-content: center; z-index: 1000; }
        .login-box { background: #161b22; border: 1px solid #30363d; border-radius: 24px; padding: 40px; width: 320px; text-align: center; box-shadow: 0 0 30px rgba(0,0,0,0.5); }
        .login-box h2 { margin-bottom: 24px; background: linear-gradient(135deg, #58a6ff, #3fb950); -webkit-background-clip: text; background-clip: text; color: transparent; }
        .login-box input { width: 100%; padding: 12px; margin: 8px 0; background: #0d1117; border: 1px solid #30363d; border-radius: 12px; color: white; font-size: 14px; }
        .login-box button { width: 100%; padding: 12px; background: linear-gradient(135deg, #238636, #2ea043); border: none; border-radius: 12px; color: white; font-weight: bold; cursor: pointer; margin-top: 16px; }
        .login-box button:hover { transform: translateY(-1px); }
        .error-text { color: #f85149; font-size: 12px; margin-top: 12px; }
        .main-content { display: none; }
        .header { background: linear-gradient(135deg, rgba(22,27,34,0.95), rgba(13,17,23,0.95)); backdrop-filter: blur(10px); border-radius: 20px; padding: 14px 24px; margin-bottom: 20px; border: 1px solid rgba(48,54,61,0.5); text-align: center; }
        .header h1 { font-size: 24px; font-weight: 700; background: linear-gradient(135deg, #58a6ff, #3fb950, #f0883e); -webkit-background-clip: text; background-clip: text; color: transparent; }
        .header-sub { font-size: 12px; color: #8b949e; margin-top: 4px; }
        .loop-badge { display: inline-block; background: #d29922; color: #0a0c10; padding: 2px 10px; border-radius: 30px; font-size: 11px; font-weight: bold; margin-left: 10px; }
        .progress-bar { height: 2px; background: #21262d; border-radius: 2px; overflow: hidden; margin-top: 10px; }
        .progress-fill { height: 100%; background: linear-gradient(90deg, #3fb950, #58a6ff); width: 0%; transition: width 0.3s ease; }
        .stats-top { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
        .stat-card { background: #161b22; border-radius: 14px; padding: 10px 12px; text-align: center; cursor: pointer; border: 1px solid #30363d; transition: all 0.2s; }
        .stat-card:hover { transform: translateY(-1px); border-color: #58a6ff; background: #1a1f2e; }
        .stat-number { font-size: 22px; font-weight: 700; color: #58a6ff; }
        .stat-label { font-size: 10px; color: #8b949e; margin-top: 3px; font-weight: 500; }
        .cycle-info { font-size: 11px; color: #d29922; margin-top: 5px; }
        .results-section { background: #161b22; border-radius: 20px; border: 1px solid #30363d; overflow: hidden; margin-bottom: 20px; }
        .section-header { padding: 14px 20px; background: #0d1117; border-bottom: 1px solid #30363d; font-weight: 600; font-size: 15px; }
        .section-header i { color: #58a6ff; margin-right: 8px; }
        .filter-bar { display: flex; gap: 10px; padding: 12px 20px; background: #0d1117; border-bottom: 1px solid #21262d; flex-wrap: wrap; align-items: center; }
        .search-input { padding: 6px 14px; background: #010409; border: 1px solid #30363d; border-radius: 30px; color: white; width: 220px; font-size: 12px; }
        .filter-btn { padding: 5px 14px; background: #21262d; border: none; border-radius: 30px; color: #8b949e; cursor: pointer; font-size: 11px; font-weight: 500; }
        .filter-btn.active { background: #58a6ff; color: white; }
        .balance-filter { display: flex; gap: 6px; margin-left: auto; }
        .balance-filter-btn { padding: 4px 10px; background: #21262d; border: none; border-radius: 30px; color: #8b949e; cursor: pointer; font-size: 10px; }
        .balance-filter-btn.active { background: #3fb950; color: white; }
        .table-container { max-height: 520px; overflow-y: auto; }
        table { width: 100%; border-collapse: separate; border-spacing: 0; }
        th { background: #0d1117; padding: 12px 14px; text-align: left; font-size: 12px; font-weight: 600; color: #8b949e; cursor: pointer; position: sticky; top: 0; border-bottom: 1px solid #30363d; }
        th:hover { color: #58a6ff; }
        td { padding: 10px 14px; font-size: 12px; border-bottom: 1px solid #21262d; }
        tr:last-child td { border-bottom: none; }
        .balance-positive { color: #3fb950; font-weight: 600; }
        .balance-medium { color: #d29922; font-weight: 600; }
        .balance-zero { color: #f85149; }
        .copy-btn, .retry-btn { background: transparent; border: none; cursor: pointer; font-size: 11px; padding: 3px 8px; border-radius: 6px; transition: all 0.2s; display: inline-flex; align-items: center; gap: 4px; }
        .copy-btn { color: #58a6ff; margin-left: 8px; }
        .copy-btn:hover { background: #30363d; color: #3fb950; }
        .retry-btn { color: #d29922; margin-left: 8px; }
        .retry-btn:hover { background: #30363d; color: #f0883e; }
        .bottom-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 0; }
        .card { background: #161b22; border-radius: 20px; border: 1px solid #30363d; overflow: hidden; }
        .card-header { padding: 12px 18px; background: #0d1117; border-bottom: 1px solid #30363d; font-weight: 600; font-size: 13px; }
        .card-header i { color: #58a6ff; margin-right: 6px; }
        textarea { width: 100%; min-height: 200px; background: #010409; border: none; padding: 14px; color: #e6edf3; font-family: 'Courier New', monospace; font-size: 11px; resize: vertical; }
        textarea:focus { outline: none; background: #0a0c10; }
        .button-group { padding: 14px; display: flex; gap: 8px; flex-wrap: wrap; border-top: 1px solid #21262d; }
        .btn { padding: 6px 16px; border: none; border-radius: 8px; font-weight: 600; cursor: pointer; transition: all 0.2s; font-size: 12px; }
        .btn-primary { background: linear-gradient(135deg, #238636, #2ea043); color: white; }
        .btn-primary:hover { transform: translateY(-1px); }
        .btn-danger { background: linear-gradient(135deg, #da3633, #f85149); color: white; }
        .btn-danger:hover { transform: translateY(-1px); }
        .btn-secondary { background: #6e7681; color: white; }
        .btn-secondary:hover { background: #8b949e; }
        .btn-warning { background: #d29922; color: #0a0c10; }
        .btn-warning:hover { background: #f0883e; }
        .btn-info { background: #1f6feb; color: white; }
        .btn-info:hover { background: #388bfd; }
        .terminal { background: #010409; height: 280px; overflow-y: auto; padding: 10px; font-family: 'Courier New', monospace; font-size: 10px; }
        .terminal-line { padding: 4px 0; color: #b1bac4; border-bottom: 1px solid #1a1f2e; }
        .terminal-line .time { color: #58a6ff; margin-right: 10px; }
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: #161b22; border-radius: 3px; }
        ::-webkit-scrollbar-thumb { background: #30363d; border-radius: 3px; }
        ::-webkit-scrollbar-thumb:hover { background: #58a6ff; }
        @media (max-width: 900px) {
            body { padding: 12px; }
            .bottom-grid { grid-template-columns: 1fr; }
            .stats-top { grid-template-columns: repeat(2, 1fr); }
            .balance-filter { margin-left: 0; margin-top: 8px; }
            .filter-bar { flex-direction: column; align-items: stretch; }
            .search-input { width: 100%; }
        }
    </style>
</head>
<body>
<div id="loginOverlay" class="login-overlay">
    <div class="login-box">
        <h2><i class="fas fa-shield-alt"></i> Authentication</h2>
        <input type="text" id="loginUsername" placeholder="Username" autocomplete="off">
        <input type="password" id="loginPassword" placeholder="Password">
        <button onclick="doLogin()"><i class="fas fa-unlock-alt"></i> Login</button>
        <div id="loginError" class="error-text"></div>
    </div>
</div>

<div id="mainContent" class="main-content">
<div class="container">
    <div class="header">
        <h1><i class="fas fa-crown"></i> Adjarabet Bot v27.0 | LOOP MODE <span class="loop-badge"><i class="fas fa-sync-alt"></i> INFINITE LOOP</span></h1>
        <div class="header-sub">🔄 Automatically restarts from beginning after each full cycle | Stop → Start resumes from current cycle</div>
        <div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
    </div>

    <div class="stats-top">
        <div class="stat-card" onclick="setFilter('all')"><div class="stat-number" id="totalCount">0</div><div class="stat-label">TOTAL</div></div>
        <div class="stat-card" onclick="setFilter('success')"><div class="stat-number" id="successCount">0</div><div class="stat-label">✅ SUCCESS</div></div>
        <div class="stat-card" onclick="setFilter('failed')"><div class="stat-number" id="failedCount">0</div><div class="stat-label">❌ FAILED</div></div>
        <div class="stat-card" onclick="setFilter('timeout')"><div class="stat-number" id="timeoutCount">0</div><div class="stat-label">⏰ TIMEOUT</div></div>
    </div>

    <div class="results-section">
        <div class="section-header"><i class="fas fa-chart-line"></i> Results Dashboard <span style="font-size: 10px; color: #6e7681;">▼ Click headers to sort | 📋 Copy | ⟳ Retry</span></div>
        <div class="filter-bar">
            <input type="text" id="searchInput" class="search-input" placeholder="🔍 Search username...">
            <button class="filter-btn active" data-filter="all" onclick="setFilter('all')">All</button>
            <button class="filter-btn" data-filter="success" onclick="setFilter('success')">✅ Success</button>
            <button class="filter-btn" data-filter="failed" onclick="setFilter('failed')">❌ Failed</button>
            <button class="filter-btn" data-filter="timeout" onclick="setFilter('timeout')">⏰ Timeout</button>
            <div class="balance-filter">
                <span style="font-size: 10px; color: #8b949e;">💰</span>
                <button class="balance-filter-btn active" data-balance="all" onclick="setBalanceFilter('all')">All</button>
                <button class="balance-filter-btn" data-balance="low" onclick="setBalanceFilter('low')">&lt;10</button>
                <button class="balance-filter-btn" data-balance="mid" onclick="setBalanceFilter('mid')">10-100</button>
                <button class="balance-filter-btn" data-balance="high" onclick="setBalanceFilter('high')">100+</button>
            </div>
        </div>
        <div class="table-container">
            <table id="resultsTable">
                <thead><tr><th onclick="sortBy('status')"><i class="fas fa-flag"></i> Status</th><th onclick="sortBy('username')"><i class="fas fa-user"></i> Username</th><th onclick="sortBy('password')"><i class="fas fa-key"></i> Password</th><th onclick="sortBy('balance')"><i class="fas fa-coins"></i> Balance</th><th>Action</th></tr></thead>
                <tbody id="resultsBody"><tr><td colspan="5" style="text-align:center; padding:40px;"><i class="fas fa-spinner fa-pulse"></i> Waiting...</td></tr></tbody>
            </table>
        </div>
    </div>

    <div class="bottom-grid">
        <div class="card">
            <div class="card-header"><i class="fas fa-users"></i> Accounts Input (username:password)</div>
            <textarea id="accounts" placeholder="user1:pass123&#10;user2:pass456&#10;user3:pass789"></textarea>
            <div class="button-group">
                <button class="btn btn-primary" onclick="startBot()"><i class="fas fa-play"></i> Start Loop Mode</button>
                <button class="btn btn-danger" onclick="stopBot()"><i class="fas fa-stop"></i> Stop</button>
                <button class="btn btn-warning" onclick="resetBot()"><i class="fas fa-bomb"></i> Reset All</button>
                <button class="btn btn-info" onclick="clearResultsOnly()"><i class="fas fa-eraser"></i> Clear Results</button>
                <button class="btn btn-secondary" onclick="clearTerminal()"><i class="fas fa-trash"></i> Clear Terminal</button>
            </div>
        </div>
        <div class="card">
            <div class="card-header"><i class="fas fa-terminal"></i> Live Console</div>
            <div class="terminal" id="terminal">
                <div class="terminal-line"><span class="time">●</span> 🚀 Bot ready v27.0 - LOOP MODE</div>
                <div class="terminal-line"><span class="time">●</span> 🔄 Infinite loop: restarts after each full cycle!</div>
                <div class="terminal-line"><span class="time">●</span> 💡 3 tabs open simultaneously!</div>
            </div>
        </div>
    </div>
</div>
</div>

<script>
let ws = null, allResults = [], currentFilter = 'all', currentBalanceFilter = 'all', currentSort = { field: 'balance', dir: 'desc' };
let wsToken = null;
let highBalanceAlerted = new Set();

function playHighBalanceSound() {
    try {
        const audioContext = new (window.AudioContext || window.webkitAudioContext)();
        function beep(freq, duration, delay, volume = 0.4) {
            setTimeout(() => {
                const osc = audioContext.createOscillator();
                const gain = audioContext.createGain();
                osc.connect(gain);
                gain.connect(audioContext.destination);
                osc.frequency.value = freq;
                gain.gain.value = volume;
                osc.start();
                gain.gain.exponentialRampToValueAtTime(0.00001, audioContext.currentTime + duration);
                osc.stop(audioContext.currentTime + duration);
            }, delay * 1000);
        }
        beep(880, 0.2, 0);
        beep(880, 0.2, 0.3);
        beep(880, 0.2, 0.6);
        if (audioContext.state === 'suspended') audioContext.resume();
    } catch(e) {}
}

function checkAndAlertHighBalance(result) {
    const balanceVal = parseFloat(result.balance_value) || 0;
    const HIGH_BALANCE_THRESHOLD = 50000;
    if (balanceVal >= HIGH_BALANCE_THRESHOLD && !highBalanceAlerted.has(result.username)) {
        highBalanceAlerted.add(result.username);
        playHighBalanceSound();
        addLog(`🔔 HIGH BALANCE! ${result.username} | ${result.balance}`);
    }
}

async function doLogin() {
    const username = document.getElementById('loginUsername').value;
    const password = document.getElementById('loginPassword').value;
    const errorDiv = document.getElementById('loginError');
    if(!username || !password) { errorDiv.innerText = 'Enter credentials'; return; }
    try {
        const res = await fetch('/token', {
            method: 'POST',
            headers: { 'Authorization': 'Basic ' + btoa(username + ':' + password) }
        });
        const data = await res.json();
        if(res.ok && data.token) {
            wsToken = data.token;
            document.getElementById('loginOverlay').style.display = 'none';
            document.getElementById('mainContent').style.display = 'block';
            connectWebSocket();
            await fetchResults();
            startAutoRefresh();
            addLog('✅ Login successful | LOOP MODE ACTIVE');
        } else {
            errorDiv.innerText = data.detail || 'Invalid credentials';
        }
    } catch(e) { errorDiv.innerText = 'Connection error'; }
}

function connectWebSocket() {
    if(ws) ws.close();
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(`${protocol}//${location.host}/ws?token=${wsToken}`);
    ws.onopen = () => addLog('WebSocket connected');
    ws.onmessage = (e) => {
        let data = e.data;
        if (data.startsWith('RESULT:')) {
            try {
                let result = JSON.parse(data.substring(7));
                let idx = allResults.findIndex(x => x.username === result.username);
                idx >= 0 ? allResults[idx] = result : allResults.push(result);
                renderResults(); updateStats();
                addLog(`${result.status} ${result.username} | ${result.balance}`);
                checkAndAlertHighBalance(result);
            } catch(e) {}
        } else if (data.startsWith('PROGRESS:')) {
            let p = data.substring(9).split('/');
            document.getElementById('progressFill').style.width = (p[0]/p[1]*100) + '%';
        } else if (data.startsWith('SUMMARY:')) {
            addLog(data.substring(8));
        } else if (data.startsWith('STATUS:loop_cycle_')) {
            addLog('🔄 ' + data.substring(13).toUpperCase() + ' STARTED');
        } else if (data.startsWith('STATUS:waiting_next_cycle:')) {
            let sec = data.substring(24);
            addLog(`⏳ Waiting ${sec}s before next cycle...`);
        } else if (data.startsWith('STATUS:starting_next_cycle')) {
            addLog('🔄 Starting next cycle...');
        } else if (data.startsWith('STATUS:completed')) {
            addLog('✅ Cycle completed!');
        } else if (data.startsWith('STATUS:loop_stopped')) {
            addLog('⏹ Loop stopped by user');
        }
    };
}

async function fetchResults() {
    try { const res = await fetch('/results'); if(res.ok) { allResults = await res.json(); renderResults(); updateStats(); } } catch(e) {}
}
function startAutoRefresh() { setInterval(fetchResults, 5000); }
function renderResults() {
    let filtered = allResults.filter(r => {
        if(currentFilter !== 'all') {
            if(currentFilter === 'success' && r.status !== '✅') return false;
            if(currentFilter === 'failed' && r.status !== '❌') return false;
            if(currentFilter === 'timeout' && r.status !== '⏰') return false;
        }
        if(currentBalanceFilter !== 'all') {
            let num = parseFloat(r.balance_value) || 0;
            if(currentBalanceFilter === 'low' && num >= 10) return false;
            if(currentBalanceFilter === 'mid' && (num < 10 || num > 100)) return false;
            if(currentBalanceFilter === 'high' && num <= 100) return false;
        }
        return true;
    });
    let search = document.getElementById('searchInput')?.value.toLowerCase() || '';
    if(search) filtered = filtered.filter(r => r.username.toLowerCase().includes(search));
    filtered.sort((a,b) => {
        let av = currentSort.field === 'balance' ? (a.balance_value || 0) : (a[currentSort.field] || '').toString().toLowerCase();
        let bv = currentSort.field === 'balance' ? (b.balance_value || 0) : (b[currentSort.field] || '').toString().toLowerCase();
        if(typeof av === 'number') return currentSort.dir === 'asc' ? av - bv : bv - av;
        return currentSort.dir === 'asc' ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
    });
    let balanceClass = (v) => { let n = parseFloat(v)||0; return n>100?'balance-positive':n>10?'balance-medium':'balance-zero'; };
    document.getElementById('resultsBody').innerHTML = filtered.map(r => `<tr><td style="font-size:18px">${r.status}</td><td><div class="username-cell"><strong style="color:#58a6ff">${escapeHtml(r.username)}</strong><button class="copy-btn" onclick="copyToClipboard('${escapeHtml(r.username)}','username',this)"><i class="fas fa-copy"></i></button></div></td><td><div class="password-cell">${escapeHtml(r.password)}<button class="copy-btn" onclick="copyToClipboard('${escapeHtml(r.password)}','password',this)"><i class="fas fa-key"></i></button></div></td><td class="${balanceClass(r.balance_value)}">${r.balance||'0 ֏'}</td><td class="error-cell">${escapeHtml(r.error||'-')}<button class="retry-btn" onclick="retryAccount('${escapeHtml(r.username)}')"><i class="fas fa-sync-alt"></i> Retry</button></div></td></tr>`).join('');
}
function updateStats() {
    document.getElementById('totalCount').innerText = allResults.length;
    document.getElementById('successCount').innerText = allResults.filter(r=>r.status==='✅').length;
    document.getElementById('failedCount').innerText = allResults.filter(r=>r.status==='❌').length;
    document.getElementById('timeoutCount').innerText = allResults.filter(r=>r.status==='⏰').length;
}
function sortBy(field) {
    if(currentSort.field === field) currentSort.dir = currentSort.dir==='asc'?'desc':'asc';
    else { currentSort.field = field; currentSort.dir = field==='balance'?'desc':'asc'; }
    renderResults();
}
function setFilter(f) { currentFilter = f; document.querySelectorAll('.filter-btn').forEach(btn=>btn.classList.toggle('active',btn.dataset.filter===f)); renderResults(); }
function setBalanceFilter(f) { currentBalanceFilter = f; document.querySelectorAll('.balance-filter-btn').forEach(btn=>btn.classList.toggle('active',btn.dataset.balance===f)); renderResults(); }
function escapeHtml(s) { if(!s) return ''; return s.replace(/[&<>]/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;'})[m]); }
function addLog(msg) { let term=document.getElementById('terminal'); let div=document.createElement('div'); div.className='terminal-line'; div.innerHTML=`<span class="time">[${new Date().toLocaleTimeString()}]</span> ${msg}`; term.appendChild(div); if(term.children.length>100) term.removeChild(term.firstChild); }
function clearTerminal() { document.getElementById('terminal').innerHTML = ''; }
async function copyToClipboard(text,type,btn) { await navigator.clipboard.writeText(text); let orig=btn.innerHTML; btn.innerHTML='✓'; setTimeout(()=>btn.innerHTML=orig,1000); }
async function retryAccount(username) { addLog(`Retrying ${username}`); await fetch(`/retry/${encodeURIComponent(username)}`,{method:'POST'}); }
async function startBot() { let acc=document.getElementById('accounts').value; if(!acc.trim()) return; await fetch('/start',{method:'POST',body:acc}); addLog('🚀 Bot started in LOOP MODE - will run continuously!'); }
async function stopBot() { await fetch('/stop',{method:'POST'}); addLog('⏹ Bot stopped - will not start next cycle'); }
async function resetBot() { if(confirm('Reset all? This will stop the bot and clear all data.')){ await fetch('/reset',{method:'POST'}); allResults=[]; renderResults(); updateStats(); document.getElementById('accounts').value=''; highBalanceAlerted.clear(); addLog('🔄 Reset complete'); } }
async function clearResultsOnly() { if(confirm('Clear results?')){ await fetch('/clear-results',{method:'POST'}); allResults=[]; renderResults(); updateStats(); highBalanceAlerted.clear(); addLog('🗑 Results cleared'); } }
document.getElementById('accounts').value = localStorage.getItem('bot_accounts') || '';
document.getElementById('accounts').addEventListener('input',()=>localStorage.setItem('bot_accounts',document.getElementById('accounts').value));
document.getElementById('searchInput').addEventListener('input',()=>renderResults());
</script>
</body>
</html>'''

# ================= MOBILE UI =================
MOBILE_HTML = '''<!DOCTYPE html>
<html lang="hy">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover, user-scalable=yes">
    <title>Mobile Monitor | Adjarabet</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { background: linear-gradient(135deg, #0a0c10 0%, #0d1117 100%); color: #e6edf3; font-family: 'Inter', sans-serif; padding: 10px; min-height: 100vh; }
        .pin-overlay { position: fixed; top:0; left:0; width:100%; height:100%; background: rgba(0,0,0,0.95); backdrop-filter: blur(12px); display: flex; align-items: center; justify-content: center; z-index: 1000; }
        .pin-box { background: #161b22; border: 1px solid #30363d; border-radius: 28px; padding: 30px 25px; width: 300px; text-align: center; }
        .pin-box h2 { margin-bottom: 20px; background: linear-gradient(135deg, #58a6ff, #3fb950); -webkit-background-clip: text; background-clip: text; color: transparent; }
        .pin-box input { width: 100%; padding: 14px; background: #0d1117; border: 1px solid #30363d; border-radius: 14px; color: white; font-size: 20px; text-align: center; letter-spacing: 6px; }
        .pin-box button { width: 100%; padding: 12px; background: linear-gradient(135deg, #238636, #2ea043); border: none; border-radius: 14px; color: white; font-weight: bold; cursor: pointer; margin-top: 16px; }
        .pin-error { color: #f85149; font-size: 12px; margin-top: 12px; }
        .mobile-dashboard { display: none; }
        .header { background: rgba(22,27,34,0.95); border-radius: 18px; padding: 10px 14px; margin-bottom: 12px; text-align: center; border: 1px solid #30363d; }
        .header h1 { font-size: 16px; background: linear-gradient(135deg, #58a6ff, #3fb950); -webkit-background-clip: text; background-clip: text; color: transparent; }
        .last-update { font-size: 9px; color: #6e7681; margin-top: 3px; }
        .toolbar { display: flex; justify-content: flex-end; margin-bottom: 12px; }
        .refresh-all-btn { background: #1f6feb; border: none; border-radius: 30px; color: white; padding: 6px 14px; font-size: 11px; font-weight: 500; cursor: pointer; display: flex; align-items: center; gap: 5px; }
        .accounts-list { display: flex; flex-direction: column; gap: 10px; }
        .account-card { background: #161b22; border-radius: 16px; border: 1px solid #30363d; overflow: hidden; }
        .account-row { display: flex; justify-content: space-between; align-items: center; padding: 12px 14px; border-bottom: 1px solid #21262d; }
        .account-row:last-child { border-bottom: none; }
        .label { font-size: 9px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 3px; }
        .username-value { font-size: 14px; font-weight: 600; color: #58a6ff; word-break: break-all; }
        .password-value { font-size: 12px; font-family: monospace; color: #e6edf3; word-break: break-all; }
        .balance-value { font-size: 16px; font-weight: 700; }
        .balance-positive { color: #3fb950; }
        .balance-medium { color: #d29922; }
        .balance-zero { color: #f85149; }
        .copy-btn { background: transparent; border: 1px solid #30363d; border-radius: 20px; padding: 3px 8px; color: #58a6ff; cursor: pointer; font-size: 10px; margin-left: 6px; }
        .refresh-row-btn { background: #21262d; border: none; border-radius: 30px; padding: 5px 10px; color: #d29922; cursor: pointer; font-size: 10px; display: flex; align-items: center; gap: 3px; }
        .status-badge { font-size: 16px; margin-right: 6px; }
        .row-header { display: flex; align-items: center; flex-wrap: wrap; gap: 6px; }
        .error-text { font-size: 9px; color: #f85149; margin-top: 3px; }
        .footer { text-align: center; padding: 12px; font-size: 9px; color: #6e7681; border-top: 1px solid #21262d; margin-top: 16px; }
        @media (max-width: 480px) {
            body { padding: 8px; }
            .account-row { padding: 10px; flex-direction: column; align-items: flex-start; gap: 8px; }
            .row-header { width: 100%; justify-content: space-between; }
        }
    </style>
</head>
<body>
<div id="pinOverlay" class="pin-overlay">
    <div class="pin-box">
        <h2><i class="fas fa-lock"></i> Mobile Access</h2>
        <input type="password" id="pinInput" placeholder="0000" maxlength="6">
        <button onclick="verifyPin()">Access</button>
        <div id="pinError" class="pin-error"></div>
    </div>
</div>
<div id="mobileDashboard" class="mobile-dashboard">
    <div class="header">
        <h1><i class="fas fa-mobile-alt"></i> Mobile Monitor</h1>
        <div class="last-update" id="lastUpdate">Loading...</div>
    </div>
    <div class="toolbar">
        <button class="refresh-all-btn" onclick="manualRefresh()"><i class="fas fa-sync-alt"></i> Refresh All</button>
    </div>
    <div class="accounts-list" id="accountsList">
        <div style="text-align:center; padding:30px;"><i class="fas fa-spinner fa-pulse"></i> Loading...</div>
    </div>
    <div class="footer"><i class="fas fa-chart-line"></i> Auto-refresh 5s | Sorted by balance (highest first)</div>
</div>
<script>
let mobileResults = [], refreshInterval = null;

function sortByBalanceDesc(data) {
    return [...data].sort((a, b) => {
        const balanceA = parseFloat(a.balance_value) || 0;
        const balanceB = parseFloat(b.balance_value) || 0;
        return balanceB - balanceA;
    });
}

async function verifyPin() {
    const pin = document.getElementById('pinInput').value;
    const errorDiv = document.getElementById('pinError');
    if(!pin) { errorDiv.innerText = 'Enter PIN'; return; }
    try {
        const res = await fetch('/mobile/verify', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ pin: pin })
        });
        const data = await res.json();
        if(data.success) {
            document.getElementById('pinOverlay').style.display = 'none';
            document.getElementById('mobileDashboard').style.display = 'block';
            loadResults();
            startAutoRefresh();
        } else {
            errorDiv.innerText = 'Invalid PIN';
            document.getElementById('pinInput').value = '';
        }
    } catch(e) { errorDiv.innerText = 'Connection error'; }
}

async function loadResults() {
    try {
        const res = await fetch('/results');
        if(res.ok) {
            const data = await res.json();
            mobileResults = sortByBalanceDesc(data);
            renderMobileList();
            document.getElementById('lastUpdate').innerHTML = 'Last: '+new Date().toLocaleTimeString();
        }
    } catch(e) {}
}

function startAutoRefresh() { if(refreshInterval) clearInterval(refreshInterval); refreshInterval = setInterval(loadResults, 5000); }
function manualRefresh() { loadResults(); }

async function refreshSingleAccount(username) {
    const btnId = 'refresh-btn-'+username.replace(/[^a-zA-Z0-9]/g, '_');
    const btn = document.getElementById(btnId);
    if(btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-pulse"></i>'; }
    try {
        await fetch('/retry/'+encodeURIComponent(username), { method: 'POST' });
        setTimeout(() => loadResults(), 2000);
    } catch(e) {}
    setTimeout(() => { if(btn) { btn.disabled = false; btn.innerHTML = '<i class="fas fa-sync-alt"></i> Refresh'; } }, 3000);
}

function copyToClipboard(text, btn) {
    navigator.clipboard.writeText(text).then(() => {
        const orig = btn.innerHTML;
        btn.innerHTML = '<i class="fas fa-check"></i>';
        setTimeout(() => btn.innerHTML = orig, 1200);
    });
}

function renderMobileList() {
    const container = document.getElementById('accountsList');
    if(mobileResults.length === 0) {
        container.innerHTML = '<div style="text-align:center; padding:30px;"><i class="fas fa-inbox"></i> No results</div>';
        return;
    }
    const balanceClass = (v) => { let n = parseFloat(v)||0; return n>100?'balance-positive':n>10?'balance-medium':'balance-zero'; };
    container.innerHTML = mobileResults.map(acc => {
        const safeId = acc.username.replace(/[^a-zA-Z0-9]/g, '_');
        return `<div class="account-card">
            <div class="account-row"><div class="row-header"><span class="status-badge">${acc.status}</span><span class="username-value"><strong>${escapeHtml(acc.username)}</strong></span><button class="copy-btn" onclick="copyToClipboard('${escapeHtml(acc.username)}', this)"><i class="fas fa-copy"></i></button></div><button class="refresh-row-btn" id="refresh-btn-${safeId}" onclick="refreshSingleAccount('${escapeHtml(acc.username)}')"><i class="fas fa-sync-alt"></i> Refresh</button></div>
            <div class="account-row"><div style="flex:1"><div class="label"><i class="fas fa-key"></i> Password</div><div class="password-value">${escapeHtml(acc.password)} <button class="copy-btn" onclick="copyToClipboard('${escapeHtml(acc.password)}', this)"><i class="fas fa-copy"></i></button></div></div></div>
            <div class="account-row"><div style="flex:1"><div class="label"><i class="fas fa-coins"></i> Balance</div><div class="balance-value ${balanceClass(acc.balance_value)}">${acc.balance || '0 ֏'}</div></div></div>
            ${acc.error ? `<div class="account-row"><div class="error-text"><i class="fas fa-exclamation-triangle"></i> ${escapeHtml(acc.error)}</div></div>` : ''}
        </div>`;
    }).join('');
}

function escapeHtml(s) { if(!s) return ''; return s.replace(/[&<>]/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;'})[m]); }

document.getElementById('pinInput').addEventListener('keypress', (e) => { if(e.key === 'Enter') verifyPin(); });
</script>
</body>
</html>'''

# ================= FASTAPI APP =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    await bot.init()
    logger.info("=" * 50)
    logger.info("🎮 ADJARABET BOT v27.0 - LOOP MODE")
    logger.info(f"📍 Host: {CONFIG['host']}:{CONFIG['port']}")
    logger.info(f"⚡ REAL Concurrent tabs: {CONFIG['concurrent_limit']}")
    logger.info(f"🔄 LOOP MODE: {'ON' if CONFIG['loop_mode'] else 'OFF'}")
    logger.info(f"⏱ Loop delay: {CONFIG['loop_delay']}s between cycles")
    logger.info(f"🔧 Headless: {CONFIG['headless']} | Timeout: {CONFIG['timeout_nav']}ms")
    logger.info("✅ Bot will continuously restart from beginning after each full cycle")
    logger.info(f"🔐 Main Auth: {AUTH_USERNAME} / {AUTH_PASSWORD[:3]}***")
    logger.info(f"🔐 Mobile PIN: {MOBILE_PIN}")
    logger.info("=" * 50)
    yield
    await bot.cleanup()

app = FastAPI(title="Adjarabet Bot Loop Mode", version="27.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def root():
    return HTMLResponse(HTML_UI)

@app.post("/token")
async def get_token(credentials: HTTPBasicCredentials = Depends(verify_auth)):
    token = token_manager.create_token()
    return {"token": token}

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
        await asyncio.sleep(0.5)
    
    processing_task = asyncio.create_task(bot.start(accounts, manager))
    return {"status": "started", "total": len(accounts), "loop_mode": CONFIG["loop_mode"]}

@app.post("/stop")
async def stop_bot():
    await bot.stop()
    return {"status": "stopped"}

@app.post("/reset")
async def reset_bot():
    await bot.reset()
    return {"status": "reset"}

@app.post("/clear-results")
async def clear_results():
    await bot.clear_results()
    return {"status": "cleared"}

@app.post("/retry/{username}")
async def retry_account(username: str):
    all_data = await bot.get_results()
    account_data = None
    for acc in all_data:
        if acc["username"] == username:
            account_data = acc
            break
    if not account_data:
        return JSONResponse({"error": "Account not found"}, 404)
    acc = Account(account_data["username"], account_data["password"])
    asyncio.create_task(retry_single_account(acc))
    return {"status": "retry_started", "username": username}

@app.get("/results")
async def get_results():
    return await bot.get_results()

@app.get("/health")
async def health():
    return {
        "status": "healthy", 
        "current_index": bot.current_index, 
        "total": len(bot.all_accounts), 
        "total_processed": len(bot.results),
        "concurrent_limit": CONFIG["concurrent_limit"],
        "loop_mode": CONFIG["loop_mode"]
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = None):
    if not token or not token_manager.verify_token(token):
        await websocket.close(code=1008, reason="Unauthorized")
        return
    token_manager.remove_token(token)
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

# ================= MOBILE ENDPOINTS =================
@app.get("/mobile")
async def mobile_page():
    return HTMLResponse(MOBILE_HTML)

@app.post("/mobile/verify")
async def verify_mobile_pin(request: Request):
    try:
        data = await request.json()
        pin = data.get("pin", "")
        if pin == MOBILE_PIN:
            return {"success": True}
        return {"success": False}
    except:
        return {"success": False}

if __name__ == "__main__":
    import uvicorn
    Path("results").mkdir(exist_ok=True)
    print("\n" + "=" * 60)
    print("🎮 ADJARABET BOT v27.0 - LOOP MODE")
    print("=" * 60)
    print(f"📍 Main UI: http://{CONFIG['host']}:{CONFIG['port']}")
    print(f"📍 Mobile Monitor: http://{CONFIG['host']}:{CONFIG['port']}/mobile")
    print(f"🔐 Main Login: {AUTH_USERNAME} / {AUTH_PASSWORD}")
    print(f"🔐 Mobile PIN: {MOBILE_PIN}")
    print(f"🔧 Headless: {CONFIG['headless']}")
    print(f"⚡ REAL Concurrent: {CONFIG['concurrent_limit']} tabs at the SAME TIME!")
    print(f"🔄 LOOP MODE: ON - Bot will restart from beginning after each full cycle!")
    print(f"⏱ Delay between cycles: {CONFIG['loop_delay']} seconds")
    print("=" * 60 + "\n")
    uvicorn.run(app, host=CONFIG["host"], port=CONFIG["port"], log_level="warning", access_log=False)
