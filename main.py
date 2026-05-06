import asyncio
import json
import logging
import os
import re
import signal
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
    "timeout": int(os.getenv("TIMEOUT", 35000)),
    "max_retries": int(os.getenv("MAX_RETRIES", 1)),
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
        """Initialize browser once (without global context)"""
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
    
    async def login_account(self, account: Account) -> Account:
        """Login single account - EACH ACCOUNT GETS ITS OWN CONTEXT"""
        context = None
        page = None
        
        try:
            # NEW CONTEXT FOR EACH ACCOUNT - CRITICAL FIX
            context = await self.browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                ignore_https_errors=True
            )
            
            page = await context.new_page()
            page.set_default_timeout(PRODUCTION_CONFIG["timeout"])
            
            await page.goto('https://www.adjarabet.am/hy', wait_until='domcontentloaded')
            await asyncio.sleep(1)
            
            # LOGIN PROCESS
            await page.wait_for_selector('input[name="userIdentifier"]', timeout=15000)
            
            await page.fill('input[name="userIdentifier"]', account.username)
            await asyncio.sleep(0.5)
            
            await page.fill('input[type="password"]', account.password)
            await asyncio.sleep(0.5)
            
            await page.click('[data-test-id="header-login-button"]')
            
            # WAIT FOR NAVIGATION AFTER LOGIN
            await page.wait_for_load_state("networkidle")
            
            # GET BALANCE
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
            self.log.warning(f"⏰ {account.username} - Timeout")
            return account
            
        except Exception as e:
            account.status = AccountStatus.FAILED
            account.error = str(e)[:100]
            self.log.error(f"❌ {account.username}: {str(e)[:80]}")
            return account
            
        finally:
            # Clean up page and context for this account
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
            match = re.search(r'([\d,]+\.?\d*)', text.replace(',', ''))
            if match:
                return float(match.group(1))
        except:
            pass
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
                break
            
            result = await self.login_account(account)
            self.results.append(result)
            self.processed = i
            
            # Save results
            await self._save_results()
            
            # Send real-time updates
            await manager.broadcast(f"RESULT:{json.dumps(result.to_dict())}")
            await manager.broadcast(f"PROGRESS:{i}/{self.total_accounts}")
            
            await asyncio.sleep(0.5)
        
        # Send summary
        successful = sum(1 for r in self.results if r.status == AccountStatus.SUCCESS)
        rate = (successful / self.total_accounts * 100) if self.total_accounts > 0 else 0
        
        await manager.broadcast(f"SUMMARY:{successful}/{self.total_accounts}:{rate:.1f}")
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

# HTML UI (your provided UI - unchanged)
HTML_UI = '''<!DOCTYPE html>
<html lang="hy">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
    <title>Adjarabet Bot | Ultimate Pro v7.0</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            background: linear-gradient(135deg, #0a0e1a 0%, #0f1420 50%, #0a0e1a 100%);
            color: #e4e6eb;
            font-family: 'Inter', sans-serif;
            min-height: 100vh;
            padding: 20px;
            position: relative;
        }
        body::before {
            content: '';
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: radial-gradient(circle at 20% 50%, rgba(88, 166, 255, 0.05) 0%, transparent 50%),
                        radial-gradient(circle at 80% 80%, rgba(63, 185, 80, 0.03) 0%, transparent 50%);
            pointer-events: none;
            z-index: 0;
        }
        .app-container { max-width: 1800px; margin: 0 auto; position: relative; z-index: 1; }
        .header {
            background: linear-gradient(135deg, rgba(22, 27, 34, 0.98) 0%, rgba(13, 17, 23, 0.98) 100%);
            backdrop-filter: blur(20px);
            border-radius: 24px;
            padding: 20px 32px;
            margin-bottom: 24px;
            border: 1px solid rgba(88, 166, 255, 0.2);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
        }
        .header-content { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 20px; }
        .title-section { display: flex; align-items: center; gap: 18px; }
        .logo-icon {
            width: 55px; height: 55px;
            background: linear-gradient(135deg, #58a6ff, #1f6feb);
            border-radius: 18px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 30px;
            animation: pulseGlow 2s infinite;
        }
        @keyframes pulseGlow {
            0%, 100% { box-shadow: 0 0 0 0 rgba(88, 166, 255, 0.5); }
            50% { box-shadow: 0 0 0 15px rgba(88, 166, 255, 0); }
        }
        h1 {
            font-size: 32px;
            font-weight: 800;
            background: linear-gradient(135deg, #58a6ff, #79c0ff, #3fb950);
            -webkit-background-clip: text;
            background-clip: text;
            color: transparent;
            letter-spacing: -0.5px;
        }
        .version { font-size: 12px; color: #8b949e; background: rgba(33, 38, 45, 0.8); padding: 4px 12px; border-radius: 20px; margin-left: 12px; }
        .status-card { display: flex; align-items: center; gap: 20px; background: rgba(33, 38, 45, 0.8); padding: 10px 24px; border-radius: 60px; backdrop-filter: blur(10px); border: 1px solid rgba(88, 166, 255, 0.2); }
        .status-dot { width: 14px; height: 14px; border-radius: 50%; transition: all 0.3s ease; }
        .status-dot.running { background: #3fb950; box-shadow: 0 0 15px #3fb950; animation: pulse 1.5s infinite; }
        .status-dot.stopped { background: #f85149; }
        @keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.6; transform: scale(1.15); } }
        .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin-bottom: 24px; }
        .stat-card {
            background: linear-gradient(135deg, rgba(22, 27, 34, 0.95) 0%, rgba(13, 17, 23, 0.95) 100%);
            backdrop-filter: blur(10px);
            border-radius: 20px;
            padding: 22px;
            border: 1px solid rgba(48, 54, 61, 0.5);
            transition: all 0.3s ease;
            cursor: pointer;
            position: relative;
            overflow: hidden;
        }
        .stat-card:hover { transform: translateY(-5px); border-color: #58a6ff; box-shadow: 0 10px 30px rgba(88, 166, 255, 0.2); }
        .stat-icon { font-size: 36px; margin-bottom: 12px; }
        .stat-label { font-size: 12px; color: #8b949e; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
        .stat-number { font-size: 42px; font-weight: 800; background: linear-gradient(135deg, #fff, #58a6ff); -webkit-background-clip: text; background-clip: text; color: transparent; }
        .main-grid { display: grid; grid-template-columns: 1fr 1.2fr; gap: 24px; margin-bottom: 24px; }
        .card {
            background: linear-gradient(135deg, rgba(22, 27, 34, 0.97) 0%, rgba(13, 17, 23, 0.97) 100%);
            backdrop-filter: blur(10px);
            border-radius: 24px;
            border: 1px solid rgba(48, 54, 61, 0.5);
            overflow: hidden;
        }
        .card-header { padding: 20px 28px; background: rgba(33, 38, 45, 0.6); border-bottom: 1px solid rgba(48, 54, 61, 0.5); display: flex; align-items: center; gap: 12px; }
        .card-header i { font-size: 26px; color: #58a6ff; }
        .card-header h3 { font-size: 18px; font-weight: 600; color: #e4e6eb; }
        .card-body { padding: 24px; }
        .search-accounts { margin-bottom: 16px; position: relative; }
        .search-accounts input { width: 100%; padding: 12px 16px 12px 40px; background: #0d1117; border: 1px solid #30363d; border-radius: 12px; color: #e4e6eb; font-size: 14px; transition: all 0.3s ease; }
        .search-accounts input:focus { outline: none; border-color: #58a6ff; box-shadow: 0 0 0 3px rgba(88, 166, 255, 0.1); }
        .search-accounts i { position: absolute; left: 14px; top: 50%; transform: translateY(-50%); color: #8b949e; }
        .accounts-count { font-size: 12px; color: #8b949e; margin-top: 8px; }
        .accounts-input { width: 100%; min-height: 280px; background: #0d1117; color: #e4e6eb; border: 2px solid #30363d; border-radius: 16px; padding: 16px; font-family: 'Courier New', monospace; font-size: 13px; resize: vertical; transition: all 0.3s ease; }
        .accounts-input:focus { outline: none; border-color: #58a6ff; box-shadow: 0 0 0 4px rgba(88, 166, 255, 0.1); }
        .button-group { display: flex; gap: 12px; margin-top: 20px; flex-wrap: wrap; }
        .btn { padding: 12px 28px; border: none; border-radius: 14px; font-weight: 600; font-size: 14px; cursor: pointer; transition: all 0.3s ease; display: inline-flex; align-items: center; gap: 10px; font-family: 'Inter', sans-serif; }
        .btn-primary { background: linear-gradient(135deg, #238636, #2ea043); color: white; }
        .btn-primary:hover { transform: translateY(-2px); box-shadow: 0 5px 20px rgba(35, 134, 54, 0.4); }
        .btn-danger { background: linear-gradient(135deg, #da3633, #f85149); color: white; }
        .btn-danger:hover { transform: translateY(-2px); box-shadow: 0 5px 20px rgba(218, 54, 51, 0.4); }
        .btn-secondary { background: linear-gradient(135deg, #6e7681, #8b949e); color: white; }
        .btn-secondary:hover { transform: translateY(-2px); }
        .terminal-container { background: #010409; border-radius: 16px; height: 450px; overflow: hidden; display: flex; flex-direction: column; }
        .terminal-header { background: #161b22; padding: 14px 20px; border-bottom: 1px solid #30363d; display: flex; justify-content: space-between; align-items: center; }
        .terminal-title { display: flex; align-items: center; gap: 10px; font-size: 14px; font-weight: 500; }
        .terminal-btn { background: none; border: none; color: #8b949e; cursor: pointer; padding: 6px 12px; border-radius: 8px; transition: all 0.2s; }
        .terminal-btn:hover { background: #21262d; color: #58a6ff; }
        .terminal { flex: 1; overflow-y: auto; padding: 16px; font-family: 'Courier New', monospace; font-size: 12px; }
        .terminal-line { padding: 4px 0; border-bottom: 1px solid rgba(22, 27, 34, 0.3); white-space: pre-wrap; word-break: break-all; animation: fadeIn 0.3s ease; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(5px); } to { opacity: 1; transform: translateY(0); } }
        .search-box { margin-bottom: 16px; padding: 0 16px; display: flex; gap: 12px; flex-wrap: wrap; }
        .search-input { flex: 1; padding: 10px 16px; background: #0d1117; border: 1px solid #30363d; border-radius: 12px; color: #e4e6eb; font-size: 14px; }
        .filter-buttons { display: flex; gap: 8px; }
        .filter-btn { padding: 8px 16px; background: #21262d; border: 1px solid #30363d; border-radius: 10px; color: #8b949e; cursor: pointer; transition: all 0.2s; font-size: 12px; }
        .filter-btn.active { background: #58a6ff; color: white; border-color: #58a6ff; }
        .table-container { max-height: 450px; overflow-y: auto; border-radius: 16px; }
        .results-table table { width: 100%; border-collapse: collapse; }
        .results-table th { background: #161b22; padding: 14px; text-align: left; font-size: 13px; font-weight: 600; color: #8b949e; position: sticky; top: 0; z-index: 10; cursor: pointer; }
        .results-table th:hover { background: #21262d; color: #58a6ff; }
        .results-table td { padding: 12px 14px; border-bottom: 1px solid #21262d; font-size: 13px; }
        .results-table tr { transition: all 0.2s ease; animation: slideIn 0.2s ease; }
        @keyframes slideIn { from { opacity: 0; transform: translateX(-10px); } to { opacity: 1; transform: translateX(0); } }
        .results-table tr:hover { background: rgba(33, 38, 45, 0.6); }
        .copy-btn { background: none; border: none; color: #58a6ff; cursor: pointer; padding: 4px 8px; border-radius: 6px; transition: all 0.2s; }
        .copy-btn:hover { background: rgba(88, 166, 255, 0.2); }
        .copy-btn.copied { color: #3fb950; }
        .balance-positive { color: #3fb950; font-weight: bold; }
        .balance-zero { color: #f85149; }
        .sort-indicator { margin-left: 5px; font-size: 10px; }
        .toast { position: fixed; bottom: 30px; right: 30px; background: linear-gradient(135deg, #21262d, #161b22); border-left: 4px solid #58a6ff; padding: 14px 24px; border-radius: 12px; animation: slideInRight 0.3s ease; z-index: 1000; }
        @keyframes slideInRight { from { transform: translateX(100%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
        .progress-bar-container { margin-top: 10px; width: 100%; height: 6px; background: #21262d; border-radius: 3px; overflow: hidden; }
        .progress-bar { height: 100%; background: linear-gradient(90deg, #238636, #3fb950); width: 0%; transition: width 0.3s ease; }
        @media (max-width: 1024px) { .main-grid { grid-template-columns: 1fr; } .stats-grid { grid-template-columns: repeat(2, 1fr); } }
    </style>
</head>
<body>
    <div class="app-container">
        <div class="header">
            <div class="header-content">
                <div class="title-section">
                    <div class="logo-icon"><i class="fas fa-robot"></i></div>
                    <div>
                        <h1>Adjarabet Bot <span class="version">Pro v7.0</span></h1>
                        <p style="font-size: 13px; color: #8b949e; margin-top: 5px;">Real-Time • Live Updates • Professional</p>
                    </div>
                </div>
                <div class="status-card">
                    <div class="status-indicator">
                        <div class="status-dot stopped" id="statusDot"></div>
                        <span id="statusText">Stopped</span>
                    </div>
                    <div class="status-indicator">
                        <i class="fas fa-clock" style="color: #8b949e;"></i>
                        <span id="uptime">00:00:00</span>
                    </div>
                </div>
            </div>
        </div>

        <div class="stats-grid">
            <div class="stat-card" onclick="setFilter('all')">
                <div class="stat-icon">📊</div>
                <div class="stat-label">Total</div>
                <div class="stat-number" id="totalCount">0</div>
            </div>
            <div class="stat-card" onclick="setFilter('success')">
                <div class="stat-icon">✅</div>
                <div class="stat-label">Success</div>
                <div class="stat-number" id="successCount">0</div>
            </div>
            <div class="stat-card" onclick="setFilter('failed')">
                <div class="stat-icon">❌</div>
                <div class="stat-label">Failed</div>
                <div class="stat-number" id="failedCount">0</div>
            </div>
            <div class="stat-card" onclick="setFilter('timeout')">
                <div class="stat-icon">⏰</div>
                <div class="stat-label">Timeout</div>
                <div class="stat-number" id="timeoutCount">0</div>
            </div>
        </div>

        <div class="main-grid">
            <div class="card">
                <div class="card-header"><i class="fas fa-users"></i><h3>Account Management</h3></div>
                <div class="card-body">
                    <div class="search-accounts">
                        <i class="fas fa-search"></i>
                        <input type="text" id="accountsSearchInput" placeholder="🔍 Search in accounts...">
                    </div>
                    <textarea id="accounts" class="accounts-input" placeholder="username1:password1&#10;username2:password2"></textarea>
                    <div class="accounts-count" id="accountsCount">0 accounts loaded</div>
                    <div class="button-group">
                        <button class="btn btn-primary" onclick="startBot()"><i class="fas fa-play"></i> Start Bot</button>
                        <button class="btn btn-danger" onclick="stopBot()"><i class="fas fa-stop"></i> Stop</button>
                        <button class="btn btn-secondary" onclick="clearAllAccounts()"><i class="fas fa-trash-alt"></i> Clear All</button>
                        <button class="btn btn-secondary" onclick="loadSample()"><i class="fas fa-file-alt"></i> Sample</button>
                    </div>
                </div>
            </div>

            <div class="card">
                <div class="card-header"><i class="fas fa-terminal"></i><h3>Live Console</h3></div>
                <div class="card-body" style="padding: 0;">
                    <div class="terminal-container">
                        <div class="terminal-header">
                            <div class="terminal-title"><i class="fas fa-code"></i><span>Bot Logs</span></div>
                            <div class="terminal-actions">
                                <button class="terminal-btn" onclick="clearTerminal()" title="Clear"><i class="fas fa-eraser"></i></button>
                                <button class="terminal-btn" onclick="exportLogs()" title="Export"><i class="fas fa-download"></i></button>
                            </div>
                        </div>
                        <div id="terminal" class="terminal">
                            <div class="terminal-line">🎮 Welcome to Adjarabet Bot Pro v7.0</div>
                            <div class="terminal-line">💡 Results appear in REAL-TIME as they are checked</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="card results-table">
            <div class="card-header">
                <i class="fas fa-chart-line"></i><h3>Results Dashboard</h3>
                <div style="flex: 1; margin: 0 20px;"><div class="progress-bar-container"><div class="progress-bar" id="progressBar"></div></div></div>
                <button class="terminal-btn" onclick="refreshResults()" style="margin-left: auto;"><i class="fas fa-sync-alt"></i> Refresh</button>
            </div>
            <div class="card-body" style="padding: 0;">
                <div class="search-box">
                    <input type="text" id="searchInput" class="search-input" placeholder="🔍 Search by username or password...">
                    <div class="filter-buttons">
                        <button class="filter-btn active" data-filter="all" onclick="setFilter('all')">All</button>
                        <button class="filter-btn" data-filter="success" onclick="setFilter('success')">✅ Success</button>
                        <button class="filter-btn" data-filter="failed" onclick="setFilter('failed')">❌ Failed</button>
                        <button class="filter-btn" data-filter="timeout" onclick="setFilter('timeout')">⏰ Timeout</button>
                    </div>
                </div>
                <div class="table-container">
                    <table class="results-table">
                        <thead>
                            <tr>
                                <th onclick="sortBy('status')">Status <span class="sort-indicator" id="sort-status"></span></th>
                                <th onclick="sortBy('username')">Username <span class="sort-indicator" id="sort-username"></span></th>
                                <th onclick="sortBy('password')">Password <span class="sort-indicator" id="sort-password"></span></th>
                                <th onclick="sortBy('balance')">Balance <span class="sort-indicator" id="sort-balance">▼</span></th>
                                <th onclick="sortBy('error')">Error <span class="sort-indicator" id="sort-error"></span></th>
                            </tr>
                        </thead>
                        <tbody id="resultsBody">
                            <tr><td colspan="5" style="text-align: center; color: #8b949e; padding: 40px;">Waiting for results...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <script>
        let ws = null;
        let allResults = [];
        let originalAccounts = '';
        let currentFilter = 'all';
        let currentSort = { field: 'balance', direction: 'desc' };
        let botStartTime = null;
        let uptimeInterval = null;
        let currentProgress = { processed: 0, total: 0 };
        let loggedUsernames = new Set();

        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
            
            ws.onopen = () => { addTerminalLine('🟢 Connected - Real-time updates active', 'success'); loadResults(); loggedUsernames.clear(); };
            ws.onmessage = (event) => {
                const data = event.data;
                if (data.startsWith('RESULT:')) {
                    try {
                        const result = JSON.parse(data.substring(7));
                        addOrUpdateResult(result);
                        if (!loggedUsernames.has(result.username)) {
                            loggedUsernames.add(result.username);
                            addTerminalLine(`${result.status} ${result.username} | ${result.balance || '0'}`, 
                                result.status === '✅' ? 'success' : result.status === '❌' ? 'error' : 'warning');
                        }
                    } catch(e) {}
                } else if (data.startsWith('PROGRESS:')) {
                    const parts = data.substring(9).split('/');
                    currentProgress = { processed: parseInt(parts[0]), total: parseInt(parts[1]) };
                    updateProgressBar();
                } else if (data.startsWith('SUMMARY:')) {
                    const parts = data.substring(8).split(':');
                    addTerminalLine(`📊 Summary: ${parts[0]} (${parts[2]}%)`, 'info');
                } else if (data.startsWith('STATUS:')) {
                    const status = data.substring(7);
                    updateBotStatus(status === 'started');
                    if (status === 'started') loggedUsernames.clear();
                } else {
                    addTerminalLine(data.replace(/^(INFO|ERROR|WARNING):\s*/, ''), 
                        data.includes('ERROR') ? 'error' : data.includes('WARNING') ? 'warning' : 'info');
                }
            };
            ws.onclose = () => { addTerminalLine('🔴 Disconnected - Reconnecting...', 'warning'); setTimeout(connectWebSocket, 3000); };
        }

        function updateProgressBar() {
            const progressBar = document.getElementById('progressBar');
            if (currentProgress.total > 0) progressBar.style.width = (currentProgress.processed / currentProgress.total * 100) + '%';
            else progressBar.style.width = '0%';
        }

        function addOrUpdateResult(newResult) {
            const existingIndex = allResults.findIndex(r => r.username === newResult.username);
            if (existingIndex >= 0) allResults[existingIndex] = newResult;
            else allResults.push(newResult);
            renderResults();
            updateStats();
        }

        function parseBalance(balanceStr) {
            if (!balanceStr) return 0;
            const match = balanceStr.match(/[\\d,.]+/);
            return match ? parseFloat(match[0].replace(/,/g, '')) : 0;
        }

        function sortResults(results) {
            return [...results].sort((a, b) => {
                let aVal, bVal;
                switch(currentSort.field) {
                    case 'balance': aVal = parseBalance(a.balance); bVal = parseBalance(b.balance); break;
                    case 'username': aVal = (a.username || '').toLowerCase(); bVal = (b.username || '').toLowerCase(); break;
                    case 'password': aVal = (a.password || '').toLowerCase(); bVal = (b.password || '').toLowerCase(); break;
                    case 'status': aVal = a.status || ''; bVal = b.status || ''; break;
                    case 'error': aVal = (a.error || '').toLowerCase(); bVal = (b.error || '').toLowerCase(); break;
                    default: return 0;
                }
                return currentSort.direction === 'asc' ? (aVal > bVal ? 1 : -1) : (aVal < bVal ? 1 : -1);
            });
        }

        function filterResults(results) {
            if (currentFilter === 'all') return results;
            const statusMap = { 'success': '✅', 'failed': '❌', 'timeout': '⏰' };
            return results.filter(r => r.status === statusMap[currentFilter]);
        }

        function renderResults() {
            const searchTerm = document.getElementById('searchInput').value.toLowerCase();
            let filtered = filterResults(allResults);
            filtered = filtered.filter(r => (r.username || '').toLowerCase().includes(searchTerm) || (r.password || '').toLowerCase().includes(searchTerm));
            const sorted = sortResults(filtered);
            const tbody = document.getElementById('resultsBody');
            if (sorted.length === 0) tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: #8b949e; padding: 40px;">No results found</td></tr>';
            else {
                tbody.innerHTML = sorted.map(acc => {
                    const balanceValue = parseBalance(acc.balance);
                    return `<tr>
                        <td style="font-size: 20px;">${acc.status || '-'}</td>
                        <td><div style="display: flex; align-items: center; justify-content: space-between;"><strong style="color: #58a6ff;">${escapeHtml(acc.username || '')}</strong><button class="copy-btn" onclick="copyText('${escapeHtml(acc.username || '')}', this)"><i class="fas fa-copy"></i></button></div></td>
                        <td><div style="display: flex; align-items: center; justify-content: space-between;"><span style="font-family: monospace;">${escapeHtml(acc.password || '')}</span><button class="copy-btn" onclick="copyText('${escapeHtml(acc.password || '')}', this)"><i class="fas fa-copy"></i></button></div></td>
                        <td class="${balanceValue > 0 ? 'balance-positive' : 'balance-zero'}">${acc.balance || '0'}</td>
                        <td style="color: #8b949e; font-size: 12px;">${acc.error || '-'}</td>
                    </tr>`;
                }).join('');
            }
            updateSortIndicators();
        }

        function updateStats() {
            document.getElementById('totalCount').textContent = allResults.length;
            document.getElementById('successCount').textContent = allResults.filter(r => r.status === '✅').length;
            document.getElementById('failedCount').textContent = allResults.filter(r => r.status === '❌').length;
            document.getElementById('timeoutCount').textContent = allResults.filter(r => r.status === '⏰').length;
        }

        function updateSortIndicators() {
            ['status', 'username', 'password', 'balance', 'error'].forEach(field => {
                const el = document.getElementById(`sort-${field}`);
                if (el) el.innerHTML = currentSort.field === field ? (currentSort.direction === 'asc' ? '▲' : '▼') : '';
            });
        }

        function sortBy(field) {
            if (currentSort.field === field) currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
            else { currentSort.field = field; currentSort.direction = field === 'balance' ? 'desc' : 'asc'; }
            renderResults();
        }

        function setFilter(filter) {
            currentFilter = filter;
            document.querySelectorAll('.filter-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.filter === filter));
            renderResults();
        }

        function escapeHtml(str) { if (!str) return ''; return str.replace(/[&<>]/g, m => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' })[m]); }
        
        function copyText(text, btn) {
            navigator.clipboard.writeText(text).then(() => {
                const icon = btn.querySelector('i');
                icon.className = 'fas fa-check';
                btn.classList.add('copied');
                setTimeout(() => { icon.className = 'fas fa-copy'; btn.classList.remove('copied'); }, 1500);
                showToast('Copied to clipboard');
            });
        }

        function addTerminalLine(text, type = 'info') {
            const term = document.getElementById('terminal');
            const div = document.createElement('div');
            div.className = 'terminal-line';
            div.innerHTML = `<span style="color: #6e7681;">[${new Date().toLocaleTimeString()}]</span> ${text}`;
            term.appendChild(div);
            div.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
            while (term.children.length > 500) term.removeChild(term.firstChild);
        }

        function showToast(message) {
            const toast = document.createElement('div');
            toast.className = 'toast';
            toast.innerHTML = `<i class="fas fa-info-circle"></i> ${message}`;
            document.body.appendChild(toast);
            setTimeout(() => toast.remove(), 2500);
        }

        function updateBotStatus(running) {
            const dot = document.getElementById('statusDot');
            const text = document.getElementById('statusText');
            if (running) {
                dot.className = 'status-dot running';
                text.textContent = 'Running';
                botStartTime = Date.now();
                if (uptimeInterval) clearInterval(uptimeInterval);
                uptimeInterval = setInterval(() => {
                    if (botStartTime) {
                        const elapsed = Math.floor((Date.now() - botStartTime) / 1000);
                        document.getElementById('uptime').textContent = `${String(Math.floor(elapsed/3600)).padStart(2,'0')}:${String(Math.floor((elapsed%3600)/60)).padStart(2,'0')}:${String(elapsed%60).padStart(2,'0')}`;
                    }
                }, 1000);
                loggedUsernames.clear();
            } else {
                dot.className = 'status-dot stopped';
                text.textContent = 'Stopped';
                if (uptimeInterval) clearInterval(uptimeInterval);
                document.getElementById('uptime').textContent = '00:00:00';
                currentProgress = { processed: 0, total: 0 };
                updateProgressBar();
            }
        }

        function clearTerminal() { document.getElementById('terminal').innerHTML = '<div class="terminal-line">🧹 Terminal cleared</div>'; }
        
        async function startBot() {
            const accounts = document.getElementById('accounts').value;
            if (!accounts.trim()) { addTerminalLine('❌ Please enter accounts first!', 'error'); return; }
            addTerminalLine('🚀 Starting bot...', 'info');
            loggedUsernames.clear();
            try {
                const response = await fetch('/start', { method: 'POST', body: accounts });
                const data = await response.json();
                if (data.status === 'started') { allResults = []; renderResults(); updateStats(); currentProgress = { processed: 0, total: 0 }; updateProgressBar(); }
            } catch(e) { addTerminalLine(`❌ Error: ${e.message}`, 'error'); }
        }

        async function stopBot() {
            addTerminalLine('⏹ Stopping bot...', 'warning');
            try { await fetch('/stop', { method: 'POST' }); } catch(e) { addTerminalLine(`❌ Error: ${e.message}`, 'error'); }
        }

        async function clearAllAccounts() {
            document.getElementById('accounts').value = '';
            originalAccounts = '';
            localStorage.removeItem('bot_accounts');
            updateAccountsCount();
            addTerminalLine('🗑 All accounts cleared', 'info');
            try { await fetch('/accounts', { method: 'DELETE' }); } catch(e) {}
        }

        async function loadResults() {
            try {
                const response = await fetch('/results');
                const data = await response.json();
                if (Array.isArray(data) && data.length > 0) { allResults = data; data.forEach(r => loggedUsernames.add(r.username)); renderResults(); updateStats(); }
            } catch(e) {}
        }

        function refreshResults() { loadResults(); showToast('Results refreshed'); }
        
        function updateAccountsCount() {
            const textarea = document.getElementById('accounts');
            const lines = textarea.value.split('\\n').filter(l => l.trim() && l.includes(':')).length;
            document.getElementById('accountsCount').innerHTML = `${lines} accounts loaded`;
            originalAccounts = textarea.value;
            localStorage.setItem('bot_accounts', textarea.value);
        }

        function loadSample() {
            const sample = `user1:pass123\\nuser2:pass456\\nuser3:pass789`;
            document.getElementById('accounts').value = sample;
            originalAccounts = sample;
            updateAccountsCount();
            addTerminalLine('📝 Sample accounts loaded', 'info');
        }

        function exportLogs() {
            const term = document.getElementById('terminal');
            const logs = Array.from(term.children).map(l => l.textContent).join('\\n');
            const blob = new Blob([logs], { type: 'text/plain' });
            const a = document.createElement('a');
            a.href = URL.createObjectURL(blob);
            a.download = `logs_${new Date().toISOString().slice(0,19).replace(/:/g,'-')}.txt`;
            a.click();
            URL.revokeObjectURL(a.href);
            showToast('Logs exported');
        }

        window.addEventListener('load', () => {
            const saved = localStorage.getItem('bot_accounts');
            if (saved) { document.getElementById('accounts').value = saved; originalAccounts = saved; updateAccountsCount(); }
            const textarea = document.getElementById('accounts');
            textarea.addEventListener('input', updateAccountsCount);
            document.getElementById('searchInput').addEventListener('input', renderResults);
            const searchInput = document.getElementById('accountsSearchInput');
            searchInput.addEventListener('input', function() {
                const term = this.value.toLowerCase();
                if (!term && originalAccounts) document.getElementById('accounts').value = originalAccounts;
                else if (term && originalAccounts) {
                    const filtered = originalAccounts.split('\\n').filter(l => l.toLowerCase().includes(term)).join('\\n');
                    document.getElementById('accounts').value = filtered;
                }
                updateAccountsCount();
            });
            connectWebSocket();
            loadResults();
            setInterval(loadResults, 2000);
        });
    </script>
</body>
</html>'''

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger = Logger()
    logger.info("=" * 50)
    logger.info("ADJARABET BOT v12.0 - OPTIMIZED")
    logger.info("=" * 50)
    
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

app = FastAPI(title="Adjarabet Bot API", version="12.0", lifespan=lifespan)

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
        
        # Parse accounts
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
