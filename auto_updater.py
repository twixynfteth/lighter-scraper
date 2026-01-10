#!/usr/bin/env python3
"""
Lighter Auto-Updater for Railway
Runs daily: scrapes top holders, exports CSV, pushes to GitHub
"""

import os
import subprocess
import sqlite3
import json
import asyncio
import aiohttp
import ssl
import random
from datetime import datetime, timedelta
from typing import Optional, List

# Configuration
PROXY = os.environ.get('PROXY_URL', '')
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '')
GITHUB_REPO = os.environ.get('GITHUB_REPO', 'twixynfteth/lighter-dashboard')
DB_PATH = 'lighter_data.db'
CSV_PATH = 'top_holders.csv'
TOP_HOLDERS = 10000

BASE_URL = "https://mainnet.zklighter.elliot.ai/api/v1/account"


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                account_index INTEGER PRIMARY KEY,
                address TEXT,
                balance REAL,
                raw_data TEXT,
                last_updated TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS balance_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_index INTEGER,
                balance REAL,
                recorded_date TEXT,
                UNIQUE(account_index, recorded_date)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_balance ON accounts(balance DESC)')
        conn.commit()
        conn.close()
    
    def get_top_account_indexes(self, limit: int) -> List[int]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT account_index FROM accounts WHERE balance > 0 ORDER BY balance DESC LIMIT ?', (limit,))
        indexes = [row[0] for row in cursor.fetchall()]
        conn.close()
        return indexes
    
    def save_accounts(self, accounts: List[dict]):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        today = datetime.utcnow().strftime('%Y-%m-%d')
        
        for acc in accounts:
            cursor.execute('''
                INSERT OR REPLACE INTO accounts (account_index, address, balance, raw_data, last_updated)
                VALUES (?, ?, ?, ?, ?)
            ''', (acc['index'], acc['address'], acc['balance'], acc.get('raw_data'), acc['fetched_at']))
            
            if acc['balance'] and acc['balance'] > 0:
                cursor.execute('''
                    INSERT OR REPLACE INTO balance_history (account_index, balance, recorded_date)
                    VALUES (?, ?, ?)
                ''', (acc['index'], acc['balance'], today))
        
        conn.commit()
        conn.close()
    
    def export_csv(self, output_path: str, limit: int = None):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today = datetime.utcnow().strftime('%Y-%m-%d')
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        query = '''
            SELECT 
                a.account_index,
                a.address,
                a.balance,
                COALESCE(h.balance, a.balance) as yesterday_balance,
                a.balance - COALESCE(h.balance, a.balance) as change_24h,
                a.raw_data
            FROM accounts a
            LEFT JOIN balance_history h ON a.account_index = h.account_index AND h.recorded_date = ?
            WHERE a.balance > 0
            ORDER BY a.balance DESC
        '''
        if limit:
            query += f' LIMIT {limit}'
        
        cursor.execute(query, (yesterday,))
        rows = cursor.fetchall()
        conn.close()
        
        # Extract all tokens from raw_data
        all_tokens = set()
        parsed_rows = []
        
        for row in rows:
            account_index, address, balance, yesterday_balance, change_24h, raw_data = row
            tokens = {}
            
            if raw_data:
                try:
                    data = json.loads(raw_data)
                    if 'accounts' in data and len(data['accounts']) > 0:
                        account = data['accounts'][0]
                        if 'assets' in account:
                            for asset in account['assets']:
                                symbol = asset.get('symbol', 'UNKNOWN')
                                bal = float(asset.get('balance', 0))
                                if bal > 0 and symbol != 'LIT':
                                    tokens[symbol] = bal
                                    all_tokens.add(symbol)
                except:
                    pass
            
            parsed_rows.append({
                'account_index': account_index,
                'address': address,
                'balance': balance,
                'yesterday_balance': yesterday_balance,
                'change_24h': change_24h,
                'tokens': tokens
            })
        
        all_tokens = sorted(all_tokens)
        
        with open(output_path, 'w') as f:
            header = ['account_index', 'address', 'balance', 'yesterday_balance', 'change_24h'] + all_tokens
            f.write(','.join(header) + '\n')
            
            for row in parsed_rows:
                line = [
                    str(row['account_index']),
                    row['address'] or '',
                    f"{row['balance']:.2f}",
                    f"{row['yesterday_balance']:.2f}",
                    f"{row['change_24h']:.2f}"
                ]
                for token in all_tokens:
                    line.append(f"{row['tokens'].get(token, 0):.8f}")
                f.write(','.join(line) + '\n')
        
        print(f"✅ Exported {len(parsed_rows)} accounts to {output_path}")
    
    def get_stats(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM accounts WHERE balance > 0')
        count = cursor.fetchone()[0]
        cursor.execute('SELECT SUM(balance) FROM accounts')
        total = cursor.fetchone()[0] or 0
        conn.close()
        return count, total


class Scraper:
    def __init__(self, proxy_url: str, database: Database):
        self.proxy_url = proxy_url
        self.db = database
        self.success = 0
        self.errors = 0
    
    def _extract_address(self, data: dict) -> Optional[str]:
        if 'accounts' in data and len(data['accounts']) > 0:
            return data['accounts'][0].get('l1_address')
        return None
    
    def _extract_balance(self, data: dict) -> Optional[float]:
        if 'accounts' in data and len(data['accounts']) > 0:
            account = data['accounts'][0]
            if 'assets' in account:
                for asset in account['assets']:
                    if asset.get('symbol') == 'LIT':
                        try:
                            return float(asset.get('balance', 0))
                        except:
                            pass
            return 0.0
        return None
    
    async def fetch_account(self, session: aiohttp.ClientSession, index: int) -> Optional[dict]:
        url = f"{BASE_URL}?by=index&value={index}"
        try:
            kwargs = {'timeout': aiohttp.ClientTimeout(total=15)}
            if self.proxy_url:
                kwargs['proxy'] = self.proxy_url
            
            async with session.get(url, **kwargs) as response:
                if response.status == 200:
                    data = await response.json()
                    self.success += 1
                    return {
                        'index': index,
                        'address': self._extract_address(data),
                        'balance': self._extract_balance(data),
                        'raw_data': json.dumps(data),
                        'fetched_at': datetime.utcnow().isoformat()
                    }
                elif response.status == 404:
                    return {'index': index, 'address': None, 'balance': 0.0, 'raw_data': None, 'fetched_at': datetime.utcnow().isoformat()}
        except Exception as e:
            self.errors += 1
        return None
    
    async def scrape_indexes(self, indexes: List[int], concurrent: int = 50, burst_size: int = 2000, pause: int = 30):
        print(f"🚀 Scraping {len(indexes)} accounts...")
        
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(limit=concurrent * 2, ssl=ssl_context)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            for i in range(0, len(indexes), burst_size):
                batch = indexes[i:i+burst_size]
                semaphore = asyncio.Semaphore(concurrent)
                
                async def fetch_with_sem(idx):
                    async with semaphore:
                        return await self.fetch_account(session, idx)
                
                tasks = [fetch_with_sem(idx) for idx in batch]
                results = await asyncio.gather(*tasks)
                
                valid = [r for r in results if r is not None]
                self.db.save_accounts(valid)
                
                print(f"   Progress: {min(i+burst_size, len(indexes))}/{len(indexes)} | Success: {self.success} | Errors: {self.errors}")
                
                if i + burst_size < len(indexes):
                    print(f"   Pausing {pause}s...")
                    await asyncio.sleep(pause)
        
        print(f"✅ Scrape complete! Success: {self.success}, Errors: {self.errors}")


def push_to_github(csv_path: str, github_token: str, repo: str):
    """Push CSV to GitHub repo."""
    import base64
    import requests
    
    print(f"📤 Pushing to GitHub: {repo}")
    
    with open(csv_path, 'r') as f:
        content = f.read()
    
    content_b64 = base64.b64encode(content.encode()).decode()
    
    # Get current file SHA
    url = f"https://api.github.com/repos/{repo}/contents/top_holders.csv"
    headers = {
        'Authorization': f'token {github_token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    response = requests.get(url, headers=headers)
    sha = response.json().get('sha') if response.status_code == 200 else None
    
    # Update file
    data = {
        'message': f'Auto-update {datetime.utcnow().strftime("%Y-%m-%d %H:%M")} UTC',
        'content': content_b64,
    }
    if sha:
        data['sha'] = sha
    
    response = requests.put(url, headers=headers, json=data)
    
    if response.status_code in [200, 201]:
        print("✅ GitHub updated successfully!")
    else:
        print(f"❌ GitHub error: {response.status_code} - {response.text}")


async def main():
    print("="*60)
    print(f"🚀 Lighter Auto-Updater - {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC")
    print("="*60)
    
    db = Database(DB_PATH)
    
    # Get current stats
    count, total = db.get_stats()
    print(f"📊 Current DB: {count:,} accounts, {total:,.0f} LIT")
    
    # Get top account indexes to update
    indexes = db.get_top_account_indexes(TOP_HOLDERS)
    
    if len(indexes) < 1000:
        # Database is empty or small - do initial scrape of first 50K accounts
        print("⚠️ Database needs initial data. Scraping accounts 0-50000...")
        indexes = list(range(0, 50000))
        random.shuffle(indexes)
    
    # Scrape
    scraper = Scraper(PROXY, db)
    await scraper.scrape_indexes(indexes, concurrent=50, burst_size=2000, pause=30)
    
    # Retry failed (scrape again to fill gaps)
    count_after, _ = db.get_stats()
    if count_after < len(indexes) * 0.95:  # Less than 95% success
        print("\n🔄 Retrying to fill gaps...")
        indexes_retry = db.get_top_account_indexes(TOP_HOLDERS)
        if len(indexes_retry) > 0:
            missing = [i for i in range(max(indexes_retry)) if i not in set(indexes_retry)][:5000]
            if missing:
                await scraper.scrape_indexes(missing, concurrent=30, burst_size=1000, pause=45)
    
    # Export CSV
    db.export_csv(CSV_PATH, TOP_HOLDERS)
    
    # Push to GitHub
    if GITHUB_TOKEN and GITHUB_REPO:
        push_to_github(CSV_PATH, GITHUB_TOKEN, GITHUB_REPO)
    else:
        print("⚠️ GitHub credentials not set. Skipping push.")
    
    # Final stats
    final_count, final_total = db.get_stats()
    print(f"\n📊 Final DB: {final_count:,} accounts, {final_total:,.0f} LIT")
    print("✅ Daily update complete!")


if __name__ == '__main__':
    asyncio.run(main())
