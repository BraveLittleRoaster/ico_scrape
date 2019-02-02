# modified fetch function with semaphore
import random
import asyncio
from aiohttp import ClientSession
import time
import re
from multiprocessing import Pool, cpu_count
import os
from aiohttp_socks import SocksConnector
import sqlite3
from bs4 import BeautifulSoup

def rand_proxy():
    conn = sqlite3.connect('./ico_data.db')
    cur = conn.cursor()
    cur.execute("SELECT proxy, proxy_type FROM proxies WHERE is_enabled = 1 ORDER BY RANDOM();")
    row = cur.fetchone()
    if row is None:
        print("No proxy") # Retry if there is no proxy, useful if we are fetching by country.
    conn.close()

    return row

async def fetch(url, session):
    try:
        async with session.get(url) as response:
            return await response.read()
    except Exception as e:
        print(f"Got error {e}")


async def bound_fetch(self, sem, url, session):
    # Getter function with semaphore.
    async with sem:
        response = await self.fetch(url, session)
        print(f"Got Response: {response.status}, data: {await response.read()}")


async def main():
    rproxy = rand_proxy()
    proxy_url = f"{rproxy[1]}://{rproxy[0]}"

    connector = SocksConnector.from_url(proxy_url)
    session = ClientSession(connector=connector, headers={"User-Agent": "test"})

    async with session.get('https://www.whatismybrowser.com/detect/what-is-my-user-agent') as resp:
        response = await resp
    print(response.status)
    print(response.read())
    #bs = BeautifulSoup(data)
    #ua_container = bs.find("div", {"class": "detected_result"})
    #print(ua_container)


if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()