import requests
from bs4 import BeautifulSoup
import html5lib
import sqlite3
import json, re
from multiprocessing import Pool
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool as DPool
from functools import partial
from tenacity import retry, wait_random, retry_if_exception_type, stop_after_attempt, after_log
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import selenium.common.exceptions
import socks
import random
import logging, coloredlogs


coloredlogs.install()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


class ScrapeException(Exception):

    pass

class ScrapeIcoBench:

    def __init__(self):

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"}
        # Initialize the Database.
        self.DB_FILE = './ico_data.db'
        with open('./setup.sql', 'r') as f:
            sql_setup = f.read()
        conn = sqlite3.connect(self.DB_FILE)
        cur = conn.cursor()
        cur.executescript(sql_setup)
        conn.commit()
        conn.close()

    @retry(wait=wait_random(15, 30), retry=retry_if_exception_type((requests.RequestException, ScrapeException)))
    def scrape_financials(self, ico_url):

        url = f"{ico_url}/financial"
        resp = requests.get(url, headers=self.headers)
        if resp.status_code == 503:
            logger.warning("[!] Got a 503 Error. Waiting and retrying...")
            raise ScrapeException()
        html = resp.content
        bs = BeautifulSoup(html, features="html5lib")

        results = {}

        invest_info = bs.find("div", {"class": "box_right"})
        if invest_info is None:
            logger.warning(f"[!] Could not find financial data at {ico_url}. Retrying.")
            raise ScrapeException()
        rows = invest_info.find_all("div", {"class": "row"})
        if rows is not None:
            for row in rows:
                label = row.find("div", {"class": "label"}).text
                value = row.find("div", {"class": "value"}).text
                if label == 'Raised':
                    value = value.replace('\t', '') # strip tabs out
                results[label] = value

        return results

    @retry(wait=wait_random(15, 30), retry=retry_if_exception_type((requests.RequestException, ScrapeException)), stop=stop_after_attempt(20))
    def scrape_description_url(self, ico_url):

        resp = requests.get(ico_url, headers=self.headers)
        if resp.status_code == 503:
            logger.warning("[!] Got a 503 Error. Waiting and retrying...")
            raise ScrapeException()
        html = resp.content
        bs = BeautifulSoup(html, features="html5lib")
        try:
            description = bs.find("div", {"id": "about"}).text
        except AttributeError:
            logger.warning(f"[-] Couldn't find About section at {ico_url}. We are probably being rate limited. Retrying...")
            raise ScrapeException()
        try:
            link = bs.find("a", {"class": "button_big"}, href=True)['href']
        except (AttributeError, TypeError) as err:
            logger.warning(f"[@] WARNING: Could not find a link for {ico_url}")
            link = None

        return {"link": link, "description": description}

    @retry(wait=wait_random(15, 30), retry=retry_if_exception_type((requests.RequestException, ScrapeException)))
    def scrape_team(self, ico_url):

        results = {}

        url = f"{ico_url}/team"

        resp = requests.get(url, headers=self.headers)
        if resp.status_code == 503:
            logger.warning("[!] Got a 503 Error. Waiting and retrying...")
            raise ScrapeException
        html = resp.content

        bs = BeautifulSoup(html, features="html5lib")

        outer_container = bs.find("div", {"class": "tab_content"})
        if outer_container is None:
            logger.warning(f"[!] Could not find the tab_content container in {ico_url}")
            raise ScrapeException # retry if we are being rate limited.
        team_member = outer_container.find_all("div", {"class": "col_3"})
        logger.info(f"[+] Found {len(team_member)} team members for {ico_url}.")
        for member in team_member:
            soc_urls = []
            member_name = member.find("h3", {"class": "notranslate"}).text
            socials = member.find("div", {"class": "socials"})
            if socials is not None:
                social_urls = socials.find_all("a", href=True)
                for url in social_urls:
                    soc_urls.append(url['href'])

            results[member_name] = soc_urls

        return results

    @retry(wait=wait_random(15, 30), retry=retry_if_exception_type(ScrapeException))
    def scrape_icobench(self, page_num):

        ico_base_url = "https://icobench.com"
        url = f"https://icobench.com/icos?page={page_num}&filterSort=name-asc"
        logger.info(f"[-] Getting page number: {page_num}")

        conn = sqlite3.connect(self.DB_FILE)
        cur = conn.cursor()

        resp = requests.get(url, headers=self.headers)
        if resp.status_code == 503:
            logger.warning(f"[!] Got a 503 Error at page {page_num}. Waiting and retrying...")
            raise ScrapeException()
        html = resp.content
        bs = BeautifulSoup(html, features="html5lib")

        table = bs.find("div", {"class": "ico_list"})
        if table is None:
            logger.warning(f"[!] ERROR with {page_num}. Couldn't find ICOs.")
            raise ScrapeException(f"[!] No table object found for {page_num}. Retrying...")
        table_rows = table.find_all("td", {"class": "ico_data"})
        logger.info(f"[-] Found {len(table_rows)} total ICOs on page {page_num}.")
        for row in table_rows:

            attrs = row.find("p", {"class": "notranslate"})
            # Get a list of countries if available.
            countries = re.search('Countries:(.*)', attrs.text)
            countries_list = None
            if countries is not None:
                countries_list = countries.group(1).lstrip(' ')

            pre_ico = False
            link = row.find("a", {"class": "name notranslate"}, href=True)
            ico_url = ico_base_url + link['href']
            ico_name = link.text
            # format our ICO name.
            if "(PreICO)" in ico_name:
                pre_ico = True
                # Strip the bad chars and other junk out.
                ico_name = ico_name.replace('(PreICO)', '')
                ico_name = ico_name.replace('\xa0', '')
                ico_name = ico_name.replace(' ', '')
                logger.debug(f"[+] Found Pre-ICO: {ico_name}")
            else:
                # Strip the bad chars and whitespace out.
                ico_name = ico_name.replace('\xa0', '')
                ico_name = ico_name.replace(' ', '')
                logger.debug(f"[+] Found ICO: {ico_name}")

            is_in_db = cur.execute("SELECT * FROM ico_data WHERE ico_name=?;", (ico_name,))
            if is_in_db.fetchall():
                # This check lets us avoid sending unnecessary requests to avoid rate limiting.
                #logger.info(f"[-] Already have data for {ico_name}. Ignoring.")
                pass
            ignore_check = True
            if ignore_check:

                description_url = self.scrape_description_url(ico_url) # Get a link to the URL and some basic info about the ICO
                team_members = self.scrape_team(ico_url) # Get all the team members and their social media profiles.
                fin_results = self.scrape_financials(ico_url) # Get the financial data.
                try:
                    logger.debug(f"[-] Inserting data for {ico_name}.")
                    cur.execute(f"INSERT INTO ico_data (ico_name, description, ico_url, ico_team, pre_ico, financials, "
                                f"countries) VALUES (?,?,?,?,?,?,?);", (ico_name,
                                                                        description_url.get('description'),
                                                                        description_url.get('link'),
                                                                        json.dumps(team_members),
                                                                        pre_ico,
                                                                        json.dumps(fin_results),
                                                                        countries_list))

                except sqlite3.IntegrityError as err:
                    logger.debug(f"[/] Already have data for {ico_name}. Ignoring.") # The check should avoid this.
                    pass # ignore if the ICO is already in the Database.
                except (sqlite3.DatabaseError, sqlite3.ProgrammingError, sqlite3.InterfaceError) as err:
                    logger.error(f"[!] BAD SQL WHEN PROCESSING: {ico_name}. ERROR: {err}")
                    logger.error(f"\tHere is the SQL:\n1. {ico_name}\n2. {description_url.get('description')}\n"
                                 f"3. {description_url.get('link')}\n4. {json.dumps(team_members)}\n\t"
                                 f"4a. {type(json.dumps(team_members))}\n5. {pre_ico}")

                conn.commit()

        conn.commit()
        conn.close()
        return True

class SeleniumScrapeAngel:

    def __init__(self, mode=0):

        logger.info("[^] Spawning session...")

        self.driver = webdriver.Firefox()
        self.wait = WebDriverWait(self.driver, 10)
        self.s_login()

        # initialize and setup the database.
        logger.info("[^] Initializing Database...")
        self.DB_PATH = "./ico_data.db"
        with open('./setup.sql', 'r') as f:
            setup_sql = f.read()
        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()
        cur.executescript(setup_sql)
        conn.commit()
        conn.close()

        self.s_login()

    def s_login(self):

        login_url = "https://angel.co/login"
        username = "sikka.ruchi@gmail.com"
        password = "papa0409"

        self.driver.get(login_url)

        # wait for the login field to appear.
        user_field = self.wait.until(EC.presence_of_element_located((By.ID, "user_email")))
        user_field.clear()
        user_field.send_keys(username)
        pass_field = self.wait.until(EC.presence_of_element_located((By.ID, "user_password")))
        pass_field.clear()
        pass_field.send_keys(password)
        pass_field.send_keys(Keys.ENTER)
        #submit_button = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "c-button c-button--blue s-vgPadLeft1_5 s-vgPadRight1_5")))
        # Open a new tab.

    def fuzz_urls(self, ico_name):

        base_url = "https://angel.co/"
        urls = []
        formatted_ico_name = ico_name.lower()
        urls.append(base_url + formatted_ico_name) # always have at least one URL to check
        if "." in formatted_ico_name:

            # Using GTLDs
            hyphen_ico = formatted_ico_name.replace('.', '-')
            urls.append(base_url + hyphen_ico) # Replace periods with hyphens. Example: Liquidity.Network becomes liquidity-network
            for n in range(1,3):
                urls.append(base_url + hyphen_ico + f"-{n}")

            # Strip GTLDs
            split_name = formatted_ico_name.split('.')
            urls.append(base_url + split_name[0])
            for n in range(1,3):
                urls.append(base_url + split_name[0] + f"-{n}") # check if there's more than one Page for the ICO.

        else:
            for n in range(1,3):
                urls.append(base_url + formatted_ico_name + f'-{n}') # check if there's more than one Page for the ICO.

        return urls

    def s_scrape_company(self, ico_name):

        urls = self.fuzz_urls(ico_name)
        results_list = []
        for url in urls:
            logger.info(f"[-] Searching {url} ...")
            self.driver.get(url)
            if self.driver.current_url == url:
                bs = BeautifulSoup(self.driver.page_source, features="html5lib")
                profile_pic = bs.find("img", {"class": "js-avatar-img"})
                error_status = bs.find("p", {"class": "g-helvetica_ultra u-fontSize36 u-colorMuted"})
                if profile_pic:
                    # If a profile pic is present, we are on someone's profile
                    pass
                else:
                    if error_status:
                        if "404" in error_status.text:
                            logger.debug("[-] Ignoring 404...")
                        else:
                            try:
                                self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "group")))
                                result = self.s_scrape_comapny_parser(self.driver.page_source, ico_name)
                                results_list.append(result)
                            except selenium.common.exceptions.TimeoutException:
                                logger.debug("[@] Could have hit a profile...")
                    else:
                        self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "group")))
                        result = self.s_scrape_comapny_parser(self.driver.page_source, ico_name)
                        results_list.append(result)
            else:
                pass # We got redirected. Ignore this.

        self.s_scrape_person(results_list, ico_name)

    def s_scrape_comapny_parser(self, html, ico_name):

        logger.info("[-] Recieved page source. Parsing... ")

        personnel_urls = []

        bs = BeautifulSoup(html, features="html5lib")
        if "https://angel.co/captcha?" in str(html):
            logger.warning(f"[!] CAPTCHA detected when searching {ico_name}")
            raise ScrapeException()

        founders = bs.find("div", {"class": "founders section"})
        investors = bs.find("div", {"class": "past_financing section"})
        team_members = bs.find("div", {"class": "section team"})

        if founders:
            current_founders = founders.find_all("div", {"data-role": "founder"})
            past_founders = founders.find_all("div", {"data-role": "past_founder"})

            if current_founders:
                logger.debug(f"[-] Got founders at {ico_name}")
                for founder in current_founders:
                    try:
                        personnel_url = founder.find("a", {"class": "profile-link"}, href=True)['href']
                        personnel_urls.append({'url': personnel_url, 'founder_flag': True})
                    except TypeError:
                        pass  # Sometimes this class renders hidden with no elements.
            if past_founders:
                logger.debug(f"[-] Got past founders at {ico_name}")
                for founder in past_founders:
                    try:
                        personnel_url = founder.find("a", {"class": "profile-link"}, href=True)['href']
                        personnel_urls.append({'url': personnel_url, 'founder_flag': True})
                    except TypeError:
                        pass  # Sometimes this class renders hidden with no elements.
        if investors:
            logger.debug(f"[-] Got investors at {ico_name}")
            for investor in investors:
                try:
                    personnel_url = investor.find("a", {"class": "profile-link"}, href=True)['href']
                    personnel_urls.append({'url': personnel_url, 'founder_flag': False})
                except TypeError:
                    pass  # Sometimes this class renders hidden with no elements.
        if team_members:
            logger.debug(f"[-] Got team members at {ico_name}")
            for team_member in team_members:
                try:
                    personnel_url = team_member.find("a", {"class": "profile-link"}, href=True)['href']
                    personnel_urls.append({'url': personnel_url, 'founder_flag': False})
                except TypeError:
                    pass # Sometimes this class renders hidden with no elements.


        logger.info(f"[+] Found {len(personnel_urls)} profiles.")
        return personnel_urls

    def s_scrape_person(self, return_list, ico_name):
        for obj in return_list:
            for url in obj:
                personnel_url = url.get('url')
                self.driver.get(personnel_url)
                self.driver.implicitly_wait(2)
                html = self.driver.page_source

                result = self.s_scrape_person_parser(ico_name=ico_name, member_info=url, html=html)
        logger.info(f"[*] Finished with {ico_name}")

    def s_scrape_person_parser(self, ico_name, member_info, html):

        member_url = member_info.get('url')
        founder_flag = member_info.get('founder_flag')
        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()

        bs = BeautifulSoup(html, features="html5lib")

        captcha_detect = bs.find("textarea", {"id": "g-recaptcha-response"})
        if captcha_detect:
            logger.warning(f"[!] Detected CAPTCHA for {member_url}. Retrying...")
            raise ScrapeException()

        tags = bs.find("div", {"class": "subheader-tags"})
        socials = bs.find("div", {"class": "darkest dps64 profiles-show fls45 links _a _jm"})

        try:
            name = bs.find("h1", {"class": "u-fontSize25 u-fontSize24SmOnly u-fontWeight500"}).text
        except AttributeError as err:
            try:
                name = bs.find("h1", {"class": "u-fontSize32 u-fontSize24SmOnly u-fontWeight500 s-vgBottom0_5"}).text
            except AttributeError as err:
                logger.warning("[!] Could not find name for some reason. Retrying...")
                raise ScrapeException()
        if name is None:
            logger.warning(f"[!] Couldn't find member name. Retrying...")
            raise ScrapeException()
        # Strip junk chars out of the name field.
        name = name.replace('\n\n\nReport this profile\n\n\n\n', '')
        name = name.replace('\n', '')
        name = name.replace('\r\n', '')

        profile_pic_url = bs.find("img", {"class": "js-avatar-img"})['src'] # Get the URL of the profile pic.
        experience = bs.find("div", {"class": "experience s-grid0"})
        #education = bs.find("div", {"class": "education s-grid0"})
        about = bs.find("div", {"class": "about s-grid0"})
        user_id_number = bs.find("div", {"class": "dps64 profiles-show fhr17 header _a _jm"})['data-user_id']
        logger.warning(f"[+] Got a user number: {user_id_number}")
        investments = bs.find("div", {"class": "investments s-grid0"})

        if profile_pic_url:

            r = requests.get(url=profile_pic_url, headers=self.headers)
            if r.status_code == 200:
                r.raw.decode_content = True
                profile_pic = sqlite3.Binary(r.content) # store the binary image
                if profile_pic is None:
                    logger.warning("[@] WARNING: Unable to download profile pic...")
            else:
                logger.warning(f"[-] Encountered a non-200 status code at {member_url}. Retrying...")
                profile_pic = None
                raise ScrapeException()
            r.close()
        else:
            profile_pic = None

        if tags:
            member_tags = []
            location = None
            tooltips = tags.find_all("span", {"class": "s-vgRight0_5 tag tiptip"})
            non_tooltips = tags.find_all("span", {"class": "s-vgRight0_5 tag"})

            if tooltips:
                for tooltip in tooltips:
                    if tooltip.find("span", {"class": "fontello-location icon"}):
                        # If this span contains the location icon, record the location.
                        location = tooltip['title']
                    else:
                        member_tags.append(tooltip['title'])

            if non_tooltips:
                for n_tip in non_tooltips:
                    if n_tip.find("span", {"class": "fontello-location icon"}):
                        # If this span contains the location icon, record the location.
                        location = n_tip['title']
                    else:
                        member_tags.append(n_tip['title'])

            member_tags = "|".join(member_tags)
            if location is None:
                logger.warning(f"[@] WARNING: Could not find a location for {member_url}. Check logs.")
        else:
            location = None
            member_tags = []

        if socials:
            # Parse the social media profiles out of the spans.
            try:
                linkedin_url = socials.find("a", {"data-field": "linkedin_url"}, href=True)['href']
            except TypeError:
                linkedin_url = None
            try:
                twitter_url = socials.find("a", {"data-field": "twitter_url"}, href=True)['href']
            except TypeError:
                twitter_url = None
            try:
                facebook_url = socials.find("a", {"data-field": "facebook_url"}, href=True)['href']
            except TypeError:
                facebook_url = None
            try:
                github_url = socials.find("a", {"data-field": "github_url"}, href=True)['href']
            except TypeError:
                github_url = None
            try:
                dribble_url = socials.find("a", {"data-field": "dribbble_url"}, href=True)['href']
            except TypeError:
                dribble_url = None
            try:
                behance_url = socials.find("a", {"data-field": "behance_url"}, href=True)['href']
            except TypeError:
                behance_url = None
            try:
                blog_url = socials.find("a", {"data-field": "blog_url"}, href=True)['href']
            except TypeError:
                blog_url = None
            try:
                personal_url = socials.find("a", {"data-field": "online_bio_url"}, href=True)['href']
            except TypeError:
                personal_url = None
        else:
            linkedin_url = twitter_url = facebook_url = github_url = dribble_url = behance_url = blog_url = personal_url = None

        if experience:
            member_experience = {}
            all_jobs = experience.find_all("div", {"class": "text"})
            member_title = all_jobs[0].find("span", {"class": "medium-font"}).text # Their most recent job title.

            for job in all_jobs:

                company = job.find("a", {"class": "u-unstyledLink"}, href=True).text
                job_title = job.find("span", {"class": "medium-font"}).text
                member_experience[company] = job_title

            member_experience = json.dumps(member_experience) # Convert this to a string so we can throw it in sqlite.
        else:
            member_experience = None
            member_title = None

        if about:
            skills = about.find("div", {"data-field": "tags_skills"})
            if skills:
                member_skills = []
                for skill in skills.find_all("a", href=True):
                    member_skills.append(skill.text)
                member_skills = ', '.join(member_skills) # convert this to a string.
            else:
                member_skills = None
        else:
            member_skills = None

        if investments:
            logger.info(f"[+] Found investments for {user_id_number}.")
            investment_api_url = f"https://angel.co/startup_roles/investments?user_id={user_id_number}"
            req = self.s.get(investment_api_url, headers=self.headers)
            if req.status_code == 200:
                # Serialize the results to a string
                member_investments = json.loads(req.content)
                member_investments = json.dumps(member_investments)
            else:
                logger.warning(f"[@] WARNING: Could not get investments for {member_url}")
                member_investments = None
        else:
            member_investments = None
        # Fugly beast of an insert statement..
        try:
            cur.execute("INSERT INTO investor_data (member_name, ico_name, origin_url, profile_pic, member_title, "
                        "member_tags, soc_linkedin, soc_twitter, soc_facebook, soc_github, soc_dribble, soc_behance,"
                        "soc_blog, soc_personal_site, member_experience, member_location, member_skills,"
                        "member_investments, is_founder) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (name,
                        ico_name,
                        member_url,
                        profile_pic,
                        member_title,
                        member_tags,
                        linkedin_url,
                        twitter_url,
                        facebook_url,
                        github_url,
                        dribble_url,
                        behance_url,
                        blog_url,
                        personal_url,
                        member_experience,
                        location,
                        member_skills,
                        member_investments,
                        founder_flag))

        except sqlite3.IntegrityError as err:
            logger.info("[-] Already have data for this member.")

        conn.commit()
        conn.close()

        return True

class ScrapeAngel:

    def __init__(self):

        logger.info("[^] Spawning session...")
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
                                      " Chrome/71.0.3578.98 Safari/537.36"}

        # initialize and setup the database.
        logger.info("[^] Initializing Database...")
        self.DB_PATH = "./ico_data.db"
        with open('./setup.sql', 'r') as f:
            setup_sql = f.read()
        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()
        cur.executescript(setup_sql)
        conn.commit()
        conn.close()

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

    @retry(retry=retry_if_exception_type(ScrapeException))
    def rand_proxy(self):

        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()
        #countries = ["US", "CA", "MX", "UK", "DE"]
        #random_country = countries[random.randint(0, len(countries))]
        #cur.execute("SELECT proxy, proxy_type FROM proxies WHERE is_enabled = 1 AND country=? ORDER BY RANDOM();", (random_country,))
        cur.execute("SELECT proxy, proxy_type FROM proxies WHERE is_enabled = 1 ORDER BY RANDOM();")
        row = cur.fetchone()
        if row is None:
            raise ScrapeException() # Retry
        conn.close()

        return row

    def login(self):
        # Helps us avoid captchas.
        login_url = "https://angel.co/login"
        s = requests.Session()
        req = s.get(login_url)
        html = req.content
        bs = BeautifulSoup(html, features="html5lib")
        try:
            csrf_token = bs.find("meta", {"name": "csrf-token"})['content']
        except AttributeError as e:
            logger.error("[!] ERROR! Could not find csrf token!")
            raise ScrapeException()

        post_params = {
            "utf8": "✓",
            "authenticity_token": {csrf_token},
            "login_only": True,
            "user[email]": "sikka.ruchi@gmail.com",
            "user[password]": "papa0409"
        }

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        p_req = s.post(login_url, data=post_params, headers=headers)
        if p_req.url == "https://angel.co/":
            logger.info("[+] Login Successful.")
        else:
            logger.error(f"[!] Possible login issue:\n{req.url}\n{req.status_code}\n{req.content.decode('utf8')}")
            exit()

        return s

    def fuzz_urls(self, ico_name):

        base_url = "https://angel.co/"
        urls = []
        formatted_ico_name = ico_name.lower()
        urls.append(base_url + formatted_ico_name) # always have at least one URL to check
        if "." in formatted_ico_name:

            # Using GTLDs
            hyphen_ico = formatted_ico_name.replace('.', '-')
            urls.append(base_url + hyphen_ico) # Replace periods with hyphens. Example: Liquidity.Network becomes liquidity-network
            for n in range(1,3):
                urls.append(base_url + hyphen_ico + f"-{n}")

            # Strip GTLDs
            split_name = formatted_ico_name.split('.')
            urls.append(base_url + split_name[0])
            for n in range(1,3):
                urls.append(base_url + split_name[0] + f"-{n}") # check if there's more than one Page for the ICO.

        else:
            for n in range(1,3):
                urls.append(base_url + formatted_ico_name + f'-{n}') # check if there's more than one Page for the ICO.

        return urls

    @retry(wait=wait_random(15, 30), retry=retry_if_exception_type(ScrapeException), stop=stop_after_attempt(10), after=after_log(logger, logging.DEBUG))
    def scrape_person(self, ico_name, session, member_info):

        member_url = member_info.get('url')
        founder_flag = member_info.get('founder_flag')
        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()
        try:
            req = session.get(url=member_url)
        except (requests.exceptions.ConnectionError, socks.GeneralProxyError):
            logger.error(f"[!] Error with proxy at {member_url}. Removing from pool before retrying.")
            # First update the session to use a new proxy before retrying.
            current_proxy = session.proxies.get('http').split("//")
            self.update_proxy(proxy=current_proxy[1], state=0)  # remove the proxy from the pool before retrying.
            rand_proxy = self.rand_proxy()
            session.proxies.update({'http': f'{rand_proxy[1]}://{rand_proxy[0]}',
                                    'https': f'{rand_proxy[1]}://{rand_proxy[0]}'})
            raise ScrapeException()
        if req.status_code == 503:
            logger.warning("[!] We are being rate limited. Retrying...")
            raise ScrapeException()
        elif req.status_code != 200:
            logger.warning(f"[!] {req.status_code} status code for {member_url}. Removing proxy and retrying")
            current_proxy = session.proxies.get('http').split("//")
            self.update_proxy(proxy=current_proxy[1], state=0)  # remove the proxy from the pool before retrying.
            rand_proxy = self.rand_proxy()
            session.proxies.update({'http': f'{rand_proxy[1]}://{rand_proxy[0]}',
                                    'https': f'{rand_proxy[1]}://{rand_proxy[0]}'})

            session.cookies.clear()
            raise ScrapeException()

        logger.debug(f"[^] Parsing member information at {member_url}")
        html = req.content

        bs = BeautifulSoup(html, features="html5lib")

        captcha_detect = bs.find("textarea", {"id": "g-recaptcha-response"})
        if captcha_detect:
            logger.warning(f"[!] Detected CAPTCHA for {member_url}. Retrying...")
            # First update the session to use a new proxy before retrying.
            rand_proxy = self.rand_proxy()
            session.proxies.update({'http': f'{rand_proxy[1]}://{rand_proxy[0]}',
                                    'https': f'{rand_proxy[1]}://{rand_proxy[0]}'})
            session.cookies.clear()
            raise ScrapeException()

        tags = bs.find("div", {"class": "subheader-tags"})
        socials = bs.find("div", {"class": "darkest dps64 profiles-show fls45 links _a _jm"})

        try:
            name = bs.find("h1", {"class": "u-fontSize25 u-fontSize24SmOnly u-fontWeight500"}).text
        except AttributeError as err:
            try:
                name = bs.find("h1", {"class": "u-fontSize32 u-fontSize24SmOnly u-fontWeight500 s-vgBottom0_5"}).text
            except AttributeError as err:
                logger.warning(f"[!] Couldn't find name at {member_url}, running checks.")
                detect_ipban = bs.find("h3", {"class": "s-h3"})

                if detect_ipban:
                    if detect_ipban.text == "Your IP address has been temporarily blocked for security reasons.":
                        logger.warning(f"[!] Proxy IP banned detected when scraping {member_url}.")
                        current_proxy = session.proxies.get('http').split("//")
                        self.update_proxy(proxy=current_proxy[1],
                                          state=0)  # remove the proxy from the pool before retrying.
                        session.cookies.clear()
                        raise ScrapeException()
                raise ScrapeException()
        if name is None:
            logger.warning(f"[!] Couldn't find member name at {member_url}. Retrying...")
            raise ScrapeException()

        name = name.replace('\n\n\nReport this profile\n\n\n\n', '')
        name = name.replace('\n', '')
        name = name.replace('\r\n', '')

        try:
            profile_pic_url = bs.find("img", {"class": "js-avatar-img"})['src'] # Get the URL of the profile pic.
        except AttributeError:
            logger.warning(f"[@] WARNING: Can't get profile pic URL for {member_url}")
        experience = bs.find("div", {"class": "experience s-grid0"})
        #education = bs.find("div", {"class": "education s-grid0"}) # Unused. This tends to be dynamically generated and redundant anyways.
        about = bs.find("div", {"class": "about s-grid0"})
        try:
            user_id_number = bs.find("div", {"class": "dps64 profiles-show fhr17 header _a _jm"})['data-user_id']
        except TypeError:
            # Probably got hit with a captcha.
            rand_proxy = self.rand_proxy()
            session.proxies.update({'http': f'{rand_proxy[1]}://{rand_proxy[0]}',
                                    'https': f'{rand_proxy[1]}://{rand_proxy[0]}'})
            session.cookies.clear()
            raise ScrapeException()
        logger.debug(f"[+] Got a user number: {user_id_number}")
        investments = bs.find("div", {"class": "investments s-grid0"})

        if profile_pic_url:

            r = session.get(url=profile_pic_url)
            if r.status_code == 200:
                r.raw.decode_content = True
                profile_pic = sqlite3.Binary(r.content) # store the binary image
                if profile_pic is None:
                    logger.warning("[@] WARNING: Unable to download profile pic...")
            elif r.status_code == 302:
                r.raw.decode_content = True
                profile_pic = sqlite3.Binary(r.content) # store the binary image
                if profile_pic is None:
                    logger.warning("[@] WARNING: Unable to download profile pic...")
            else:
                logger.error(f"[-] Encountered a non-handled status code ({req.status_code}) at {member_url}. Skipping profile pic download...")
                profile_pic = None
            r.close()
        else:
            profile_pic = None

        if tags:
            member_tags = []
            location = None
            tooltips = tags.find_all("span", {"class": "s-vgRight0_5 tag tiptip"})
            non_tooltips = tags.find_all("span", {"class": "s-vgRight0_5 tag"})

            if tooltips:
                for tooltip in tooltips:
                    if tooltip.find("span", {"class": "fontello-location icon"}):
                        # If this span contains the location icon, record the location.
                        location = tooltip['title']
                    else:
                        member_tags.append(tooltip['title'])

            if non_tooltips:
                for n_tip in non_tooltips:
                    if n_tip.find("span", {"class": "fontello-location icon"}):
                        # If this span contains the location icon, record the location.
                        location = n_tip['title']
                    else:
                        member_tags.append(n_tip['title'])

            member_tags = "|".join(member_tags)
            if location is None:
                logger.warning(f"[@] WARNING: Could not find a location for {member_url}. Check logs.")
        else:
            location = None
            member_tags = []

        if socials:
            # Parse the social media profiles out of the spans.
            try:
                linkedin_url = socials.find("a", {"data-field": "linkedin_url"}, href=True)['href']
            except TypeError:
                linkedin_url = None
            try:
                twitter_url = socials.find("a", {"data-field": "twitter_url"}, href=True)['href']
            except TypeError:
                twitter_url = None
            try:
                facebook_url = socials.find("a", {"data-field": "facebook_url"}, href=True)['href']
            except TypeError:
                facebook_url = None
            try:
                github_url = socials.find("a", {"data-field": "github_url"}, href=True)['href']
            except TypeError:
                github_url = None
            try:
                dribble_url = socials.find("a", {"data-field": "dribbble_url"}, href=True)['href']
            except TypeError:
                dribble_url = None
            try:
                behance_url = socials.find("a", {"data-field": "behance_url"}, href=True)['href']
            except TypeError:
                behance_url = None
            try:
                blog_url = socials.find("a", {"data-field": "blog_url"}, href=True)['href']
            except TypeError:
                blog_url = None
            try:
                personal_url = socials.find("a", {"data-field": "online_bio_url"}, href=True)['href']
            except TypeError:
                personal_url = None
        else:
            linkedin_url = twitter_url = facebook_url = github_url = dribble_url = behance_url = blog_url = personal_url = None

        if experience:
            member_experience = {}
            all_jobs = experience.find_all("div", {"class": "text"})
            member_title = all_jobs[0].find("span", {"class": "medium-font"}).text # Their most recent job title.

            for job in all_jobs:

                company = job.find("a", {"class": "u-unstyledLink"}, href=True).text
                job_title = job.find("span", {"class": "medium-font"}).text
                member_experience[company] = job_title

            member_experience = json.dumps(member_experience) # Convert this to a string so we can throw it in sqlite.
        else:
            member_experience = None
            member_title = None

        if about:
            skills = about.find("div", {"data-field": "tags_skills"})
            if skills:
                member_skills = []
                for skill in skills.find_all("a", href=True):
                    member_skills.append(skill.text)
                member_skills = ', '.join(member_skills) # convert this to a string.
            else:
                member_skills = None
        else:
            member_skills = None

        if investments:
            logger.debug(f"[+] Found investments for {user_id_number}.")
            investment_api_url = f"https://angel.co/startup_roles/investments?user_id={user_id_number}"
            req = session.get(investment_api_url)
            if req.status_code == 200:
                # Serialize the results to a string
                member_investments = json.loads(req.content)
                member_investments = json.dumps(member_investments)
            else:
                logger.warning(f"[@] WARNING: Could not get detected investments for {member_url}")
                member_investments = None
        else:
            member_investments = None
        # Fugly beast of an insert statement..
        try:
            cur.execute("INSERT INTO investor_data (member_name, ico_name, origin_url, profile_pic, member_title, "
                        "member_tags, soc_linkedin, soc_twitter, soc_facebook, soc_github, soc_dribble, soc_behance,"
                        "soc_blog, soc_personal_site, member_experience, member_location, member_skills,"
                        "member_investments, is_founder) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (name,
                        ico_name,
                        member_url,
                        profile_pic,
                        member_title,
                        member_tags,
                        linkedin_url,
                        twitter_url,
                        facebook_url,
                        github_url,
                        dribble_url,
                        behance_url,
                        blog_url,
                        personal_url,
                        member_experience,
                        location,
                        member_skills,
                        member_investments,
                        founder_flag))

        except sqlite3.IntegrityError as err:
            logger.info(f"[-] Already have data for this member: {name}")

        conn.commit()
        conn.close()

        return True

    @retry(wait=wait_random(15, 30), retry=retry_if_exception_type(ScrapeException), after=after_log(logger, logging.DEBUG))
    def scrape_company(self, ico_name):

        # Fuzz urls for better accuracy.

        s = requests.Session()
        s.headers.update(self.headers)
        rand_proxy = self.rand_proxy()
        s.proxies.update({'http': f'{rand_proxy[1]}://{rand_proxy[0]}',
                          'https': f'{rand_proxy[1]}://{rand_proxy[0]}'})

        urls = self.fuzz_urls(ico_name)
        personnel_urls = []
        logger.debug(f"[^] Scraping information for {ico_name}.")
        for url in urls:
            try:
                req = s.get(url, allow_redirects=False) # We need to detect redirects.
            except (requests.exceptions.ConnectionError, socks.GeneralProxyError):
                logger.error(f"[!] Error with proxy at {url}, removing from the pool and retrying.")
                self.update_proxy(proxy=rand_proxy[1], state=0)  # remove the proxy from the pool before retrying.
                raise ScrapeException()
            if req.status_code == 404:
                logger.debug(f"[-] No ICO found at {url}")
                pass # ignore 404s.
            elif req.status_code == 503:
                logger.warning(f"[!] We are being rate limited at {url}. Retrying...")
                raise ScrapeException()
            elif req.status_code == 301:
                logger.debug(f"[@] We got redirected when accessing {url}. Ignoring...")
                pass
            else:
                html = req.content
                bs = BeautifulSoup(html, features="html5lib")
                if "https://angel.co/captcha?" in str(html):
                    logger.warning(f"[!] CAPTCHA detected when searching {ico_name}")
                    self.update_proxy(proxy=rand_proxy[1], state=0) # remove the proxy from the pool before retrying.
                    s.cookies.clear()
                    raise ScrapeException()
                detect_notfound = bs.find("p", {"class": "g-helvetica_ultra u-fontSize36 u-colorMuted"})
                detect_ipban = bs.find("h3", {"class": "s-h3"})

                if detect_ipban:
                    if detect_ipban.text == "Your IP address has been temporarily blocked for security reasons.":
                        logger.error(f"[!] Proxy is IP banned when accessing {url}. Retrying after recycling proxy.")
                        self.update_proxy(proxy=rand_proxy[1],
                                          state=0)  # remove the proxy from the pool before retrying.
                        s.cookies.clear()
                        raise ScrapeException()

                if detect_notfound:
                    logger.debug(f"[!] GOT DETECT 404: {detect_notfound}")
                    logger.debug(repr(detect_notfound.text))
                    if "404" in detect_notfound.text:
                        logger.debug(f"[-] Got 404 for {ico_name}")
                        return False
                founders = bs.find("div", {"class": "founders section"})
                investors = bs.find("div", {"class": "past_financing section"})
                team_members = bs.find("div", {"class": "section team"})

                if founders:
                    current_founders = founders.find_all("div", {"data-role": "founder"})
                    past_founders = founders.find_all("div", {"data-role": "past_founder"})

                    if current_founders:
                        for founder in current_founders:
                            try:
                                personnel_url = founder.find("a", {"class": "profile-link"}, href=True)['href']
                            except AttributeError as err:
                                logger.error(f"[!] Error with getting investor URL for {ico_name}. {err}")
                            personnel_urls.append({'url': personnel_url, 'founder_flag': True})
                    if past_founders:
                        logger.debug(f"[-] Got past founders at {url}")
                        for founder in past_founders:
                            try:
                                personnel_url = founder.find("a", {"class": "profile-link"}, href=True)['href']
                            except AttributeError as err:
                                logger.error(f"[!] Error with getting past founder URL for {ico_name}. {err}")
                            personnel_urls.append({'url': personnel_url, 'founder_flag': True})

                if investors:
                    for investor in investors:
                        try:
                            personnel_url = investor.find("a", {"class": "profile-link"}, href=True)['href']
                        except AttributeError as err:
                            logger.error(f"[!] Error with getting investor URL for {ico_name}. {err}")
                        personnel_urls.append({'url': personnel_url, 'founder_flag': False})
                if team_members:
                    for team_member in team_members:
                        try:
                            personnel_url = team_member.find("a", {"class": "profile-link"}, href=True)['href']
                        except AttributeError as err:
                            logger.error(f"[!] Error with getting team member URL for {ico_name}. {err}")
                        personnel_urls.append({'url': personnel_url, 'founder_flag': False})

        if len(personnel_urls) > 0:
            logger.info(f"[+] Found {len(personnel_urls)} profiles for {ico_name}.")
            pd = DPool()
            func = partial(self.scrape_person, ico_name, s)
            pd.map(func, personnel_urls)
#        for personnel_url in personnel_urls:
#            self.scrape_person(ico_name=ico_name, session=s, member_info=personnel_url)

        logger.info(f"[+] Done with {ico_name}.")
        return True

# Create a function called "chunks" with two arguments, l and n:
def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i + n]


if __name__ == "__main__":

    #sico = ScrapeIcoBench()
    sico = ScrapeAngel()
    sico.fetch_proxies()
    p = Pool(processes=1000)

    conn = sqlite3.connect('./ico_data.db')
    cur = conn.cursor()
    cur.execute('SELECT ico_name FROM ico_data ORDER BY RANDOM();')
    rows = cur.fetchall()
    icos = []
    for row in rows:
        icos.append(row[0])
    #cpu_chunk = len(rows) / cpu_count() - 1
    #cpu_chunk = int(cpu_chunk)
    #for row in chunks(rows, cpu_chunk):
    #    print(row)

    p.map(sico.scrape_company, icos)
    p.join()
    p.close()
    #for ico in icos:
    #    sico.scrape_company(ico_name=ico)
    #sico.scrape_company('dadi')