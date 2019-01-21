import requests
from bs4 import BeautifulSoup
import html5lib
import sqlite3
import json, re
from multiprocessing import Pool
from tenacity import retry, wait_random, retry_if_exception_type

class ScapeException(Exception):

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

    @retry(wait=wait_random(15, 30), retry=retry_if_exception_type((requests.RequestException, ScapeException)))
    def scrape_financials(self, ico_url):

        url = f"{ico_url}/financial"
        resp = requests.get(url, headers=self.headers)
        if resp.status_code == 503:
            print("[!] Got a 503 Error. Waiting and retrying...")
            raise ScapeException()
        html = resp.content
        bs = BeautifulSoup(html, features="html5lib")

        results = {}

        invest_info = bs.find("div", {"class": "box_right"})
        if invest_info is None:
            print(f"[!] Could not find financial data at {ico_url}. Retrying.")
            raise ScapeException()
        rows = invest_info.find_all("div", {"class": "row"})
        if rows is not None:
            for row in rows:
                label = row.find("div", {"class": "label"}).text
                value = row.find("div", {"class": "value"}).text
                if label == 'Raised':
                    value = value.replace('\t', '') # strip tabs out
                results[label] = value

        return results

    @retry(wait=wait_random(15, 30), retry=retry_if_exception_type((requests.RequestException, ScapeException)))
    def scrape_description_url(self, ico_url):

        resp = requests.get(ico_url, headers=self.headers)
        if resp.status_code == 503:
            print("[!] Got a 503 Error. Waiting and retrying...")
            raise ScapeException()
        html = resp.content
        bs = BeautifulSoup(html, features="html5lib")

        description = bs.find("div", {"id": "about"}).text
        if description is None:
            print(f"[-] Couldn't find About section at {ico_url}. We are probably being rate limited. Retrying...")
            raise ScapeException()
        link = bs.find("a", {"class": "button_big"}, href=True)

        return {"link": link['href'], "description": description}

    @retry(wait=wait_random(15, 30), retry=retry_if_exception_type((requests.RequestException, ScapeException)))
    def scrape_team(self, ico_url):

        results = {}

        url = f"{ico_url}/team"

        resp = requests.get(url, headers=self.headers)
        if resp.status_code == 503:
            print("[!] Got a 503 Error. Waiting and retrying...")
            raise ScapeException
        html = resp.content

        bs = BeautifulSoup(html, features="html5lib")

        outer_container = bs.find("div", {"class": "tab_content"})
        if outer_container is None:
            print(f"[!] Could not find the tab_content container in {ico_url}")
            raise ScapeException # retry if we are being rate limited.
        team_member = outer_container.find_all("div", {"class": "col_3"})
        print(f"[-] Found {len(team_member)} team members for {ico_url}.")
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

    @retry(wait=wait_random(15, 30), retry=retry_if_exception_type(ScapeException))
    def scrape_icobench(self, page_num):

        ico_base_url = "https://icobench.com"
        url = f"https://icobench.com/icos?page={page_num}&filterSort=name-asc"
        print(f"[-] Getting page number: {page_num}")

        conn = sqlite3.connect(self.DB_FILE)
        cur = conn.cursor()

        resp = requests.get(url, headers=self.headers)
        if resp.status_code == 503:
            print(f"[!] Got a 503 Error at page {page_num}. Waiting and retrying...")
            raise ScapeException()
        html = resp.content
        bs = BeautifulSoup(html, features="html5lib")

        table = bs.find("div", {"class": "ico_list"})
        if table is None:
            print(f"[!] ERROR with {page_num}. Couldn't find ICOs.")
            raise ScapeException(f"[!] No table object found for {page_num}. Retrying...")
        table_rows = table.find_all("td", {"class": "ico_data"})
        print(f"[-] Found {len(table_rows)} total ICOs on page {page_num}.")
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
                print(f"[+] Found Pre-ICO: {ico_name}")
            else:
                # Strip the bad chars and whitespace out.
                ico_name = ico_name.replace('\xa0', '')
                ico_name = ico_name.replace(' ', '')
                print(f"[+] Found ICO: {ico_name}")

            is_in_db = cur.execute("SELECT * FROM ico_data WHERE ico_name=?;", (ico_name,))
            if is_in_db.fetchall():
                # This check lets us avoid sending unnecessary requests to avoid rate limiting.
                print(f"[-] Already have data for {ico_name}. Ignoring.")

            else:

                description_url = self.scrape_description_url(ico_url) # Get a link to the URL and some basic info about the ICO
                team_members = self.scrape_team(ico_url) # Get all the team members and their social media profiles.
                fin_results = self.scrape_financials(ico_url) # Get the financial data.
                try:
                    print(f"[-] Inserting data for {ico_name}.")
                    cur.execute(f"INSERT INTO ico_data (ico_name, description, ico_url, ico_team, pre_ico, financials, "
                                f"countries) VALUES (?,?,?,?,?,?,?);", (ico_name,
                                                                        description_url.get('description'),
                                                                        description_url.get('link'),
                                                                        json.dumps(team_members),
                                                                        pre_ico,
                                                                        json.dumps(fin_results),
                                                                        countries_list))

                except sqlite3.IntegrityError as err:
                    print(f"[/] Already have data for {ico_name}. Ignoring.") # The check should avoid this.
                    pass # ignore if the ICO is already in the Database.
                except (sqlite3.DatabaseError, sqlite3.ProgrammingError, sqlite3.InterfaceError) as err:
                    print(f"[!] BAD SQL WHEN PROCESSING: {ico_name}. ERROR: {err}")
                    print(f"\tHere is the SQL:\n1. {ico_name}\n2. {description_url.get('description')}\n"
                          f"3. {description_url.get('link')}\n4. {json.dumps(team_members)}\n\t"
                          f"4a. {type(json.dumps(team_members))}\n5. {pre_ico}")

                conn.commit()

        conn.commit()
        conn.close()
        return True

class ScrapeAngel:

    def __init__(self):

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"}
        # initialize and setup the database.
        self.DB_PATH = "./ico_data.db"
        with open('./setup.sql', 'r') as f:
            setup_sql = f.read()
        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()
        cur.executescript(setup_sql)
        conn.commit()
        conn.close()

    def fuzz_urls(self, ico_name):

        base_url = "https://angel.co/"
        urls = []
        formatted_ico_name = ico_name.lower()
        if "." in formatted_ico_name:

            # Using GTLDs
            hyphen_ico = formatted_ico_name.replace('.', '-')
            urls.append(base_url + hyphen_ico) # Replace periods with hyphens. Example: Liquidity.Network becomes liquidity-network
            urls.append(base_url + hyphen_ico + "-1")

            # Strip GTLDs
            split_name = formatted_ico_name.split('.')
            urls.append(base_url + split_name[0])
            urls.append(base_url + split_name[0] + "-1") # check if there's more than one Page for the ICO.

        else:

            urls.append(base_url + formatted_ico_name + '-1') # check if there's more than one Page for the ICO.

        return urls

    def scrape_person(self, member_url, ico_name, founder_flag):

        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()
        req = requests.get(url=member_url, headers=self.headers)
        if req.status_code == 503:
            print("[!] We are being rate limited. Retrying...")
            raise ScapeException()
        elif req.status_code != 200:
            print("[!] Non 200 status code.")
            raise ScapeException()

        print(f"[-] Parsing member information at {member_url}")
        html = req.content
        #print(req.content.decode('utf-8'))
        bs = BeautifulSoup(html, features="html5lib")

        captcha_detect = bs.find("textarea", {"id": "g-recaptcha-response"})
        if captcha_detect:
            print(f"[!] Detected CAPTCHA for {member_url}. Retrying...")
            raise ScapeException()

        tags = bs.find("div", {"class": "subheader-tags"})
        socials = bs.find("div", {"class": "darkest dps64 profiles-show fls45 links _a _jm"})

        try:
            name = bs.find("h1", {"class": "u-fontSize25 u-fontSize24SmOnly u-fontWeight500"}).text
        except AttributeError as err:
            print(f'[!] Error: {err}\nCouldnt find name for some reason. Search: {req.content.decode("utf8")}')
        if name is None:
            print(f"[!] Couldn't find member name. Retrying...")
            raise ScapeException()
        profile_pic_url = bs.find("img", {"class": "js-avatar-img"})['src'] # Get the URL of the profile pic.
        experience = bs.find("div", {"class": "experience s-grid0"})
        education = bs.find("div", {"class": "education s-grid0"})
        about = bs.find("div", {"class": "about s-grid0"})
        user_id_number = bs.find("div", {"class": "dps64 profiles-show fhr17 header _a _jm"})['data-user_id']
        print(f"[+] Got a user number: {user_id_number}")
        investments = bs.find("div", {"class": "investments s-grid0"})

        if profile_pic_url:

            r = requests.get(url=profile_pic_url, headers=self.headers)
            if r.status_code == 200:
                r.raw.decode_content = True
                profile_pic = sqlite3.Binary(r.content) # store the binary image
                if profile_pic is None:
                    print("[@] WARNING: Unable to download profile pic...")
            else:
                print(f"[-] Encountered a non-200 status code at {member_url}. Retrying...")
            r.close()

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
                print(f"[@] WARNING: Could not find a location for {member_url}. Check logs.")

        if socials:
            spans = socials.find("div", {"class": "link"})
            # Parse the social media profiles out of the spans.
            if spans is not None:
                linkedin_url = socials.find("a", {"data-field": "linkedin_url"}, href=True)['href']
                twitter_url = socials.find("a", {"data-field": "twitter_url"}, href=True)['href']
                facebook_url = socials.find("a", {"data-field": "facebook_url"}, href=True)['href']
                github_url = socials.find("a", {"data-field": "github_url"}, href=True)['href']
                dribble_url = socials.find("a", {"data-field": "dribbble_url"}, href=True)['href']
                behance_url = socials.find("a", {"data-field": "behance_url"}, href=True)['href']
                blog_url = socials.find("a", {"data-field": "blog_url"}, href=True)['href']
                personal_url = socials.find("a", {"data-field": "online_bio_url"}, href=True)['href']
            else:
                # Fugly as shit but set all these to NONE.
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

        if education:
            print(f"[-] Found education.")
            member_education = {}
            schools = education.find_all("div", {"class": "college-row-view"})
            for school in schools:

                university = school.find("span").text
                major = school.find("div", {"class": "degree u-colorGray3"})
                print(f"[-] University: {university}, Major: {major}")
                member_education[university] = major

            member_education = json.dumps(member_education)

        if about:

            skills = about.find("div", {"class": "tags_skills"})
            if skills:
                member_skills = []
                for skill in skills.find_all("a", href=True):
                    member_skills.append(skill.text)
                member_skills = ', '.join(member_skills) # convert this to a string.
            else:
                member_skills = None

        if investments:
            print(f"[-] Found investments")
            investment_api_url = f"https://angel.co/startup_roles/investments?user_id={user_id_number}"
            req = requests.get(investment_api_url, headers=self.headers)
            if req.status_code == 200:
                # Serialize the results to a string
                member_investments = json.loads(req.content)
                member_investments = json.dumps(member_investments)
            else:
                print(f"[@] WARNING: Could not get investments for {member_url}")
        # Fugly beast of an insert statement..
        try:
            cur.execute("INSERT INTO investor_data (member_name, ico_name, origin_url, profile_pic, member_title, "
                        "member_tags, soc_linkedin, soc_twitter, soc_facebook, soc_github, soc_dribble, soc_behance,"
                        "soc_blog, soc_personal_site, member_experience, member_location, member_education, member_skills,"
                        "member_investments, is_founder) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
                        member_education,
                        member_skills,
                        member_investments,
                        founder_flag))

        except sqlite3.IntegrityError as err:
            print("[-] Already have data for this member.")

        conn.commit()
        conn.close()

        return True

    #@retry(wait=wait_random(15, 30), retry=retry_if_exception_type(ScapeException))
    def scrape_company(self, ico_name):

        # Fuzz urls for better accuracy.
        urls = self.fuzz_urls(ico_name)
        for url in urls:
            req = requests.get(url, headers=self.headers)
            if req.status_code == 404:
                pass # ignore 404s.
            elif req.status_code == 503:
                print("[!] We are being rate limited. Retrying...")
                raise ScapeException()
            else:
                html = req.content
                bs = BeautifulSoup(html, features="html5lib")

                founders = bs.find("div", {"class": "founders_section"})
                investors = bs.find("div", {"class": "past_financing section"})
                team_members = bs.find("div", {"class": "section team"})

                current_founders = founders.find_all("div", {"data-role": "founder"})
                past_founders = founders.find_all("div", {"data-role": "past_founder"})

                for founder in current_founders:
                    # TODO: Initiate the profile scrape
                    pass



if __name__ == "__main__":

    #sico = ScrapeIcoBench()
    sico = ScrapeAngel()
    p = Pool()

    #p.map(sico.scrape_icobench, range(1,438))

    sico.scrape_person("https://angel.co/yuan-li-24", '0xcert', False)
    #sico.scrape_icobench(1)
    #results = sico.scrape_description("https://icobench.com/ico/hygh")
    #results = sico.scrape_financials("https://icobench.com/ico/cryptocoin-insurance")
    #print(results)