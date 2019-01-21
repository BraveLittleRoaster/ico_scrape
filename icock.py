#!/usr/bin/env python3
from argparse import ArgumentParser
from bs4 import BeautifulSoup
from collections import namedtuple
from multiprocessing.dummy import Pool as ThreadPool
import json
import requests

BASE_URL = "https://icobench.com"

Team = namedtuple("Team", ["name", "link", "team"])
Member = namedtuple("Member", ["name", "title", "links"])

def tryget(link, num_retries=10):
    for _ in range(num_retries):
        resp = requests.get(link)
        if resp.status_code // 100 == 2:
            return resp
    return None

def populate_teams(teams, workers=0, verbose=False):
    pool = ThreadPool(workers or len(teams))

    def get_team_members(team):
        resp = tryget(team.link)
        if resp is None:
            print("[!] Unable to retrieve data for team {}".format(team.name))
            return

        soup = BeautifulSoup(resp.text, "html.parser")
        for c3 in soup.find_all("div", {"class": "col_3"}):
            name = c3.find("h3")
            title = c3.find("h4")

            if not (name or title):
                continue

            links = {}
            socials = c3.find("div", {"class": "socials"})
            if socials:
                for social in socials.find_all("a"):
                    links[social.text] = social["href"]

            team.team.append(Member(
                name=name.text if name else None,
                title=title.text if title else None,
                links=links,
            ))

        if verbose and team and team.team:
            print("[+] " + team.name + ": " + ", ".join(mem.name for mem in team.team if mem and mem.name) + "\n")

    pool.map(get_team_members, teams)


def parse_ico_list(text):
    teams = list()
    soup = BeautifulSoup(text, "html.parser")
    lst = soup.find("div", {"class": "ico_list"})
    for link in lst.find_all("a", {"class": "name", "href": lambda l: l.startswith("/ico/")}):
        teams.append(Team(
            name=link.text.strip("\xa0"),
            link=BASE_URL+link["href"],
            team=list(),
        ))
    return teams

def main():
    ap = ArgumentParser()
    ap.add_argument("--start", default=1, type=int, help="The starting page (default: 1)")
    ap.add_argument("--end", default=438, type=int, help="The ending page (default: 438)")
    ap.add_argument("--out", default="output.json", type=str, help="Output file format")
    ap.add_argument("--workers", default=5, type=int, help="Size of the thread pool to use.")
    ap.add_argument("--country", default=None, type=str, help="The country to filter by (ex: usa)")
    ap.add_argument("--verbose", action="store_true", help="Verbose Output (simplified for stdout)")

    args = ap.parse_args()
    fmt = BASE_URL + "/icos?page={}" + ((args.country and "&filterCountry={}".format(args.country)) or "")

    for i in range(args.start, args.end+1):
        url = fmt.format(i)
        resp = tryget(url)
        if resp is None:
            print("[!] FAILED Getting members on page {}".format(i))
            continue
        teams = parse_ico_list(resp.text)
        populate_teams(teams, workers=args.workers, verbose=args.verbose)

        for team in teams:
            with open(args.out, "a+") as fout:
                print(json.dumps(team, indent=2), file=fout)

        print("[!] SUCCESS scraped teams on page {}/{}".format(i, args.end))

if __name__ == "__main__":
    main()
