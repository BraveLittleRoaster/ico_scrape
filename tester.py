import sqlite3
import requests


def fetch_proxies():
    DB_PATH = "./ico_data.db"
    with open('./setup.sql', 'r') as f:
        setup_sql = f.read()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executescript(setup_sql)
    conn.commit()
    conn.close()

    proxy_uname = "robert.paul6@gmail.com"
    proxy_pass = "rYH14XxbN7fmJdagXasE"
    proxy_api = f"http://list.didsoft.com/get?email={proxy_uname}&pass={proxy_pass}&pid=sockspremium&version=socks4"

    conn = sqlite3.connect('./ico_data.db')
    cur = conn.cursor()

    req = requests.get(proxy_api)

    with open('./proxy.cache', 'wb') as f:
        f.write(req.content)

    with open('./proxy.cache', 'r') as f:
        proxies = f.readlines()

    for proxy in proxies:
        prox = proxy.split("#")
        try:
            cur.execute('INSERT INTO proxies (proxy, proxy_type, country) VALUES (?,?,?);',
                        (prox[0], prox[1], prox[2].rstrip('\n')))
        except sqlite3.IntegrityError:
            print(f"[-] Already have proxy {prox[0]} added.")
        conn.commit()
    conn.close()

#fetch_proxies()

conn = sqlite3.connect('./ico_data.db')
cur = conn.cursor()
cur.execute("SELECT member_name FROM investor_data;")
rows = cur.fetchall()

for row in rows:

    name = row[0]
    name = name.replace('\n', '')

    cur.execute("UPDATE investor_data SET member_name = ? WHERE member_name = ?;", (name, row[0]))
    conn.commit()

conn.commit()
conn.close()
