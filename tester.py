import sqlite3
import requests

headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"}
r = requests.get("https://d1qb2nb5cznatu.cloudfront.net/users/7737477-large?1523467707", headers=headers)
r.raw.decode_content = True
jpg = r.content
print(sqlite3.Binary(jpg))