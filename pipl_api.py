import requests
import json

def whitepage_search(phone_num):

    API_KEY = "350e31dd720147b295329204c380c094"
    #url = f"https://proapi.whitepages.com/3.0/person?api_key={API_KEY}&name={f_name}+{l_name}"
    url = f"https://proapi.whitepages.com/3.0/phone?phone={phone_num}&api_key={API_KEY}"

    req = requests.get(url)
    resp = json.loads(req.content, encoding='utf8')

    print(json.dumps(resp))

def pipl_search(phone):
    #API_KEY = "fekmqg6rtiwqu7w3iuq79ci3" # Inexpensive contact deets.
    API_KEY = "sr192e01ilmihpf78yjgd5ds" # Expensive business

    url = f"http://api.pipl.com/search/?phone={phone}&key={API_KEY}"

    req = requests.get(url)
    resp = json.loads(req.content, encoding='utf8')

    print(json.dumps(resp))


if __name__ == "__main__":

    numbers = [
        "6463429165",
        "5086775120",
        "9499398198",
        "3392234507",
        "2024383031",
        "9155408444",
        "8016287389",
        "9413216054",
        "3054966656",
        "4109632923"
    ]
    """
    for phone_num in numbers:
        whitepage_search(phone_num)
    """

    with open('usa.json', 'r', encoding='utf8') as f:
        some_json = f.read()

    #for number in numbers:
    #    pipl_search(number)

    pipl_search("8567979169")