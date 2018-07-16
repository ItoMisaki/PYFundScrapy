from urllib.request import urlopen
from bs4 import BeautifulSoup

url = "http://fund.eastmoney.com/js/fundcode_search.js"

if __name__ == "__main__":

    html = urlopen(url)

    all_fund_txt = html.readlines()