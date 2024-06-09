import time
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen, Request
from parse_str import *

def get_symbols() -> list:
    fname = "./etf_list/etf_list_240605_union.csv"
    etf_list = pd.read_csv(fname)
    symbols = sorted(etf_list['symbol'].to_list())
    return symbols

# 임시
def get_etf_attr1(symbol: str):
    time.sleep(1)

    date = pd.Timestamp.now().strftime("%Y%m%d")
    symbol = symbol.upper()
    aum = None
    expense_ratio = None
    shares_out = None
    dividend_ttm = None
    dividend_yield = None
    holdings = None

    url = Request(f"https://stockanalysis.com/etf/{symbol}/", headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(url)
    bs_obj = bs(html, "html.parser")
    trs = bs_obj.find_all('tr')

    for tr in (trs):
        tds = tr.find_all('td')
        if len(tds) > 0 :
            if tds[0].get_text() == "Assets":
                aum = tr.find_all('td')[1].get_text().replace("$", "")

            elif tds[0].get_text() == "Expense Ratio":
                expense_ratio = tr.find_all('td')[1].get_text()

            elif tds[0].get_text() == "Shares Out":
                shares_out = tr.find_all('td')[1].get_text()

            elif tds[0].get_text() == "Dividend (ttm)":
                dividend_ttm = tr.find_all('td')[1].get_text().replace("$", "")

            elif tds[0].get_text() == "Dividend Yield":
                dividend_yield = tr.find_all('td')[1].get_text()

            elif tds[0].get_text() == "Holdings":
                holdings = tr.find_all('td')[1].get_text()

    data = {
        'date': date,
        'symbol': symbol, 
        'aum': aum, 
        'expense_ratio': expense_ratio,
        'shares_out': shares_out,
        'dividend_ttm': dividend_ttm,
        'dividend_yield': dividend_yield,
        'holdings': holdings
    }
    df = pd.DataFrame([data])
    df['aum'] = df['aum'].apply(str_to_int)
    df['expense_ratio'] = df['expense_ratio'].apply(percentage_to_float)
    df['shares_out'] = df['shares_out'].apply(str_to_int)    
    df['dividend_yield'] = df['dividend_yield'].apply(percentage_to_float)
    
    return df


def get_etf_profile(symbol: str):
    # API URL
    url = f"https://api.stockanalysis.com/api/symbol/e/{symbol}/overview"

    # API 요청 보내기
    response = requests.get(url)
    response = response.json()

    etf_data = response.get("data", {})
    data = etf_data.get("infoTable")
    if not etf_data:
        print(f"Empty Response: {symbol}")
        return None
    
    data_dict = {item[0]: item[1] for item in data}
    df = pd.DataFrame([data_dict])
    
    # 컬럼명 수정
    df.rename(columns={
        "Asset Class": "asset_class",
        "Category": "category",
        "Region": "region",
        "Stock Exchange": "stock_exchange",
        "Ticker Symbol": "symbol",
        "Provider": "fund_family",
        "Index Tracked": "index_tracked"
    }, inplace=True)

    # 결측치 삽입
    if "asset_class" not in df.columns:
        df["asset_class"] = "n/a"
    if "category" not in df.columns:
        df["category"] = "n/a"
    if "region" not in df.columns:
        df["region"] = "n/a"
    if "index_tracked" not in df.columns:
        df["index_tracked"] = "n/a"
    if "fund_family" not in df.columns:
        df["fund_family"] = "n/a"

    # 컬럼 순서 정렬
    df = df[[
       "symbol", "asset_class", "category", "region", "stock_exchange", "fund_family", "index_tracked"
    ]]
    
    print(f"Successfully Processed: {symbol}")
    return df


def get_etf_info(symbol: str): #measures
    # API URL
    url = f"https://api.stockanalysis.com/api/symbol/e/{symbol}/overview"

    # API 요청 보내기
    response = requests.get(url)
    data = response.json()

    # ETF의 주요 정보 추출 및 출력
    etf_data = data.get("data", {})
    if not etf_data:
        print(f"Empty Response: {symbol}")
        return None

    aum = str_to_int(etf_data.get("aum").replace("$", "")) if etf_data.get("aum") else None
    nav = etf_data.get("nav").replace("$", "") if etf_data.get("nav") else None
    expense_ratio = percentage_to_float(etf_data.get("expenseRatio"))
    shares_outstanding = str_to_int(etf_data.get("sharesOut"))
    dividend_yield = percentage_to_float(etf_data.get("dividendYield"))
    inception = datetime.strptime(etf_data.get("inception"), "%b %d, %Y").strftime("%Y-%m-%d") if etf_data.get("inception") else None
    description = etf_data.get("description")
    holdings_count = etf_data.get("holdings")

    if etf_data.get("holdingsTable"):
        holdings_top10_percentage = etf_data.get("holdingsTable").get("top10")
        holdings_date = datetime.strptime(etf_data.get("holdingsTable").get("updated"), "%b %d, %Y").strftime("%Y-%m-%d")
    else:
        holdings_top10_percentage = "n/a"
        holdings_date = "n/a"
        

    data_dict = {
        "symbol": symbol,
        "aum": aum,
        "nav": nav,
        "expense_ratio": expense_ratio,
        "shares_outstanding": shares_outstanding,
        "dividend_yield": dividend_yield,
        "inception": inception,
        "description": description,
        "holdings_count": holdings_count,
        "holdings_top10_percentage": holdings_top10_percentage,
        "holdings_date": holdings_date 
    }
    df = pd.DataFrame([data_dict])
    
    print(f"Successfully Processed: {symbol}")
    return df


def get_etf_holdings(symbol: str):
    # API URL
    url = f"https://api.stockanalysis.com/api/symbol/e/{symbol}/holdings"

    # API 요청 보내기
    response = requests.get(url)
    response = response.json()

    # ETF의 주요 정보 추출
    etf_data = response.get("data", {})
    holdings = etf_data.get('holdings')
    if not holdings:
        print(f"Empty Response: {symbol}")
        return None
    
    holdings = pd.DataFrame(holdings)
    holdings.rename(columns={
        "s": "symbol",
        "n": "name",
        "as": "weight",
        "sh": "shares"
    }, inplace=True)

    if "symbol" not in holdings.columns: # symbol 정보가 딕셔너리에 없는 경우 존재
        holdings["symbol"] = "n/a"
    if "shares" not in holdings.columns: # symbol 정보가 딕셔너리에 없는 경우 존재
        holdings["shares"] = "n/a"  

    holdings['symbol'] = holdings['symbol'].str.replace(r'^[$#]', '', regex=True) # 종목 명에 특수문자 들어감 (ETF와 개별주 구분자인듯)
    holdings['weight'] = holdings['weight'].str.replace(',', '').apply(percentage_to_float)

    date_str = etf_data.get('date')
    date = datetime.strptime(date_str, "%b %d, %Y").strftime("%Y-%m-%d") if date_str is not None else None
    holdings['as_of_date'] = date

    holdings = holdings[[
        "as_of_date", "no", "symbol", "name", "weight", "shares"
    ]]

    print(f"Successfully Processed: {symbol}")
    return holdings


# print(get_etf_info("saaa"))