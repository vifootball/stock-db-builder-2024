import pandas as pd
import investpy

def get_currency_profile() -> pd.DataFrame:
    currency = investpy.currency_crosses.get_currency_crosses()
    base_cur = ['KRW', 'USD']
    currency = currency[currency['base'].isin(base_cur)].reset_index(drop=True)
    def _encode_symbol(name):
        base_cur, second_cur = name.split('/')
        symbol = f'{second_cur}=X' if base_cur == 'USD' else f'{base_cur}{second_cur}=X'
        return symbol
    currency['symbol'] = currency['name'].apply(_encode_symbol)
    currency = currency.rename(columns={"full_name": "description"})
    currency['type'] = 'CURRENCY'
    currency = currency[['type', 'symbol', 'name', 'description']]

    return currency

def get_currencies() -> list:
    currency_profile = get_currency_profile()
    currency_list = currency_profile['symbol'].to_list()
    return currency_list
