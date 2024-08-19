import pandas as pd
import investpy
import financedatabase as fd
import constants

# Sources:
# 1. investpy
# 2. fd (Finance Database)
# 3. Yahoo Finance
# 4. FRED (Federal Reserve Economic Data)

def get_indices_masters_investpy() -> pd.DataFrame:
    # symbol 문자열 앞에 ^를 붙여야 함
    countries = ['united states', 'south korea']
    indices = investpy.get_indices()
    indices = indices[indices['country'].isin(countries)].reset_index(drop=True)
    indices['symbol'] = '^' + indices['symbol']
    indices['domain'] = 'INDEX'
    indices = indices[['domain', 'symbol', 'full_name']]
    indices = indices.rename(columns={'full_name': 'name'})
    return indices


def get_indices_masters_fd() -> pd.DataFrame:
    # 미국은 5만개라서 한국만 수집
    indices = fd.Indices().select() 
    indices = indices[indices['market'] == 'kr_market'].reset_index()[['symbol', 'name']]
    indices['domain'] = 'INDEX'
    indices = indices[['domain', 'symbol', 'name']]
    return indices


def get_indices_masters_yahoo() -> pd.DataFrame:
    world_indices = constants.Indices.YAHOO_WORLD_INDICES
    world_indices = pd.DataFrame(world_indices)

    comm_indices = constants.Indices.YAHOO_COMMODITIES
    comm_indices = pd.DataFrame(comm_indices)

    indices = pd.concat([world_indices,comm_indices]).reset_index()
    indices['domain'] = 'INDEX'
    indices = indices[['domain', 'symbol', 'name']]
    return indices


def get_indices_masters_fred() -> pd.DataFrame:
    indices = constants.Indices.FRED
    indices = pd.DataFrame(indices)
    indices['domain'] = 'INDEX'
    indices = indices[['domain', 'symbol', 'name']]
    return indices


def concat_indices_masters():
    # 중복제거 필요
    indices_masters_investpy = pd.read_csv('./downloads/index_master/index_master_investpy.csv')
    indices_masters_fd = pd.read_csv('./downloads/index_master/index_master_fd.csv')
    indices_masters_yahoo = pd.read_csv('./downloads/index_master/index_master_yahoo.csv')
    indices_masters_fred = pd.read_csv('./downloads/index_master/index_master_fred.csv')
    indices_masters = pd.concat([
        indices_masters_investpy,
        indices_masters_fd,
        indices_masters_yahoo,
        indices_masters_fred
    ]).reset_index(drop=True)
    indices_masters = indices_masters.drop_duplicates(subset='symbol')
    
    header = pd.DataFrame(columns=['domain', 'symbol', 'name'])
    indices_masters = pd.concat([header, indices_masters])
    indices_masters.to_csv('./downloads/index_master.csv', index=False)
