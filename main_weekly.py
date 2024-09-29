from etf import *
from utils import *
from postgresql_helper import *
from postgresql_queries import *
from datetime import datetime

# path setting for Cron: 다음 명령어로 체크 print(os.path.abspath('.'))
os.chdir('/Users/chungdongwook/dongwook-src/stock-db-builder-2024')

# [1] Holdings
# [2] Info
# [3] Merge ETF Master
# [4] Concat Symbol Master
# [5] Load Files to DB

if __name__ == '__main__':

    # Holdings
    collect_etf_holdings()
    concatenated_etf_holdings = concat_csv_files_in_dir(get_dirpath=os.path.join("downloads", "etf_holdings"))
    concatenated_etf_holdings.to_csv(os.path.join("downloads", "etf_holdings.csv"), index=False)


    # Info
    collect_etf_infos()
    concatenated_etf_infos = concat_csv_files_in_dir(get_dirpath=os.path.join("downloads", "etf_info"))
    concatenated_etf_infos.to_csv(os.path.join("downloads", "etf_infos.csv"), index=False)


    # Merge ETF Master (Info를 수집했기 Master를 다시 만들어줘야 함)
    etf_list = pd.read_csv("./downloads/etf_list/etf_list_240605_union.csv")
    etf_profile = pd.read_csv('./downloads/etf_profiles.csv')
    etf_info = pd.read_csv('./downloads/etf_infos.csv')

    etf_list.rename(columns={"fund_name": "name"}, inplace=True)
    etf_list['domain'] = 'ETF'
    cols = ['domain', 'symbol', 'name']
    etf_list = etf_list[cols]

    etf_master = etf_list.merge(etf_info, how='left', on='symbol')
    etf_master = etf_master.merge(etf_profile, how='left', on='symbol')
    etf_master.to_csv('./downloads/master/etf_master.csv', index=False)


    # Concat Symbol Master
    etf_master = pd.read_csv('./downloads/master/etf_master.csv')
    currency_master = pd.read_csv('./downloads/master/currency_master.csv')
    index_master = pd.read_csv('./downloads/master/index_master.csv')

    symbol_master = pd.concat([etf_master, currency_master, index_master]).reset_index(drop=True)
    symbol_master.to_csv('./downloads/master/symbol_master.csv', index=False)


    # LOAD FILES TO DB

    # COPY FILE TO DB: DW_L0_SYMBOL_MASTER
    copy_csv_file_to_db(
        csv_file_path = os.path.join("downloads", "master", "symbol_master.csv"),
        table_name = 'DW_L0_SYMBOL_MST',
        create_table_query = CREATE_TABLE_DW_L0_SYMBOL_MST
    )

    # COPY FILE TO DB: DW_L1_HOLDINGS
    copy_csv_file_to_db(
        csv_file_path = os.path.join("downloads", "etf_holdings.csv"),
        table_name = 'DW_L1_HOLDINGS',
        create_table_query = CREATE_TABLE_DW_L1_HOLDINGS
    )

    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]
    print(f"Done: {end_time}")