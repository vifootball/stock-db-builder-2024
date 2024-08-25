import os
import pandas as pd
import datetime as dt
from tqdm import tqdm
import warnings


pd.options.mode.chained_assignment = None

def calculate_grade(master_symbol, history_symbol, history_market, unit_period='all_time'):
    """unit_priod에 맞는 grade 생성"""
    history_symbol['date'] = pd.to_datetime(history_symbol['date'])
    history_market['date'] = pd.to_datetime(history_market['date'])

    # unit_period에 따른 start_date 계산
    # 가능한것보다 더 긴 시점을 선택하면 all_time과 결과 똑같음
    if unit_period == 'all_time':
        start_date = history_symbol['date'].min()
    elif unit_period == '5year':
        start_date = history_symbol['date'].max() - pd.DateOffset(years=5)
    elif unit_period == '10year':
        start_date = history_symbol['date'].max() - pd.DateOffset(years=10)
    elif unit_period == '15year':
        start_date = history_symbol['date'].max() - pd.DateOffset(years=15)
    else:
        raise ValueError("unit_period must be one of ['all_time', '5year', '10year', '15year']")

    # 지정된 기간으로 데이터 필터링
    history_symbol = history_symbol[history_symbol['date'] >= start_date]
    end_date = history_symbol['date'].max()

    # first_date = history_symbol['date'].min()
    # last_date = history_symbol['date'].max()

    df = {}
    df['symbol'] = history_symbol['symbol'].iloc[0]
    df['start_date'] = start_date
    df['end_date'] = end_date
    # df['last_update'] = end_date.strftime("%Y-%m-%d")
    df['unit_period'] = unit_period

    df['fund_age_day'] = (dt.datetime.today() - pd.to_datetime(master_symbol['inception_date'])).dt.days.squeeze()
    df['fund_age_year'] = df['fund_age_day'] / 365.25
    df['expense_ratio'] = master_symbol['expense_ratio'].squeeze()
    df['nav'] = history_symbol.loc[history_symbol['date'] == end_date]['price'].squeeze()
    df['shares_outstanding'] = master_symbol['shares_outstanding'].squeeze()
    # df['aum'] = (df['nav'] * df['shares_outstanding']).squeeze() # 마스터 매일 최신화 할 수 없으면 이걸로 하기
    df['aum'] = master_symbol['aum'].squeeze() # 마스터도 매일 최신화 할 수 있으며 이걸로 하기
    df['total_return' ] = (history_symbol['price'].loc[history_symbol['date'] == end_date].squeeze() / history_symbol['price'].loc[history_symbol['date'] == start_date].squeeze()) - 1
    df['cagr'] = (1 + df['total_return']) ** (1 / ((end_date - start_date).days / 365.25) ) - 1
    df['std_yearly_return'] = history_symbol.set_index('date')['price'].resample('YE').last().pct_change().std() # 올해는 최근일 기준
    df['drawdown_max'] = ((history_symbol['price'] / history_symbol['price'].cummax()) - 1).min()
    df['div_ttm'] = history_symbol['dividend_ttm'].loc[history_symbol['date'] == end_date].squeeze()
    df['div_yield_ttm'] = history_symbol['dividend_rate_ttm'].loc[history_symbol['date'] == end_date].squeeze()
    df['div_paid_cnt_ttm'] = history_symbol['dividend_paid_count_ttm'].loc[history_symbol['date'] == end_date].squeeze()

    # 일단위
    df['mkt_corr_daily'] = history_symbol.set_index('date')['price'].pct_change().corr(history_market.set_index('date')['price'].pct_change())
    # Week-Fridays(주단위 금요일) 기준으로 샘플링하고 해당 기간 내 마지막 값(금요일)을 가져옴
    df['mkt_corr_weekly'] = history_symbol.set_index('date')['price'].resample('W-FRI').last().pct_change().corr(history_market.set_index('date')['price'].resample('W-FRI').last().pct_change())
    # Month-End(월단위 말일) 기준으로 샘플링하고 해당 기간 내 마지막 값(월 말일)을 가져옴
    df['mkt_corr_monthly'] = history_symbol.set_index('date')['price'].resample('ME').last().pct_change().corr(history_market.set_index('date')['price'].resample('ME').last().pct_change())
    # Year-End(연단위 말일) 기준으로 샘플링하고 해당 기간 내 마지막 값(연 말일)을 가져옴
    df['mkt_corr_yearly'] = history_symbol.set_index('date')['price'].resample('YE').last().pct_change().corr(history_market.set_index('date')['price'].resample('YE').last().pct_change())
    df['volume_dollar_3m_avg'] = history_symbol['volume_of_dollar_3m_avg'].ffill().loc[history_symbol['date'] == end_date].squeeze()

    # df=pd.DataFrame(df, index=[0])
    df=pd.DataFrame([df])
    return df


def generate_grades_by_period(master_symbol, history_symbol, history_market):
    """다양한 기간에 대한 summary grade 생성."""
    
    inception_date = pd.to_datetime(master_symbol['inception_date']).squeeze()
    current_date = pd.to_datetime(history_symbol['date'].max())
    
    # 사용 가능한 unit_periods 동적으로 계산
    years_elapsed = (current_date - inception_date).days / 365.25
    unit_periods = ['all_time']
    
    if years_elapsed > 5:
        unit_periods.append('5year')
    if years_elapsed > 10:
        unit_periods.append('10year')
    if years_elapsed > 15:
        unit_periods.append('15year')
    
    grades = [calculate_grade(master_symbol, history_symbol, history_market, unit_period=period) for period in unit_periods]
    
    return pd.concat(grades, ignore_index=True)


def pivot_grades(grades):
    dims = [
        'symbol',
        'start_date',
        'end_date',
        'unit_period',
    ]
    all_cols = grades.columns.to_list()
    # measures = list(set(all_cols) - set(dims))
    
    grades_pivotted = pd.melt(grades, id_vars=dims, var_name='var_name').sort_values(by=dims).reset_index(drop=True)
    return grades_pivotted


def collect_grades():
    warnings.filterwarnings('ignore', category=RuntimeWarning)

    # (1) 고정 데이터 로드
    # (2) History 데이터 경로 설정
    # (3) 반복문으로 History에 대한 Grade 수집
    # (4) 수집한 Grades를 Concat & 저장
    # (5) Concat한 Grades를 Pivot & 저장

    # (1) 고정 데이터 로드
    masterdata = pd.read_csv("./downloads/etf_masters.csv")
    history_market = pd.read_csv("./downloads/history/etf/SPY_history.csv")

    # (2) History 데이터 경로 설정
    dirpath = './downloads/history/etf'
    fname_list = sorted(os.listdir('./downloads/history/etf'))

    # (3) 반복문으로 History에 대한 Grade 수집
    print("[Collect Grades] [1/3] Collecting Grades: Processing")
    grades = []
    for fname in tqdm(fname_list[:], mininterval=0.5):
        fpath = os.path.join(dirpath, fname)
        
        history_symbol = pd.read_csv(fpath)
        symbol = history_symbol['symbol'][0]
        master_symbol = masterdata[masterdata['symbol']==symbol]
        df = generate_grades_by_period(master_symbol=master_symbol, history_symbol=history_symbol, history_market=history_market)
        grades.append(df)
    print("[Collect Grades] [1/3] Collecting Grades: Completed")

    # (4) 수집한 Grades를 Concat & 저장
    print("[Collect Grades] [2/3] Concatenating Grades: Processing")
    grades = pd.concat(grades).reset_index(drop=True)
    grades.to_csv('./downloads/etf_grades.csv', index=False)
    print("[Collect Grades] [2/3] Concatenating Grades: Completed & Saved")

    # (5) Concat한 Grades를 Pivot & 저장
    print("[Collect Grades] [3/3] Pivotting Grades: Processing")
    grades_pivotted = pivot_grades(grades)
    grades_pivotted.to_csv('./downloads/etf_grades_pivotted.csv', index=False)
    print("[Collect Grades] [3/3] Pivotting Grades: Completed & Saved")


if __name__ == '__main__':
    collect_grades()