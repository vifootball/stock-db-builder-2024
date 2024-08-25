
import os
import pandas as pd
from utils import *
from tqdm import tqdm
import ray
import time
from datetime import datetime
from constants import *


def get_corr(base_history, target_history, unit_period='all_time'):
    """unit_priod에 맞는 correlation 계산"""
    if unit_period == 'all_time':
        pass
    elif unit_period == '5year':
        year_5_ago = pd.to_datetime(base_history['date']).max() - pd.DateOffset(years=5)
        base_history = base_history[pd.to_datetime(base_history['date']) >= year_5_ago].reset_index(drop=True)
        target_history = target_history[pd.to_datetime(target_history['date']) >= year_5_ago].reset_index(drop=True)
    elif unit_period == '10year':
        year_10_ago = pd.to_datetime(base_history['date']).max() - pd.DateOffset(years=10)
        base_history = base_history[pd.to_datetime(base_history['date']) >= year_10_ago].reset_index(drop=True)
        target_history = target_history[pd.to_datetime(target_history['date']) >= year_10_ago].reset_index(drop=True)
    elif unit_period == '15year':
        year_15_ago = pd.to_datetime(base_history['date']).max() - pd.DateOffset(years=15)
        base_history = base_history[pd.to_datetime(base_history['date']) >= year_15_ago].reset_index(drop=True)
        target_history = target_history[pd.to_datetime(target_history['date']) >= year_15_ago].reset_index(drop=True)
    else:
        raise ValueError("unit_period must be one of ['all_time', '5year', '10year', '15year']")

    base_history['date'] = pd.to_datetime(base_history['date'])
    target_history['date'] = pd.to_datetime(target_history['date'])
    corr_data = {}
    corr_data['unit_period'] = unit_period
    corr_data['symbol'] = base_history['symbol'][0]
    corr_data['target_symbol'] = target_history['symbol'][0]
    corr_data['start_date'] = base_history['date'].min()
    corr_data['end_date'] = base_history['date'].max()
    corr_data['corr_yearly'] = base_history.set_index('date')['price'].resample('YE').last().pct_change().corr(target_history.set_index('date')['price'].resample('YE').last().pct_change())
    corr_data['corr_monthly'] = base_history.set_index('date')['price'].resample('ME').last().pct_change().corr(target_history.set_index('date')['price'].resample('ME').last().pct_change())
    corr_data['corr_weekly'] = base_history.set_index('date')['price'].resample('W').last().pct_change().corr(target_history.set_index('date')['price'].resample('W').last().pct_change())
    corr_data['corr_daily'] = base_history.set_index('date')['price'].resample('D').last().pct_change().corr(target_history.set_index('date')['price'].resample('D').last().pct_change())
    corr_data = pd.DataFrame([corr_data])
    
    return corr_data


def get_availaible_corrs(base_history, target_history):
    base_max_date = pd.to_datetime(base_history['date']).max()
    base_min_date = pd.to_datetime(base_history['date']).min()
    base_total_years = (base_max_date - base_min_date).days / 365.25   

    target_max_date = pd.to_datetime(target_history['date']).max()
    target_min_date = pd.to_datetime(target_history['date']).min()
    target_total_years = (target_max_date - target_min_date).days / 365.25

    corr_list = []
    # All time: Target의 기간이 더 길어야만 구할 수 있음
    if base_total_years <= target_total_years:
        corr = get_corr(base_history, target_history, unit_period='all_time')
        corr_list.append(corr)
    
    if base_total_years >= 15 and target_total_years >= 15:
        corr = get_corr(base_history, target_history, unit_period='15year')
        corr_list.append(corr)
    
    if base_total_years >= 10 and target_total_years >= 10:
        corr = get_corr(base_history, target_history, unit_period='10year')
        corr_list.append(corr)

    if base_total_years >= 5 and target_total_years >= 5:
        corr = get_corr(base_history, target_history, unit_period='5year')
        corr_list.append(corr)

    if len(corr_list) >= 1 :
        df = pd.concat(corr_list)
        # df['summary_corr_pk'] = df['symbol'] + "-" + df['target_symbol'] + "-`" + df['unit_period']
    # if len(df) >= 1:
        return df
    else:
        return None

def collect_corrs():
    # 진행상황 알 수 있는 프린트 문 작성하기

    # 히스토리 데이터 경로 설정
    dirpath = './downloads/history/etf'
    base_fname_list = sorted([x for x in os.listdir('./downloads/history/etf') if x.endswith('csv')])
    target_fname_list = sorted([x for x in os.listdir('./downloads/history/etf') if x.endswith('csv')])

    # base 마다 target과 의 상관관계를 계산하여 파일로 저장
    for base_fname in tqdm(base_fname_list[:], mininterval=0.5):
        time.sleep(0.3)
        corrs_list = []
        
        # Load base history
        base_fpath = os.path.join(dirpath, base_fname)
        base_history = pd.read_csv(base_fpath)
        base_symbol = base_history['symbol'][0]

        print(f'[Collect Correlations] [{base_symbol}] Calculating Correlations: Processing')
        # target을 하나씩 불러와서 base와 상관관계 계산
        for target_fname in tqdm(target_fname_list[:],mininterval=0.5):
            
            # Load target history
            target_fpath = os.path.join(dirpath, target_fname)
            target_history = pd.read_csv(target_fpath)

            corrs = get_availaible_corrs(base_history, target_history)
            corrs_list.append(corrs)
        
        corrs_list = pd.concat(corrs_list)
        
        # 상관관계 순위 매기기
        corrs_list['rank_yearly_asc'] = corrs_list.groupby('unit_period')['corr_yearly'].rank(method='max')
        corrs_list['rank_yearly_desc'] = corrs_list.groupby('unit_period')['corr_yearly'].rank(method='max', ascending=False)
        corrs_list['rank_monthly_asc'] = corrs_list.groupby('unit_period')['corr_monthly'].rank(method='max')
        corrs_list['rank_monthly_desc'] = corrs_list.groupby('unit_period')['corr_monthly'].rank(method='max', ascending=False)
        corrs_list['rank_weekly_asc'] = corrs_list.groupby('unit_period')['corr_weekly'].rank(method='max')
        corrs_list['rank_weekly_desc'] = corrs_list.groupby('unit_period')['corr_weekly'].rank(method='max', ascending=False)
        corrs_list['rank_daily_asc'] = corrs_list.groupby('unit_period')['corr_daily'].rank(method='max')
        corrs_list['rank_daily_desc'] = corrs_list.groupby('unit_period')['corr_daily'].rank(method='max', ascending=False)
        
        print(f'[Collect Correlations] [{base_symbol}] Calculating Correlations: Completed')

        # base에 대한 corrs 파일 저장
        os.makedirs('downloads/correlation/', exist_ok=True)
        corrs_list.to_csv(f'downloads/correlation/{base_symbol}_correlations.csv', index=False)
        


    