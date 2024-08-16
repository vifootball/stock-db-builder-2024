
def get_correlation():
    def calculate_corr(base_history, target_history, unit_period='all_time'):
    
    if unit_period == 'all_time':
        pass
    elif unit_period == '5year':
        year_5_ago = pd.to_datetime(history_symbol['date']).max() - pd.DateOffset(years=5)
        base_history = base_history[pd.to_datetime(base_history['date']) >= year_5_ago].reset_index(drop=True)
        target_history = target_history[pd.to_datetime(target_history['date']) >= year_5_ago].reset_index(drop=True)
    # elif unit_period == '10year':
    #     start_date = history_symbol['date'].max() - pd.DateOffset(years=10)
    # elif unit_period == '15year':
    #     start_date = history_symbol['date'].max() - pd.DateOffset(years=15)
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


def get_available_correlations():
    pass