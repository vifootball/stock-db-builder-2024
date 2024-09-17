# DW_L0_SYMBOL_MST
# --
# DW_L1_HOLDINGS
# DW_L1_HISTORY
# --
# DM_GRADE
# DM_GRADE_PIVOT
# DM_CORRELATION


CREATE_TABLE_DW_L0_SYMBOL_MST = f"""
CREATE TABLE IF NOT EXISTS {"DW_L0_SYMBOL_MST"} (

    domain TEXT,
    symbol TEXT,
    name TEXT,
    aum REAL,
    nav REAL,

    expense_ratio REAL,
    shares_outstanding REAL,
    dividend_yield REAL,
    inception_date DATE,
    description TEXT,
    
    holdings_count REAL,
    holdings_top10_percentage REAL,
    holdings_date DATE,
    asset_class TEXT,
    category TEXT,

    region TEXT,
    stock_exchange TEXT,
    fund_family TEXT,
    index_tracked TEXT
);
"""

CREATE_TABLE_DW_L1_HOLDINGS = f"""
CREATE TABLE IF NOT EXISTS {"DW_L1_HOLDINGS"} (

    symbol TEXT,
    as_of_date DATE,
    no REAL,
    holding_symbol TEXT,
    name TEXT,

    weight REAL,
    shares REAL
);
"""

CREATE_TABLE_DW_L1_HISTORY = f"""
CREATE TABLE IF NOT EXISTS {"DW_L1_HISTORY"} (

    -- TODO: indexing, partitioning (?)

    date DATE,
    symbol TEXT,
    open REAL,
    high REAL,
    low REAL,

    close REAL,
    volume REAL,
    dividend REAL,
    stock_split REAL,
    price REAL,
    
    price_all_time_high REAL,
    drawdown_current REAL,
    drawdown_max REAL,
    volume_of_share REAL,
    volume_of_share_3m_avg REAL,
    
    volume_of_dollar REAL,
    volume_of_dollar_3m_avg REAL,
    dividend_paid_or_not REAL,
    dividend_paid_count_ttm REAL,
    dividend_ttm REAL,
    
    dividend_rate REAL,
    dividend_rate_ttm REAL,
    normal_day_tf REAL,
    date_id INT,
    transaction_id TEXT,
    
    price_change REAL,
    price_change_rate REAL,
    price_change_sign REAL,
    price_7d_ago REAL,
    weekly_price_change REAL,

    weekly_price_change_rate REAL,
    price_30d_ago REAL,
    monthly_price_change REAL,
    monthly_price_change_rate REAL,
    price_365d_ago REAL,

    yearly_price_change REAL,
    yearly_price_change_rate REAL
);
"""

CREATE_TABLE_DM_GRADE = f"""
CREATE TABLE IF NOT EXISTS {"DM_GRADE"} (

    symbol TEXT,
    start_date DATE,
    end_date DATE,
    unit_period TEXT,
    fund_age_day REAL,

    fund_age_year REAL,
    expense_ratio REAL,
    nav REAL,
    shares_outstanding REAL,
    aum REAL,

    total_return REAL,
    cagr REAL,
    std_yearly_return REAL,
    drawdown_max REAL,
    div_ttm REAL,

    div_yield_ttm REAL,
    div_paid_cnt_ttm REAL,
    mkt_corr_daily REAL,
    mkt_corr_weekly REAL,
    mkt_corr_monthly REAL,

    mkt_corr_yearly REAL,
    volume_dollar_3m_avg REAL
);
"""

CREATE_TABLE_DM_GRADE_PIVOT = f"""
CREATE TABLE IF NOT EXISTS {"DM_GRADE_PIVOT"} (

    symbol TEXT,
    start_date DATE,
    end_date DATE,
    unit_period TEXT,
    var_name TEXT,

    value REAL
);
"""

CREATE_TABLE_DM_CORRELATION = f"""
CREATE TABLE IF NOT EXISTS {"DM_CORRELATION"} (
    -- pk 걸어줘야하나

    unit_period TEXT,
    symbol TEXT,
    target_symbol TEXT,
    start_date DATE,
    end_date DATE,
    
    corr_yearly REAL,
    corr_monthly REAL,
    corr_weekly REAL,
    corr_daily REAL,
    rank_yearly_asc REAL,
    
    rank_yearly_desc REAL,
    rank_monthly_asc REAL,
    rank_monthly_desc REAL,
    rank_weekly_asc REAL,
    rank_weekly_desc REAL,

    rank_daily_asc REAL,
    rank_daily_desc REAL
);
"""