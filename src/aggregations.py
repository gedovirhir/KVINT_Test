from typing import Optional
import time

from pandas import DataFrame

def cnt_all_attempts(df: DataFrame) -> int:
    return len(df)

def cnt_att_dur(df: DataFrame, sec_start: Optional[int] = None, sec_end: Optional[int] = None) -> int:
    if sec_start and sec_end:
        df = df[(df['duration'] >= sec_start) & (df['duration'] < sec_end)]
    
    elif sec_start:
        df = df[df['duration'] >= sec_start]
    
    elif sec_end:
        df = df[df['duration'] < sec_end]
    
    return len(df)

def min_price_att(df: DataFrame) -> float:
    return df['duration'].min() * 10

def max_price_att(df: DataFrame) -> float:
    return df['duration'].max() * 10

def avg_dur_att(df: DataFrame) -> float:
    return df['duration'].mean()

def sum_price_att_over_15(df: DataFrame) -> float:
    return df[df['duration'] > 15]['duration'].sum() * 10

AGGREGATIONS = {
    "cnt_all_attempts": cnt_all_attempts,
    "cnt_att_dur": {
        "10_sec": lambda df: cnt_att_dur(df, sec_end=10),
        "10_30_sec": lambda df: cnt_att_dur(df, sec_start=10, sec_end=30), 
        "30_sec": lambda df: cnt_att_dur(df, sec_start=30)
    },
    "min_price_att": min_price_att,
    "max_price_att": max_price_att,
    "avg_dur_att": avg_dur_att,
    "sum_price_att_over_15": sum_price_att_over_15
}


def filter_by_phone(df: DataFrame, phone: int):
    return df[df.phone == phone]
