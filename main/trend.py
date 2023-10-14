import pandas as pd
import pymysql
from sqlalchemy import create_engine
    
import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor

def calculate_monthly_counts(stacks, df_final):
    monthly_counts = {}
    
    for stack in stacks:
        k = df_final[df_final["keyword_code"] == stack]
        monthly_count = k['posting_month'].value_counts().sort_index()
        monthly_counts[stack] = monthly_count
    
    return monthly_counts

    
    
def trend(stacks):
    pymysql.install_as_MySQLdb()
    import MySQLdb

    engine = create_engine("mysql+mysqldb://root:" + "dmlwls123" + "@localhost/Saramin", connect_args={'charset': 'utf8mb4'})

    conn = engine.connect()
    n_df = pd.read_sql('SELECT keyword_code, posting_date FROM jobsearch', engine)
    
    n_df["posting_date"] = pd.to_datetime(n_df["posting_date"])
    n_df["posting_month"] = n_df["posting_date"].dt.strftime('%Y-%m')
    
    df_expanded = n_df['keyword_code'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('keyword_code')
    df_final = pd.concat([df_expanded, n_df.drop('keyword_code', axis=1)], axis=1).reset_index(drop=True)
    
    # 벡터화 연산을 통한 계산 수행
    monthly_counts = calculate_monthly_counts(stacks, df_final)

    return monthly_counts


def optimize_trend(stacks):
    pymysql.install_as_MySQLdb()
    import MySQLdb

    engine = create_engine("mysql+mysqldb://root:" + "dmlwls123" + "@localhost/Saramin", connect_args={'charset': 'utf8mb4'})

    conn = engine.connect()

    with ThreadPoolExecutor() as executor:
        futures = []
        monthly_counts = {}
        chunk_size = 100  # 데이터를 작은 청크로 분할하여 병렬 처리
        
        for i in range(0, len(stacks), chunk_size):
            stack_chunk = stacks[i:i+chunk_size]
            
            future = executor.submit(trend, stack_chunk)
            futures.append(future)
        
        for future in futures:
            result = future.result()
            monthly_counts.update(result)
    
    return monthly_counts