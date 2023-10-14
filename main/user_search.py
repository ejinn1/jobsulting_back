import pandas as pd
import re
import pymysql
import datetime
import numpy as np
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MultiLabelBinarizer
from django.http import JsonResponse



def search_company(company_name):
    pymysql.install_as_MySQLdb()
    import MySQLdb

    engine = create_engine("mysql+mysqldb://root:" + "dmlwls123" + "@localhost/Saramin", connect_args={'charset': 'utf8mb4'})
    conn = engine.connect()
    df = pd.read_sql('SELECT * FROM jobsearch', engine)

    # Parse position.job-code.code into separate rows
    df_code = df['keyword_code'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('code')

    # 기술스택 전처리 -----------------------------------------------------------------------------------------------
    # Join with jobcode table to get keyword information
    df_jobcode = pd.read_sql('SELECT * FROM jobcode', engine)

    # Convert 'code' column to integer type
    df_code = df_code.astype(int)

    # Filter rows that are not in df_jobcode
    df_code_filtered = df_code[df_code.isin(df_jobcode['code']).values]
    df_code_filtered = df_code_filtered.reset_index()
    df_code_filtered.columns = ['id', 'code']
    df_code_filtered = df_code_filtered.drop_duplicates()

    # Merge with df to get company detail name
    df_merged = df_code_filtered.merge(df[['id', 'company_detail_name']], on='id', how='left')

    # Merge with df_jobcode to get keyword
    df_merged = df_merged.merge(df_jobcode[['code', 'keyword']], on='code', how='left')

    # 직무 유형 전처리 -----------------------------------------------------------------------------------------------
    # Parse jobtype_code into separate rows
    df_jobtype_code = df['jobtype_code'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('jobtype_code')

    # Join with jobtypecode table to get jobtype information
    df_jobtypecode = pd.read_sql('SELECT * FROM jobtypecode', engine)

    # Convert 'code' column to integer type
    df_jobtype_code = df_jobtype_code.astype(int)

    # Filter rows that are not in df_jobtypecode
    df_jobtype_code_filtered = df_jobtype_code[df_jobtype_code.isin(df_jobtypecode['code']).values]
    df_jobtype_code_filtered = df_jobtype_code_filtered.reset_index()
    df_jobtype_code_filtered.columns = ['id', 'code']
    df_jobtype_code_filtered = df_jobtype_code_filtered.drop_duplicates()

    # Merge with df_jobtypecode to get jobtype
    df_jobtype_code_filtered = df_jobtype_code_filtered.merge(df_jobtypecode[['code', 'type']], on='code', how='left')

    # 학력 전처리 -----------------------------------------------------------------------------------------------
    # df에서 id와 educationlevel_name 열 추출
    df_educationlevel = df[['id', 'educationlevel_name']].copy()

    # # 중복된 id 제거
    df_educationlevel = df_educationlevel.drop_duplicates()

    # 급여 전처리 -----------------------------------------------------------------------------------------------
    # df에서 id와 salary_name 열 추출
    df_salaryname = df[['id', 'salary_name']].copy()

    # 중복된 id 제거
    df_salaryname = df_salaryname.drop_duplicates()

    # 경력 전처리 -----------------------------------------------------------------------------------------------
    # df에서 id와 experiencelevel 열 추출
    df_experiencelevel = df[['id', 'experiencelevel_code', 'experiencelevel_min', 'experiencelevel_max']].copy()

    # 'experiencelevel_code' 열을 숫자로 변환
    df_experiencelevel['experiencelevel_code'] = df_experiencelevel['experiencelevel_code'].astype(int)

    # 'experiencelevel_min' 열을 숫자로 변환
    df_experiencelevel['experiencelevel_min'] = df_experiencelevel['experiencelevel_min'].astype(int)

    # 'experiencelevel_max' 열을 숫자로 변환
    df_experiencelevel['experiencelevel_max'] = df_experiencelevel['experiencelevel_max'].astype(int)

    # # 중복된 id 제거
    df_experiencelevel = df_experiencelevel.drop_duplicates()
    
    # 위치 전처리 -----------------------------------------------------------------------------------------------
    # df에서 id와 location_name 열 추출
    df_location = df[['id', 'location_name']].copy()
    df_location = df_location.dropna(subset=['location_name'])
    df_location['location_name'] = df_location['location_name'].str.replace('&gt;', '')
    df_location['location_name'] = df_location['location_name'].str.replace(r'\s+', ' ', regex=True)
    df_location['location_name'] = df_location['location_name'].str.split(',').apply(lambda x: [location.strip() for location in x])

    valid_locations = ['서울 서울전체', '서울 강남구', '서울 강동구', '서울 강북구', '서울 강서구', '서울 관악구', '서울 광진구', '서울 구로구', '서울 금천구', '서울 노원구',
                        '서울 도봉구', '서울 동대문구', '서울 동작구', '서울 마포구', '서울 서대문구', '서울 서초구', '서울 성동구', '서울 성북구', '서울 송파구', '서울 양천구',
                        '서울 영등포구', '서울 용산구', '서울 은평구', '서울 종로구', '서울 중구', '서울 중랑구', '경기 경기전체', '경기 가평군', '경기 고양시', '경기 고양시 덕양구',
                        '경기 고양시 일산동구', '경기 고양시 일산서구', '경기 과천시', '경기 광명시', '경기 광주시', '경기 구리시', '경기 군포시', '경기 김포시', '경기 남양주시', '경기 동두천시',
                        '경기 부천시', '경기 부천시 소사구', '경기 부천시 오정구', '경기 부천시 원미구', '경기 성남시', '경기 성남시 분당구', '경기 성남시 수정구', '경기 성남시 중원구', '경기 수원시', '세종 세종특별자치시',
                        '경기 수원시 권선구', '경기 수원시 영통구', '경기 수원시 장안구', '경기 수원시 팔달구', '경기 시흥시', '경기 안산시', '경기 안산시 단원구', '경기 안산시 상록구', '경기 안성시', '경기 안양시',
                        '경기 안양시 동안구', '경기 안양시 만안구', '경기 양주시', '경기 양평군', '경기 여주시', '경기 연천군', '경기 오산시', '경기 용인시', '경기 용인시 기흥구', '경기 용인시 수지구',
                        '경기 용인시 처인구', '경기 의왕시', '경기 의정부시', '경기 이천시', '경기 파주시', '경기 평택시', '경기 포천시', '경기 하남시', '경기 화성시', '광주 광주전체',
                        '광주 광산구', '광주 남구', '광주 동구', '광주 북구', '광주 서구', '대구 대구전체', '대구 남구', '대구 달서구', '대구 달성군', '대구 동구',
                        '대구 북구', '대구 서구', '대구 수성구', '대구 중구', '대전 대전전체', '대전 대덕구', '대전 동구', '대전 서구', '대전 유성구', '대전 중구',
                        '부산 부산전체', '부산 강서구', '부산 금정구', '부산 기장군', '부산 남구', '부산 동구', '부산 동래구', '부산 부산진구', '부산 북구', '부산 사상구',
                        '부산 사하구', '부산 서구', '부산 수영구', '부산 연제구', '부산 영도구', '부산 중구', '부산 해운대구', '울산 울산전체', '울산 남구', '울산 동구',
                        '울산 북구', '울산 울주군', '울산 중구', '인천 인천전체', '인천 강화군', '인천 계양구', '인천 미추홀구', '인천 남동구', '인천 동구', '인천 부평구',
                        '인천 서구', '인천 연수구', '인천 옹진군', '인천 중구', '강원 강원전체', '강원 강릉시', '강원 고성군', '강원 동해시', '강원 삼척시', '강원 속초시',
                        '강원 양구군', '강원 양양군', '강원 영월군', '강원 원주시', '강원 인제군', '강원 정선군', '강원 철원군', '강원 춘천시', '강원 태백시', '강원 평창군',
                        '강원 홍천군', '강원 화천군', '강원 횡성군', '경남 경남전체', '경남 거제시', '경남 거창군', '경남 고성군', '경남 김해시', '경남 남해군', '경남 창원시 마산회원구',
                        '경남 창원시 마산합포구', '경남 창원시 성산구', '경남 창원시 의창구', '경남 밀양시', '경남 사천시', '경남 산청군', '경남 양산시', '경남 의령군', '경남 진주시', '경남 창원시 진해구',
                        '경남 창녕군', '경남 창원시', '경남 통영시', '경남 하동군', '경남 함안군', '경남 함양군', '경남 합천군', '경북 경북전체', '경북 경산시', '경북 경주시',
                        '경북 고령군', '경북 구미시', '경북 군위군', '경북 김천시', '경북 문경시', '경북 봉화군', '경북 상주시', '경북 성주군', '경북 안동시', '경북 영덕군',
                        '경북 영양군', '경북 영주시', '경북 영천시', '경북 예천군', '경북 울릉군', '경북 울진군', '경북 의성군', '경북 청도군', '경북 청송군', '경북 칠곡군',
                        '경북 포항시', '경북 포항시 남구', '경북 포항시 북구', '전남 전남전체', '전남 강진군', '전남 고흥군', '전남 곡성군', '전남 광양시', '전남 구례군', '전남 나주시',
                        '전남 담양군', '전남 목포시', '전남 무안군', '전남 보성군', '전남 순천시', '전남 신안군', '전남 여수시', '전남 영광군', '전남 영암군', '전남 완도군',
                        '전남 장성군', '전남 장흥군', '전남 진도군', '전남 함평군', '전남 해남군', '전남 화순군', '전북 전북전체', '전북 고창군', '전북 군산시', '전북 김제시',
                        '전북 남원시', '전북 무주군', '전북 부안군', '전북 순창군', '전북 완주군', '전북 익산시', '전북 임실군', '전북 장수군', '전북 전주시', '전북 전주시 덕진구',
                        '전북 전주시 완산구', '전북 정읍시', '전북 진안군', '충북 충북전체', '충북 괴산군', '충북 단양군', '충북 보은군', '충북 영동군', '충북 옥천군', '충북 음성군',
                        '충북 제천시', '충북 증평군', '충북 진천군', '충북 청원군', '충북 청주시', '충북 청주시 상당구', '충북 청주시 흥덕구', '충북 충주시', '충북 청주시 청원구', '충북 청주시 서원구',
                        '충남 충남전체', '충남 계룡시', '충남 공주시', '충남 금산군', '충남 논산시', '충남 당진시', '충남 보령시', '충남 부여군', '충남 서산시', '충남 서천군',
                        '충남 아산시', '충남 연기군', '충남 예산군', '충남 천안시', '충남 천안시 동남구', '충남 천안시 서북구', '충남 청양군', '충남 태안군', '충남 홍성군', '제주 제주전체',
                        '제주 서귀포시', '제주 제주시']

    # 'location_name' 열에서 유효한 지역명만 남기고 나머지는 삭제
    df_location['location_name'] = df_location['location_name'].apply(lambda x: [loc for loc in x if loc in valid_locations])

    # 'location_name' 열에서 빈 리스트인 행은 삭제
    df_location = df_location[df_location['location_name'].apply(len) > 0]
    
    # 제목 전처리 ———————————————————————————————————————————————
    df_title = df[['id', 'title']].copy()
    df_title = df_title.dropna(subset=['title'])

    # 마감일 전처리 -----------------------------------------------------------------------------------------------
    # df에서 id와 expiartion_date 열 추출
    df_expirationdate = df[['id', 'expiration_timestamp']].copy()

    # 현재 시간 가져오기
    current_time = datetime.datetime.now()

    # expiration_timestamp 열을 datetime 형식으로 변환합니다.
    df_expirationdate['expiration_timestamp'] = pd.to_datetime(df_expirationdate['expiration_timestamp'])

    # 현재 시간보다 넘어간 기업 공고 삭제하기
    df_expirationdate = df_expirationdate[df_expirationdate['expiration_timestamp'] > current_time]
    # -----------------------------------------------------------------------------------------------
    # 구인 공고 데이터 전처리화
    company_data = pd.DataFrame({'id': df_merged['id'].unique().tolist(),
                                'Company': df_merged.groupby('id')['company_detail_name'].first().tolist(),
                                'Skills': df_merged.groupby('id')['keyword'].apply(list).tolist(),
                                })

    # 그룹화된 jobtype 추가
    df_jobtype_grouped = df_jobtype_code_filtered.groupby('id')['type'].apply(list).reset_index()
    company_data = company_data.merge(df_jobtype_grouped, on='id', how='left')
    company_data = company_data.dropna(subset=['type'])
    company_data = company_data.reset_index(drop=True)

    # company_data에 educationlevel_name 열 추가
    company_data = company_data.merge(df_educationlevel, on='id', how='left')

    # 'educationlevel_name' 열에서 '이상'이라는 단어 제거 (전처리)
    company_data['educationlevel_name'] = company_data['educationlevel_name'].str.replace('이상', '')
    company_data = company_data.dropna(subset=['educationlevel_name'])
    company_data = company_data.reset_index(drop=True)

    # company_data에 salary_name 열 추가
    company_data = company_data.merge(df_salaryname, on='id', how='left')
    company_data = company_data.dropna(subset=['salary_name'])
    company_data = company_data.reset_index(drop=True)

    # company_data에 experiencelevel 열 추가
    company_data = company_data.merge(df_experiencelevel, on='id', how='left')

    # company_data에 expirationdate 열 추가
    company_data = company_data.merge(df_expirationdate, on='id', how='left')
    company_data = company_data.dropna(subset=['expiration_timestamp'])
    company_data = company_data.reset_index(drop=True)
    
    # company_data에 location 열 추가
    company_data = company_data.merge(df_location, on='id', how='left')
    company_data = company_data.dropna(subset=['location_name'])
    company_data = company_data.reset_index(drop=True)
    
    # company_data에 title 열 추가
    company_data = company_data.merge(df_title, on='id', how='left')
    company_data = company_data.dropna(subset=['title'])
    company_data = company_data.reset_index(drop=True)

    # ',' 쉼표 삭제
    company_data['salary_name'] = company_data['salary_name'].str.replace(',', '')

    # '주급', '시급', '일급', '건당'이 포함된 행을 삭제
    company_data = company_data[~company_data['salary_name'].str.contains('주급|시급|일급|건당')]
    company_data = company_data.reset_index(drop=True)

    # '만원' 단어 삭제
    company_data['salary_name'] = company_data['salary_name'].str.replace('만원', '')

    # '월급'으로 시작하는 경우 처리
    monthly_wage_mask = company_data['salary_name'].str.startswith('월급')
    company_data.loc[monthly_wage_mask, 'salary_name'] = \
        company_data.loc[monthly_wage_mask, 'salary_name'].str.replace('월급', '').apply(lambda x: int(re.sub(r'[^\d.]', '', x)) * 12)

    # '연봉'으로 시작하는 항목에 대해 '연봉'을 삭제하고 나머지 문자열은 그대로 저장
    company_data.loc[company_data['salary_name'].str.startswith('연봉', na=False), 'salary_name'] = \
        company_data.loc[company_data['salary_name'].str.startswith('연봉', na=False), 'salary_name'].str.replace('연봉', '', regex=False)

    # '면접후 결정', '회사내규에 따름'을 제외한 모든 항목을 숫자로 변환
    company_data.loc[~company_data['salary_name'].isin(['면접후 결정', '회사내규에 따름']), 'salary_name'] = \
        company_data.loc[~company_data['salary_name'].isin(['면접후 결정', '회사내규에 따름']), 'salary_name'].apply(pd.to_numeric, errors='coerce')

    # 검색 기능 시작 -----------------------------------------------------------------------------------------------
    # user_company = input("기업 검색: ")
    user_company = company_name
    print(user_company)

    # 검색값이 포함된 기업 필터링
    # filtered_data = company_data[company_data['Company'].str.contains(user_company, regex=False)]
    filtered_data = company_data[company_data['Company'].str.contains(user_company, regex=False)]
    print(filtered_data)
    
    return filtered_data.to_dict(orient="records")

