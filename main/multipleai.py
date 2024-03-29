import pandas as pd
import re
import pymysql
import datetime
import numpy as np
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics import pairwise_distances
from geopy.distance import geodesic
from gensim.models import Word2Vec

import time

def run_argoritm(location, salary, career, education, work_type, skills):

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

    # 마감일 전처리 -----------------------------------------------------------------------------------------------
    # df에서 id와 expiartion_date 열 추출
    df_expirationdate = df[['id', 'expiration_timestamp']].copy()

    # 현재 시간 가져오기
    current_time = datetime.datetime.now()

    # expiration_timestamp 열을 datetime 형식으로 변환합니다.
    df_expirationdate['expiration_timestamp'] = pd.to_datetime(df_expirationdate['expiration_timestamp'])

    # 현재 시간보다 넘어간 기업 공고 삭제하기
    df_expirationdate = df_expirationdate[df_expirationdate['expiration_timestamp'] > current_time]

    # 조회수 전처리 -----------------------------------------------------------------------------------------------
    # df에서 id와 expiartion_date 열 추출
    df_read = df[['id', 'read_cnt']].copy()
    df_read = df_read.drop_duplicates()

    # 'experiencelevel_code' 열을 숫자로 변환
    df_read['read_cnt'] = df_read['read_cnt'].astype(int)

    # 키워드 전처리 -----------------------------------------------------------------------------------------------
    df_keyword = df[['id', 'keyword']].copy()
    df_keyword = df_keyword.dropna(subset=['keyword'])
    df_keyword['keyword'] = df_keyword['keyword'].str.split(',')

    # 제목 전처리 -----------------------------------------------------------------------------------------------
    df_title = df[['id', 'title']].copy()
    df_title = df_title.dropna(subset=['title'])

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

    # company_data에 read_cnt 열 추가
    company_data = company_data.merge(df_read, on='id', how='left')
    company_data = company_data.dropna(subset=['read_cnt'])
    company_data = company_data.reset_index(drop=True)

    # company_data에 keyword 열 추가
    company_data = company_data.merge(df_keyword, on='id', how='left')
    company_data = company_data.dropna(subset=['keyword'])
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

    company_data = company_data.assign(skill_score=0.0, jobtype_score=0.0, education_score=0.0, salary_score=0.0, experience_score=0.0, location_score=0.0, total=0.0,
                                    skill_star=0, jobtype_star=0, education_star=0, salary_star=0, experience_star=0, location_star=0)

    # 기술스택 입력 및 유사도 계산 후 점수화  -----------------------------------------------------------------------------------------------
    # user_skills = input("사용자가 보유한 기술 스택을 입력하세요 (여러 개일 경우 쉼표로 구분): ")
    
    start_time = time.perf_counter()
    
    user_skills = ','.join(skills)
    print(user_skills)
    user_skills = user_skills.split(',')
    user_skills = [stack.strip() for stack in user_skills]

    # 벡터화
    mlb = MultiLabelBinarizer()
    company_skills_encoded = mlb.fit_transform(company_data['Skills'])
    user_skills_encoded = mlb.transform([user_skills])

    # 유사도 측정 (기술스택)
    similarity_scores = cosine_similarity(user_skills_encoded, company_skills_encoded)

    # 점수 계산
    max_score = similarity_scores.max()
    min_score = similarity_scores.min()

    normalized_scores = (similarity_scores - min_score) / (max_score - min_score)

    # skill_score 열에 저장
    company_data['skill_score'] = normalized_scores.flatten()
    
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    print(f"스택 코드 실행 시간: {elapsed_time:.10f} 초")

    # 직무유형 입력 및 유사도 계산 후 점수화  -----------------------------------------------------------------------------------------------
    # user_jobtypes = input("사용자의 직무 유형을 입력하세요 (여러 개일 경우 쉼표로 구분): ")
    
    start_time = time.perf_counter()
    
    user_jobtypes = work_type
    user_jobtypes = user_jobtypes.split(',')
    user_jobtypes = [jobtype.strip() for jobtype in user_jobtypes]

    company_jobtypes_encoded = mlb.fit_transform(company_data['type'])
    user_jobtypes_encoded = mlb.transform([user_jobtypes])

    # 유사도 측정 (직무유형)
    similarity_scores = cosine_similarity(user_jobtypes_encoded, company_jobtypes_encoded)

    # 점수 계산
    max_score = similarity_scores.max()
    min_score = similarity_scores.min()

    normalized_scores = (similarity_scores - min_score) / (max_score - min_score)

    # skill_score 열에 저장
    company_data['jobtype_score'] = normalized_scores.flatten()
    
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    print(f"직무유형 코드 실행 시간: {elapsed_time:.10f} 초")

    # 학력 입력 및 유사도 계산 후 점수화 -----------------------------------------------------------------------------------------------
    # user_education = input("사용자의 학력을 입력하세요: ")
    
    start_time = time.perf_counter()
    
    user_education = education

    # 학력에 따른 점수 설정
    if user_education == "고등학교졸업":
        education_score = {
            '고등학교졸업': 1,
            '대학졸업(2,3년)': 0,
            '대학교졸업(4년)': 0,
            '석사졸업': 0,
            '박사졸업': 0,
            '학력무관': 1
        }
    elif user_education == "대학졸업(2,3년)":
        education_score = {
            '고등학교졸업': 0.5,
            '대학졸업(2,3년)': 1,
            '대학교졸업(4년)': 0,
            '석사졸업': 0,
            '박사졸업': 0,
            '학력무관': 0.75
        }
    elif user_education == "대학교졸업(4년)":
        education_score = {
            '고등학교졸업': 0.33,
            '대학졸업(2,3년)': 0.67,
            '대학교졸업(4년)': 1,
            '석사졸업': 0,
            '박사졸업': 0,
            '학력무관': 0.67
        }
    elif user_education == "석사졸업":
        education_score = {
            '고등학교졸업': 0.25,
            '대학졸업(2,3년)': 0.5,
            '대학교졸업(4년)': 0.75,
            '석사졸업': 1,
            '박사졸업': 0,
            '학력무관': 0.63
        }
    elif user_education == "박사졸업":
        education_score = {
            '고등학교졸업': 0.2,
            '대학졸업(2,3년)': 0.4,
            '대학교졸업(4년)': 0.6,
            '석사졸업': 0.8,
            '박사졸업': 1,
            '학력무관': 0.5
        }
    elif user_education == "미응답":
        education_score = {
            '고등학교졸업': 1,
            '대학졸업(2,3년)': 1,
            '대학교졸업(4년)': 1,
            '석사졸업': 1,
            '박사졸업': 1,
            '학력무관': 1
        }

    # company_data에 학력 점수 계산하여 추가
    company_data['education_score'] = company_data['educationlevel_name'].map(education_score)

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    print(f"학력 코드 실행 시간: {elapsed_time:.10f} 초")
    # 급여(연봉) 입력 및 유사도 계산 후 점수화 -----------------------------------------------------------------------------------------------
    # user_salary = int(input("사용자의 희망 연봉을 입력하세요: "))
    
    start_time = time.perf_counter()
    
    user_salary = int(salary)

    # '회사내규에 따름'과 '면접후 결정'을 1로 저장
    company_data.loc[company_data['salary_name'].isin(['회사내규에 따름', '면접후 결정']), 'salary_score'] = 1

    # 숫자로 된 연봉에 대해 정규화된 점수화 계산
    numeric_salaries = pd.to_numeric(company_data['salary_name'], errors='coerce')
    max_salary = numeric_salaries.max()
    min_salary = numeric_salaries.min()
    normalized_salaries = (numeric_salaries - min_salary) / (max_salary - min_salary)

    # 사용자가 입력한 연봉을 기준으로 정규화된 점수 계산
    user_normalized_salary = (user_salary - min_salary) / (max_salary - min_salary)

    # 점수화된 유사도 계산
    score = np.where(numeric_salaries > user_salary, 1 + (user_normalized_salary - normalized_salaries), 1 - (user_normalized_salary - normalized_salaries))

    # score의 길이를 company_data의 길이와 동일하게 맞춤
    score = pd.Series(score, index=company_data.index)

    # 최종 점수를 company_data['salary_score'] 열에 저장
    company_data.loc[~company_data['salary_name'].isin(['회사내규에 따름', '면접후 결정']), 'salary_score'] = score

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    print(f"연봉 코드 실행 시간: {elapsed_time:.10f} 초")

    # 급여(연봉) 입력 및 유사도 계산 후 점수화 -----------------------------------------------------------------------
    # 사용자의 경력 년수를 입력 받습니다.
    # years_of_experience = int(input("경력 년수를 입력하세요 (경력이 없을 경우 0 입력): "))
    
    start_time = time.perf_counter()
    
    years_of_experience = int(career)

    # 경력에 따른 점수를 계산하는 함수를 정의합니다.
    def calculate_score(experience_level, min_experience, max_experience, years_of_experience):
        if experience_level == 0:  # 경력무관
            return 1.0
        elif experience_level == 1:  # 신입
            if years_of_experience == 0:  # 사용자의 경력이 없는 경우
                return 0.0
            elif years_of_experience <= max_experience:
                return 1.0 - (max_experience - years_of_experience) / max_experience
            else:
                return 0.0
        elif experience_level == 2:  # 경력
            if years_of_experience < min_experience or years_of_experience > max_experience:
                return 0.0
            elif years_of_experience == min_experience or years_of_experience == max_experience:
                return 0.5
            else:
                return 0.5 + (years_of_experience - min_experience) / (max_experience - min_experience)
        elif experience_level == 3:  # 신입/경력
            if years_of_experience <= max_experience:
                return 1.0 - (max_experience - years_of_experience) / max_experience
            else:
                return 0.0
        else:
            return 0.0

    # 기업 데이터를 순회하며 점수를 계산
    for index, row in company_data.iterrows():
        experience_level = row['experiencelevel_code']
        min_experience = row['experiencelevel_min']
        max_experience = row['experiencelevel_max']

        # 점수를 계산
        if years_of_experience < min_experience:
            score = 0.0
        elif max_experience == 0:
            score = 1.0
        else:
            score = calculate_score(experience_level, min_experience, max_experience, years_of_experience)

        # 계산된 점수를 할당합니다.
        company_data.at[index, 'experience_score'] = score
        
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    print(f"경력 코드 실행 시간: {elapsed_time:.10f} 초")

    # 지역 입력 및 유사도 계산 후 점수화 -----------------------------------------------------------------------
    # user_location = input("거주하는 지역을 입력하세요: ")
    
    start_time = time.perf_counter()
    
    user_location = location

    # 지역별 위도, 경도 값 설정
    location_coordinates = pd.DataFrame({
        'location_name': ['서울 서울전체', '서울 강남구', '서울 강동구', '서울 강북구', '서울 강서구', '서울 관악구', '서울 광진구', '서울 구로구', '서울 금천구', '서울 노원구',
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
                        '제주 서귀포시', '제주 제주시'],
        'latitude': [37.5665, 37.4954, 37.5492, 37.6479, 37.5651, 37.4784, 37.5485, 37.4969, 37.4602, 37.6552,
                    37.6659, 37.5835, 37.4921, 37.5635, 37.5791, 37.4837, 37.5500, 37.5900, 37.5145, 37.5172,
                    37.5262, 37.5328, 37.6176, 37.5723, 37.5665, 37.6181, 37.5609, 37.5891, 37.5145, 37.6688,
                    37.6838, 37.6931, 37.4335, 37.4772, 37.4274, 37.5945, 37.3537, 37.6542, 37.6369, 37.903,
                    37.5034, 37.4824, 37.5971, 37.5059, 37.4192, 37.3929, 37.4375, 37.438, 37.2636, 36.4808,
                    37.2586, 37.2411, 37.3243, 37.2794, 37.3815, 37.3222, 37.3357, 37.3118, 37.007, 37.4011,
                    37.3927, 37.4004, 37.8061, 37.6167, 37.2059, 38.0261, 37.1492, 37.2664, 37.2839, 37.3235,
                    37.2343, 37.344, 37.7384, 37.2799, 37.7599, 36.9944, 37.8961, 37.539, 37.1995, 35.1595,
                    35.1579, 35.1261, 35.1768, 35.1746, 35.1508, 35.8459, 35.8302, 35.7177, 35.7667, 35.8856,
                    35.8891, 35.8682, 35.8575, 35.866, 36.3504, 36.3502, 36.3747, 36.3381, 36.3743, 36.3503,
                    35.2122, 35.2225, 35.243, 35.1366, 35.2049, 35.2049, 35.1631, 35.1985, 35.2131, 35.1526,
                    35.0994, 35.0972, 35.1663, 35.1763, 35.0928, 35.1068, 35.1631, 35.5396, 35.5382, 35.5322,
                    35.6313, 35.6286, 35.5709, 37.4563, 37.562, 37.4649, 37.4601, 37.4007, 37.4839, 37.5055,
                    37.5567, 37.4092, 37.2429, 37.4717, 37.8859, 37.7631, 38.3787, 37.8845, 37.4498, 38.2048,
                    38.204, 37.4369, 37.1834, 37.3445, 37.8858, 38.1043, 37.8833, 37.1749, 37.1859, 37.6502,
                    37.7049, 38.1054, 37.4986, 35.4606, 34.8787, 35.6849, 34.9758, 35.227, 34.8161, 35.1557,
                    35.2213, 35.2372, 35.1839, 35.4994, 36.8874, 35.3425, 35.3385, 35.1928, 35.1923, 35.1547,
                    35.4921, 35.2383, 34.8577, 35.0687, 35.2790, 35.5207, 35.5698, 36.4919, 35.8265, 35.8562,
                    35.7139, 36.1214, 36.2783, 36.1323, 36.6011, 36.4159, 36.3849, 36.5750, 36.4911, 36.5329,
                    36.6655, 36.8283, 35.9736, 36.6566, 37.4970, 36.9912, 36.3530, 35.6674, 36.4345, 36.0509,
                    36.0188, 35.2102, 36.0401, 36.0508, 35.1183, 34.9366, 34.9607, 34.9769, 35.1946, 34.9882,
                    35.3213, 34.8119, 34.9907, 34.7629, 34.9506, 34.7905, 34.7604, 35.2742, 34.7932, 34.3233,
                    35.3074, 34.7161, 34.4972, 34.4827, 34.6348, 34.9355, 35.7160, 35.9656, 35.7062, 35.8277,
                    35.4125, 35.8685, 35.7244, 35.3746, 35.9131, 35.9490, 35.6139, 35.7005, 35.8238, 35.8628,
                    35.8056, 35.5698, 35.7074, 35.8866, 36.6357, 36.4916, 36.4936, 36.3516, 36.3045, 36.3029,
                    37.1299, 36.7859, 36.8549, 36.6384, 36.6398, 36.6354, 36.6286, 36.9975, 36.6342, 36.6353,
                    36.8223, 36.3169, 36.4599, 36.1551, 36.3316, 36.3490, 36.3441, 36.7846, 36.7741, 36.2674,
                    36.7836, 36.3221, 36.6824, 36.8075, 36.8024, 36.7794, 36.4160, 36.7446, 36.6021, 33.4996,
                    33.2548, 33.5008],
        'longitude': [127.9469, 127.1247, 127.0412, 127.0254, 126.8561, 126.9528, 127.0838, 126.8885, 126.9009, 127.0679,
                    127.0368, 127.0671, 126.9437, 126.9547, 126.9367, 126.9910, 127.0366, 127.0164, 127.1195, 126.8663,
                    126.8972, 126.9875, 126.9291, 126.9870, 126.9989, 127.0927, 127.1788, 127.5108, 126.8342, 126.8636,
                    126.7688, 126.7606, 126.9945, 126.8668, 127.2556, 127.1396, 126.9002, 126.7152, 127.2165, 127.0560,
                    126.7850, 126.7930, 126.7926, 126.7900, 127.1288, 127.1356, 127.1424, 127.1510, 127.0293, 127.2892,
                    126.9711, 127.0568, 127.0192, 127.0126, 126.8059, 126.7920, 126.8739, 126.9445, 127.2717, 126.9228,
                    126.9553, 126.9160, 127.0456, 127.6292, 127.6358, 127.0722, 127.0772, 127.1783, 127.1269, 127.0948,
                    127.2098, 126.9744, 127.0568, 127.4429, 126.7730, 127.1128, 127.2009, 127.2165, 127.0428, 126.7970,
                    126.7644, 126.9235, 126.9189, 126.9113, 126.8663, 128.6014, 128.5683, 128.4572, 128.5663, 128.7040,
                    128.5912, 128.5551, 128.6790, 128.5979, 127.4198, 127.4313, 127.4194, 127.3680, 127.3686, 127.4116,
                    129.0403, 129.0077, 129.0893, 129.1840, 129.0822, 129.0492, 129.0880, 129.0426, 129.0124, 128.9875,
                    129.0223, 129.0210, 129.0814, 129.0701, 129.0655, 129.0364, 129.0492, 129.3114, 129.4124, 129.4211,
                    129.3656, 129.1682, 129.0399, 126.7052, 126.4929, 126.7350, 126.6546, 126.7052, 126.6365, 126.7247,
                    126.6932, 126.6520, 126.6283, 126.6248, 128.8716, 128.8968, 128.4684, 129.1146, 128.5912, 128.5994,
                    128.4918, 128.6186, 128.4671, 127.9496, 128.3780, 128.7244, 127.2759, 127.7342, 128.9851, 128.3907,
                    127.7308, 127.7058, 127.7140, 128.0390, 128.8819, 128.9007, 128.3425, 128.7115, 127.9265, 128.5770,
                    128.5728, 128.6695, 128.6522, 128.7625, 128.0717, 127.8779, 129.0411, 128.3030, 128.1172, 128.8576,
                    128.4951, 128.4753, 128.4302, 127.7334, 128.5578, 128.3314, 128.7041, 128.8055, 129.2116, 129.2925,
                    128.2522, 128.3393, 128.7277, 128.1206, 128.1934, 128.7380, 128.1527, 128.2895, 128.7160, 129.3656,
                    129.1207, 128.9516, 128.9384, 129.1763, 130.8978, 129.3769, 128.6978, 128.7179, 129.0802, 128.5147,
                    129.3410, 129.3410, 129.3410, 126.5219, 126.7710, 127.2892, 127.2892, 127.6896, 127.7261, 126.7067,
                    126.9260, 126.3948, 126.4782, 127.4989, 127.6585, 126.9308, 127.6637, 126.6780, 126.7179, 126.7345,
                    126.7830, 126.9108, 126.2605, 126.5206, 126.4349, 126.9990, 127.1207, 126.7118, 127.0078, 127.3685,
                    127.6626, 127.4713, 127.1397, 127.7649, 126.9770, 127.0276, 127.5245, 127.5140, 127.1519, 127.1347,
                    127.1466, 126.8531, 127.4372, 127.7346, 127.7862, 128.3495, 128.7200, 128.9897, 128.5664, 128.4372,
                    128.2032, 127.7255, 127.7068, 127.4436, 127.4402, 127.4728, 127.4985, 127.4890, 127.4280, 127.4297,
                    126.9578, 127.2489, 127.1190, 127.4889, 127.0986, 126.6312, 126.6155, 126.9120, 126.7163, 126.7159,
                    127.0018, 126.6770, 126.8945, 127.4350, 127.1319, 127.1046, 126.8065, 126.3009, 126.6860, 126.5459,
                    126.5653, 126.5219]
        })

    # 사용자가 입력한 지역의 위도와 경도 찾기
    user_coordinates = location_coordinates[location_coordinates['location_name'] == user_location]
    user_latitude = user_coordinates['latitude'].iloc[0]
    user_longitude = user_coordinates['longitude'].iloc[0]

    # 기업별 위치와의 거리 계산
    distances = []

    for index, row in company_data.iterrows():
        locations = row['location_name']
        min_distance = float('inf')
        for location in locations:
            location_coordinates_single = location_coordinates[location_coordinates['location_name'] == location]
            if not location_coordinates_single.empty:
                latitude = location_coordinates_single['latitude'].iloc[0]
                longitude = location_coordinates_single['longitude'].iloc[0]
                distance = geodesic((latitude, longitude), (user_latitude, user_longitude)).km
                if distance < min_distance:
                    min_distance = distance
        distances.append(min_distance)

    # 가장 가까운 기업과 가장 먼 기업의 거리 계산
    min_distance = min(distances)
    max_distance = max(distances)

    # 점수 계산
    company_data['location_distance'] = distances
    company_data['location_score'] = company_data.apply(lambda row: 0.3 + (1 - (row['location_distance'] - min_distance) / (max_distance - min_distance)) * 0.7, axis=1)
    
    
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    print(f"지역 코드 실행 시간: {elapsed_time:.10f} 초")

    # 최종 계산 -----------------------------------------------------------------------------------------------
    # skill_score, jobtype_score, education_score, salary_score, experience_score, location_score를 곱한 값 계산
    company_data['total'] = 100 * company_data['skill_score'] * company_data['jobtype_score'] * company_data['education_score'] * company_data['salary_score'] * company_data['experience_score'] * company_data['location_score']

    # total 열을 기준으로 내림차순 정렬하여 데이터프레임 재정렬
    company_data = company_data.sort_values('total', ascending=False)

    start_time = time.perf_counter()

    # 상위 특정 개수의 행 선택
    num_rows = 3
    target_rows = company_data.head(num_rows)

    # 조건에 따라 skill_star 값 할당
    for index, row in target_rows.iterrows():
        skill_score = row['skill_score']
        if 0.0 <= skill_score < 0.2:
            company_data.at[index, 'skill_star'] = 1
        elif 0.2 <= skill_score < 0.4:
            company_data.at[index, 'skill_star'] = 2
        elif 0.4 <= skill_score < 0.6:
            company_data.at[index, 'skill_star'] = 3
        elif 0.6 <= skill_score < 0.8:
            company_data.at[index, 'skill_star'] = 4
        elif skill_score >= 0.8:
            company_data.at[index, 'skill_star'] = 5

    # 조건에 따라 jobtype_star 값 할당
    for index, row in target_rows.iterrows():
        jobtype_score = row['jobtype_score']
        if 0.0 <= jobtype_score < 0.2:
            company_data.at[index, 'jobtype_star'] = 1
        elif 0.2 <= jobtype_score < 0.4:
            company_data.at[index, 'jobtype_star'] = 2
        elif 0.4 <= jobtype_score < 0.6:
            company_data.at[index, 'jobtype_star'] = 3
        elif 0.6 <= jobtype_score < 0.8:
            company_data.at[index, 'jobtype_star'] = 4
        elif jobtype_score >= 0.8:
            company_data.at[index, 'jobtype_star'] = 5

    # 조건에 따라 education_star 값 할당
    for index, row in target_rows.iterrows():
        education_score = row['education_score']
        if 0.0 <= education_score < 0.2:
            company_data.at[index, 'education_star'] = 1
        elif 0.2 <= education_score < 0.4:
            company_data.at[index, 'education_star'] = 2
        elif 0.4 <= education_score < 0.6:
            company_data.at[index, 'education_star'] = 3
        elif 0.6 <= education_score < 0.8:
            company_data.at[index, 'education_star'] = 4
        elif education_score >= 0.8:
            company_data.at[index, 'education_star'] = 5

    # 조건에 따라 salary_star 값 할당
    for index, row in target_rows.iterrows():
        salary_score = row['salary_score']
        if 0.0 <= salary_score < 0.2:
            company_data.at[index, 'salary_star'] = 1
        elif 0.2 <= salary_score < 0.4:
            company_data.at[index, 'salary_star'] = 2
        elif 0.4 <= salary_score < 0.6:
            company_data.at[index, 'salary_star'] = 3
        elif 0.6 <= salary_score < 0.8:
            company_data.at[index, 'salary_star'] = 4
        elif salary_score >= 0.8:
            company_data.at[index, 'salary_star'] = 5

    # 조건에 따라 experience_star 값 할당
    for index, row in target_rows.iterrows():
        experience_score = row['experience_score']
        if 0.0 <= experience_score < 0.2:
            company_data.at[index, 'experience_star'] = 1
        elif 0.2 <= experience_score < 0.4:
            company_data.at[index, 'experience_star'] = 2
        elif 0.4 <= experience_score < 0.6:
            company_data.at[index, 'experience_star'] = 3
        elif 0.6 <= experience_score < 0.8:
            company_data.at[index, 'experience_star'] = 4
        elif experience_score >= 0.8:
            company_data.at[index, 'experience_star'] = 5

    # 조건에 따라 location_star 값 할당
    for index, row in target_rows.iterrows():
        location_score = row['location_score']
        if 0.0 <= location_score < 0.2:
            company_data.at[index, 'location_star'] = 1
        elif 0.2 <= location_score < 0.4:
            company_data.at[index, 'location_star'] = 2
        elif 0.4 <= location_score < 0.6:
            company_data.at[index, 'location_star'] = 3
        elif 0.6 <= location_score < 0.8:
            company_data.at[index, 'location_star'] = 4
        elif location_score >= 0.8:
            company_data.at[index, 'location_star'] = 5
            
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    print(f"별 코드 실행 시간: {elapsed_time:.10f} 초")

    company_data.to_excel("test1.xlsx")

    # 대표 키워드 추출 -----------------------------------------------------------------------------------------------
    
    start_time = time.perf_counter()
    
    top_keywords = company_data['keyword'].head(15).tolist()
    keywords = [item for sublist in top_keywords for item in sublist]
    keywords = [''.join(keyword.split()) for keyword in keywords]

    # Word2Vec 모델 학습
    sentences = [kw.split() for kw in keywords]
    model = Word2Vec(sentences, min_count=1)

    # 대표 키워드 추출
    representative_keyword = None
    max_similarity = -1

    # 각 키워드와 다른 키워드들 간의 유사도 계산
    for keyword in keywords:
        similarity = sum(cosine_similarity([model.wv[keyword]], [model.wv[kw]])[0][0] for kw in keywords)

        if similarity > max_similarity:
            max_similarity = similarity
            representative_keyword = keyword

            
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    print(f"키워드 코드 실행 시간: {elapsed_time:.10f} 초")

    # print("대표 키워드:", representative_keyword)
    
    return (representative_keyword, company_data.iloc[[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15], 0], company_data.iloc[[0,1,2], [21,22,23,24,25]])
