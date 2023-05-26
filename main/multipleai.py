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
    df_location['location_name'] = df_location['location_name'].str[:2]
    desired_locations = ['서울', '경기', '광주', '대구', '대전', '부산', '울산', '인천', '강원', '경남', '경북', '전남', '전북', '충북', '충남', '제주', '세종']
    df_location = df_location[df_location['location_name'].str.startswith(tuple(desired_locations), na=False)]

    # 중복된 id 제거
    df_location = df_location.drop_duplicates()

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

    company_data = company_data.assign(skill_score=0.0, jobtype_score=0.0, education_score=0.0, salary_score=0.0, experience_score=0.0, location_score=0.0, total=0.0)

    # 기술스택 입력 및 유사도 계산 후 점수화  -----------------------------------------------------------------------------------------------
    # user_skills = input("사용자가 보유한 기술 스택을 입력하세요 (여러 개일 경우 쉼표로 구분): ")
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

    # 직무유형 입력 및 유사도 계산 후 점수화  -----------------------------------------------------------------------------------------------
    # user_jobtypes = input("사용자의 직무 유형을 입력하세요 (여러 개일 경우 쉼표로 구분): ")
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

    # 학력 입력 및 유사도 계산 후 점수화 -----------------------------------------------------------------------------------------------
    # user_education = input("사용자의 학력을 입력하세요: ")
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
            '학력무관': 1
        }
    elif user_education == "대학교졸업(4년)":
        education_score = {
            '고등학교졸업': 0.33,
            '대학졸업(2,3년)': 0.67,
            '대학교졸업(4년)': 1,
            '석사졸업': 0,
            '박사졸업': 0,
            '학력무관': 1
        }
    elif user_education == "석사졸업":
        education_score = {
            '고등학교졸업': 0.25,
            '대학졸업(2,3년)': 0.5,
            '대학교졸업(4년)': 0.75,
            '석사졸업': 1,
            '박사졸업': 0,
            '학력무관': 1
        }
    elif user_education == "박사졸업":
        education_score = {
            '고등학교졸업': 0.2,
            '대학졸업(2,3년)': 0.4,
            '대학교졸업(4년)': 0.6,
            '석사졸업': 0.8,
            '박사졸업': 1,
            '학력무관': 1
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

    # 급여(연봉) 입력 및 유사도 계산 후 점수화 -----------------------------------------------------------------------------------------------
    # user_salary = int(input("사용자의 희망 연봉을 입력하세요: "))
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

    # 급여(연봉) 입력 및 유사도 계산 후 점수화 -----------------------------------------------------------------------
    # 사용자의 경력 년수를 입력 받습니다.
    # years_of_experience = int(input("경력 년수를 입력하세요 (경력이 없을 경우 0 입력): "))
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

    # 기업 데이터를 순회하며 점수를 계산합니다.
    for index, row in company_data.iterrows():
        experience_level = row['experiencelevel_code']
        min_experience = row['experiencelevel_min']
        max_experience = row['experiencelevel_max']

        # 점수를 계산합니다.
        if years_of_experience < min_experience:
            score = 0.0
        elif max_experience == 0:
            score = 1.0
        else:
            score = calculate_score(experience_level, min_experience, max_experience, years_of_experience)

        # 계산된 점수를 할당합니다.
        company_data.at[index, 'experience_score'] = score

    # 지역 입력 및 유사도 계산 후 점수화 -----------------------------------------------------------------------
    # user_location = input("거주하는 지역을 입력하세요: ")
    user_location = location

    # 지역별 위도, 경도 값 설정
    location_coordinates = pd.DataFrame({
        'location_name': ['서울', '경기', '광주', '대구', '대전', '부산', '울산', '인천', '강원', '경남', '경북', '전남', '전북', '충북', '충남', '제주', '세종'],
        'latitude': [37.5665, 37.4138, 35.1595, 35.8714, 36.3504, 35.1796, 35.5384, 37.4563, 37.8854, 35.2383, 36.4919, 34.8679, 35.8205, 36.6357, 36.6588, 33.4996, 36.4808],
        'longitude': [126.9780, 127.5183, 126.8526, 128.6018, 127.3845, 129.0756, 129.3114, 126.7052, 128.7340, 128.6922, 128.8889, 126.9910, 127.1080, 127.4917, 126.8123, 126.5312, 127.2892]
    })

    # 입력한 지역과 기업 지역 간의 거리 계산
    def calculate_distance(row):
        company_location = row['location_name']
        user_lat = location_coordinates.loc[location_coordinates['location_name'] == user_location, 'latitude'].values[0]
        user_lon = location_coordinates.loc[location_coordinates['location_name'] == user_location, 'longitude'].values[0]
        company_lat = location_coordinates.loc[location_coordinates['location_name'] == company_location, 'latitude'].values[0]
        company_lon = location_coordinates.loc[location_coordinates['location_name'] == company_location, 'longitude'].values[0]
        distance = geodesic((user_lat, user_lon), (company_lat, company_lon)).kilometers
        return distance

    # 거리에 따라 점수 부여 및 저장
    company_data['location_score'] = company_data.apply(lambda row: 1 - calculate_distance(row), axis=1)

    # 점수를 0.5에서 1 사이로 매핑
    max_score = company_data['location_score'].max()
    min_score = company_data['location_score'].min()
    normalized_scores = (company_data['location_score'] - min_score) / (max_score - min_score)
    mapped_scores = 0.5 + normalized_scores * 0.5
    company_data['location_score'] = mapped_scores

    # 최종 계산 -----------------------------------------------------------------------------------------------
    # skill_score, jobtype_score, education_score, salary_score, experience_score, location_score를 곱한 값 계산
    company_data['total'] = 100 * company_data['skill_score'] * company_data['jobtype_score'] * company_data['education_score'] * company_data['salary_score'] * company_data['experience_score'] * company_data['location_score']

    # total 열을 기준으로 내림차순 정렬하여 데이터프레임 재정렬
    company_data = company_data.sort_values('total', ascending=False)
    print(company_data)
    
    return (company_data.iloc[[1,2,3,4,5,6,7,8], 0])

    # company_data.to_excel("test1.xlsx")
    

# run_argoritm("서울", 4000, 0, "대학교졸업(4년)", "정규직", "Java")