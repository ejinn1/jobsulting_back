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


def  mini_jobsulting(company_id, skills, jobtype, education, location, career, salary):

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

    # 사용자 데이터 입력 -----------------------------------------------------------------------------------------------
    # 사용자 입력 및 데이터 전처리
    # user_skills = input("사용자가 보유한 기술 스택을 입력하세요 (여러 개일 경우 쉼표로 구분): ")
    user_skills = ','.join(skills)
    user_skills = user_skills.split(',')
    user_skills = [stack.strip() for stack in user_skills]

    # user_jobtypes = input("사용자의 직무 유형을 입력하세요 (여러 개일 경우 쉼표로 구분): ")
    user_jobtypes = jobtype
    user_jobtypes = user_jobtypes.split(',')
    user_jobtypes = [jobtype.strip() for jobtype in user_jobtypes]

    # user_educationlevel = input("사용자의 학력을 입력하세요: ")
    user_educationlevel = education

    # user_salary = int(input("사용자의 희망 연봉을 입력하세요: "))
    user_salary = int(salary)

    # years_of_experience = int(input("경력 년수를 입력하세요 (경력이 없을 경우 0 입력): "))
    years_of_experience = int(career)

    # user_location = input("거주하는 지역을 입력하세요: ")
    user_location = location

    # user_company_id = int(input("검색하고자 하는 기업의 ID를 입력하세요: "))
    user_company_id = company_id

    # company_data에서 기업 정보 찾기
    found_company = company_data.loc[company_data['id'] == user_company_id]

    # 기술 스택 조회 -----------------------------------------------------------------------------------------------
    # 찾은 기업 정보 출력
    
    if not found_company.empty:
        print("기업 정보:")
        print("ID:", found_company['id'].iloc[0])
        print("Company:", found_company['Company'].iloc[0])
        print("Skills:", found_company['Skills'].iloc[0])
    else:
        print("해당 ID의 기업 정보를 찾을 수 없습니다.")

    # 사용자가 가지고 있지 않은 기술 스택 찾기
    missing_skills = [skill for skill in found_company['Skills'].iloc[0] if skill not in user_skills]


    # 기술 스택 비교 결과 출력
    skills_result = ""
    if missing_skills:
        skills_str = " ".join(missing_skills)
        skills_result = f"사용자가 가지고 있지 않은 기술 스택: {skills_str}"
    else:
        skills_result = "해당 기업에서 요구하는 기술 스택을 모두 보유 중입니다."


    # 직무 유형 조회 -----------------------------------------------------------------------------------------------
    # 기업별로 직무 유형 확인
    jobtype_result = ""
    for index, row in company_data.iterrows():
        if row['id'] == user_company_id:
            company_types = row['type']
            match_found = False
            for jobtype in user_jobtypes:
                if any(jobtype.lower() in job.lower() for job in company_types):
                    match_found = True
                    break
            if match_found:
                jobtype_result = "직무 유형을 만족합니다."
            else:
                jobtype_result = f"직무 유형을 만족하지 않습니다. {company_types}"
            break
    else:
        jobtype_result = "해당 기업 ID에 대한 정보를 찾을 수 없습니다."


    # 학력 조회 -----------------------------------------------------------------------------------------------
    # 학력 순위 지정
    education_ranks = {
        '고등학교졸업': 0,
        '대학졸업(2,3년)': 1,
        '대학교졸업(4년)': 2,
        '석사졸업': 3,
        '박사졸업': 4,
    }

    # 기업별로 학력 요구사항 확인
    education_result = ""
    for index, row in company_data.iterrows():
        if row['id'] == user_company_id:
            company_education = row['educationlevel_name']
            user_rank = education_ranks.get(user_educationlevel, -1)
            company_rank = education_ranks.get(company_education, -1)

            if company_rank == -1:  # 학력 무관 요구
                education_result = "기업에서 요구하는 학력을 만족합니다."
            elif user_rank >= 0 and company_rank >= 0:
                if user_rank >= company_rank:
                    education_result = "기업에서 요구하는 학력을 만족합니다."
                else:
                    education_result = f"기업에서 요구하는 학력을 만족하지 않습니다. {company_education}"
            else:
                education_result = f"기업에서 요구하는 학력을 만족하지 않습니다. {company_education}"
            break
    else:
        education_result = "해당 기업 ID에 대한 정보를 찾을 수 없습니다."


    # 연봉 조회 -----------------------------------------------------------------------------------------------
    # 기업별로 연봉 요구사항 확인
    salary_result = ""
    for index, row in company_data.iterrows():
        if row['id'] == user_company_id:
            company_salary = row['salary_name']
            if isinstance(company_salary, str):
                salary_result = company_salary
            elif isinstance(company_salary, int):
                if user_salary <= company_salary:
                    salary_result = "사용자가 원하는 연봉을 만족합니다."
                else:
                    salary_diff = user_salary - company_salary
                    salary_result = str(salary_diff)
            break
        else:
            salary_result = "해당 기업 ID에 대한 정보를 찾을 수 없습니다."
 

    # 경력 조회 -----------------------------------------------------------------------------------------------
    # 해당 ID를 만족하는 기업들의 정보 찾기
    matching_companies = company_data[company_data['id'] == user_company_id]

    career_result = ""
    if not matching_companies.empty:
        for _, row in matching_companies.iterrows():
            experience_level_code = row['experiencelevel_code']
            experience_level_min = row['experiencelevel_min']
            experience_level_max = row['experiencelevel_max']

            if years_of_experience == 0 and experience_level_code in [0, 1]:  # 신입, 경력무관 요구
                career_result = "충분한 경력입니다."

            elif years_of_experience == 0 and experience_level_code in [2, 3]:  # 경력, 신입/경력 요구
                if years_of_experience >= experience_level_min:
                    career_result = "충분한 경력입니다."
                else:
                    experience_gap = experience_level_min - years_of_experience
                    career_result = f"경력이 {experience_gap}년 부족합니다."

                if experience_level_max != 0 and years_of_experience > experience_level_max:
                    experience_excess = years_of_experience - experience_level_max
                    career_result += f" 경력이 {experience_excess}년 초과하였습니다."

            elif years_of_experience != 0 and experience_level_code in [1, 2, 3, 0]:  # 경력 요구
                if years_of_experience >= experience_level_min:
                    career_result = "충분한 경력입니다."
                else:
                    experience_gap = experience_level_min - years_of_experience
                    career_result = f"경력이 {experience_gap}년 부족합니다."

                if experience_level_max != 0 and years_of_experience > experience_level_max:
                    experience_excess = years_of_experience - experience_level_max
                    career_result += f" 경력이 {experience_excess}년 초과하였습니다."
            else:
                career_result = "유효하지 않은 경력 요구 코드입니다."
    else:
        career_result = "해당 ID를 만족하는 회사 정보를 찾을 수 없습니다."


    # 지역 조회 -----------------------------------------------------------------------------------------------
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

    location_result = ""
    for index, row in company_data.iterrows():
        if row['id'] == user_company_id:
            company_locations = row['location_name']

            user_location_coordinates = location_coordinates.loc[location_coordinates['location_name'] == user_location]

            distances = []  # 각 기업과 사용자 위치 사이의 거리를 저장할 리스트

            if isinstance(company_locations, str):
                company_locations = [company_locations]  # 문자열인 경우 리스트로 변환

            for location in company_locations:
                location = location.strip()  # 공백 제거
                company_coordinates = location_coordinates.loc[location_coordinates['location_name'] == location]

                if not user_location_coordinates.empty and not company_coordinates.empty:
                    user_latitude = user_location_coordinates['latitude'].values[0]
                    user_longitude = user_location_coordinates['longitude'].values[0]
                    company_latitude = company_coordinates['latitude'].values[0]
                    company_longitude = company_coordinates['longitude'].values[0]
                    company_coordinates = (company_latitude, company_longitude)
                    user_coordinates = (user_latitude, user_longitude)
                    distance = geodesic(user_coordinates, company_coordinates).km
                    distances.append(distance)

            if distances:
                min_distance = min(distances)
                location_result = f"가장 가까운 기업과의 거리: {min_distance}km"
            else:
                location_result = "사용자 또는 기업의 위치 정보를 찾을 수 없습니다."

            break
        
    import json
    
    response = {
        'skills_result' : skills_result,
        'jobtype_result': jobtype_result,
        'education_result': education_result,
        'salary_result': salary_result,
        'career_result': career_result,
        'location_result': location_result,
         }
    
    return response

    # else:
    #     print("해당 기업 ID에 대한 정보를 찾을 수 없습니다.")