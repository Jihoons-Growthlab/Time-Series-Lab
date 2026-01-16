import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import DATETIME, DATE, BIGINT, DOUBLE, VARCHAR, TEXT

#  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< 
#   Summary
#   일반 csv 파일을 MySQL 테이블에 밀어 넣기
#   
#  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# 1) 입력/테이블 설정
CSV_PATH = r"/Users/hoon/Desktop/personal_project/Time-Series-Lab/dataset/raw_data.csv"
TABLE_NAME = "raw_data"
DATE_COLS = ["MFG_DT"]  # <- 여기에 날짜 컬럼명 넣기 (예: ["DT", "측정일시"])

# 2) MySQL 연결
DB_URL = "mysql+pymysql://root:1234@127.0.0.1:3306/tsdb?charset=utf8mb4"
engine = create_engine(DB_URL)

# 3) CSV 읽기 + 날짜 파싱
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

# 컬럼명 정리(공백/줄바꿈 제거)
df.columns = (
    df.columns.astype(str)
    .str.replace("\n", " ", regex=False)
    .str.strip()
)

# 날짜 컬럼 파싱
for c in DATE_COLS:
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce")  # 파싱 실패는 NaT
    else:
        raise ValueError(f"DATE_COLS에 지정한 컬럼 '{c}' 이(가) CSV에 없습니다. 현재 컬럼: {list(df.columns)}")

# 4) (선택) MySQL 타입 매핑
# - 날짜 컬럼은 DATETIME(또는 DATE)로 강제
# - 나머지는 기본 추론에 맡기되, 문제가 있으면 여기에서 계속 확장하면 됨
dtype_map = {c: DATETIME(fsp=6) for c in DATE_COLS}  # 마이크로초까지(필요 없으면 DATETIME()로)

# 5) 적재 (처음은 replace로 테이블 자동 생성, 이후는 append 권장)
df.to_sql(
    TABLE_NAME,
    con=engine,
    if_exists="replace",   # 운영에서는 보통 append + 사전 DDL
    index=False,
    chunksize=5000,
    method="multi",
    dtype=dtype_map
)

print(f"✅ Done: {TABLE_NAME} rows={len(df)}")