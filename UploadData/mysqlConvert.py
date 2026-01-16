import os
import math
import pymysql
from datetime import datetime, timezone

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "1234")
MYSQL_DB = os.getenv("MYSQL_DB", "tsdb")

MEASUREMENT = os.getenv("MEASUREMENT", "raw_data")
OUT_FILE = "/Users/hoon/Desktop/personal_project/Time-Series-Lab/dataset/oyt.lp" #os.getenv("OUT_FILE", "out.lp")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

# tag는 최소만 추천(카디널리티 폭발 방지)
TAG_COLS = ["EQUIP_ID"]  # 필요하면 "SALE_CD" 추가 가능

# field는 숫자 위주
FIELD_COLS = [
    "MOLD_POS", "REAL_POWER", "CP", "AX",
    "IN_RADIUS", "OUT_RADIUS", "POWER",
    "POWER1", "POWER2", "POWER3", "POWER4", "POWER5",
]
# 문자열 field도 넣고 싶으면 여기에 추가 (용량/성능 고려)
STRING_FIELD_COLS = ["MOLD_IN_TOP", "MOLD_IN_BOT", "MOLD_OUT_TOP", "MOLD_OUT_BOT"]

def esc_tag(v: str) -> str:
    # tag key/value escape: space, comma, equals
    return (v.replace("\\", "\\\\")
             .replace(" ", "\\ ")
             .replace(",", "\\,")
             .replace("=", "\\="))

def esc_str_field(v: str) -> str:
    # string field must be double-quoted; escape backslash and quotes
    return v.replace("\\", "\\\\").replace('"', '\\"')

def is_finite(x) -> bool:
    return x is not None and isinstance(x, (int, float)) and math.isfinite(x)

def dt_to_ns(dt: datetime) -> int:
    # MySQL datetime(6) -> epoch ns
    # dt is naive; treat as local time? safest is treat as UTC if your data is UTC.
    # 여기서는 "로컬(KST)로 저장된 값"일 가능성이 높아서, 일단 naive를 UTC로 간주하지 않고
    # timezone을 붙이지 않은 채 epoch로 바꾸면 환경에 따라 달라질 수 있음.
    # 확실하게 하려면 DB 타임존 정책에 맞춰 아래를 조정해.
    if dt.tzinfo is None:
        # UTC로 가정 (원하면 KST로 바꿀 수 있음)
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)

conn = pymysql.connect(
    host=MYSQL_HOST, port=MYSQL_PORT,
    user=MYSQL_USER, password=MYSQL_PASSWORD,
    database=MYSQL_DB,
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

sql = f"""
SELECT
  EQUIP_ID, MFG_DT, SALE_CD, MOLD_POS, REAL_POWER, CP, AX,
  MOLD_IN_TOP, MOLD_IN_BOT, MOLD_OUT_TOP, MOLD_OUT_BOT,
  IN_RADIUS, OUT_RADIUS, POWER, POWER1, POWER2, POWER3, POWER4, POWER5
FROM raw_data
WHERE MFG_DT IS NOT NULL
ORDER BY MFG_DT ASC
"""

with conn.cursor() as cur, open(OUT_FILE, "w", encoding="utf-8") as f:
    cur.execute(sql)
    count = 0
    while True:
        rows = cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for r in rows:
            equip = (r.get("EQUIP_ID") or "").strip()
            mfg_dt = r.get("MFG_DT")
            if not equip or mfg_dt is None:
                continue

            # measurement + tags
            tags = [f"EQUIP_ID={esc_tag(equip)}"]

            # (옵션) SALE_CD를 tag로 넣고 싶으면:
            # sale = (r.get("SALE_CD") or "").strip()
            # if sale:
            #     tags.append(f"SALE_CD={esc_tag(sale)}")

            tag_part = ",".join(tags)

            # fields
            fields = []

            # 숫자 fields
            for c in FIELD_COLS:
                v = r.get(c)
                if is_finite(v):
                    # int vs float: MOLD_POS는 정수로 넣는 게 깔끔
                    if c == "MOLD_POS":
                        fields.append(f"{c}={int(v)}i")
                    else:
                        fields.append(f"{c}={float(v)}")

            # 문자열 fields(원하면 저장)
            for c in STRING_FIELD_COLS:
                v = r.get(c)
                if v is not None and str(v).strip() != "":
                    fields.append(f'{c}="{esc_str_field(str(v))}"')

            if not fields:
                continue

            ts_ns = dt_to_ns(mfg_dt)
            line = f"{MEASUREMENT},{tag_part} " + ",".join(fields) + f" {ts_ns}"
            f.write(line + "\n")
            count += 1

print(f"Saved line protocol: {OUT_FILE} (lines={count})")
conn.close()