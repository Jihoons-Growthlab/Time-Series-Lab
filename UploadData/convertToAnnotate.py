import csv

#  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< 
#   Summary
#   일반 csv 파일을 infulxedDB에 맞게 csv 파일을 변환해주기위한 코드
#   리눅스로 진행 가능하지만 관리 편의를 위해 파이썬으로 진행
#  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>


IN_PATH = "/Users/hoon/Desktop/personal_project/Time-Series-Lab/dataset/raw_data.csv"
OUT_PATH = "/Users/hoon/Desktop/personal_project/Time-Series-Lab/dataset/raw_datConvertedAnnotated.csv"
MEASUREMENT = "raw_data"

header = [
    "#datatype measurement,tag,dateTime:RFC3339,string,long,double,double,double,string,string,string,string,double,double,double,double,double,double,double",
    "#group    true,true,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false",
    f"#default  {MEASUREMENT}",
    ",_measurement,EQUIP_ID,_time,SALE_CD,MOLD_POS,REAL_POWER,CP,AX,MOLD_IN_TOP,MOLD_IN_BOT,MOLD_OUT_TOP,MOLD_OUT_BOT,IN_RADIUS,OUT_RADIUS,POWER,POWER1,POWER2,POWER3,POWER4,POWER5",
]

with open(IN_PATH, newline="", encoding="utf-8") as fin, open(OUT_PATH, "w", newline="", encoding="utf-8") as fout:
    reader = csv.reader(fin)
    writer = csv.writer(fout)

    # write annotated header
    for line in header:
        fout.write(line + "\n")

    # read first row (original header) and skip
    original_header = next(reader, None)

    for row in reader:
        if not row or len(row) < 3:
            continue

        # 기대 컬럼 수(원본): 19개
        # EQUIP_ID,MFG_DT,SALE_CD,MOLD_POS,REAL_POWER,CP,AX,MOLD_IN_TOP,MOLD_IN_BOT,MOLD_OUT_TOP,MOLD_OUT_BOT,IN_RADIUS,OUT_RADIUS,POWER,POWER1,POWER2,POWER3,POWER4,POWER5
        # 부족하면 패딩
        if len(row) < 19:
            row = row + [""] * (19 - len(row))
        # 초과하면 잘라냄(깨진 데이터 방지)
        if len(row) > 19:
            row = row[:19]

        equip_id = row[0].strip()
        mfg_dt = row[1].strip()

        if not equip_id or not mfg_dt:
            continue

        # YYYY-MM-DD -> RFC3339
        # 이미 T가 들어있으면 그대로 두는 방어 로직
        if "T" not in mfg_dt:
            mfg_dt = mfg_dt + "T00:00:00Z"

        out_row = ["" , MEASUREMENT, equip_id, mfg_dt] + row[2:]
        writer.writerow(out_row)

print(f"Saved: {OUT_PATH}")