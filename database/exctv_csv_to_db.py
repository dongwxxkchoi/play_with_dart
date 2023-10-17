import sqlite3
import pandas as pd
import os
import argparse
import asyncio
from tqdm.notebook import tqdm
from db import DB

def make_table(db):
    executive_list_create_query = '''CREATE TABLE executive_list ( 
                        corp_code INTEGER, 
                        year INTEGER,
                        quarter INTEGER,
                        pid TEXT,
                        position TEXT,
                        registration TEXT,
                        full_time TEXT,
                        job TEXT,
                        relationship_with_shareholder TEXT,
                        employ_duration TEXT,
                        expire_date TEXT,   
                        PRIMARY KEY (corp_code, year, quarter, name) , 
                        FOREIGN KEY (corp_code) REFERENCES corp(corp_code)
                        )'''
    db.run_query(executive_list_create_query)




def map_pid(db, df):
    idtfy_df = df[['이름', '생년월일', '성별']]
    idtfy_df.drop_duplicates(inplace=True)

    query = "SELECT PID FROM executive_details WHERE (name = ? and birth_date = ? and sex = ?)"

    for i, row in idtfy_df.iterrows():
        param = (row['이름'], row['생년월일'], row['성별'])
        if db.run_fetch(query, param) is not None:
            print(df)

def insert_exctv(db: DB, df: pd.DataFrame, year: str, quarter: str) -> None:
    insert_query = '''INSERT INTO executive
                        (corp_code, year, quarter, name, birth_date, sex, position, registration, full_time, job, relationship_with_shareholder, employ_duration, expire_date) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

    pid = map_pid(df)

    for i, row in tqdm(df.iterrows()):
        bowl = (
                row['corp_code'], pid['corp-name'], year, quarter,
                row['생년월일'], row['성별'], row['직위'], row['등기여부'], row['상근여부'],
                row['담당업무'], row['최대주주와의관계'], row['재직기간'], row['임기만료일'])
        try:
            db.run_query(insert_query, bowl)
        except Exception as e:
            print(e, bowl)

def csv_to_df(year: str, quarter: str) -> pd.DataFrame:
    folder_path = "../data"
    file_name = f"2021_2022-상장사-drop_dup-{year}년도-{quarter}분기.csv"
    file_path = os.path.join(folder_path, file_name)

    drop_cols = ['발행주식총수', '시가총액', '종가']
    exctv_df = pd.read_csv(file_path, encoding='utf-8', dtype=object).drop(columns = drop_cols)

    return exctv_df


async def main(csv_filename: str, year: str, quarter: str):
    db = DB("dart_corp_old.db")
    db.open_connection()
    exctv_df = csv_to_df(year, quarter)
    try:
        make_table(db)
    except Exception as e:
        print("Table already created")

    insert_exctv(exctv_df, year, quarter)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # parser.add_argument('--csv_filename', type=str, help='임원 목록 csv file')
    parser.add_argument('--year', type=str, help='년도')
    parser.add_argument('--quarter', type=str, help='분기(1,2,3,4)')

    args = parser.parse_args()
    asyncio.run(main(args.csv_filename, args.year, args.quarter))
