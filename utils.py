import httpx
import xml.etree.ElementTree as ET
import asyncio
import csv
import os

# API_KEY = "31360f2f08cce6067efc45d644823d3a6cd9b20e" # 준호
API_URL = 'https://opendart.fss.or.kr/api'
API_KEY = "751ef3ce2f3fbf0f8574e9bdbe6a3a1f3a5f96be" # 동욱
# API_KEY = "5eeaf000d2cb1a1aa7ba369d5b9f2a76957d97b6" # 순환


# xml 열기
def open_xml(filename):
    tree = ET.parse(filename)
    root = tree.getroot()
    return root

# year만 얻기
def get_year_from_date(date: str):
    # 2015-12-30 -> 2015만 return
    return date.split('-')[0]


def get_reprt_code_from_quarter(quarter: str):
    if quarter == '1':
        return '11013'
    elif quarter == '2':
        return '11012'
    elif quarter == '3':
        return '11014'
    elif quarter == '4':
        return '11011'


# DART에
async def request_to_dart(url, row, year, quarter):
    whole_url = f"{API_URL}/{url}"

    # year = get_year_from_date(row['날짜'])
    reprt_code = get_reprt_code_from_quarter(quarter)
    corp_code = row['corp_code']

    async with httpx.AsyncClient() as client:
        params = {'crtfc_key': API_KEY, 'corp_code': corp_code,
                  'bsns_year': year, 'reprt_code': reprt_code}
        response = await client.get(whole_url, params=params)
        if response.json()['status'] != '000':
            print(f'{row["종목명"]}\'s data is missing')
            return
        return response.json()


def get_new_csv_filename(csv_filename: str, year, quarter) -> str:
    return f"./data/{csv_filename.split('.csv')[0]}-{year}년도-{quarter}분기.csv"


async def async_callback(row, callback, year, quarter, **kwargs):
    try:
        callback_result = asyncio.create_task(callback(row, year, quarter, **kwargs))
        await callback_result
        if callback_result is None:
            return row
        return callback_result
    except Exception as e:
        print(e, row)
        return None


async def iterate_csv(csv_filename, year, quarter, *callbacks, **kwargs):
    i = 0
    results = []
    csv_filename = os.path.join("./data", csv_filename)
    with open(csv_filename, 'rt') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            # row 하나씩 비동기로 처리
            callback_list = []
            # callback 함수가 하나 밖에 없는 경우?
            if len(callbacks) == 1:
                # executives의 case
                task = asyncio.create_task(async_callback(row, callbacks[0], year, quarter, **kwargs))
                result = await task

                try:
                    if result.result() is not None:
                        for exctv in result.result():
                            results.append({**row, **exctv})
                except Exception as e:
                    print(e, result)

                # list면 다 관계, 합치기 X
                if isinstance(result, list):
                    for each in result:
                        results.append({**row, **each})
                    continue

            else:
                result = row
                for callback in callbacks:
                    callback_list.append(async_callback(row, callback, year, quarter, **kwargs))
                # callback task 하나 씩 더하기

                # completed = await asyncio.gather(*callback_list)
                completed, _ = await asyncio.wait(callback_list)
                for task in completed[0]:
                    each_result = task.result()
                    if each_result is None:
                        continue
                    # 이 땐 개별 dict을 하나로 합치는 과정 필요
                    if isinstance(result, dict):
                        result = {**result, **each_result}
                results.append(result)

            if i % 50 == 0:
                print(f"{i} is done")

            i += 1

    return results


def list_to_csv(results: list[dict], new_filename: str):
    with open(new_filename, 'wt', newline='\n') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=results[0].keys())
        # writer = csv.DictWriter(csvfile, fieldnames=results[0].keys())
        writer.writeheader()
        for row in results:
            writer.writerow(row)