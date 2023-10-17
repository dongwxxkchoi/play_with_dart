import argparse
from utils import get_new_csv_filename, request_to_dart, iterate_csv, list_to_csv
import asyncio

# API_KEY = ""
API_URL = 'https://opendart.fss.or.kr/api'


def get_dict_by_key(response: list[dict], **kwargs) -> dict:
    for dictionary in response:
        if all(k in dictionary and dictionary[k] == v for k, v in kwargs.items()):
            return dictionary


def get_dict_with_max_value(response: list[dict], key):
    def parse_string_to_int(val: str) -> int:
        try:
            return int(val.replace(',', ''))
        except ValueError:
            return 0
    return max(response, key=lambda x: parse_string_to_int(x.get(key, 0)))


def marshall_floating_shares(response):
    element = get_dict_by_key(response, se='합계')
    return {'자사주': element['tesstk_co'],
            '유통주식수': element['distb_stock_co']}


def marshall_largest_shareholders(response):
    element = get_dict_with_max_value(response, 'trmend_posesn_stock_co')
    largest_shareholders_shares = element['trmend_posesn_stock_co']
    return {'최대주주주식수': largest_shareholders_shares}


def marshall_small_shareholders(response):
    element = get_dict_by_key(response, se='소액주주')
    return {'소액주주수': element['shrholdr_co'],
            '소액주주주식수': element['hold_stock_co'],
            '소액주주지분율': element['hold_stock_rate']}


# 주식 총수 현황
async def get_floating_shares(row):
    response = await request_to_dart('stockTotqySttus.json', row)
    if response is None:
        return
    return marshall_floating_shares(response['list'])

# 최대 주주 현황
async def get_largest_shareholders(row):
    response = await request_to_dart('hyslrSttus.json', row)
    if response is None:
        return
    return marshall_largest_shareholders(response['list'])

# 소액 주주 현황
async def get_small_shareholders(row):
    response = await request_to_dart('mrhlSttus.json', row)
    if response is None:
        return
    return marshall_small_shareholders(response['list'])


async def main(csv_filename):
    results = await iterate_csv(csv_filename, get_floating_shares,
                                get_largest_shareholders, get_small_shareholders)
    new_csv_filename = get_new_csv_filename(csv_filename)
    list_to_csv(results, new_csv_filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv_filename', type=str, help='상장사 목록 csv')

    args = parser.parse_args()
    import time
    a = time.time()
    asyncio.run(main(args.csv_filename))
    print(f"takes {time.time() - a}")
