import argparse
import asyncio
from utils import get_new_csv_filename, list_to_csv, request_to_dart, iterate_csv


def copied_list_of_dicts(list_of_dicts: list[dict]):
    copied_list_of_dicts = list_of_dicts.copy()
    for dictionary in copied_list_of_dicts:
        for key, value in dictionary.items():
            dictionary[key] = value.replace('\n', '')
    return copied_list_of_dicts


def marshall_executives(response):
    response = copied_list_of_dicts(response)
    return list(map(lambda element:
                    {'이름': element['nm'],
                     '생년월일': element['birth_ym'],
                     '성별': element['sexdstn'],
                     '직위': element['ofcps'],
                     '등기여부': element['rgist_exctv_at'],
                     '상근여부': element['fte_at'],
                     '담당업무': element['chrg_job'],
                     '최대주주와의관계': element['mxmm_shrholdr_relate'],
                     '재직기간': element['hffc_pd'],
                     '임기만료일': element['tenure_end_on']}, response))


# 임원 현황
async def get_executives(row, year, quarter):
    response = await request_to_dart('exctvSttus.json', row, year, quarter)
    if response is None:
        return
    return marshall_executives(response['list'])


async def main(csv_filename, year, quarter):
    results = await iterate_csv(csv_filename, year, quarter, get_executives)
    new_csv_filename = get_new_csv_filename(csv_filename, year, quarter)
    list_to_csv(results, new_csv_filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv_filename', type=str, help='상장사 목록 csv')
    parser.add_argument('--year', type=str, help='년도')
    parser.add_argument('--quarter', type=str, help='분기(1,2,3,4)')

    args = parser.parse_args()
    import time
    a = time.time()
    asyncio.run(main(args.csv_filename, args.year, args.quarter))
    print(f"takes {time.time() - a}")