from db import DB
import argparse
import asyncio


def select_with_params(corp_name, year, quarter):
    query = f'''
                SELECT ed.name, ed.birth_date, ed.sex, el.position, el.registration, el.full_time, el.job, el.relationship_with_shareholder, el.employ_duration, el.expire_date
                FROM executive_list as el
                INNER JOIN executive_details as ed on el.pid = ed.pid
                WHERE el.corp_code in (SELECT corp.corp_code
                                                FROM corp
                                                WHERE corp.stock_name = "{corp_name}") and
                                                el.year = {year} and
                                                el.quarter = {quarter}
        '''
    return query

async def main(corp_name: str, year: int, quarter: int):
    db = DB("dart_corp.db")
    db.open_connection()

    query = select_with_params(corp_name, year, quarter)

    # task = asyncio.create_task(db.run_fetch(query))
    # data = await task

    data = db.run_fetch(query)

    for exc in data:
        print(exc)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--corp_name', type=str, help='기업명(종목명)')
    parser.add_argument('--year', type=int, help='년도')
    parser.add_argument('--quarter', type=int, help='분기(1,2,3,4)')

    args = parser.parse_args()
    # asyncio.run(main(args.corp_name))
    asyncio.run(main(args.corp_name, args.year, args.quarter))

