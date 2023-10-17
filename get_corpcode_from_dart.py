import argparse
import csv
import tqdm


from utils import get_new_csv_filename, list_to_csv, open_xml


def find_corp_by_stock_code(xml_root, stock_code) -> dict:
    # xml에서 list tag iterate
    for list_element in xml_root.findall('list'):
        # 인자로 받은 csv file의 stock_code와 같은 stock_code 갖는 xml 속 data 찾기
        if list_element.find('stock_code').text == stock_code:
            result = {}
            # 찾으면 save child
            for child in list_element:
                result[child.tag] = child.text
            return result


def append_to_list(elem: dict, results: list[dict]) -> list[dict]:
    new_list = results.copy()
    new_list.append(elem)
    return new_list


def make_csv_result(row, corp_result):
    # excluded '날짜': row['날짜']
    return {'종목코드': row['종목코드'], '종목명': row['종목명'], '시장구분': row['시장구분'],
            '종가': row['종가'], '시가총액': row['시가총액'],
            '발행주식총수': row['발행주식총수'], 'corp_code': corp_result['corp_code']}


def main(csv_filename, xml_filename):
    # xml_file (by dart)
    xml_root = open_xml(xml_filename)
    with open(csv_filename, 'rt') as csv_file:
        reader = csv.DictReader(csv_file)
        results = []
        for row in reader:
            # 1차적으로, 종목 코드로 xml 안에 데이터를 분류하자
            result = find_corp_by_stock_code(xml_root, row['종목코드'])
            if result is None:
                continue
            # 여기서 우선주, 거르는 로직이? (순환님)
            # csv 형식으로 만들어 최종 결과에 더하기
            results.append(make_csv_result(row, result))
    # csv 이름 변경해 올리기
    new_csv_filename = get_new_csv_filename(csv_filename)
    # list를 csv로 내보내기
    list_to_csv(results, new_csv_filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv_filename', type=str, help='상장사 목록 csv')
    parser.add_argument('--xml_filename', type=str, help='dart corp_code xml')

    args = parser.parse_args()

    main(args.csv_filename, args.xml_filename)
