
## 다트 크롤링

### 파일 구조
```
// pandas로 csv 조작 모듈
extract_year.py: 2015-12-30 -> 2015 추출해서 저장
filter_csv.py: csv 파일 내에서 특정 헤더만 추출해서 저장
merge_csv.py: 여러 csv 파일을 하나로 합침

get_corpcode_from_dart.py: dart API 요청 보낼 시 필요한 corp_code를 xml 내에서 찾는 모듈
get_dart_info.py: 소액주주, 자사주, 최대주주 주식 수 등 개별 행에 1개씩 있는 데이터
get_executives.py: 임원진 등 개별 행에 여러 개씩 있는 데이터
```


### install
```bash
python3 -m venv venv
pip install -r requirements.txt
```

### RUN

```bash
python filter_csv.py --csv_filename data/2015_2017-상장사.csv --headers 종목코드
python merge_csv.py --csv_filenames data/2015_2017-상장사.csv data/2018_2020-상장사.csv

python get_dart_info.py --csv_filename data/상장사-corp_code.csv
python get_executives_api.py --csv_filename data/상장사-corp_code.csv
```

### TODO
- [ ] 1 혹은 다 여부에 따른 추출 쉽게 조절하게 만들기