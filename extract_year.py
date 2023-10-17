import argparse
import pandas as pd

from utils import get_new_csv_filename


def extract_year_and_save(csv_filename):
    # Use a list comprehension to read each file into a DataFrame
    dtype_dict = {'종목코드': str, 'corp_code': str}
    df = pd.read_csv(csv_filename, dtype=dtype_dict)
    df['연도'] = pd.to_datetime(df['날짜']).dt.year
    df['종목코드']
    df = df.drop('날짜', axis=1)
    df.to_csv(get_new_csv_filename(csv_filename), index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge multiple CSV files.")

    # nargs='+' captures one or more command line arguments in a list
    parser.add_argument("--csv_filename",
                        help="The list of CSV files to be merged.")
    args = parser.parse_args()
    extract_year_and_save(args.csv_filename)
