import argparse
import pandas as pd

from utils import get_new_csv_filename


def filter_csv(input_filename, headers):
    df = pd.read_csv(input_filename)
    filtered_df = df[headers]
    filtered_df.to_csv(get_new_csv_filename(input_filename), index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter a CSV file by column headers.")
    parser.add_argument("--csv_filename", help="The name of the CSV file to be processed.")
    parser.add_argument("--headers", nargs='+', help="The list of headers to keep in the CSV file.")

    args = parser.parse_args()
    filter_csv(args.csv_filename, args.headers)
