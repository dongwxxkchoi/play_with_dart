import argparse
import pandas as pd

from utils import get_new_csv_filename


def merge_csv(filenames):
    # Use a list comprehension to read each file into a DataFrame
    input_filename = filenames[0]
    dfs = [pd.read_csv(filename) for filename in filenames]
    merged_df = pd.concat(dfs, ignore_index=True)
    merged_df.to_csv(get_new_csv_filename(input_filename), index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge multiple CSV files.")

    # nargs='+' captures one or more command line arguments in a list
    parser.add_argument("--csv_filenames", nargs='+',
                        help="The list of CSV files to be merged.")
    args = parser.parse_args()
    merge_csv(args.csv_filenames)
