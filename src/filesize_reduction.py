import os
import pandas as pd

from src.util import read_txt


def get_files(directory, filename_prefix):
    dir_list = os.listdir(directory)
    files = []
    for fn in dir_list:
        if fn.startswith(filename_prefix):
            files.append(directory + fn)

    return files


def reduce_file(file_path, orig_directory):
    minimized_df = pd.DataFrame()
    with(open(file_path, "r")) as file:
        for chunk in pd.read_json(file, lines=True, chunksize=100_000):
            minimized_df = minimized_df.append(chunk[minimized_cols])
    new_directory = file_path.replace(orig_directory, "../out/minimized/")
    with(open(new_directory, "w")) as file:
        minimized_df.to_json(file, orient="records", lines=True)


minimized_cols = read_txt("../in/minimized_columns.txt")
directory = "../out/sampled/"
files = get_files(directory, "tweets_")
for idx, file in enumerate(files):
    reduce_file(file_path=file, orig_directory=directory)
    print("Reduced file " + str(idx) + "/" + str(len(files)))
