import argparse
import os
import pandas as pd
import numpy as np

from pathlib import Path
from typing import Optional


def gazer_files(csv_path: Optional[str] = None) -> list:
    cur_dir = csv_path or os.getcwd()
    for file in os.listdir(cur_dir):
        file = os.path.join(cur_dir, file)
        if file.endswith(".csv"):
            yield file


def get_data_from_file(file_path: str,
                       mark: Optional[str] = None) -> pd.DataFrame:
    str_data = Path(file_path).read_text()
    header_row_position = None
    for cnt, line in enumerate(str_data.split("\n")):
        if line.startswith("name"):
            header_row_position = cnt
            break

    if header_row_position is None:
        raise Exception("header_row_position not found!")

    df = pd.read_csv(file_path, header=header_row_position-1)

    try:
        params = df.iloc[0, ].index.values[0]
        df.reset_index(inplace=True)
        df.columns = df.iloc[0]
        df.drop([0, ], axis=0, inplace=True)
        df["file"] = file_path
        if mark:
            df["type"] = mark
    except:
        print("Error parse: ", file_path)
    df = df.assign(params=params)
    return df


def get_data_from_files(files: list, mark: str = None) -> pd.DataFrame:
    all_data = pd.DataFrame()
    for file in files:
        all_data = pd.concat([all_data, get_data_from_file(file, mark)],
                             ignore_index=True)
    return all_data


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    df.drop(["time_unit", ], axis=1, inplace=True)
    df["threads"] = df["name"].astype("str").str.split("/").str[-1].str.split("threads:").str[-1]
    df["block_size"] = df[df["name"].astype("str").str.contains("Block size:")]["name"].astype("str").str.split("Block size:").str[-1].str.split("/").str[0]
    df["session"] = df[df["name"].astype("str").str.contains("session:")]["name"].astype("str").str.split("session:").str[-1].str.split("/").str[0]
    df["group"] = df["name"].astype("str").str.split("/", n=1).str[0]
    df["func"] = df["name"].astype("str").str.split("/", n=2).str[1]
    df.loc[df["func"].astype("str").str.contains("session|Block size|threads", regex=True), ["func", ]] = np.nan
    df.loc[df["func"].isnull(), ["func", ]] = df[df["func"].isnull()]["group"]

    df["iterations"] = df["iterations"].astype(np.uint64)
    df["threads"] = df["threads"].astype(np.uint64)
    df["session"] = df[~df["session"].isnull()]["session"].astype(np.uint64)
    df["bytes_per_second"] = df[~df["bytes_per_second"].isnull()]["bytes_per_second"].astype("float").round(0).astype(np.uint64)
    df["items_per_second"] = df[~df["items_per_second"].isnull()]["items_per_second"].astype("float").round(0).astype(np.uint64)
    df["block_size"] = df[~df["block_size"].isnull()]["block_size"].astype("float").round(0).astype(np.uint64)
    df["cpu_time"] = df["cpu_time"].astype("float").round(0).astype(np.uint64)
    df["real_time"] = df["real_time"].astype("float").round(0).astype(np.uint64)

    order = ["type", "params", "group", "func",  "block_size", "iterations",
             "threads", "session", "cpu_time", "real_time", "items_per_second",
             "bytes_per_second"]

    return df[order]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--fips', type=str, help='Folder results for fips')
    parser.add_argument('--gost', type=str, help='Folder results for gost')

    args = parser.parse_args()

    df = pd.DataFrame()

    if args.gost:
        gost_files = gazer_files(args.gost)
        df_gost = get_data_from_files(gost_files, "GOST")
        df = pd.concat([df, df_gost], ignore_index=True)

    if args.fips:
        fips_files = gazer_files(args.fips)
        df_fips = get_data_from_files(fips_files, "FIPS")
        df = pd.concat([df, df_fips], ignore_index=True)

    if not df.empty:
        df = prepare(df)
        df.to_excel("output.xlsx")
        print(df)
