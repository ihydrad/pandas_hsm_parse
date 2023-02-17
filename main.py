import argparse
import os
import pandas as pd


def gazer_files(fips: bool, csv_path: str = None) -> list:
    cur_dir = csv_path or os.getcwd()

    for file in os.listdir(cur_dir):
        file = os.path.join(cur_dir, file)
        if file.endswith(".csv"):
            yield file


def get_data_from_file(file_path: str, mark: str = None) -> pd.DataFrame:
    df=pd.read_csv(file_path)

    try:
        df.drop(df.columns[0], axis=1, inplace=True)  # drop head: Elliptic curve: GOST R 34.10-2001 CryptoPro A (256 bits)
        df.reset_index(inplace=True)
        df.columns = df.iloc[0]  #  head names = row[0]
        df.drop([0, ], axis=0, inplace=True)  # drop row[0]
        df["file"] = file_path
        if mark:
            df["type"] = mark
    except:
        print("Error parse: ", file_path)

    return df


def get_data_from_files(files: list, mark: str = None) -> pd.DataFrame:
    all_data = pd.DataFrame()

    for file in files:
        all_data = all_data.append(get_data_from_file(file_path=file, mark=mark), ignore_index=True)

    return all_data


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    import numpy as np
    print("prepare")
    df.dropna(axis=1, how='all', inplace=True)
    df.drop(["time_unit", ], axis=1, inplace=True)
    df["threads"] = df["name"].astype("str").str.split("/").str[-1].str.split("threads:").str[-1]
    df["block_size"] = df[df["name"].astype("str").str.contains("Block size:")]["name"].astype("str").str.split("Block size:").str[-1].str.split("/").str[0]
    df["session"] = df[df["name"].astype("str").str.contains("session:")]["name"].astype("str").str.split("session:").str[-1].str.split("/").str[0]
    df["group"] = df["name"].astype("str").str.split("/", n=1).str[0]
    df["func"] = df["name"].astype("str").str.split("/", n=2).str[1]
    df.loc[df["func"].astype("str").str.contains("session|Block size|threads", regex=True), ["func", ]] = np.nan
    df.loc[df["func"].isnull(), ["func", ]] = df[df["func"].isnull()]["group"]
    
    df["iterations"]=df["iterations"].astype("int32")
    df["threads"]=df["threads"].astype("int32")
    df["session"] = df[~df["session"].isnull()]["session"].astype("int32")
    df["bytes_per_second"] = df[~df["bytes_per_second"].isnull()]["bytes_per_second"].astype("float").round(0).astype("int32")
    df["items_per_second"] = df[~df["items_per_second"].isnull()]["items_per_second"].astype("float").round(0).astype("int32")
    df["block_size"] = df[~df["block_size"].isnull()]["block_size"].astype("float").round(0).astype("int32")
    df["cpu_time"] = df["cpu_time"].astype("float").round(0).astype("int32")
    df["real_time"] = df["real_time"].astype("float").round(0).astype("int32")
    
    df = df[["type", "group", "func",  "block_size", "iterations", "threads", "session", "cpu_time", "real_time", "items_per_second", "bytes_per_second"]]
    
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parser hsm csv data (performance)')
    
    parser.add_argument('--fips', type=str, help='Folder results for fips')
    parser.add_argument('--gost', type=str, help='Folder results for gost')

    args = parser.parse_args()

    df = pd.DataFrame()
    fips_files = []
    gost_files= []

    if args.gost:  
        gost_files = gazer_files(csv_path=args.gost, fips=False)
        df_gost = get_data_from_files(files=gost_files, mark="GOST")
        df = df.append(df_gost, ignore_index=True)

    if args.fips:
        fips_files = gazer_files(csv_path=args.fips, fips=True)
        df_fips = get_data_from_files(files=fips_files, mark="FIPS")
        df = df.append(df_fips, ignore_index=True)
    
    if not df.empty:
        df = prepare(df)
        df.to_excel("output.xlsx")
        print(df)
