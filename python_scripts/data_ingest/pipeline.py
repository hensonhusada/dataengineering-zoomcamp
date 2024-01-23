from sqlalchemy import create_engine
from time import time
import pandas as pd
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--host", default="localhost")
parser.add_argument("-p", "--port", type=int, default=5432)
parser.add_argument("-d", "--database", default="public")
parser.add_argument("user")
parser.add_argument("password")
parser.add_argument("url", type=str)
parser.add_argument("table_name", type=str)
args = parser.parse_args()

def main(args):
    host = args.host
    port = args.port
    db = args.database
    db_user = args.user
    db_pass = args.password
    url = args.url
    table_name = args.table_name

    if url.endswith(".csv.gz"):
        output_file = "output.csv.gz"
    else:
        output_file = "output.csv"

    print("DOWNLOADING FILE")
    os.system(f"wget {url} -O {output_file}")

    print("CREATING ENGINE")
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{host}:{port}/{db}")

    df_iter = pd.read_csv(output_file, iterator=True, chunksize=10000)

    df = next(df_iter)

    df.head(0).to_sql(table_name, engine, if_exists="replace")

    df.to_sql(table_name, engine, if_exists="append")

    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.to_sql(table_name, engine, if_exists="append")
            t_end = time()
            print(f"inserted another chunk of data, took {t_end-t_start}seconds")
        except StopIteration:
            print("Finished inserting data")
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser("DOWNLOAD CSV FILE TO POSTGRES")

    parser.add_argument("-t", "--host", default="localhost")
    parser.add_argument("-p", "--port", type=int, default=5432)
    parser.add_argument("-d", "--database", default="public")
    parser.add_argument("user")
    parser.add_argument("password")
    parser.add_argument("url", type=str)
    parser.add_argument("table_name", type=str)

    args = parser.parse_args()

    main(args)