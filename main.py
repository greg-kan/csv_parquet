import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlite3


DATA_BASE = '/home/greg/data1/evolution'
SQLITE_FOLDER = '/home/greg/data1/evolution/rosbag2/hound-1-toys-bag'
SQLITE_FILE = '2023_12_22_12_51_57_0.db3'


def csv_parquet():
    print('Starting')

    csv_data = pd.read_csv(f"{DATA_BASE}/{'csv'}/brokerage_monthly_money_202404221716.csv")
    print(csv_data)

    table = pa.Table.from_pandas(csv_data)
    pq.write_table(table, f"{DATA_BASE}/{'parquet'}/brokerage_monthly_money_202404221716.parquet")

    partition_col = 'report_id'
    pq.write_to_dataset(table, root_path=f"{DATA_BASE}/{'parquet'}/partitioned_data",
                        partition_cols=[partition_col], compression='snappy')

    partitioned_data = pd.read_parquet(f"{DATA_BASE}/{'parquet'}/partitioned_data/")
    print(partitioned_data)


def sqlite_parquet():
    sqlite_full_path = f"{SQLITE_FOLDER }/{SQLITE_FILE}"
    conn = sqlite3.connect(sqlite_full_path)
    # c = conn.cursor()
    table_name = 'messages'  # topics
    df = pd.read_sql(f'SELECT * from {table_name}', conn)
    # print(df)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f"{DATA_BASE}/{'parquet'}/hound-1-toys-bag/{table_name}.parquet")
    print('Writing completed.')
    from_parquet_data = pd.read_parquet(f"{DATA_BASE}/{'parquet'}/hound-1-toys-bag/{table_name}.parquet")
    print(len(from_parquet_data))
    print(from_parquet_data.info())


if __name__ == "__main__":
    # csv_parquet()
    sqlite_parquet()

