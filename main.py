import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


DATA_BASE = '/home/greg/data1/evolution'


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
    pass


if __name__ == "__main__":
    csv_parquet()
    sqlite_parquet()

