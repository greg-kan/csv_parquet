import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def routine():
    print('Starting')

    csv_data = pd.read_csv("data/brokerage_monthly_money_202404221716.csv")
    print(csv_data)

    # table = pa.Table.from_pandas(csv_data)
    # pq.write_table(table, "data/brokerage_monthly_money_202404221716.parquet")
    #
    # partition_col = 'report_id'
    # pq.write_to_dataset(table, root_path='data/partitioned_data', partition_cols=[partition_col], compression='snappy')

    partitioned_data = pd.read_parquet('data/partitioned_data/')
    print(partitioned_data)


if __name__ == "__main__":
    routine()
