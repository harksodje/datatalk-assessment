import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
import time
from typing import List

credentials = service_account.Credentials.from_service_account_file(
    'google-acc.json',
)
print(type(credentials))
project_id = 'integrated-net-411608'
dataset = 'data_source_trips'
years = [2019, 2020]
months = 12
chunksize = 5000000

# yellow taxi data
table_yellow = 'yellow_taxi_data_2019_2020'
baseurl_yellow = \
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{}-{}.csv.gz"
yellow_cols_rename = {'tpep_pickup_datetime': 'lpep_pickup_datetime', 
                    'tpep_dropoff_datetime': 'lpep_dropoff_datetime'}
yellow_cols_date  = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']


# green taxi data
table_green = 'green_taxi_data_2019_2020'
baseurl_green = \
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{}-{}.csv.gz"
green_cols_rename = {}
green_cols_date = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']


# schema for yellow and green taxi data
df_datatype = {
        "VendorID": pd.Float64Dtype(),
        "store_and_fwd_flag": pd.CategoricalDtype(),
        "RatecodeID": pd.Int64Dtype(),
        "PULocationID": pd.Int64Dtype(),
        "DOLocationID": pd.Int64Dtype(),
        "passenger_count": pd.Float64Dtype(),
        "trip_distance": pd.Float64Dtype(),
        "fare_amount": pd.Float64Dtype(),
        "extra": pd.Float64Dtype(),
        "mta_tax": pd.Float64Dtype(),
        "tip_amount": pd.Float64Dtype(),
        "tolls_amount": pd.Float64Dtype(),
        # "ehail_fee": pd.Float64Dtype(),
        "improvement_surcharge": pd.Float64Dtype(),
        "total_amount": pd.Float64Dtype(),
        "payment_type": pd.CategoricalDtype(),
        "trip_type": pd.Float64Dtype(),
        "congestion_surcharge": pd.Float64Dtype(),
}

# fhv data
fhv_cols_rename = {"dropOff_datetime": 'dropoff_datetime'}
fhv_cols_dates = ['pickup_datetime', 'dropOff_datetime']
baseurl_fhv = \
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{}-{}.csv.gz"

# fhv schema
fhv_data_type = {
    "dispatching_base_num": pd.CategoricalDtype(),
    "PULocationID": pd.Int64Dtype(),
    "DOLocationID": pd.Int64Dtype(),
    "SR_Flag": pd.CategoricalDtype(),
    "Dispatching_base_num": pd.CategoricalDtype()
}

# fhv url 


def api_to_bigquery(
    project_id: str,
    dataset: str,
    table: str,
    credentials: any,
    base_url: str,
    data_type: dict ,
    years: list[int],
    cols_date : List[any] = None, 
    cols_rename: dict = None,
    months: int = 1,
    chunksize: int = 100000
):
    """Uploads data from API to BigQuery with chunking."""
    # table id in bigquery format
    table_id = f'{dataset}.{table}'
    # years to access from the source
    for year in years:
        # months to access from the source
        for month in range(1, months+1):
            
            # reconstruct the url with the year and month
            url = base_url.format(str(year), str(month).zfill(2))
            print(url)
            # read the data from the url with chunking
            df = pd.read_csv(url, compression='gzip', chunksize=chunksize,
                             dtype=data_type, parse_dates=cols_date
                             )
            print(  "months", months, "year", year,)
            try:
                # upload the data to bigquery with chunking
                while True:
                    start_time = time.time()
                    chunk_df = next(df)
                    if cols_rename is not None:
                        chunk_df.rename(columns=cols_rename, inplace=True)
                    pandas_gbq.to_gbq(chunk_df, table_id, project_id=project_id, 
                                      if_exists='append', credentials=credentials)
                    end_time = time.time()
                    print(f'Uploaded {len(chunk_df)} rows from {year}-{month}')
                    execution_time = (end_time - start_time)/60
                    print(f"""
                          Execution time to upload a chunck of data {execution_time} mins @ ::: 
                          {len(chunk_df)} rows from {year}-{month}""")
            except StopIteration as e:
                print(e)


# # upload yellow taxi data

api_to_bigquery(
    project_id=project_id,
    dataset=dataset,
    table=table_yellow,
    credentials=credentials,
    base_url=baseurl_yellow,
    data_type=df_datatype,
    years=years,
    months=months,
    chunksize=chunksize,
   cols_date=yellow_cols_date,
   cols_rename=yellow_cols_rename
)

# upload green taxi data
api_to_bigquery(
    project_id=project_id,
    dataset=dataset,
    table=table_green,
    credentials=credentials,
    base_url=baseurl_green,
    data_type=df_datatype,
    years=years,
    months=months,
    chunksize=chunksize,
  cols_date=green_cols_date,
)

# upload fhv data

api_to_bigquery(
    project_id=project_id,
    dataset=dataset,
    table='fhv_tripdata_2019_2020',
    credentials=credentials,
    base_url=baseurl_fhv,
    data_type={},
    years=[2019],
    months=months,
    chunksize=chunksize,
    cols_date = fhv_cols_dates,
    cols_rename=fhv_cols_rename
)