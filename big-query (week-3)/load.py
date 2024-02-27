import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    baseurl = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{}.parquet"
    df = pd.DataFrame()
    # range months
    for i in range(12):
        month_id = str(i+1).zfill(2)
        url = baseurl.format(month_id)
        
        # get data
        response = requests.get(url)
        try:
            response.raise_for_status()
            df_month = pd.read_parquet(io.BytesIO(response.content))
            df = pd.concat([df, df_month], ignore_index=True)
        except Exception as e:
            print(f"Failed to get release: {e}")
            
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
    df.astype(df_datatype)
    print(len(df))
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
