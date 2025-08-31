from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import boto3


s3_client = boto3.client('s3')

target_bucket_name = "redfin-transformed-data-bkt"

# url link from ~ https://www.redfin.com/news/data-center/
url_by_city = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz'

def extract_data(**kwargs):
    url = kwargs['url']
    date_now_string = datetime.now().strftime("%d%m%Y%H%M%S")
    file_str = f"redfin_data_{date_now_string}"
    output_file_path = f"/home/ubuntu/{file_str}.csv"

    # Stream read in chunks to avoid OOM
    chunks = pd.read_csv(url, compression="gzip", sep="\t", chunksize=100000)

    with open(output_file_path, "w", encoding="utf-8") as f:
        first = True
        for chunk in chunks:
            chunk.to_csv(f, index=False, header=first)
            first = False

    # Return dict for clean XCom access
    return {"output_file_path": output_file_path, "object_key": file_str}
    
def transform_data(**kwargs):
    ti = kwargs['ti']
    output_dict = ti.xcom_pull(task_ids="task_extract_redfin_data")

    data_path = output_dict["output_file_path"]
    object_key = output_dict["object_key"]

    cols = [
        'PERIOD_BEGIN','PERIOD_END','PERIOD_DURATION','REGION_TYPE','REGION_TYPE_ID','TABLE_ID',
        'IS_SEASONALLY_ADJUSTED','CITY','STATE','STATE_CODE','PROPERTY_TYPE','PROPERTY_TYPE_ID',
        'MEDIAN_SALE_PRICE','MEDIAN_LIST_PRICE','MEDIAN_PPSF','MEDIAN_LIST_PPSF','HOMES_SOLD',
        'INVENTORY','MONTHS_OF_SUPPLY','MEDIAN_DOM','AVG_SALE_TO_LIST','SOLD_ABOVE_LIST',
        'PARENT_METRO_REGION_METRO_CODE','LAST_UPDATED'
    ]

    # Open file in chunks to avoid memory crash
    chunksize = 100_000
    transformed_chunks = []

    for chunk in pd.read_csv(data_path, chunksize=chunksize):
        
        chunk['CITY'] = chunk['CITY'].str.replace(',', '')
        
        chunk = chunk[cols].dropna()

        chunk['PERIOD_BEGIN'] = pd.to_datetime(chunk['PERIOD_BEGIN'])
        chunk['PERIOD_END'] = pd.to_datetime(chunk['PERIOD_END'])

        chunk["PERIOD_BEGIN_IN_YEARS"] = chunk['PERIOD_BEGIN'].dt.year
        chunk["PERIOD_END_IN_YEARS"] = chunk['PERIOD_END'].dt.year
        chunk["PERIOD_BEGIN_IN_MONTHS"] = chunk['PERIOD_BEGIN'].dt.month
        chunk["PERIOD_END_IN_MONTHS"] = chunk['PERIOD_END'].dt.month

        month_map = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
        chunk["PERIOD_BEGIN_IN_MONTHS"] = chunk["PERIOD_BEGIN_IN_MONTHS"].map(month_map)
        chunk["PERIOD_END_IN_MONTHS"] = chunk["PERIOD_END_IN_MONTHS"].map(month_map)

        transformed_chunks.append(chunk)

    # Concatenate only after processing chunks
    df = pd.concat(transformed_chunks, ignore_index=True)

    print('Num of rows:', len(df))
    print('Num of cols:', len(df.columns))

    csv_data = df.to_csv(index=False)
    s3_client.put_object(Bucket=target_bucket_name, Key=f"{object_key}.csv", Body=csv_data)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG('redfin_analytics_dag',
         default_args=default_args,
        #  schedule_interval=@weekly),
         catchup=False) as dag:

         extract_redfin_data = PythonOperator(
            task_id='task_extract_redfin_data',
            python_callable=extract_data,
            op_kwargs={'url': url_by_city}
         )

         transform_redfin_data = PythonOperator(
            task_id='task_transform_redfin_data',
            python_callable=transform_data,
         )

         load_to_s3 = BashOperator(
            task_id='task_load_to_s3',
            bash_command='aws s3 mv {{ task_instance.xcom_pull(task_ids="task_extract_redfin_data")["output_file_path"] }} s3://redfin-raw-data-bkt'
         )
         
         extract_redfin_data >> transform_redfin_data >> load_to_s3