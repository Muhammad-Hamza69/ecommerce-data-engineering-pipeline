import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_CONN_ID = "snowflake_conn"

# ----------------------------
# Default args
# ----------------------------
default_args = {
    "owner": "snowflakedatapipelinepro",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ----------------------------
# DAG Definition
# ----------------------------
dag = DAG(
    dag_id="customer_orders_datapipeline_dynamic_batch_id",
    default_args=default_args,
    description="Runs data pipeline",
    start_date=datetime.datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
)

# ----------------------------
# S3 Tasks (dynamic batch_id)
# ----------------------------
task_customer_landing_to_processing = BashOperator(
    task_id="customer_landing_to_processing",
    bash_command="""
    aws s3 cp s3://s3-ecommerce-data-pipeline-mhs/raw/customers/ \
    s3://s3-ecommerce-data-pipeline-mhs/transform/{{ ts_nodash }}/ --recursive
    """,
    dag=dag,
)

task_customers_processing_to_processed = BashOperator(
    task_id="customer_processing_to_processed",
    bash_command="""
    aws s3 mv s3://s3-ecommerce-data-pipeline-mhs/transform/{{ ts_nodash }}/ \
    s3://s3-ecommerce-data-pipeline-mhs/processed/{{ ts_nodash }}/ --recursive
    """,
    dag=dag,
)

task_orders_landing_to_processing = BashOperator(
    task_id="orders_landing_to_processing",
    bash_command="""
    aws s3 cp s3://s3-ecommerce-data-pipeline-mhs/raw/orders/ \
    s3://s3-ecommerce-data-pipeline-mhs/transform/{{ ts_nodash }}/ --recursive
    """,
    dag=dag,
)

task_orders_processing_to_processed = BashOperator(
    task_id="orders_processing_to_processed",
    bash_command="""
    aws s3 mv s3://s3-ecommerce-data-pipeline-mhs/transform/{{ ts_nodash }}/ \
    s3://s3-ecommerce-data-pipeline-mhs/processed/{{ ts_nodash }}/ --recursive
    """,
    dag=dag,
)

post_task = BashOperator(
    task_id="post_dbt",
    bash_command="echo 0",
    dag=dag,
)

# ----------------------------
# Snowflake Hook Helper
# ----------------------------
def get_snowflake_hook():
    return SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse="COMPUTE_WH",
        database="RETAIL_DB",
        schema="RETAIL_SCHEMA",
        role="ACCOUNTADMIN",
    )

# ----------------------------
# Snowflake Tasks
# ----------------------------
def run_orders_query():
    context = get_current_context()
    batch_id = context["ts_nodash"]

    hook = get_snowflake_hook()

    query = f"""
    COPY INTO RETAIL_DB.RETAIL_SCHEMA.ORDERS_RAW
    FROM (
        SELECT '{batch_id}', t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9
        FROM @ORDERS_RAW_STAGE t
    );
    """

    hook.run(query)


def run_customers_query():
    context = get_current_context()
    batch_id = context["ts_nodash"]

    hook = get_snowflake_hook()

    query = f"""
    COPY INTO RETAIL_DB.RETAIL_SCHEMA.CUSTOMERS_RAW
    FROM (
        SELECT '{batch_id}', t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8
        FROM @CUSTOMER_RAW_STAGE t
    );
    """

    hook.run(query)


def run_transform_query():
    hook = get_snowflake_hook()

    query = """
    INSERT INTO RETAIL_DB.RETAIL_SCHEMA.ORDER_CUSTOMER_DATE_PRICE
    SELECT
        c.C_NAME,
        o.O_ORDERDATE,
        SUM(o.O_TOTALPRICE),
        c.C_BATCH_ID
    FROM RETAIL_DB.RETAIL_SCHEMA.ORDERS_RAW o
    JOIN RETAIL_DB.RETAIL_SCHEMA.CUSTOMERS_RAW c
        ON o.O_CUSTKEY = c.C_CUSTKEY
        AND o.O_BATCH_ID = c.C_BATCH_ID
    WHERE o.O_ORDERSTATUS = 'F'
    GROUP BY c.C_NAME, o.O_ORDERDATE, c.C_BATCH_ID
    ORDER BY o.O_ORDERDATE;
    """

    hook.run(query)

# ----------------------------
# Python Tasks
# ----------------------------
snowflake_orders_task = PythonOperator(
    task_id="snowflake_raw_insert_order",
    python_callable=run_orders_query,
    dag=dag,
)

snowflake_customers_task = PythonOperator(
    task_id="snowflake_raw_insert_customers",
    python_callable=run_customers_query,
    dag=dag,
)

snowflake_transform_task = PythonOperator(
    task_id="snowflake_order_customers_small_transformation",
    python_callable=run_transform_query,
    dag=dag,
)

# ----------------------------
# Dependencies
# ----------------------------
[
    task_orders_landing_to_processing
    >> snowflake_orders_task
    >> task_orders_processing_to_processed,

    task_customer_landing_to_processing
    >> snowflake_customers_task
    >> task_customers_processing_to_processed,
] >> snowflake_transform_task >> post_task