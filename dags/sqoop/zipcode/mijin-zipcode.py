from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
import pendulum
from mario_airflow_utils import gen_bash_task

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'pd24',
    'depends_on_past': True,
    'start_date': datetime(year=2023, month=6, day=13, hour=0, minute=0, tzinfo=local_tz),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}
test_dag = DAG(
    dag_id = 'mimi-mijin-zipcode',
    description= 'pd24 mimi mijin zipcode pipeline',
    tags= ['sqoop','hive'],
    max_active_runs= 1,
    concurrency= 10,
    # schedule_interval = '10 8 * * 1-5', # 매일 오전 8시 10분 월화수목금만 실행
    schedule_interval= '10 8 * * *', # 매일 오전 8시 10분 실행
    user_defined_macros={'local_dt': lambda execution_date: execution_date.in_timezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")},
    # user_defined_macros={'local_dt': lambda ds: ds.in_timezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")},
    default_args = default_args
    )

# Define the BashOperator task
# https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html

check_execute_task = BashOperator(
    task_id='check.execute',
    bash_command="""   
        echo "date                            => `date`"        
        echo "logical_date                    => {{logical_date}}"
        echo "execution_date                  => {{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}"
        echo "next_execution_date             => {{next_execution_date.strftime("%Y-%m-%d %H:%M:%S")}}"
        echo "prev_execution_date             => {{prev_execution_date.strftime("%Y-%m-%d %H:%M:%S")}}"
        echo "local_dt(execution_date)        => {{local_dt(execution_date)}}"
        echo "local_dt(next_execution_date)   => {{local_dt(next_execution_date)}}"
        echo "local_dt(prev_execution_date)   => {{local_dt(prev_execution_date)}}"
        echo "===================================================================="
        echo "data_interval_start             => {{data_interval_start}}"
        echo "data_interval_end               => {{data_interval_end}}"
        echo "ds => {{ds}}"
        echo "ds_nodash => {{ds_nodash}}"
        echo "ds_nodash => {{ds_nodash}}"
        echo "ts  => {{ts}}"
        echo "ts_nodash_with_tz  => {{ts_nodash_with_tz}}"
        echo "prev_data_interval_start_success  => {{prev_data_interval_start_success}}"
        echo "prev_data_interval_end_success => {{prev_data_interval_end_success}}"
        echo "prev_data_interval_end_success => {{prev_data_interval_end_success}}"
        echo "prev_start_date_success => {{prev_start_date_success}}"
        echo "dag => {{dag}}"
        echo "task => {{task}}"
        echo "macros => {{macros}}"
        echo "task_instance => {{task_instance}}"
        echo "ti => {{ti}}"
        echo "====================================================================="
        echo "dag_run.logical_date => {{dag_run.logical_date}}"
        echo "execution_date => {{execution_date}}"
        echo "====================================================================="
        #2020-11-11 형식의 날짜 반환
        echo "exe_kr = {{execution_date.add(hours=9).strftime("%Y-%m-%d")}}"
        #20201212 형식의 날짜 반환
        echo "exe_kr_nodash = {{execution_date.add(hours=9).strftime("%Y%m%d")}}"
        #2020-11-11 형식의 날짜 반환 + 한달 더하기
        echo "exe_kr_add_months = {{execution_date.add(hours=9).add(months=1).strftime("%Y-%m-%d")}}"
        #2020-11-11 형식의 날짜 반환 + 하루 더하기
        echo "exe_kr_add_days = {{execution_date.add(hours=9).add(days=1).strftime("%Y-%m-%d")}}"
        #2020-11-11 형식의 날짜 반환 - 일주일 빼기
        echo "exe_kr_a_week_ago = {{execution_date.add(hours=9).add(days=-7).strftime("%Y-%m-%d")}}"
        #2020-11-11 형식의 날짜 반환 - 한달 빼기
        echo "exe_kr_a_month_ago = {{execution_date.add(hours=9).add(months=-1).strftime("%Y-%m-%d")}}"
        #2020-11-11 형식의 날짜 반환 - 1년 빼기
        echo "exe_kr_1_year_ago = {{execution_date.add(hours=9).add(years=-1).strftime("%Y-%m-%d")}}"
        #2020-11-11 형식의 날짜 반환 - 2년 빼기
        echo "exe_kr_2_year_ago = {{execution_date.add(hours=9).add(years=-2).strftime("%Y-%m-%d")}}"
        #2020-11-11 형식의 날짜 반환 - 하루 빼기
        echo "exe_kr_yesterday = {{execution_date.add(hours=9).add(days=-1).strftime("%Y-%m-%d")}}"
        echo "====================================================================="
     
    """,
    dag = test_dag
)


HQL_PATH = '/home/mijin/code/data-pipeline/dags/sqoop/zipcode/'

# POST CODE
load_zipcode = gen_bash_task("load.zipcode", f"hive -f {HQL_PATH}/zipcode1/load.hql", test_dag)
raw_zipcode = gen_bash_task("raw_zipcode", f"hive -f {HQL_PATH}/zipcode1/raw.hql --hivevar part_value=" + "\"'{{ ds }}'\"", test_dag)
base_zipcode = gen_bash_task("base.zipcode", f"hive -f {HQL_PATH}/zipcode1/base.hql --hivevar part_value=" + "\"'{{ ds }}'\"", test_dag)

# UNIQUE 구/동 테이블
base_gudong_mst = gen_bash_task("base.gudong.mst", f"hive -f{HQL_PATH}/gudong/base.hql --hivevar part_value=" + "\"'{{ ds }}'\"", test_dag)

# For my information
# "\"'{{ ds }}'\""
# "\"'2023-01-01'\""
# ""'2023-01-01'""
# "'2023-01-01'"
# example : a="abc\"aaaa"


# DummyOperator
end_task = DummyOperator(task_id="end", dag=test_dag)

#WF
check_execute_task >> load_zipcode

load_zipcode >> raw_zipcode >> base_zipcode >> base_gudong_mst >> end_task