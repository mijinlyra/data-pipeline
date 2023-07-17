from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 4),
    'retries': 0,
}
test_dag = DAG(
    'mimi_spotify',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

def gen_bash_task(name: str, cmd: str, trigger="all_success"):
        """airflow bash task 생성
            - trigger-rules : https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#trigger-rules
    """
    bash_task = BashOperator(
            task_id=name,
            bash_command=cmd,
            trigger_rule=trigger,
            dag=test_dag
    )
    return bash_task


# Define the BashOperator task
HQL_PATH = '/home/mijin/code/data-pipeline/dags/hive/mijin/songs'

load_table = gen_bash_task("load.tmp", f"hive -f {HQL_PATH}/step-1-load-temp.hql", test_dag)

make_raw = gen_bash_task("make.raw", f"hive -f {HQL_PATH}/step-2-make-raw.hql", test_dag)

make_base = gen_bash_task("make.base", f"hive -f {HQL_PATH}/step-3-make-base.hql", test_dag)
                          
# Set task dependencies
load_table >> make_raw >> make_base
