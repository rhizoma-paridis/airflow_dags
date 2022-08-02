import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from camel_k8s_config import K8sConfig
from example.cicada import godBless
from example.cicada import queryUser
from operators.wecom_operator import failure_callback_wecom

log = logging.getLogger(__name__)

args = {
    'owner': 'xpq',
    'email': ['data_group@camel4u.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': failure_callback_wecom,
}

k8sConfig = K8sConfig().buildAffinity().buildTolerations().build()

with DAG(
    dag_id='hello',
    default_args=args,
    schedule_interval='00 01 * * *',
    start_date=days_ago(1),
    tags=['example', 'hello dag'],
) as dag:

    @task(task_id='context', executor_config=k8sConfig)
    def print_context(ds=None, game='wao', **kwargs):
        log.info("ds: " + ds)
        log.info("game: " + game)
        log.info("kwargs: " + kwargs)
        return 'guess where'

    python_task = print_context()

    @task(task_id='cicada', executor_config=k8sConfig)
    def cicada(ds=None, game='aoz'):
        godBless(ds, game)

    cicada = cicada()

    @task(task_id='user', executor_config=k8sConfig)
    def user():
        queryUser()

    user = user()

    python_task >> cicada >> user