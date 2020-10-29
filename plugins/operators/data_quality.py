from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 tables=[],
                 redshift_conn_id='redshift_sparkify',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('DataQualityOperator not implemented yet')
        for table in self.tables:
            result = redshift.get_records(f"select count(1) from {table}")
            if len(result) < 1 or len(result[0]) < 1 or result[0][0] == 0:
                self.log.error(f"Table {table} has returned no rows.")
                raise KeyError(f"Data quality error. Table {table} returned no records.")
            self.log.info(f"Table {table} validated. Table has {result[0][0]} rows.")
