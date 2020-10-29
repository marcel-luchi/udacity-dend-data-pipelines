from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import os


class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_dir", )
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift_sparkify",
                 s3_bucket='',
                 s3_date='',
                 aws_conn_id="aws_credentials",
                 table='',
                 file_type='json',
                 json_path='auto',
                 *args, **kwargs
                 ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_date = s3_date
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.file_type = file_type
        self.json_path = json_path
        if file_type == "json":
            self.staging_copy = """
                 copy {} from '{}'
                 access_key_id '{}'
                 secret_access_key '{}'
                 {} '{}' """
        else:
            self.staging_copy = """
                 copy {} from '{}'
                 access_key_id '{}'
                 secret_access_key '{}'
                 '{}' """
        self.truncate_table = "truncate table {}".format(self.table)

    def execute(self, context):
        aws = AwsHook(self.aws_conn_id)
        credentials = aws.get_credentials()
        s3_path = os.path.join(self.s3_bucket, self.s3_date.format(**context))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift.run(self.truncate_table)
        redshift.run(self.staging_copy.format(self.table,
                                              s3_path,
                                              credentials.access_key,
                                              credentials.secret_key,
                                              self.file_type,
                                              self.json_path))
