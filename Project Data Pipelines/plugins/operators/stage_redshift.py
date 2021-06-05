from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential="",
                 table_name="",
                 s3_bucket="",
                 s3_key="",
                 log_json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential = aws_credential
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_path = log_json_path
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credential)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        query = "   COPY {} \
                    FROM '{}' \
                    ACCESS_KEY_ID '{}' \
                    SECRET_ACCESS_KEY '{}' \
                    FORMAT AS json '{}'; \
                    ".format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, self.log_json_path)
        redshift.run(query)



        
        
        
        