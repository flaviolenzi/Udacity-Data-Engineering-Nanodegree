from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tableS=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tableS = tableS

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        
        for table in self.tables:
            records_count = redshift_hook.get_records(f"SELECT COUNT(*) FROM {}".format(table))
            
            if len(records_count) < 1 or len(records_count[0]) < 1:
                raise ValueError(f"Data quality check failed - {} returned no results".format(table))
            
            total_records = records_count[0][0]
            if total_records < 1:
                raise ValueError(f"Data quality check failed - {} returned 0 rows".format(table))
                
            self.log.info(f"Data quality on table {} check passed with {} records".format(table, total_records))

