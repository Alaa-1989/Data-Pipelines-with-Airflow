from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql_statement_check_sql="",
                 expected_value="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement_check_sql = sql_statement_check_sql
        self.expected_value= expected_value

    def execute(self, context):
        self.log.info('DataQualityOperator started')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(self.sql_statement_check_sql)
        


        records = redshift_hook.get_records(self.sql_statement_check_sql)
        if (records[0][0] < 1):
            raise ValueError(f"DataQualityOperator failed! returned no results")
        if int(self.expected_value) != records[0][0]:
            raise ValueError(f"DataQualityOperator failed!  \n expected: {self.expected_value} \n acutal {records[0][0]}") 
        
        self.log.info(f'Success DataQualityOperator')
 