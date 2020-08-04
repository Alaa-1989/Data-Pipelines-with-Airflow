from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql_statement="",
                 append_data="",
                 table_name="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.append_data = append_data
        self.table_name = table_name

        
        
    def execute(self, context):
        self.log.info('LoadDimensionOperator started')
       
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == True:
        sql_statement = <sql_statement to insert into table>
        redshift.run(sql_statement)
    else:
        sql_statement = 'DELETE FROM %s' % self.table_name
        redshift.run(sql_statement)

        sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.sql_statement)
        redshift.run(sql_statement)

        
        self.log.info(f'Success LoadDimensionOperator')
