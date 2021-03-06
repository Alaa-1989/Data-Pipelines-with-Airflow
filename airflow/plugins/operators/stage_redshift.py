from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    copy_sql = """
        COPY {table_name}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        TIMEFORMAT AS 'epochmillisecs'
        region 'us-west-2'
        {file_type}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_type="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_type = file_type
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        
        # connection
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # clear raw data
        self.log.info("Clearing data from destination Redshift table") 
        redshift.run("DELETE FROM {}".format(self.table))

        # copy data from s3 to redshift
        self.log.info(f"Copying data {self.table} from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)   

        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table_name = self.table,
            s3_path = s3_path,
            access_key = credentials.access_key,
            secret_key = credentials.secret_key,
            file_type = self.file_type

        )
        redshift.run(formatted_sql)

        self.log.info(f"Copying data {self.table} from S3 to Redshift")



