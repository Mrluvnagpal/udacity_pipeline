# from airflow.hooks.postgres_hook import PostgresHook
# from airflow.contrib.hooks.aws_hook import AwsHook
# from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults

# class StageToRedshiftOperator(BaseOperator):
#     ui_color = '#358140'
#     template_fields = ('s3_key',)
#     copy_sql = """
#         COPY {}
#         FROM '{}'
#         ACCESS_KEY_ID '{}'
#         SECRET_ACCESS_KEY '{}'
#         FORMAT AS JSON '{}'
#     """

#     @apply_defaults
#     def __init__(self,
#                  redshift_conn_id="",
#                  aws_credentials_id="",
#                  table="",
#                  s3_bucket="",
#                  s3_key="",
#                  json_path="auto",
#                  *args, **kwargs):

#         super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
#         self.redshift_conn_id = redshift_conn_id
#         self.aws_credentials_id = aws_credentials_id
#         self.table = table
#         self.s3_bucket = s3_bucket
#         self.s3_key = s3_key
#         self.json_path = json_path

#     def execute(self, context):
#         aws_hook = AwsHook(self.aws_credentials_id)
#         credentials = aws_hook.get_credentials()
#         redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

#         self.log.info("Clearing data from destination Redshift table")
#         redshift.run(f"DELETE FROM {self.table}")

#         self.log.info("Copying data from S3 to Redshift")
#         rendered_key = self.s3_key.format(**context)
#         s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
#         formatted_sql = StageToRedshiftOperator.copy_sql.format(
#             self.table,
#             s3_path,
#             credentials.access_key,
#             credentials.secret_key,
#             self.json_path
#         )
#         redshift.run(formatted_sql)
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class StageRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    sql_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS 
        BLANKSASNULL 
        EMPTYASNULL
        JSON '{}' 
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="auto",
                 execution_date=None,
                 *args,
                 **kwargs
                 ):
        super(StageRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format
        self.execution_date = execution_date

    def execute(self, context):
        self.log.info('Starting StageRedshiftOperator')

        # Retrieve AWS credentials
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)

        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear existing data in the table
        self.log.info(f"Clearing data from destination Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        # Format the S3 path for the COPY command
        s3_dir = self.s3_key
        if self.execution_date:
            year = str(self.execution_date.strftime("%Y"))
            month = str(self.execution_date.strftime("%m"))
            # Format the path using year and month (or add day if necessary)
            s3_dir = s3_dir.format(year, month)

        s3_path = f"s3://{self.s3_bucket}/{s3_dir}"

        # Format the SQL COPY command
        formatted_sql = StageRedshiftOperator.sql_template.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.json_format
        ).replace("\n", "")

        # Log the debug information and run the COPY command
        self.log.info(f"Running SQL command: {formatted_sql}")
        redshift.run(formatted_sql)
