from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Operator to perform data quality checks on tables in Redshift.
    Ensures that tables contain data and are not empty.

    :param redshift_conn_id: Airflow connection ID for Redshift
    :type redshift_conn_id: str
    :param tables: List of tables to run data quality checks on
    :type tables: list
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 tables: list,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        """
        Executes data quality checks by ensuring each table has records.
        Raises an error if a table is empty or does not return results.
        """
        self.log.info('Starting Data Quality checks...')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            self.log.info(f'Checking data quality for table: {table}')
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if not records or not records[0] or records[0][0] < 1:
                raise ValueError(f"Data quality check failed for {table}: No records found!")
            
            self.log.info(f"Data quality check passed for {table} with {records[0][0]} records.")
