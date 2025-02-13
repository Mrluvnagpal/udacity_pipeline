from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Custom Airflow operator to load data into a dimension table in Amazon Redshift.
    This operator supports truncating the table before inserting new data.

    :param redshift_conn_id: Airflow connection ID for Redshift
    :param sql_query: SQL query to fetch data for insertion
    :param target_table: Name of the target dimension table
    :param truncate_before_load: If True, truncates the table before inserting new data
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_query='',
                 target_table='',
                 truncate_before_load=True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.target_table = target_table
        self.truncate_before_load = truncate_before_load

    def execute(self, context):
        """
        Executes the SQL query to load data into the dimension table in Redshift.
        If truncate_before_load is set to True, the table will be truncated before insertion.
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_before_load:
            self.log.info(f'Truncating table {self.target_table} before loading data.')
            redshift_hook.run(f'TRUNCATE TABLE {self.target_table}')
        
        self.log.info(f'Inserting data into {self.target_table}')
        redshift_hook.run(f'INSERT INTO {self.target_table} {self.sql_query}')
        self.log.info(f'Data successfully loaded into {self.target_table}.')
