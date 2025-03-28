from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Airflow possesses a customer operator called The DataQualityOperator, which does data quality utilizing Redshift table 
    Its job is to look for any null values in a specified column of the table.
    """

    ui_color = '#89DA59'  # UI color for the operator in Airflow

    @apply_defaults
    def __init__(self,
                 table="",  # Redshift table name to check
                 col_name="",  # Column name to check for nulls
                 redshift_conn_id="",  # Airflow connection ID for Redshift
                 *args, **kwargs):
        """
        Initializes the DataQualityOperator.

        Args:
            table (str): Redshift table name to check.
            col_name (str): Column name to check for nulls.
            redshift_conn_id (str): Airflow connection ID for Redshift.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table
        self.col_name = col_name

    def execute(self, context):
        """
        The function actions the data quality check.

        Args:
            context (dict): Airflow context dictionary.
        """
        try:
            self.log.info(f'Data quality check on table: {self.table}')

            # Get a Redshift hook using the provided connection ID
            redshift = PostgresHook(postgres_conn_id=self.conn_id)

            # Execute a SQL query to count null values in the specified column
            records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table} WHERE {self.col_name} is NULL")

            # Check if the query returned any results
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.table} returned no results")

            # Extract the number of null records from the query result
            num_records = records[0][0]

            # Check if there are any null records
            if num_records > 0:
                raise ValueError(f"Data quality check failed. {self.table} has {num_records} rows with null values in {self.col_name}")

            # Log a success message if the data quality check passes
            self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records.")

        except Exception as err:
            # Log any exceptions that occur during the data quality check
            self.log.exception(err)
            # Re-raise the exception to fail the Airflow task
            raise err