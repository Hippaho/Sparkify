from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    DataQualityOperator: The Sherlock Holmes of Airflow operators.
    It sniffs out data quality issues in your Redshift tables, particularly hunting for those pesky null values.
    Think of it as your first line of defense against unclean data.
    """

    ui_color = '#89DA59'  # A calming green, because data quality shouldn't stress you out

    @apply_defaults
    def __init__(self,
                 table="",  # Which Redshift table are we inspecting?
                 col_name="",  # Which column are we putting under the microscope?
                 redshift_conn_id="",  # How do we talk to Redshift? (Airflow connection ID)
                 *args, **kwargs):
        """
        Constructor. Sets up our data detective with the table, column, and Redshift connection.

        Args:
            table (str): The Redshift table to check.
            col_name (str): The column to scrutinize for null values.
            redshift_conn_id (str): The Airflow connection ID for Redshift.
            *args: Extra arguments for the BaseOperator.
            **kwargs: Extra keyword arguments for the BaseOperator.
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table
        self.col_name = col_name

    def execute(self, context):
        """
        Executes the data quality check. It's like sending our detective to the scene.

        Args:
            context (dict): Airflow context, holding all the clues.
        """
        try:
            self.log.info(f"Investigating {self.table} for nulls in {self.col_name}...")

            # Get our Redshift connection, the detective's direct line to the database
            redshift = PostgresHook(postgres_conn_id=self.conn_id)

            # Let's ask Redshift how many nulls we have
            records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table} WHERE {self.col_name} is NULL")

            # Did we even get a response? If not, something's fishy
            if not records or not records[0]:
                raise ValueError(f"Data quality check failed. {self.table} investigation yielded no results. Something's up.")

            # How many nulls did we find?
            num_records = records[0][0]

            # If we found any nulls, raise an alarm!
            if num_records > 0:
                raise ValueError(f"Data quality check failed. {self.table} has {num_records} nulls in {self.col_name}. Data's dirty!")

            # If we get here, all's clear. Log the good news.
            self.log.info(f"Data quality check on {self.table} passed. No nulls found in {self.col_name}.")

        except Exception as err:
            # If our detective stumbles upon an error, log it and throw a fit.
            self.log.exception(err)
            raise err