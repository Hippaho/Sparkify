from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    LoadFactOperator: This operator loads data into your Redshift fact tables.
    Just give it the SQL and the table name, and watch it go!
    """

    ui_color = '#F98866'  # A nice, fiery orange for the Airflow UI

    @apply_defaults
    def __init__(self,
                 sql="",  # The SQL query to load the fact table
                 table="",  # The name of the fact table in Redshift
                 redshift_conn_id="",  # The Airflow connection ID for Redshift
                 *args, **kwargs):
        """
        Constructor. Sets up the operator with the SQL, table, and connection info.

        Args:
            sql (str): The SQL query for loading data.
            table (str): The target table in Redshift.
            redshift_conn_id (str): The Redshift connection ID.
            *args: Extra arguments for the BaseOperator.
            **kwargs: Extra keyword arguments for the BaseOperator.
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Executes the SQL query to load data into the fact table.
        Basically, it just runs the insert statement.
        """
        try:
            # Grab a hook to our Redshift database
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

            # Let's see what SQL we're about to run, for debugging purposes
            self.log.info(f"Running this SQL: {self.sql}")

            # Fire off the SQL query!
            redshift.run(self.sql)

            # All good, log the success
            self.log.info(f"Successfully loaded data into {self.table}")

        except Exception as err:
            # Oops, something went wrong. Log the error and bail out
            self.log.exception(err)
            raise err