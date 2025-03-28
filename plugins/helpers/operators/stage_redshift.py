from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    StageToRedshiftOperator: Custom Airflow operator to copy data from S3 to Redshift staging tables.
    It uses AWS credentials and Redshift connection details to perform the copy operation.
    """

    ui_color = '#358140'  # A nice, leafy green for the Airflow UI

    # SQL template for the COPY command
    copy_sql = """
        COPY {}  -- Target Redshift table
        FROM '{}' -- S3 path to the data
        ACCESS_KEY_ID '{}' -- AWS access key
        SECRET_ACCESS_KEY '{}' -- AWS secret key
        JSON '{}' -- JSON format specification (auto or path)
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',  # Airflow connection ID for Redshift
                 aws_credentials_id='',  # Airflow connection ID for AWS credentials
                 table='',  # Target Redshift table name
                 s3_bucket='',  # S3 bucket name
                 s3_key='',  # S3 key (path) to the data
                 s3_json="", #JSON format 'auto' or path to json path file
                 *args, **kwargs):
        """
        Constructor. Sets up the operator with connection details, table, and S3 paths.

        Args:
            redshift_conn_id (str): Airflow connection ID for Redshift.
            aws_credentials_id (str): Airflow connection ID for AWS credentials.
            table (str): Target Redshift table name.
            s3_bucket (str): S3 bucket name.
            s3_key (str): S3 key (path) to the data.
            s3_json (str): 'auto' or path to the json path file.
            *args: Extra arguments for BaseOperator.
            **kwargs: Extra keyword arguments for BaseOperator.
        """
        super().__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_json = s3_json

    def execute(self, context):
        """
        Executes the COPY command to load data from S3 to Redshift.
        It handles AWS credentials retrieval, Redshift connection, and table deletion.
        """
        try:
            # Get AWS credentials from Airflow connection
            self.log.info('Connecting to AWS...')
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()

            # Get Redshift connection from Airflow connection
            self.log.info('Connecting to Redshift...')
            redshift = PostgresHook(postgres_conn_id=self.conn_id)

            # Clear the target table before loading
            self.log.info(f'Clearing data from {self.table}...')
            redshift.run(f"DELETE FROM {self.table}")

            # Construct the S3 path based on the table
            if self.table == 'staging_events':
                # Format the S3 key with execution context for dynamic paths
                rendered_key = self.s3_key.format(**context)
                s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
            elif self.table == 'staging_songs':
                s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

            self.log.info(f'S3 path: {s3_path}')

            # Format the COPY SQL command with the required parameters
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.s3_json
            )

            self.log.info(f'Executing SQL: {formatted_sql}')

            # Execute the COPY command
            redshift.run(formatted_sql)

        except Exception as err:
            # Log any errors and re-raise the exception
            self.log.exception(err)
            raise err