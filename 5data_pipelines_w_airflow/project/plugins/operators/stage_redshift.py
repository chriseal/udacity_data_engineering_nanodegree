from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",) # fields that can be parameterized 
    """
    Nice work the stage operator. I suggest adding a template field that would allow to load timestamped files from S3 based on the execution time and run backfills.

    template_fields = ("s3_key",)
    Actually it is similar with your comment below

    s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key) # can be called like: s3_key="divvy/partitioned/{execution_date.year}/{execution_date.month}/divvy_trips.csv
    """


    confirm_exists_sql = """
        SELECT true WHERE EXISTS (SELECT *
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{}');
    """

    copy_sql = """
        COPY {target_table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        REGION '{region}'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        FORMAT AS {source_file_format} '{log_fpath}'
        {extra};
    """

    @apply_defaults
    def __init__(self,
            target_table="",
            source_file_format='JSON',
            delimiter=",", # csv-specific params
            ignore_headers=1, # csv-specific params
            log_fpath='auto',
            s3_bucket="",
            s3_key="",
            aws_credentials_id="",
            redshift_conn_id="",
            region="",
            *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.target_table = target_table
        self.source_file_format = source_file_format.lower().strip()
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.log_fpath = log_fpath

        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.region = region

        assert isinstance(self.aws_credentials_id, str)
        assert isinstance(self.redshift_conn_id, str)
        assert self.source_file_format in {'csv','json'}
        assert isinstance(self.ignore_headers, int)


    def execute(self, context):
        """
        Copy one or more raw json or csv files from AWS S3 to a Redshift staging table
        """

        self.log.info('Establishing connection to: {}'.format(self.target_table.upper()))
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        extra = ''
        if self.source_file_format == 'csv':
            extra = '\n'.join([extra, "IGNOREHEADER {} DELIMITER '{}'".format(
                self.delimiter, self.ignore_headers)
            ])

        self.log.info("Confirming target table exists - {}".format(self.target_table))
        res = redshift.get_records(StageToRedshiftOperator.confirm_exists_sql.format(self.target_table))
        self.log.info("result:\t{}".format(res))

        self.log.info("Clearing data from destination Redshift {}".format(self.target_table))
        redshift.run("DELETE FROM {}".format(self.target_table))

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key) # can be called like: s3_key="divvy/partitioned/{execution_date.year}/{execution_date.month}/divvy_trips.csv"
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            target_table=self.target_table,
            s3_path=s3_path,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            region=self.region,
            source_file_format=self.source_file_format,
            log_fpath=self.log_fpath,
            extra=extra
        )
        self.log.info("Copying data from S3 to Redshift")
        redshift.run(formatted_sql)