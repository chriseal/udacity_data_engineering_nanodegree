from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
            target_table=None,
            redshift_conn_id=None,
            *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.target_table = target_table
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        self.log.info('Establishing a connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        records = redshift.get_records("SELECT COUNT(*) FROM {}".format(self.target_table))
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. {} returned no results".format(self.target_table))
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. {} contained 0 rows")

        logging.info("Data quality on table {} check passed with {} records".format(
            self.target_table, num_records))
