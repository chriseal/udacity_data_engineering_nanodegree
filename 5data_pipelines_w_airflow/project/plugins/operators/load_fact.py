from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    # http://www.geekinterview.com/question_details/82655 
    # truncate is faster than drop because it keeps table structure
    fact_sql = """
        INSERT INTO {target_table}
        {query}
    """

    @apply_defaults
    def __init__(self,
            target_table=None,
            query=None,
            redshift_conn_id=None,
            truncate_target_table=False,
            *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.target_table = target_table
        self.query = query
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        self.log.info('Establishing a connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Loading {}\n{}".format(self.target_table, self.query))
        formatted_sql = LoadFactOperator.fact_sql.format(
            target_table=self.target_table,
            query=self.query
        )
        redshift.run(formatted_sql)