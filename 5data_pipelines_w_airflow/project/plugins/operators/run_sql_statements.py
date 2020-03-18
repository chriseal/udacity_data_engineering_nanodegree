from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RunSQLStatements(BaseOperator):
    ui_color = 'C0C0C0'

    @apply_defaults
    def __init__(self,
            queries=None,
            redshift_conn_id=None,
            *args, **kwargs):

        super(RunSQLStatements, self).__init__(*args, **kwargs)
        self.queries = queries
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Establishing a connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Running {} SQL queries...".format(len(self.queries)))
        for i, query in enumerate(self.queries):
            self.log.info("Query {}".format(i+1))
            redshift.run(query)