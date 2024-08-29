from airflow.models import BaseOperator
from airflow.providers.presto.hooks.presto import PrestoHook
from airflow.utils.decorators import apply_defaults

class CustomPrestoOperator(BaseOperator):
    """
    Custom Operator to run SQL queries on a Presto cluster using PrestoHook.
    """

    @apply_defaults
    def __init__(self, sql, presto_conn_id='presto_default', *args, **kwargs):
        super(CustomPrestoOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.presto_conn_id = presto_conn_id

    def execute(self, context):
        self.log.info("Executing query: %s", self.sql)
        hook = PrestoHook(presto_conn_id=self.presto_conn_id)
        result = hook.get_first(self.sql)
        self.log.info(f"Query Result: {result}")

