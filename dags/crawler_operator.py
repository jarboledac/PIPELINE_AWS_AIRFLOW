import time
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.decorators import apply_defaults

class GlueTriggerCrawlerOperator(BaseOperator):
    @apply_defaults
    def __init__(self,aws_conn_id: str,crawler_name: str,region_name: str=None,**kwargs):
        super().__init__(**kwargs)
        self._aws_conn_id = aws_conn_id
        self._crawler_name = crawler_name,
        self._region_name = region_name

        def execute(señf, context):
            hook = AwsBaseHook(
                self._aws_conn_id, client_type = "glue",
                region_name = self._region_name
            )
            glue_client = hook.get_conn()
            response = glue_client.start_crawler(Name=self._crawler_name)
            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise RuntimeError(
                    f"An error occurred while triggering the crawler: {response}"
                )
            while True:
                time.sleep(1)
                crawler = glue_client.get_crawler(Name=self._crawler_name)
                crawler_state = crawler["Crawler"]["State"]
                if crawler_state == "READY":
                    break