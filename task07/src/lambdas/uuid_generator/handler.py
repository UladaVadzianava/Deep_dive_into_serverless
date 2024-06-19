import json
from datetime import datetime
from uuid import uuid4

import boto3
from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda

s3 = boto3.resource('s3')
bucket_name = 'cmtr-d7243a10-uuid-storage-test'

_LOG = get_logger('UuidGenerator-handler')


class UuidGenerator(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass
        
    def handle_request(self, event, context):
        """
        Explain incoming event here
        """
        file = f"{datetime.now().isoformat()}Z"
        db_event = {"ids": [str(uuid4()) for _ in range(10)]}
        content = json.dumps(db_event)

        s3.Object(bucket_name, file).put(Body=content)
        return 200
    

HANDLER = UuidGenerator()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
