import json
import uuid
from datetime import datetime

import boto3
from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda

_LOG = get_logger('ApiHandler-handler')

dynamodb = boto3.resource('dynamodb')
table_date = 'cmtr-d7243a10-Events-test'


class ApiHandler(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass
        
    def handle_request(self, event, context):
        """
        Explain incoming event here
        """

        event_data_model = {
            "id": str(uuid.uuid4()),
            "principalId": event["principalId"],
            "createdAt": datetime.utcnow().isoformat(),
            "body": event["content"]
        }

        table = dynamodb.Table(table_date)
        table.put_item(Item=event_data_model)

        return {
            "statusCode": 201,
            "body": json.dumps(event_data_model),
        }
    

HANDLER = ApiHandler()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
