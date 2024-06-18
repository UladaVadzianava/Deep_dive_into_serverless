import json
import uuid
from datetime import datetime

import boto3
from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda


dynamodb_client = boto3.resource('dynamodb')
configuration_table = 'cmtr-d7243a10-Configuration-test'
audit_table = 'cmtr-d7243a10-Audit-test'


_LOG = get_logger('AuditProducer-handler')


class AuditProducer(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass

    def put_data_to_table(self, table_model):
        table = dynamodb_client.Table(audit_table)
        table.put_item(Item=table_model)

    def handle_request(self, event, context):
        """
        Explain incoming event here
        """
        table_model = {}
        evetn_type = event['Records'][0]['eventName']

        if evetn_type == "INSERT":
            table_model = {
                "id": str(uuid.uuid4()),
                "itemKey": event['Records'][0]['dynamodb']['Keys']['key']['S'],
                "modificationTime": datetime.utcnow().isoformat(),
                "newValue": {
                    "key": event['Records'][0]['dynamodb']['NewImage']['key']['S'],
                    "value": int(event['Records'][0]['dynamodb']['NewImage']['value']['N'])
                }
            }
        elif evetn_type == "MODIFY":
            table_model = {
                "id": str(uuid.uuid4()),
                "itemKey": event['Records'][0]['dynamodb']['Keys']['key']['S'],
                "modificationTime": datetime.utcnow().isoformat(),
                "updatedAttribute": "value",
                "oldValue": int(event['Records'][0]['dynamodb']['OldImage']['value']['N']),
                "newValue": int(event['Records'][0]['dynamodb']['NewImage']['value']['N'])
            }

        self.put_data_to_table(table_model)

        return {
            "statusCode": 200,
            "body": json.dumps(table_model),
        }
    

HANDLER = AuditProducer()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
