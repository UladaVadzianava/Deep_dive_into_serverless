import json
import os
from uuid import uuid4

import boto3
from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda

dynamodb = boto3.resource('dynamodb', region_name='eu-central-1')
reservations_table_name = 'cmtr-d7243a10-Reservations-test'
tables_table_name = 'cmtr-d7243a10-Tables-test'
user_pool_name = 'cmtr-d7243a10-simple-booking-userpool-test'

_LOG = get_logger('ApiHandler-handler')


class ApiHandler(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass
        
    def handle_request(self, event, context):
        """
        Explain incoming event here
        """
        _LOG.info(f"{event=}")

        tables_table = dynamodb.Table(tables_table_name)
        reservations_table = dynamodb.Table(reservations_table_name)

        try:
            if event['path'] == '/signup' and event['httpMethod'] == 'POST':
                _LOG.info("signup post")

            elif event['path'] == '/signin' and event['httpMethod'] == 'POST':
                _LOG.info("signip post")

            elif event['path'] == '/tables' and event['httpMethod'] == 'POST':
                _LOG.info("tables post")

                item = json.loads(event['body'])
                tables_table.put_item(Item=item)

                return {"statusCode": 200,
                        "body": json.dumps({"id": item["id"]})
                        }

            elif event['path'] == '/tables' and event['httpMethod'] == 'GET':
                response = tables_table.scan()
                items = response['Items']

                return {"statusCode": 200, "body": json.dumps(items)}

            elif event['resource'] == '/tables/{tableId}' and event['httpMethod'] == 'GET':
                table_id = int(event['path'].split('/')[-1])
                _LOG.info(f"{table_id=}")

            elif event['path'] == '/reservations' and event['httpMethod'] == 'POST':
                _LOG.info("reservations post")

                item = json.loads(event['body'])
                reservation_id = uuid4()
                reservations_table.put_item(Item={"id": reservation_id, **item})

                return {"statusCode": 200,
                        "body": json.dumps({"reservationId": reservation_id})
                        }

            elif event['path'] == '/reservations' and event['httpMethod'] == 'GET':
                _LOG.info("reservations get")

                response = reservations_table.scan()
                items = response['Items']

                return {"statusCode": 200, "body": json.dumps(items)}

            else:
                raise KeyError("no method")
        except Exception as ex:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'statusCode': 400,
                    'error': 'Bad request',
                    'message': f'{ex}'
                })
            }

        return {"statusCode": 200, "event": event}
    

HANDLER = ApiHandler()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
