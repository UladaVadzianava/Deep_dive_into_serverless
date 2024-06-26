import json
import os
from uuid import uuid4

import boto3
from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda

dynamodb = boto3.resource('dynamodb', region_name='eu-central-1')
client = boto3.client('cognito-idp')
reservations_table_name = 'cmtr-d7243a10-Reservations-test'
tables_table_name = 'cmtr-d7243a10-Tables-test'
user_pool_name = 'cmtr-d7243a10-simple-booking-userpool-test'
CLIENT_APP = 'client_app'

_LOG = get_logger('ApiHandler-handler')


class ApiHandler(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass

    def signup(self, body):
        first_name = body['firstName']
        last_name = body['lastName']
        email = body['email']
        password = body['password']

        response = client.list_user_pools(MaxResults=60)
        user_pool_id = None
        for user_pool in response['UserPools']:
            if user_pool['Name'] == user_pool_name:
                user_pool_id = user_pool['Id']
                break
        _LOG.info(f'{user_pool_id=}')

        app_client_id = None
        response = client.list_user_pool_clients(UserPoolId=user_pool_id)

        for app_client in response['UserPoolClients']:
            if app_client['ClientName'] == CLIENT_APP:
                app_client_id = app_client['ClientId']
        _LOG.info(f'{app_client_id =}')

        response = client.admin_create_user(
            UserPoolId=user_pool_id,
            Username=email,
            UserAttributes=[
                {
                    'Name': 'email',
                    'Value': email
                },
                {
                    'Name': 'given_name',
                    'Value': first_name
                },
                {
                    'Name': 'family_name',
                    'Value': last_name
                },
            ],
            TemporaryPassword=password,
            MessageAction='SUPPRESS'
        )
        _LOG.info(f'{response=}')

        response = client.admin_set_user_password(
            UserPoolId=user_pool_id,
            Username=email,
            Password=password,
            Permanent=True
        )
        _LOG.info(f'{response=}')

    def signin(self, event):
        body = json.loads(event['body'])

        email = body['email']
        password = body['password']

        response = client.list_user_pools(MaxResults=60)

        user_pool_id = None
        for user_pool in response['UserPools']:
            if user_pool['Name'] == user_pool_name:
                user_pool_id = user_pool['Id']
                break

        response = client.list_user_pool_clients(
            UserPoolId=user_pool_id,
            MaxResults=10
        )

        app_client_id = None
        for app_client in response['UserPoolClients']:
            if app_client['ClientName'] == CLIENT_APP:
                app_client_id = app_client['ClientId']

        response = client.initiate_auth(
            ClientId=app_client_id,
            AuthFlow='USER_PASSWORD_AUTH',
            AuthParameters={
                'USERNAME': email,
                'PASSWORD': password
            }
        )
        _LOG.info(f'{response=}')
        id_token = response['AuthenticationResult']['IdToken']
        return {
            'statusCode': 200,
            'body': json.dumps({'accessToken': id_token})
        }

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
                body = json.loads(event['body'])
                self.signup(body)

            elif event['path'] == '/signin' and event['httpMethod'] == 'POST':
                _LOG.info("signip post")
                self.signin(event)

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
