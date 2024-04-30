import json

from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda

_LOG = get_logger('HelloWorld-handler')


class HelloWorld(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass
        
    def handle_request(self, event, context):
        """
        Explain incoming event here
        """
        # todo implement business logic
        return 200
    

HANDLER = HelloWorld()


def lambda_handler(event, context):
    return {
                    "headers": {
                        "Content-Type": "application/json"
                        },
                    "statusCode": 200,
                    "body": json.dumps(
                        {
                            "statusCode": 200,
                            "message": "Hello from Lambda"
                            }
                        )
                    }
