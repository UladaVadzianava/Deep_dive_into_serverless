from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda

from task08.src.lambdas.layers.weather.weather import Weather

_LOG = get_logger('ApiHandler-handler')


class ApiHandler(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass
        
    def handle_request(self, event, context):

        weather = Weather.get_weather

        return {
                    "headers": {
                        "Content-Type": "application/json"
                        },
                    "statusCode": 200,
                    "body": weather.json()
                    }
    

HANDLER = ApiHandler()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
