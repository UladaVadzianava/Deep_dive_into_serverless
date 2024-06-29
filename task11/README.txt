syndicate generate project --name syndicate-quickstart-project
cd syndicate-quickstart-project
syndicate generate config ...
syndicate generate lambda --name api_handler --runtime python
export SDCT_CONF=...
syndicate generate lambda --name api_handler --runtime python
vim .
syndicate generate meta dynamodb --resource_name Tables --hash_key_name id --hash_key_type S
syndicate generate meta dynamodb --resource_name Reservations --hash_key_name reservationId --hash_key_type S
syndicate generate meta api_gateway --resource_name task10_api  --deploy_stage api
syndicate generate meta api_gateway_resource --api_name task10_api --path /signup
syndicate generate meta api_gateway_resource --api_name task10_api --path /signin
syndicate generate meta api_gateway_resource --api_name task10_api --path /tables
syndicate generate meta api_gateway_resource --api_name task10_api --path /reservations
syndicate generate meta api_gateway_resource_method --api_name task10_api --path /signup --method POST --integration_type lambda --lambda_name api_handler
syndicate generate meta api_gateway_resource_method --api_name task10_api --path /signin --method POST --integration_type lambda --lambda_name api_handler
syndicate generate meta api_gateway_resource_method --api_name task10_api --path /tables --method POST --integration_type lambda --lambda_name api_handler
syndicate generate meta api_gateway_resource_method --api_name task10_api --path /tables --method GET --integration_type lambda --lambda_name api_handler
syndicate generate meta api_gateway_resource_method --api_name task10_api --path /reservations --method POST --integration_type lambda --lambda_name api_handler
syndicate generate meta api_gateway_resource_method --api_name task10_api --path /reservations --method GET --integration_type lambda --lambda_name api_handler
syndicate generate meta cognito_user_pool --resource_name simple-booking-userpool
syndicate generate meta api_gateway_authorizer --api_name task10_api --name task10-authorizer --type COGNITO_USER_POOLS --provider_name simple-booking-userpool
syndicate build
syndicate deploy 

