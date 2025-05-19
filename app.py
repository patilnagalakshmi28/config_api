import json
import boto3
import redis
import os

# DynamoDB setup
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('config_table_demo')

# Redis setup
try:
    redis_client = redis.Redis(
        host=os.environ['VALKEY_HOST'],
        port=int(os.environ.get('VALKEY_PORT', 6379)),
        decode_responses=True,
        socket_connect_timeout=2
    )
    redis_client.ping()
    print("✅ Redis connection successful.")
except Exception as e:
    print(f"❌ Redis connection failed: {e}")
    redis_client = None

def lambda_handler(event, context):
    method = event['httpMethod']

    if method == 'POST':
        body = json.loads(event['body'])
        config_key = body.get('config_key')
        config_value = body.get('config_value')

        if not config_key or not config_value:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "config_key and config_value required"})
            }

        # Store in DynamoDB
        table.put_item(Item={
            'key': config_key,
            'value': config_value
        })

        # Store in Redis
        if redis_client:
            try:
                redis_client.set(config_key, json.dumps(config_value))
            except Exception as e:
                print(f"❌ Redis SET failed: {e}")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Stored"})
        }

    elif method == 'GET':
        params = event.get('queryStringParameters')
        if not params or 'key' not in params:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing query parameter 'key'"})
            }

        key = params['key']

        # Try Redis first
        if redis_client:
            try:
                cached_value = redis_client.get(key)
                if cached_value:
                    return {
                        "statusCode": 200,
                        "body": json.dumps({
                            "key": key,
                            "value": json.loads(cached_value),
                            "source": "cache"
                        })
                    }
            except Exception as e:
                print(f"❌ Redis GET failed: {e}")

        # Fallback to DynamoDB
        try:
            response = table.get_item(Key={'key': key})
            item = response.get('Item')
            if item:
                # Cache in Redis
                if redis_client:
                    try:
                        redis_client.set(key, json.dumps(item['value']))
                    except Exception as e:
                        print(f"❌ Redis cache set failed: {e}")
                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "key": key,
                        "value": item['value'],
                        "source": "dynamodb"
                    })
                }
            else:
                return {
                    "statusCode": 404,
                    "body": json.dumps({"error": "Not found"})
                }
        except Exception as e:
            return {
                "statusCode": 500,
                "body": json.dumps({'error': f'DynamoDB read failed: {e}'})
            }

    elif method == 'PATCH':
        body = json.loads(event['body'])
        config_key = body.get('config_key')
        updated_values = body.get('config_value')

        if not config_key or not updated_values:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "config_key and config_value required"})
            }

        # Prepare update expression for nested update
        update_expression = "SET "
        expression_attribute_names = {"#v": "value"}
        expression_attribute_values = {}
        update_parts = []

        for i, (k, v) in enumerate(updated_values.items()):
            update_parts.append(f"#v.{k} = :val{i}")
            expression_attribute_values[f":val{i}"] = v

        update_expression += ", ".join(update_parts)

        try:
            table.update_item(
                Key={'key': config_key},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
        except Exception as e:
            return {
                "statusCode": 500,
                "body": json.dumps({'error': f'DynamoDB update failed: {e}'})
            }

        # Get updated item
        updated_item = table.get_item(Key={'key': config_key}).get('Item', {})

        # Update Redis
        if redis_client:
            try:
                redis_client.set(config_key, json.dumps(updated_item.get('value', {})))
            except Exception as e:
                print(f"❌ Redis update failed: {e}")

        return {
            "statusCode": 200,
            "body": json.dumps({'message': f'Updated {config_key} successfully'})
        }

    else:
        return {
            "statusCode": 405,
            "body": json.dumps({"error": "Method Not Allowed"})
        }
