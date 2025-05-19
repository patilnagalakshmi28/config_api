import json
import boto3
import redis
import os

# DynamoDB setup
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('config_table_demo')

# Lazy Redis initialization
redis_client = None

def get_redis_client():
    global redis_client
    if redis_client is None:
        try:
            redis_client = redis.Redis(
                host=os.environ['VALKEY_HOST'],
                port=int(os.environ.get('VALKEY_PORT', 6379)),
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2
            )
            redis_client.ping()
            print("✅ Redis connected.")
        except Exception as e:
            print(f"❌ Redis connection failed: {e}")
            redis_client = None
    return redis_client

def handle_post(event):
    body = json.loads(event['body'])
    config_key =str(body.get('config_key'))
    config_value = body.get('config_value')

    if not config_key or not config_value:
        return {"statusCode": 400, "body": json.dumps({"message": "config_key and config_value required"})}

    # Store in DynamoDB
    table.put_item(Item={'key': config_key, 'value': config_value})

    # Store in Redis
    client = get_redis_client()
    if client:
        try:
            client.set(config_key, json.dumps(config_value))
        except Exception as e:
            print(f"❌ Redis SET failed: {e}")

    return {"statusCode": 200, "body": json.dumps({"message": "Stored"})}

def handle_get(event):
    params = event.get('queryStringParameters') or {}
    key = params.get('key')
    if not key:
        return {"statusCode": 400, "body": json.dumps({"message": "Missing query parameter 'key'"})}

    # Try Redis
    client = get_redis_client()
    if client:
        try:
            cached_value = client.get(key)
            if cached_value:
                return {
                    "statusCode": 200,
                    "body": json.dumps({"key": key, "value": json.loads(cached_value), "source": "cache"})
                }
        except Exception as e:
            print(f"❌ Redis GET failed: {e}")

    # Fallback to DynamoDB
    try:
        response = table.get_item(Key={'key': key})
        item = response.get('Item')
        if item:
            if client:
                try:
                    client.set(key, json.dumps(item['value']))
                except Exception as e:
                    print(f"❌ Redis cache set failed: {e}")
            return {
                "statusCode": 200,
                "body": json.dumps({"key": key, "value": item['value'], "source": "dynamodb"})
            }
        else:
            return {"statusCode": 404, "body": json.dumps({"error": "Not found"})}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({'error': f'DynamoDB read failed: {e}'})}

def handle_patch(event):
    body = json.loads(event['body'])
    config_key = body.get('config_key')
    updated_values = body.get('config_value')

    if not config_key or not updated_values:
        return {"statusCode": 400, "body": json.dumps({"message": "config_key and config_value required"})}

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
        return {"statusCode": 500, "body": json.dumps({'error': f'DynamoDB update failed: {e}'})}

    updated_item = table.get_item(Key={'key': config_key}).get('Item', {})

    client = get_redis_client()
    if client:
        try:
            client.set(config_key, json.dumps(updated_item.get('value', {})))
        except Exception as e:
            print(f"❌ Redis update failed: {e}")

    return {"statusCode": 200, "body": json.dumps({'message': f'Updated {config_key} successfully'})}

def lambda_handler(event, context):
    print("📥 Event received:", json.dumps(event))
    method = event.get('httpMethod', '')

    if method == 'POST':
        return handle_post(event)
    elif method == 'GET':
        return handle_get(event)
    elif method == 'PATCH':
        return handle_patch(event)
    else:
        return {"statusCode": 405, "body": json.dumps({"error": "Method Not Allowed"})}
