import json
import boto3
import asyncio

#AWS region config
aws_region = 'us-east-2'
boto3.setup_default_session(region_name=aws_region)

#Creating a DynamoDB Document Client
dynamodb = boto3.client()
dynamodbTableName = 'product_storage'
healthPath = '/health'
productPath = '/product'
productsPath = '/products'

async def lambdaHandler(event, context):
    print('Request event: ', event)
    response = None
    
    if event['httpMethod'] == 'GET' and event['path'] == healthPath:
        response = buildResponse(200)
    elif event['httpMethod'] == 'GET' and event['path'] == productPath:
        product_id = event['queryStringParameters']['productId']
        response =  await getProduct(product_id)
    elif event['httpMethod'] == 'GET' and event['path'] == productsPath:
        response = await getProducts()
    elif event['httpMethod'] == 'POST' and event['path'] == productPath:
        response =  await postProduct(json.loads(event['body']))
    else:
        response = buildResponse(404, '404 Not Found')
    return response
    
    
def buildResponse(status_code, body=None):
    response = {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body)
    return response


async def getProduct(product_id):
    params = {
        Tablename: dynamodbTableName,
        Key: {
            'productId': productId
        }
    }
    dynamodb = boto3.client('dynamodb')
    try:
        response = await dynamodb.get_item(**params)
        return build_response(200, response)
    except Exception as error:
        print('error message:', error)
        
async def getProducts():
    params = {
        TableName: dynamodbTableName
    }
    allProducts = await getList(params, [])
    body = {
        products: allProducts
    }
    return buildResponse


async def getList (params, array):
    try:
        dynamodb = boto3.client('dynamodb')
        dynamo_data = await dynamodb.scan(**params).promise()
        array.extend(dynamo_data['Items'])
        
        if 'LastEvaluatedKey' in dynamo_data:
            params['ExclusiveStartKey'] = dynamo_data['LastEvaluatedKey']
            return await getList(params, array)
        return array

    except Exception as error:
        print('error: ', error)
    
    
async def postProduct (requestBody):
    try:
        dynamodb = boto3.client('dynamodb')
        params = {
            'TableName': dynamodbTableName,
            'Item': requestbody
        }
        await dynamodb.put_item(**params)
        
        body = {
            'Operation': 'SAVE',
            'Message': 'SUCCESS',
            'Item': requestBody
        }
        return buildResponse(200, body)

    except Exception as error:
        print('Error: ', error)