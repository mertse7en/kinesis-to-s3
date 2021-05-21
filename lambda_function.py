import json
import boto3
import base64


def lambda_handler(event, context):
    bucket_name = 'your-bucket-name'
    s3 = boto3.client('s3')
    
    try:
        for record in event["Records"]:
            print("here")
            decoded_data = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")

            print(decoded_data)
            decoded_data_dic = json.loads(decoded_data)
            
            file_name = f'{record["kinesis"]["sequenceNumber"]}.json'
            upload_byte_stream = bytes(json.dumps(decoded_data_dic).encode('UTF-8'))
            
            s3.put_object(Bucket=bucket_name, Key=file_name, Body=upload_byte_stream)
            print("put complete")
                
    except Exception as e:
        print(str(e))
