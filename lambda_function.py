import pandas as pd
import json
import boto3

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
target_bucket = "doordash-filtered-target-bucket"
sns_arn_filtered = "arn:aws:sns:eu-north-1:590183810146:doordash-filtering"
sns_arn_landing = "arn:aws:sns:eu-north-1:590183810146:doordash-landing"

def lambda_handler(event,context):
    print("Event ------------------>", event)

    # print the values of the json file
    try:
        try:
            bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
            s3_key_landing = event["Records"][0]["s3"]["object"]["key"]

            print("BUCKET NAME: ", bucket_name)
            print("KEY: ", s3_key_landing)

            response = s3_client.get_object(Bucket=bucket_name, Key=s3_key_landing)
            print(response['Body'])
            data = json.loads(response['Body'].read())

            # Load data into a Pandas DataFrame
            df = pd.DataFrame(data)
            # sample dataframe

            print(df.sample(5))

            message = "Input S3 File {} has been processed successfully !!".format(
                "s3://" + bucket_name + "/" + s3_key_landing)
            notification = sns_client.publish(Subject="SUCCESS - Daily Data Processing", TargetArn=sns_arn_landing,
                                              Message=message, MessageStructure='text')


        except Exception as err:

            print(err)
            message = "Input S3 File {} processing is Failed !!".format("s3://" + bucket_name + "/" + s3_key_landing)
            notification = sns_client.publish(Subject="SUCCESS - Daily Data Processing", TargetArn=sns_arn_landing, Message=message,
                                              MessageStructure='text')
        try:

            filtered_data = df[df['status'] == 'delivered']

            # 2. Convert filtered data back to JSON
            filtered_data_json = filtered_data.to_json(orient='records')
            s3_key_filtered = f'filtered_data_delivered.json'

            # Upload filtered data to S3
            s3_client.put_object(Bucket=target_bucket, Key=s3_key_filtered, Body=filtered_data_json)

            # Get the count of records for the current order status
            count = len(filtered_data)

            # Return a message with the count of records
            if count > 0:
                messages = f"Successfully uploaded {count} delivered orders to S3 Bucket".format("s3://" + "delivered" + "/" + s3_key_filtered)
                notification = sns_client.publish(Subject="SUCCESS - Daily Data Upload", TargetArn=sns_arn_filtered,
                                                  Message=message, MessageStructure='text')

        except Exception as err:
            print(err)
            message = f"Failed to upload delivered orders to S3 Bucket!!"
            notification = sns_client.publish(Subject="FAILED - Daily Data Upload", TargetArn=sns_arn_filtered,
                                          Message=message, MessageStructure='text')

    except Exception as err:
        print(err)
