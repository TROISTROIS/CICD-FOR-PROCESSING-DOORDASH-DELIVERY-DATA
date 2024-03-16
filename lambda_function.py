import pandas as pd
import json
import boto3

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
target_bucket_arn = "arn:aws:s3:::doordash-filtered-target-bucket"
sns_arn_filtered = "arn:aws:sns:eu-north-1:590183810146:doordash-filtering"
sns_arn_landing = "arn:aws:sns:eu-north-1:590183810146:doordash-landing"

def lambda_handler(event,context):
    print("Event ------------------>", event)

    # print the values of the json file
    try:
        try:
            bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
            s3_key = event["Records"][0]["s3"]["object"]["key"]

            print("BUCKET NAME: ", bucket_name)
            print("KEY: ", s3_key)

            response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            print(response['Body'])
            data = json.loads(response['Body'].read())

            # Load data into a Pandas DataFrame
            df = pd.DataFrame(data)
            # sample dataframe

            print(df.sample(5))

            message = "Input S3 File {} has been processed successfuly !!".format(
                "s3://" + bucket_name + "/" + s3_key)
            notification = sns_client.publish(Subject="SUCCESS - Daily Data Processing", TargetArn=sns_arn_landing,
                                              Message=message, MessageStructure='text')


        except Exception as err:

            print(err)
            message = "Input S3 File {} processing is Failed !!".format("s3://" + bucket_name + "/" + s3_key)
            notification = sns_client.publish(Subject="SUCCESS - Daily Data Processing", TargetArn=sns_arn_landing, Message=message,
                                              MessageStructure='text')
        try:

            # Filter the data
            # 1. Find unique order status values
            order_statuses = set(df['status'])

            # 2.Filter and upload data for each unique order status
            for status in order_statuses:
                filtered_data = df[df['status'] == status]

                # 2. Convert filtered data back to JSON
                filtered_data_json = filtered_data.to_json(orient='records')

                # Upload filtered data to S3
                s3_client.put_object(Bucket=target_bucket_arn, Key=f'filtered_data_{status}.json', Body=filtered_data_json)

                # Get the count of records for the current order status
                count = len(filtered_data)

                # Return a message with the count of records
                if count > 0:
                    messages = "Successfully uploaded {count} {status} orders to S3 Bucket."
                    notification = sns_client.publish(Subject="SUCCESS - Daily Data Upload", TargetArn=sns_arn_filtered,
                                                      Message=message, MessageStructure='text')

        except Exception as err:
            print(err)
            message = "Failed to upload {status} orders to S3 Bucket.!!"
            notification = sns_client.publish(Subject="FAILED - Daily Data Upload", TargetArn=sns_arn_filtered,
                                              Message=message, MessageStructure='text')

    except Exception as err:
        print(err)
