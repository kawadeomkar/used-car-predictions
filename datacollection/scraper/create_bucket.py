import uuid
import boto3

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


def create_bucket_name(bucket_prefix):
    # The generated bucket name must be between 3 and 63 chars long
    return ''.join([bucket_prefix, str(uuid.uuid4())])

def create_bucket(bucket_prefix, s3_connection):
    session = boto3.session.Session()
    bucket_name = create_bucket_name(bucket_prefix)
    current_region = session.region_name
    bucket_response = s3_connection.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
        'LocationConstraint': current_region})
    print(bucket_name, current_region)
    return bucket_name, bucket_response

first_bucket_name, first_response = create_bucket(
     bucket_prefix='cars', 
     s3_connection=s3_resource.meta.client)
first_response