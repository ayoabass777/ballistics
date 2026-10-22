"""Idempotent private archive provisioning; uses existing AWS credential chain."""
import json
import boto3
from botocore.exceptions import ClientError


def configure():
    region = boto3.Session().region_name or 'ca-central-1'
    account = boto3.client('sts').get_caller_identity()['Account']
    bucket = f'ballistics-archive-{account}-{region}'
    s3 = boto3.client('s3', region_name=region)
    try:
        s3.head_bucket(Bucket=bucket, ExpectedBucketOwner=account)
    except ClientError as exc:
        if exc.response['Error']['Code'] not in ('404', 'NoSuchBucket'):
            raise
        args = {'Bucket': bucket, 'ObjectOwnership': 'BucketOwnerEnforced'}
        if region != 'us-east-1': args['CreateBucketConfiguration'] = {'LocationConstraint': region}
        s3.create_bucket(**args)
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
        'BlockPublicAcls': True, 'IgnorePublicAcls': True,
        'BlockPublicPolicy': True, 'RestrictPublicBuckets': True})
    s3.put_bucket_ownership_controls(Bucket=bucket, OwnershipControls={'Rules':[{'ObjectOwnership':'BucketOwnerEnforced'}]})
    s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={'Rules':[
        {'ApplyServerSideEncryptionByDefault':{'SSEAlgorithm':'AES256'}}]})
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={'Status':'Enabled'})
    statement = {'Sid':'DenyInsecureTransport', 'Effect':'Deny', 'Principal':'*',
        'Action':'s3:*', 'Resource':[f'arn:aws:s3:::{bucket}', f'arn:aws:s3:::{bucket}/*'],
        'Condition':{'Bool':{'aws:SecureTransport':'false'}}}
    try: policy = json.loads(s3.get_bucket_policy(Bucket=bucket)['Policy'])
    except ClientError as exc:
        if exc.response['Error']['Code'] != 'NoSuchBucketPolicy': raise
        policy = {'Version':'2012-10-17','Statement':[]}
    policy['Statement'] = [s for s in policy['Statement'] if s.get('Sid') != statement['Sid']] + [statement]
    s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
    assert all(s3.get_public_access_block(Bucket=bucket)['PublicAccessBlockConfiguration'].values())
    assert s3.get_bucket_versioning(Bucket=bucket)['Status'] == 'Enabled'
    assert not s3.get_bucket_policy_status(Bucket=bucket)['PolicyStatus']['IsPublic']
    print(json.dumps({'bucket':bucket,'region':region,'public':False,'versioning':'Enabled','encryption':'AES256'}))
    return bucket

if __name__ == '__main__':
    from API.core.database import engine  # loads this repository's local environment
    configure()
