
import os
import abc
from collections import namedtuple
import boto3
from boto3.s3.transfer import TransferConfig

S3_ID ="AWS_ACCESS_KEY_ID"
S3_KEY="AWS_SECRET_ACCESS_KEY"
S3_REGION="AWS_REGION"
S3_BUCKET="AWS_BUCKET"

S3_TRANSFER_CONCURRENCY=5
MULTIPART_THRESHOLD=1024 * 1024 * 1000 #1GB Threshold
MULTIPART_CHUNKSIZE=1024 * 1024 * 10 #10 mb chunks

FileStoreResultObject = namedtuple('FileStoreResultObject', ['ID', 'Name', 'Size','Path','Type','IsDir','Modified','ModifiedBy'])

class IStreamingBody(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def read(self,*amt:int) -> bytes:
        pass


##############################################################
####  S3
##############################################################

class S3FileInfo:

    def __init__(self,objectSummary):
        self.objectSummary=objectSummary
    
    def name(self):
        return os.path.basename(self.objectSummary.key)
    
    def size(self):
        return self.objectSummary.size
    
    def mod_time(self):
        return self.objectSummary.last_modified
    
    def is_dir(self):
        return False
    



def NewS3FileStore(profile, bucket):
    #session = boto3.session.Session(profile_name=profile)
    session = boto3.Session(
        aws_access_key_id=os.environ[f'{profile}_{S3_ID}'],
        aws_secret_access_key=os.environ[f'{profile}_{S3_KEY}'],
        region_name=os.environ[f'{profile}_{S3_REGION}'],
    )
    return S3FileStore(session,bucket)

class S3FileStore():
    def __init__(self,session,bucket):
        self.bucket=bucket
        self.session=session
        self.resource=self.session.resource('s3')
        self.client = self.session.client('s3')

    def get_object_info(self,path):
        s3Path = path.removeprefix("/")
        objectSummary=self.resource.ObjectSummary(self.bucket,s3Path)
        return S3FileInfo(objectSummary)
    
    def get_dir(self,path):
        s3Path = path.removeprefix("/")
        if s3Path[-1]!="/":
            s3Path=s3Path+"/"
        paginator = self.client.get_paginator('list_objects_v2')
        params = {'Bucket': self.bucket, 'Prefix': s3Path, 'Delimiter':'/'}
        page_iterator = paginator.paginate(**params)
        count=0
        result=[]
        for page in page_iterator:
            for prefix in page['CommonPrefixes']:
                fso=FileStoreResultObject(
                    count,
                    prefix['Prefix'],
                    "",
                    prefix['Prefix'],
                    "",
                    True,
                    "",
                    ""
                )
                result.append(fso)
                count=count+1
            for s3object in page['Contents']:
                fso=FileStoreResultObject(
                    count,
                    os.path.basename(s3object['Key']),
                    str(s3object['Size']),
                    os.path.dirname(s3object['Key']),
                    os.path.splitext(s3object['Key'])[1][1:],
                    False,
                    s3object['LastModified'],
                    ""
                )
                result.append(fso)
                count=count+1
        return result
    
    def get_object(self,path: str) -> IStreamingBody:
        s3Path = path.removeprefix("/")
        s3object=self.resource.Object(self.bucket,s3Path)
        return s3object.get()['Body']
    
    def put_object(self,path,reader):
        s3Path = path.removeprefix("/")
        config = TransferConfig(
            multipart_threshold=MULTIPART_THRESHOLD,
            multipart_chunksize=MULTIPART_CHUNKSIZE,
            max_concurrency=S3_TRANSFER_CONCURRENCY,
        )

        self.client.upload_fileobj(
            reader,
            self.bucket,
            s3Path,
            Config=config
        )






    