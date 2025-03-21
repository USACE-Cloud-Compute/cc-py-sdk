import os
import abc
from typing import List
from collections import namedtuple
import boto3
from boto3.s3.transfer import TransferConfig

AwsAccessKeyId = "AWS_ACCESS_KEY_ID"
AwsSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
AwsDefaultRegion = "AWS_DEFAULT_REGION"
AwsS3Bucket = "AWS_S3_BUCKET"
AwsEndpoint = "AWS_ENDPOINT"

S3_TRANSFER_CONCURRENCY = 5
MULTIPART_THRESHOLD = 1024 * 1024 * 1000  # 1GB Threshold
MULTIPART_CHUNKSIZE = 1024 * 1024 * 10  # 10 mb chunks

FileStoreResultObject = namedtuple(
    "FileStoreResultObject",
    ["ID", "Name", "Size", "Path", "Type", "IsDir", "Modified", "ModifiedBy"],
)


class IStreamingBody(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def read(self, *amt: int) -> bytes:
        pass


##############################################################
####  S3
##############################################################


class S3FileInfo:
    """
    Implements the FileInfo interface for S3 object summaries.

    Methods:
    - name()->str: returns the name (i.e. the file portion of the S3 object key)
    - size()->int: returns the size of the object in bytes
    - mod_time()->date: returns the date/time that the object was last modified
    - is_dir()->bool: returns a boolean indicating if the object represents an s3 prefix
    """

    def __init__(self, objectSummary):
        self.objectSummary = objectSummary

    def name(self) -> str:
        return os.path.basename(self.objectSummary.key)

    def size(self) -> int:
        return self.objectSummary.size

    def mod_time(self):
        return self.objectSummary.last_modified

    def is_dir(self):
        return False


def NewS3FileStore(profile, bucket):
    session = boto3.Session(
        aws_access_key_id=os.environ[f"{profile}_{AwsAccessKeyId}"],
        aws_secret_access_key=os.environ[f"{profile}_{AwsSecretAccessKey}"],
        region_name=os.environ[f"{profile}_{AwsDefaultRegion}"],
    )
    endpoint = os.environ.get(f"{profile}_{AwsEndpoint}", None)
    return S3FileStore(session, endpoint, bucket)


class S3FileStore:
    """
    S3 FileStore implementation.

    Attributes:
    - bucket : str
        The name of the S3 bucket. readonly
    - session : boto3.Session
        The S3 session. readonly
    - resource: boto3.Resource
        The boto3 S3 resource object
    - client: boto3.Client
        the boto3 S3 Client object

    Methods:
    - get_object_info(path:str)->S3FileInfo: takes a path (s3 key) and returns a wrapped S3 ObjectSummary
    - get_dir(path:str)->List[FileStoreResultObject]: for the given path will return all directories (common prefixes in S3)
        and files at the path.  Does not recurse into subdirectories.
    - get_object(path:str)->IStreamingBody: for the given file object returns a IStreamingBody (e.g. binary reader)
    - put_object(path:str,reader:IStreamingBody): copies the reader to the given path in S3.
        uses the boto3 upload_fileobj and supports large multipart uploads
    """

    def __init__(self, session, endpoint, bucket):
        self.bucket = bucket
        self.session = session
        self.resource = self.session.resource("s3", endpoint_url=endpoint)
        self.client = self.session.client("s3", endpoint_url=endpoint)
        # session.client("s3", endpoint_url=endpoint)
        # s3_resource = session.resource("s3", endpoint_url=endpoint)

    def get_object_info(self, path) -> S3FileInfo:
        s3Path = path.removeprefix("/")
        objectSummary = self.resource.ObjectSummary(self.bucket, s3Path)
        return S3FileInfo(objectSummary)

    def get_dir(self, path) -> List[FileStoreResultObject]:
        s3Path = path.removeprefix("/")
        if s3Path[-1] != "/":
            s3Path = s3Path + "/"
        paginator = self.client.get_paginator("list_objects_v2")
        params = {"Bucket": self.bucket, "Prefix": s3Path, "Delimiter": "/"}
        page_iterator = paginator.paginate(**params)
        count = 0
        result = []
        for page in page_iterator:
            for prefix in page["CommonPrefixes"]:
                fso = FileStoreResultObject(count, prefix["Prefix"], "", prefix["Prefix"], "", True, "", "")
                result.append(fso)
                count = count + 1
            for s3object in page["Contents"]:
                fso = FileStoreResultObject(
                    count,
                    os.path.basename(s3object["Key"]),
                    str(s3object["Size"]),
                    os.path.dirname(s3object["Key"]),
                    os.path.splitext(s3object["Key"])[1][1:],
                    False,
                    s3object["LastModified"],
                    "",
                )
                result.append(fso)
                count = count + 1
        return result

    def get_object(self, path: str) -> IStreamingBody:
        s3Path = path.removeprefix("/")
        s3object = self.resource.Object(self.bucket, s3Path)
        return s3object.get()["Body"]

    def put_object(self, path: str, reader: IStreamingBody):
        s3Path = path.removeprefix("/")
        config = TransferConfig(
            multipart_threshold=MULTIPART_THRESHOLD,
            multipart_chunksize=MULTIPART_CHUNKSIZE,
            max_concurrency=S3_TRANSFER_CONCURRENCY,
        )

        self.client.upload_fileobj(reader, self.bucket, s3Path, Config=config)
