import os
import abc
from cc.filesapi import *
from cc.datastore import (
    DataStore,
    IStreamingBody,
    IStoreReader,
    IStoreWriter,
    IConnectionDataStore,
)


class S3DataStore(IConnectionDataStore, IStoreReader, IStoreWriter):
    """
    S3 Datastore implementation.  Uses a filesapi.S3FilesStore to handle S3.
       This Datastore implements three interfaces and provides binary object access
       in S3.

    Methods:
    - connect(ds:DataStore): creates an S3 Session using boto3
    - get(path:str,datapath:str): gets a reader for the S3 object described in the path
        interface argument datapath is ignored
    - put(reader: IStreamingBody, destpath:str, datapath:str): takes a reader and uploads
        the data into an object described by the path.  interface argument datapath is ignored
    """

    def __init__(self):
        self.filestore = None

    def connect(self, ds: DataStore):
        self.data_store = ds
        bucket = os.environ[f"{ds.profile}_{AwsS3Bucket}"]
        self.filestore = NewS3FileStore(ds.profile, bucket)

    def get(self, path: str, datapath: str) -> IStreamingBody:
        # s3 file store does not use the data path
        return self.filestore.get_object(path)

    def put(self, reader: IStreamingBody, destpath: str, datapath: str):
        # s3 file store does not use the data path
        self.filestore.put_object(destpath, reader)
