import os
import abc
from cc_py_sdk.filesapi import *
from cc_py_sdk.datastore import (
    DataStore,
    IStreamingBody,
    IStoreReader,
    IStoreWriter,
    IConnectionDataStore,
)


class S3DataStore(IConnectionDataStore, IStoreReader, IStoreWriter):

    def __init__(self):
        self.filestore = None

    def connect(self, ds: DataStore):
        self.data_store = ds
        bucket = os.environ[f"{ds.profile}_{S3_BUCKET}"]
        self.filestore = NewS3FileStore(ds.profile, bucket)

    def get(self, path: str, datapath: str) -> IStreamingBody:
        # s3 file store does not use the data path
        return self.filestore.get_object(path)

    def put(self, reader: IStreamingBody, destpath: str, datapath: str):
        # s3 file store does not use the data path
        self.filestore.put_object(destpath, reader)
