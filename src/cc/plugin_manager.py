import cc.filesapi
import os
import re
import json
import shutil
from collections import namedtuple
from typing import List, Dict
from enum import Enum
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from cc.datastore import DataStore, IConnectionDataStore
from cc.datastore_s3 import S3DataStore  # used in globals lookup
from cc.filesapi import IStreamingBody
from cc import filesapi

CcPayloadId = "CC_PAYLOAD_ID"
CcManifestId = "CC_MANIFEST_ID"
CcEventNumber = "CC_EVENT_NUMBER"
CcProfile = "CC"
CcRootPath = "CC_ROOT"
DEFAULT_CC_ROOT = "/cc_store"
PAYLOAD_FILE_NAME = "payload"
substitutionPattern = "{([^{}]*)}"

DsIoType = Enum("DsIoType", [("INPUT", 1), ("OUTPUT", 2), ("ALL", 3)])
DataSourceOpInput = namedtuple("DataSourceOpInput", ["name", "pathkey", "datakey"])

StoreType = Enum(
    "StoreType",
    [
        ("STOREREADER", 1),
        ("STOREWRITER", 2),
        ("CONNECTION", 3),
        ("SIMPLEARRAY", 4),
        ("MULTIDIMENSIONALARRAY", 5),
        ("ATTRIBUTE", 6),
    ],
)

storeTypeToClassMap = {"S3": "S3DataStore"}


def getNewClassInstance(name: str) -> any:
    constructor = globals()[name]
    return constructor()


@dataclass_json
@dataclass
class DataSource:
    """
    DataSources are references to data in a DataStore

    Attributes:
    - name : str
        The name of the data source. readonly
    - id : str
        The ID of the data store. optional/readonly
    - paths : dict[str, str]
        The set of paths that reference all of the data that consistites the full Datasource. readonly
    - data_paths : dict[str, str]
        The internal data paths for multi-dataset formats. readonly
    - store_name : str
        The name of the DataStore that holds the DataSet. readonly
    """

    name: str
    paths: dict
    data_paths: dict
    store_name: str
    id: str = ""  # allow for optional id vals


@dataclass_json
@dataclass
class Action:
    type: str
    description: str
    attributes: dict
    stores: List[DataStore]
    inputs: List[DataSource]
    outputs: List[DataSource]

    def inputs(self) -> List[DataSource]:
        return self._iomgr.inputs

    def outputs(self) -> List[DataSource]:
        return self._iomgr.outputs

    def get_store(self, name: str) -> DataStore:
        return self._iomgr.get_store

    def get_data_source(self, name: str, iotype: DsIoType) -> DataSource:
        return self._iomgr.get_data_source(name, iotype)

    def get_input_data_source(self, name: str) -> DataSource:
        return self._iomgr.get_input_data_source(name)

    def get_output_data_source(self, name: str) -> DataSource:
        return self._iomgr.get_output_data_source(name)

    def get_reader(self, data_source_name: str, pathkey: str, datapathkey: str) -> IStreamingBody:
        return self._iomgr.get_reader(data_source_name, pathkey, datapathkey)

    def get(self, data_source_name: str, pathkey: str, datapathkey: str) -> bytes:
        reader = self._iomgr.get_reader(data_source_name, pathkey, datapathkey)
        return reader.read()

    def put(
        self,
        reader: IStreamingBody,
        data_source_name: str,
        pathkey: str,
        datapathkey: str,
    ):
        return self._iomgr.put(reader, data_source_name, pathkey, datapathkey)

    def copy(self, ds1: DataSourceOpInput, ds2: DataSourceOpInput):
        return self._iomgr.copy(ds1, ds2)

    def copy_file_to_local(self, ds: DataSourceOpInput, localpath: str):
        return self._iomgr.copy_file_to_local(ds, localpath)

    def copy_file_to_remote(self, ds: DataSourceOpInput, localpath: str):
        return self._iomgr.copy_file_to_remote(ds, localpath)


@dataclass_json
@dataclass
class Payload:
    attributes: dict
    stores: List[DataStore]
    inputs: List[DataSource]
    outputs: List[DataSource]
    actions: List[Action]
    # __iomgr: any
    _iomgr: any = field(init=False)

    def __post_init__(self):
        self._iomgr = Iomgr(self.attributes, self.stores, self.inputs, self.outputs)

    def get_store(self, name: str) -> DataStore:
        return self._iomgr.get_store(name)


class PluginManager:
    def __init__(self):
        self.ccroot = os.environ[CcRootPath]
        if self.ccroot == "":
            self.ccroot = DEFAULT_CC_ROOT

        # set the CC Store
        self.store = filesapi.NewS3FileStore(CcProfile, bucket=os.environ[f"{CcProfile}_{filesapi.S3_BUCKET}"])

        # grab the payload
        self.payloadId = os.environ[CcPayloadId]
        path = f"{self.ccroot}/{self.payloadId}/{PAYLOAD_FILE_NAME}"
        reader = self.store.get_object(path)
        content = reader.read()
        self.payload = Payload.from_json(content)

        # set the payload IO Manager
        self._iomgr = Iomgr(
            self.payload.attributes,
            self.payload.stores,
            self.payload.inputs,
            self.payload.outputs,
        )

        # enumerate stores and connect to ones that implement IConnectionDataStore
        for store in self.payload.stores:
            print(store.name)
            classType = storeTypeToClassMap.get(store.store_type, None)
            if classType != None:
                instance = getNewClassInstance(classType)
                if isinstance(instance, IConnectionDataStore):
                    instance.connect(store)
                    store._session = instance

        self._substitutePathVariables()

    def get_payload(self) -> Payload:
        return self.payload

    def stores(self) -> List[DataStore]:
        return self._iomgr.stores

    def inputs(self) -> List[DataSource]:
        return self._iomgr.inputs

    def outputs(self) -> List[DataSource]:
        return self._iomgr.outputs

    def get_store(self, name: str) -> DataStore:
        return self._iomgr.get_store

    def get_data_source(self, name: str, iotype: DsIoType) -> DataSource:
        return self._iomgr.get_data_source(name, iotype)

    def get_input_data_source(self, name: str) -> DataSource:
        return self._iomgr.get_input_data_source(name)

    def get_output_data_source(self, name: str) -> DataSource:
        return self._iomgr.get_output_data_source(name)

    def get_reader(self, data_source_name: str, pathkey: str, datapathkey: str) -> IStreamingBody:
        return self._iomgr.get_reader(data_source_name, pathkey, datapathkey)

    def get(self, data_source_name: str, pathkey: str, datapathkey: str) -> bytes:
        reader = self._iomgr.get_reader(data_source_name, pathkey, datapathkey)
        return reader.read()

    def put(
        self,
        reader: IStreamingBody,
        data_source_name: str,
        pathkey: str,
        datapathkey: str,
    ):
        return self._iomgr.put(reader, data_source_name, pathkey, datapathkey)

    def copy(self, ds1: DataSourceOpInput, ds2: DataSourceOpInput):
        return self._iomgr.copy(ds1, ds2)

    def copy_file_to_local(self, ds: DataSourceOpInput, localpath: str):
        return self._iomgr.copy_file_to_local(ds, localpath)

    def copy_file_to_remote(self, ds: DataSourceOpInput, localpath: str):
        return self._iomgr.copy_file_to_remote(ds, localpath)

    def _substitutePathVariables(self):
        for input in self._iomgr.inputs:
            newpath = parameter_substitute(input.paths["default"], self._iomgr.attributes)
            print(newpath)


class Iomgr:
    def __init__(self, attrs, stores, inputs, outputs):
        self.attributes = attrs
        self.stores = stores
        self.inputs = inputs
        self.outputs = outputs

    def get_store(self, name: str) -> DataStore:
        for ds in self.stores:
            if ds.name == name:
                return ds

    def get_data_source(self, name: str, iotype: DsIoType) -> DataSource:
        sources = []

        match iotype:
            case DsIoType.INPUT | DsIoType.ALL:
                sources = self.inputs
            case DsIoType.OUTPUT | DsIoType.ALL:
                sources = sources + self.outputs

        for i in sources:
            if i.name == name:
                return i

    def get_input_data_source(self, name: str) -> DataSource:
        return self.get_data_source(name, DsIoType.INPUT)

    def get_output_data_source(self, name: str) -> DataSource:
        return self.get_data_source(name, DsIoType.OUTPUT)

    ################################
    ###### BLOB STORE ##############
    def get_reader(self, data_source_name: str, pathkey: str, datakey: str):
        data_source = self.get_input_data_source(data_source_name)
        data_store = self.get_store(data_source.store_name)
        # path=data_store.params["root"]+"/"+data_source.paths[pathkey]
        path = data_store.full_path(data_source.paths[pathkey])
        streamingBody = data_store._session.get(path, None)
        return streamingBody

    def put(self, reader: IStreamingBody, data_source_name: str, pathkey: str, datakey: str):
        data_source = self.get_output_data_source(data_source_name)
        data_store = self.get_store(data_source.store_name)
        # path=data_store.params["root"]+"/"+data_source.paths[pathkey]
        path = data_store.full_path(data_source.paths[pathkey])
        data_store._session.put(reader, path, datakey)

    def copy(self, src: DataSourceOpInput, dest: DataSourceOpInput):
        src_ds = self.get_input_data_source(src.name)
        srcstore = self.get_store(src_ds.store_name)
        dest_ds = self.get_output_data_source(dest.name)
        deststore = self.get_store(dest_ds.store_name)
        ##check that src is a storereader and dest is a storewriter!
        srcpath = srcstore.full_path(src_ds.paths[src.pathkey])
        destpath = deststore.full_path(dest_ds.paths[dest.pathkey])
        reader = srcstore._session.get(srcpath, None)
        deststore._session.put(reader, destpath, None)

    def copy_file_to_local(self, src: DataSourceOpInput, localpath: str):
        src_ds = self.get_input_data_source(src.name)
        srcstore = self.get_store(src_ds.store_name)
        srcpath = srcstore.full_path(src_ds.paths[src.pathkey])
        reader = srcstore._session.get(srcpath, None)
        with open(localpath, "wb") as f:
            shutil.copyfileobj(reader, f)

    def copy_file_to_remote(self, dest: DataSourceOpInput, localpath: str):
        dest_ds = self.get_output_data_source(dest.name)
        deststore = self.get_store(dest_ds.store_name)
        destpath = deststore.full_path(dest_ds.paths[dest.pathkey])
        with open(localpath, "rb") as f:
            deststore._session.put(f, destpath, None)


def parameter_substitute(param: str, attrs: dict) -> str:
    submatches = re.findall(substitutionPattern, param)
    for sub in submatches:
        parts = sub.split("::")
        match parts[0]:
            case "ATTR":
                newval = attrs[parts[1]]
            case "ENV":
                newval = os.environ[parts[1]]
        return param.replace(f"{{{sub}}}", newval)
