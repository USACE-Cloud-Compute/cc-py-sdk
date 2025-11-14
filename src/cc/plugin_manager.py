import os
import re
import shutil
import logging
from collections import namedtuple
from typing import Any, Dict, Optional, List
from enum import Enum
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from cc.datastore import DataStore, IConnectionDataStore
from cc.datastore_s3 import S3DataStore  # used in globals lookup
from cc.filesapi import *
from cc import filesapi
from cc import logger
from cc import action_runner
from cc.template_substitution import template_substitute

CcPayloadId = "CC_PAYLOAD_ID"
CcManifestId = "CC_MANIFEST_ID"
CcEventNumber = "CC_EVENT_NUMBER"
CcEventIdentifier = "CC_EVENT_IDENTIFIER"
CcProfile = "CC"
CcRootPath = "CC_ROOT"
DEFAULT_CC_ROOT = "/cc_store"
PAYLOAD_FILE_NAME = "payload"
substitutionPattern = "{([^{}]*)}"
Version = "0.9.0"

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

    name: str = ""
    paths: dict = field(default_factory=dict)
    store_name: str = ""
    data_paths: dict[str, str] = field(default_factory=dict)
    id: str = ""  # allow for optional id vals

    def to_json_serializable(self):
        return {
            "name": self.name,
            "paths": self.paths,
            "store_name": self.store_name,
            "data_paths": self.data_paths,
        }


@dataclass_json
@dataclass
class Action:
    """
    Class for plugin actions.  Actions can have their own private set of attributes, stores, inputs, and outputs

    Attributes:
    - type : str
        The type of the action. readonly
    - description : str
        The description of the action. readonly
    - attributes : dict[str, str]
        The set of attributes private to the action. readonly
    - stores : List[DataStore]
        The list of DataStores private to the action. readonly
    - inputs : List[DataSource]
        The list of input DataSources private to the action. readonly
    - outputs : List[DataSource]
        the list of output DataSources private to the action. readonly
    """

    name: str = ""
    type: str = ""
    description: str = ""
    attributes: dict[str, str] = field(default_factory=dict)
    stores: Optional[List["DataStore"]] = field(default_factory=list)
    inputs: Optional[List["DataSource"]] = field(default_factory=list)
    outputs: Optional[List["DataSource"]] = field(default_factory=list)

    def to_json_serializable(self):
        return {
            "name": self.name,
            "type": self.type,
            "description": self.description,
            "attributes": self.attributes,
            "stores": (
                [d.to_json_serializable() for d in self.stores]
                if self.stores is not None
                else []
            ),
            "inputs": (
                [d.to_json_serializable() for d in self.inputs]
                if self.inputs is not None
                else []
            ),
            "outputs": (
                [d.to_json_serializable() for d in self.outputs]
                if self.outputs is not None
                else []
            ),
        }

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

    def get_reader(
        self, data_source_name: str, pathkey: str, datapathkey: str
    ) -> IStreamingBody:
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

    def copy_folder_to_remote(self, ds: DataSourceOpInput, localpath: str):
        return self._iomgr.copy_folder_to_remote(ds, localpath)


@dataclass_json
@dataclass
class Payload:
    attributes: dict[str, str | list | dict] = field(default_factory=dict)
    stores: List["DataStore"] = field(default_factory=list)
    inputs: List["DataSource"] = field(default_factory=list)
    outputs: List["DataSource"] = field(default_factory=list)
    actions: List[Action] = field(default_factory=list)
    _iomgr: any = field(init=False)

    def __post_init__(self):
        self._iomgr = Iomgr(self.attributes, self.stores, self.inputs, self.outputs)
        for action in self.actions:
            action._iomgr = Iomgr(
                action.attributes, action.stores, action.inputs, action.outputs
            )

    def get_store(self, name: str) -> DataStore:
        return self._iomgr.get_store(name)


class PluginManager:
    def __init__(self):
        self.manifestId = os.environ[CcManifestId]
        self.payloadId = os.environ[CcPayloadId]

        # initialize logging configuration
        logger.initLogger()
        logging.info(f"Running: Manifest {self.manifestId}, Payload {self.payloadId}")

        self.ccroot = os.environ[CcRootPath]
        if self.ccroot == "":
            self.ccroot = DEFAULT_CC_ROOT

        # set the CC Store
        self.store = filesapi.NewS3FileStore(
            CcProfile, bucket=os.environ[f"{CcProfile}_{AwsS3Bucket}"]
        )

        # grab the payload

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

        self._substituteAttributeTemplates()
        self._substituteStoreTemplates()
        self._substituteInputTemplates()
        self._substituteOutputTemplates()
        self._substituteActionTemplates()

        # enumerate stores and connect to ones that implement IConnectionDataStore
        for store in self.payload.stores:
            classType = storeTypeToClassMap.get(store.store_type, None)
            if classType != None:
                instance = getNewClassInstance(classType)
                if isinstance(instance, IConnectionDataStore):
                    instance.connect(store)
                    store._session = instance

    def run_actions(self):
        for action in self.payload.actions:
            runner_class = action_runner.get_action_runner(action.name)
            if hasattr(runner_class, "run") and callable(getattr(runner_class, "run")):
                runner = runner_class()
                runner.pm = self
                runner.action = action
                runner.run()

    def get_payload(self) -> Payload:
        return self.payload

    def stores(self) -> List[DataStore]:
        return self._iomgr.stores

    def inputs(self) -> List[DataSource]:
        return self._iomgr.inputs

    def outputs(self) -> List[DataSource]:
        return self._iomgr.outputs

    def get_store(self, name: str) -> DataStore:
        return self._iomgr.get_store(name)

    def get_data_source(self, name: str, iotype: DsIoType) -> DataSource:
        return self._iomgr.get_data_source(name, iotype)

    def get_input_data_source(self, name: str) -> DataSource:
        return self._iomgr.get_input_data_source(name)

    def get_output_data_source(self, name: str) -> DataSource:
        return self._iomgr.get_output_data_source(name)

    def get_reader(
        self, data_source_name: str, pathkey: str, datapathkey: str
    ) -> IStreamingBody:
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

    def copy_folder_to_remote(self, ds: DataSourceOpInput, localpath: str):
        return self._iomgr.copy_folder_to_remote(ds, localpath)

    def _substituteAttributeTemplates(self):
        _handle_template_substitution(self._iomgr.attributes, self._iomgr.attributes)

    def _substituteStoreTemplates(self):
        for store in self._iomgr.stores:
            _handle_template_substitution(store.params, self._iomgr.attributes, False)

    def _substituteInputTemplates(self):
        for input in self._iomgr.inputs:
            new_name = template_substitute(
                "name", input.name, self._iomgr.attributes, False
            )
            input.name = new_name.get("name")
            _handle_template_substitution(input.paths, self._iomgr.attributes)
            _handle_template_substitution(input.data_paths, self._iomgr.attributes)

    def _substituteOutputTemplates(self):
        for output in self._iomgr.outputs:
            new_name = template_substitute(
                "name", output.name, self._iomgr.attributes, False
            )
            output.name = new_name.get("name")
            _handle_template_substitution(output.paths, self._iomgr.attributes)
            _handle_template_substitution(output.data_paths, self._iomgr.attributes)

    def _substituteActionTemplates(self):
        for action in self.payload.actions:
            # run templates for action attributes
            _handle_template_substitution(
                action._iomgr.attributes, self._iomgr.attributes
            )

            # combine action and payload attributes for conciseness
            combined_attrs = self._iomgr.attributes | action._iomgr.attributes

            for input in action._iomgr.inputs:
                new_name = template_substitute(
                    "name", input.name, combined_attrs, False
                )
                action.name = new_name.get("name")
                _handle_template_substitution(input.paths, combined_attrs)
                _handle_template_substitution(input.data_paths, combined_attrs)

            for output in action._iomgr.outputs:
                new_name = template_substitute(
                    "name", output.name, combined_attrs, False
                )
                action.name = new_name.get("name")
                _handle_template_substitution(output.paths, combined_attrs)
                _handle_template_substitution(output.data_paths, combined_attrs)


class Iomgr:
    def __init__(self, attrs, stores, inputs, outputs):
        if attrs == None:
            self.attributes = {}
        else:
            self.attributes = attrs

        if stores == None:
            self.stores = []
        else:
            self.stores = stores

        if inputs == None:
            self.inputs = []
        else:
            self.inputs = inputs

        if outputs == None:
            self.outputs = []
        else:
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

    def put(
        self, reader: IStreamingBody, data_source_name: str, pathkey: str, datakey: str
    ):
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

    def copy_folder_to_remote(self, dest: DataSourceOpInput, localpath: str):
        dest_ds = self.get_output_data_source(dest.name)
        deststore = self.get_store(dest_ds.store_name)
        destpath = deststore.full_path(dest_ds.paths[dest.pathkey])
        deststore._session.put_folder(localpath, destpath)


def _handle_template_substitution(
    templates: dict, values: dict, allow_expansion: bool = True
):
    def template_walk(d: dict | list):
        if isinstance(d, dict):
            updates = {}
            expanded = []
            for k, v in d.items():
                if isinstance(v, str):
                    filled = template_substitute(k, v, values, allow_expansion)
                    if len(filled) > 1:
                        expanded.append(k)
                    updates |= filled
                else:
                    template_walk(v)
            d |= updates
            for k in expanded:
                del d[k]
        elif isinstance(d, list):
            for i, v in enumerate(d):
                if isinstance(v, str):
                    filled = template_substitute(i, v, values, False)
                    d[i] = filled[i]
                else:
                    template_walk(v)

    template_walk(templates)
