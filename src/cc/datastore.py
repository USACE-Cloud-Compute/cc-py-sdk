import abc
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from cc.filesapi import IStreamingBody


@dataclass_json
@dataclass
class DataStore:
    """
    A class that represents a data store and its attributes.

    Attributes:
    - name : str
        The name of the data store. readonly
    - id : str
        The ID of the data store. optional/readonly
    - params : dict[str, str]
        The parameters of the data store represented as a dictionary. readonly
    - store_type : StoreType
        The type of the data store. readonly
    - ds_profile : str
        The profile of the data store. readonly
    - _session : any, optional
        The private attribute reference to the native session or connection to the data store instance type.

    Methods:
    - full_path(relative_path:str)->str: given a path within a store, returns the full path on the device.
      Basically just concatonates the store root path to the relative path.

    """

    name: str
    store_type: str
    profile: str
    params: dict = field(default_factory=dict)
    id: str = ""  # allow for optional id vals
    _session: any = field(init=False)

    def __post_init__(self):
        print(f"Initialized {self.name} store type {self.store_type}")

    def full_path(self, relative_path: str) -> str:
        return self.params["root"] + "/" + relative_path

    def to_json_serializable(self):
        return {
            "name": self.name,
            "store_type": self.store_type,
            "profile": self.profile,
            "params": self.params,
        }


class IConnectionDataStore(metaclass=abc.ABCMeta):
    """
    An interface for Data Store Instances that connect to external sources.

    Methods:
    - connect(ds:DataStore): given the type and configuration of the DataStore,
      creates a connection to the store
    """

    @abc.abstractmethod
    def connect(self, ds: DataStore):
        pass

    """
    An interface for Data Store Instances that can return binary readers.
    

    Methods:
    - get(path:str, datapath:str): given the path and datapath return a binary reader
    """


class IStoreReader(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get(self, path: str, datapath: str) -> IStreamingBody:
        pass


class IStoreWriter(metaclass=abc.ABCMeta):
    """
    An interface for Data Store Instances that can write binary data.


    Methods:
    - put(reader:IStreamingBody, path:str, datapath:str): given a reader and path and datapath, write the data
    """

    @abc.abstractmethod
    def put(self, reader: IStreamingBody, destpath: str, datapath: str):
        pass
