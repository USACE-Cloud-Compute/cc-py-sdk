import abc
from enum import Enum
import numpy as np
from typing import List, Dict
from dataclasses import dataclass, field
from cc.filesapi import IStreamingBody

LayoutOrder = Enum(
    "LayoutOrder",
    [
        ("ROWMAJOR", "row-major"),
        ("COLMAJOR", "col-major"),
        ("GLOBAL", "global"),
    ],
)

ArrayType = Enum(
    "ArrayType",
    [
        ("DENSE", False),
        ("SPARSE", True),
    ],
)


@dataclass
class ArrayDimension:
    name: str
    dimension_type: np.dtype
    domain: List[int]
    tile_extent: int


@dataclass
class CreateArrayInput:
    attributes: Dict[str, any]
    dimensions: List[ArrayDimension]
    array_path: str
    array_type: ArrayType
    cell_layout: LayoutOrder
    tile_layout: LayoutOrder


@dataclass
class PutArrayBuffers:
    attr_name: str
    buffer: any
    offsets: List[int]


@dataclass
class PutArrayInput:
    buffers: List[PutArrayBuffers]
    buffer_range: List[int]
    array_path: str
    array_type: ArrayType
    coords: List[List[int]]  # [][]int
    put_layout: LayoutOrder


@dataclass
class PutSimpleArrayInput:
    buffer: any
    dims: List[int]
    array_path: str
    tile_extent: List[int]
    cell_layout: LayoutOrder = field(default=LayoutOrder.ROWMAJOR)
    tile_layout: LayoutOrder = field(default=LayoutOrder.ROWMAJOR)
    put_layout: LayoutOrder = field(default=LayoutOrder.ROWMAJOR)


@dataclass
class GetArrayInput:
    attrs: List[str]
    array_path: str
    buffer_range: List[int] = field(default=None)
    search_order: LayoutOrder = field(default="C")
    df: bool = field(default=False)


class ISimpleArrayStore(metaclass=abc.ABCMeta):
    """
    An interface for Simple Array support in Event Stores .


    Methods:
    - put_simple_array(): given a reader and path and datapath, write the data
    """

    @abc.abstractmethod
    def put_simple_array(self, reader: IStreamingBody, destpath: str, datapath: str):
        pass
