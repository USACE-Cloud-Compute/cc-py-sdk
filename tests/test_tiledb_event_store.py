import pytest
import os
import numpy as np
import logging
from pathlib import Path


def test_tiledb_event_store():
    from cc import plugin_manager
    from cc import event_store_tiledb
    from cc import event_store

    a = event_store.LayoutOrder.COLMAJOR
    print(a.value)

    for name, value in os.environ.items():
        print("{0}: {1}".format(name, value))

    pm = plugin_manager.PluginManager()
    pl = pm.get_payload()
    estore = pl.get_store("EVENT_STORE")
    print("TESTING 123 123")
    tdb = event_store_tiledb.TileDbEventStore()
    tdb.connect(estore)

    createInput = event_store.CreateArrayInput(
        attributes={"A1": np.int32},
        dimensions=[
            event_store.ArrayDimension(name="d1", domain=[1, 4], tile_extent=2, dimension_type=np.int32),
            event_store.ArrayDimension(name="d2", domain=[1, 4], tile_extent=2, dimension_type=np.int32),
        ],
        array_path="/simulations/test1",
        array_type=event_store.ArrayType.DENSE.value,
        cell_layout=event_store.LayoutOrder.ROWMAJOR,
        tile_layout=event_store.LayoutOrder.ROWMAJOR,
    )

    tdb.create_array(createInput)

    data = np.array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12], [13, 14, 15, 16]], dtype=np.int32)

    putInput = event_store.PutArrayInput(
        buffers=[event_store.PutArrayBuffers(attr_name="A1", buffer=data, offsets=None)],
        buffer_range=[1, 4, 1, 4],
        array_path="/simulations/test1",
        array_type=event_store.ArrayType.DENSE,
        put_layout=event_store.LayoutOrder.ROWMAJOR.value,
        coords=None,
    )

    tdb.put_array(putInput)

    getInput = event_store.GetArrayInput(attrs=["A1"], array_path="/simulations/test1", buffer_range=None)

    result = tdb.get_array(getInput)

    print(result)


def test_read_tiledb_event_store():
    from cc import plugin_manager
    from cc import event_store_tiledb
    from cc import event_store

    pm = plugin_manager.PluginManager()
    pl = pm.get_payload()
    estore = pl.get_store("EVENT_STORE")
    print("TESTING 123 123")
    tdb = event_store_tiledb.TileDbEventStore()
    tdb.connect(estore)

    getInput = event_store.GetArrayInput(
        attrs=["A1"], array_path="/simulations/test1", buffer_range=[1, 3, 1, 2], df=True
    )

    result = tdb.get_array(getInput)

    print(result)
