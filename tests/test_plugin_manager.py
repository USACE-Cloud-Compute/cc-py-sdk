import pytest
import os
import logging


def test_plugin_manager():
    from cc import plugin_manager

    pm = plugin_manager.PluginManager()
    pl = pm.get_payload()
    reader = pm.get_reader("TestFile", "default", None)
    content = reader.read()
    print(content)

    # get a data source by name
    data_source = pm.get_input_data_source("TestFile")
    data_store = pm.get_store(data_source.store_name)
    session = data_store._session
    print(session)
    assert "TEST123" == pl.attributes["test123"]
    assert len(pl.stores) == 1
    assert len(pl.inputs) == 4
    assert len(pl.outputs) == 2
    assert len(pl.actions) == 1
    # assert 'TEST123' in pl.attributes["test123"]
    logging.info("TEST1234")
