import pytest
import os


def test_content():
    from cc_py_sdk import plugin_manager
    pm=plugin_manager.PluginManager()
    pl=pm.get_payload()
    assert 'TEST123' == pl.attributes["test123"]
    assert 'TEST123' in pl.attributes["test123"]