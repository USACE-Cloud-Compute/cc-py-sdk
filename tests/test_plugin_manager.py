import pytest
import os


def test_plugin_manager():
    from cc import plugin_manager

    pm = plugin_manager.PluginManager()
    pl = pm.get_payload()
    assert "TEST123" == pl.attributes["test123"]
    assert len(pl.stores) == 1
    assert len(pl.inputs) == 4
    assert len(pl.outputs) == 2
    assert len(pl.actions) == 1
    # assert 'TEST123' in pl.attributes["test123"]
