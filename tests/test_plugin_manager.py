import pytest
import os
import logging


def test_plugin_manager():
    from cc import plugin_manager
    from cc import action_runner

    pm = plugin_manager.PluginManager()
    pl = pm.get_payload()
    ####
    action_runner.register_action_runner("test", TestRunner)
    action_runner.register_action_runner("ASDFASDF", TestRunner)
    for action in pl.actions:
        name = action.name
        runner_class = action_runner.get_action_runner(name)
        if hasattr(runner_class, "run") and callable(getattr(runner_class, "run")):
            runner = runner_class()
            runner.run()
    pm.run_actions()
    ###
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


class TestRunner:
    def run(self):
        print("RUNNING!!!!!")
