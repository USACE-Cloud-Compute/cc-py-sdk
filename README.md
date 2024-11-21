# cc-python-sdk

[![PyPI version](https://badge.fury.io/py/cc-python-sdk.svg)](https://badge.fury.io/py/cc-python-sdk)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

cc-python-sdk is the Python Software Development Kit used to develop plugins for Cloud Compute.

## Installation

You can install cc-python-sdk in two ways: from source or through the pip package manager.


## Usage

Once cc-python-sdk is installed, you can start using its functionality in your Python projects. Here's a simple example:

```python
import cc

# Use the functions and classes provided by cc_sdk
```

## Documentation
the python SDK simplifies working with cloud compute infrastrcture.  It is primarily facilitating three functions:

1) reading the plugin manifest.  When plugin jobs are run, plugins can optionally use a manifest to stora attributes, input resources, output resources, and connections to data stores

2) intantiating connections to the data stores listed in the payload manifest

3) providing simple and consistant semantics for working with different data stores

the following code illustrates how the sdk can be used in a plugin.  the payload is is reading is inluded in the tests folder:

```python
pm=manager.PluginManager()

    ########## logging ############
    # the default log level is INFO
    # the log level can be set using
    # the CC_LOGGING_LEVEL environment 
    # variable 



    ###############################
    ########## payload ############
    pl=pm.get_payload()

    for attr in pl.attributes:
        print(attr)

    for store in pl.stores:
        print(store)
    
    for input in pl.inputs:
        print(input)
    
    for output in pl.outputs:
        print(output)

    for action in pl.actions:
        print(action)

    ######################################
    ## Plugin Manager utility functions ##
    ######################################
    
    dataSourceName="TestFile"
    dataSourcePath="default"

    #get a data source by name
    data_source=pm.get_input_data_source(dataSourceName)
    #you can also use pm.get_output_data_source and 
    # pm.get_data_source to retrieve data sources by name
    
    #get a store by name
    data_store=pm.get_store(data_source.store_name)

    #you shouldn't need to do this, but for direct low level access to the 
    # store you can get the raw session
    session=store._session

    #get a remote resource as a reader
    #for file or object resources the datapath is set to None
    reader=pm.get_reader(dataSourceName,dataSourcePath,None)
    content=reader.read()
    print(content)

    #get a remote resource as a byte array
    content=pm.get(dataSourceName,dataSourcePath,None)
    print(content)

    #push data to a remote resource
    reader=pm.get_reader(dataSourceName,dataSourcePath,None)
    pm.put(reader,"TestFileOut","default",None)

    #copy a file to the local container
    pm.copy_file_to_local(manager.DataSourceOpInput(
        name="TestFile", #data source name
        pathkey="default",
        datakey=None    
    ),"/data/testfile.txt")

    #copy a local file to a remote
    pm.copy_file_to_remote(
        manager.DataSourceOpInput(
        name="TestFileOut2", #data source name
        pathkey="default",
        datakey=None    
    ),"/data/testfile.txt")

    #copy one data source to another
    pm.copy(
        manager.DataSourceOpInput(
        name="TestFile", #data source name
        pathkey="default",
        datakey=None
        ),
        manager.DataSourceOpInput(
        name="TestFileOut2", #data source name
        pathkey="default",
        datakey=None
        ),
    )

    ######################################
    ## Actions                          ##
    ######################################

    #actions have most of the functionality of the plugin manager but are specific to an individual action

    action=pl.actions[0]

    print(action.type)
    print(action.description)

    #the attribute dictionary is accessible via the attribute property
    attrs=action.attributes

    #get the list of inputs
    inputs=action.inputs()

    #get the list of outputs
    outputs=action.outputs()

    #get a data source by name
    data_source=action.get_input_data_source(dataSourceName)
    #you can also use pm.get_output_data_source and 
    # pm.get_data_source to retrieve data sources by name
    
    #get a store by name
    data_store=action.get_store(data_source.store_name)

    #get a remote resource as a reader
    #for file or object resources the datapath is set to None
    reader=action.get_reader(dataSourceName,dataSourcePath,None)
    content=reader.read()
    print(content)

    #get a remote resource as a byte array
    content=action.get(dataSourceName,dataSourcePath,None)
    print(content)

    #push data to a remote resource
    reader=action.get_reader(dataSourceName,dataSourcePath,None)
    action.put(reader,"TestFileOut","default",None)

    #copy a file to the local container
    action.copy_file_to_local(manager.DataSourceOpInput(
        name="TestFile", #data source name
        pathkey="default",
        datakey=None    
    ),"/data/testfile.txt")

    #copy a local file to a remote
    action.copy_file_to_remote(
        manager.DataSourceOpInput(
        name="TestFileOut2", #data source name
        pathkey="default",
        datakey=None    
    ),"/data/testfile.txt")

    #copy one data source to another
    action.copy(
        manager.DataSourceOpInput(
        name="TestFile", #data source name
        pathkey="default",
        datakey=None
        ),
        manager.DataSourceOpInput(
        name="TestFileOut2", #data source name
        pathkey="default",
        datakey=None
        ),
    )

```



TODO. See example plugin [here](https://<>)

