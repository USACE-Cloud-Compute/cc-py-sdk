import os
import numpy as np
import pandas
import tiledb
import cc.plugin_manager as pm
from cc.datastore import DataStore
from cc.event_store import *


_defaultAttrName: str = "a"
_defaultMetadataPath: str = "/scalars"
_defaultTileExtent: int = 256


# class TileDbEventStore(ISimpleArrayStore):
class TileDbEventStore:

    def connect(self, data_store: DataStore):
        profile = data_store.profile
        root_path = data_store.params["root"]

        self.s3bucket = os.environ[f"{profile}_{pm.AwsS3Bucket}"]
        s3region = os.environ[f"{profile}_{pm.AwsDefaultRegion}"]
        s3id = os.environ[f"{profile}_{pm.AwsAccessKeyId}"]
        s3key = os.environ[f"{profile}_{pm.AwsSecretAccessKey}"]
        self.uri = f"s3://{self.s3bucket}/{root_path}/event_store"
        config = tiledb.Config()
        config["vfs.s3.region"] = s3region
        config["vfs.s3.aws_access_key_id"] = s3id
        config["vfs.s3.aws_secret_access_key"] = s3key
        config["vfs.s3.multipart_part_size"] = str(5 * 1024 * 1024)
        config["vfs.s3.max_parallel_ops"] = "2"

        self.context = tiledb.default_ctx(config)
        self._create_attribute_array()

    def _create_attribute_array(self):
        uri = self.uri + _defaultMetadataPath
        obj_type = tiledb.object_type(uri, self.context)
        if obj_type != None:
            return  # already created the array
        # need to create the array that will hold metadata
        dims = tiledb.Dim(name="rows", domain=(1, 1), tile=1, dtype=np.int32)
        dom = tiledb.Domain(dims, ctx=self.context)
        attr = tiledb.Attr(name=_defaultAttrName, dtype=np.int32)
        schema = tiledb.ArraySchema(
            domain=dom,
            sparse=False,
            attrs=[attr],
            cell_order=LayoutOrder.ROWMAJOR.value,
            tile_order=LayoutOrder.ROWMAJOR.value,
        )
        tiledb.Array.create(uri=uri, schema=schema, ctx=self.context)

    def create_array(self, input: CreateArrayInput):
        dims = []
        # @TODO add string dimension type support
        for input_dim in input.dimensions:
            dims.append(
                tiledb.Dim(
                    name=input_dim.name,
                    dtype=input_dim.dimension_type,
                    domain=input_dim.domain,
                    tile=input_dim.tile_extent,
                    ctx=self.context,
                )
            )
        dom = tiledb.Domain(*dims, ctx=self.context)

        attrs = []
        for attrkey, attrval in input.attributes.items():
            attrs.append(tiledb.Attr(name=attrkey, dtype=attrval, ctx=self.context))

        isSparse = input.array_type
        schema = tiledb.ArraySchema(
            domain=dom,
            sparse=input.array_type,
            attrs=attrs,
            cell_order=input.cell_layout.value,
            tile_order=input.tile_layout.value,
            ctx=self.context,
        )

        tiledb.Array.create(uri=self.uri + "/" + input.array_path, schema=schema, ctx=self.context)

    def put_array(self, input: PutArrayInput):

        if input.array_type == ArrayType.DENSE:
            # build input dict:
            writeinput = {}
            for buffer in input.buffers:
                writeinput[buffer.attr_name] = buffer.buffer
                # writeinput[buffer.attr_name] = (buffer.offsets, buffer.buffer)

            with tiledb.DenseArray(uri=self.uri + "/" + input.array_path, mode="w", ctx=self.context) as array:
                array[:] = writeinput
                # array[input.buffer_range] = writeinput

        elif input.array_type == ArrayType.SPARSE:
            pass

    def get_array(self, input: GetArrayInput):
        slices = []
        for i in range(0, len(input.buffer_range), 2):
            slices.append(slice(input.buffer_range[i], input.buffer_range[i + 1]))

        with tiledb.open(uri=self.uri + "/" + input.array_path, mode="r", ctx=self.context) as array:
            q = array.query(attrs=input.attrs)
            if input.df:
                return q.df[*slices]
            else:
                return q[*slices]
