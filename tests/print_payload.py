from cc.plugin_manager import Payload
import json


def print_payload(payload: Payload):
    data = {
        "attributes": payload._iomgr.attributes,
        "stores": [d.to_json_serializable() for d in payload._iomgr.stores],
        "inputs": [d.to_json_serializable() for d in payload._iomgr.inputs],
        "outputs": [d.to_json_serializable() for d in payload._iomgr.outputs],
        "actions": [d.to_json_serializable() for d in payload.actions],
    }
    with open("output_payload.json", "w") as f:
        json.dump(data, f, indent=2)
