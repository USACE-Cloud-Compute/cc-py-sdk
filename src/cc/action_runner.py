# registry.py
ACTION_RUNNER_REGISTRY: dict[str, type] = {}


def register_action_runner(name: str, cls: type):
    ACTION_RUNNER_REGISTRY[name] = cls


def get_action_runner(name: str) -> type:
    return ACTION_RUNNER_REGISTRY[name]
