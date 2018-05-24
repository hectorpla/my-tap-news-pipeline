import json

class ReadConfigError(Exception):
    def __str__(self):
        return "the config reader didn't read any thing"

def get_config(filename):
    config = None
    with open(filename) as f:
        config = json.load(f)

    if config is None:
        raise ReadConfigError()
    return config