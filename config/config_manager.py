import os
import json


def get_config():
    phase = os.environ.get("PHASE")
    if (phase is not None) and (phase.lower() == "prod"):
        to_open_file = "prod_config.json"
    else:
        to_open_file = "dev_config.json"

    with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), to_open_file)) as f:
        config = json.load(f)

    return {"db_url": config["db_url"], "port": config["port"], "user": config["user"], "pwd": config["pwd"],
            "api_url": config["api_url"]}


# if __name__ == '__main__':
#     print("==========")
#     print(os.path.abspath(os.path.dirname(__file__)))
#     print(get_config()["url"])