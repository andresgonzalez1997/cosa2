import json

def __environment(name):
    file_path = fr"environments/{name}.json"

    with open(file_path) as f: 
        return json.load(f)

dev = __environment("dev")
production = __environment("production")
staging = __environment("staging")