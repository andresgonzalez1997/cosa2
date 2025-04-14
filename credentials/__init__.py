import json

def credentials(name):
    if name == "process_account":
        file_path = r"credentials/process_account.json"

    with open(file_path) as f: return json.load(f)

process_account = credentials("process_account")