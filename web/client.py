import sys
import json
import requests

sys.path.append("..")
from libs.constants import *


file_name = sys.argv[1]
command = sys.argv[2]

with open(file_name, "r") as f:
    r = requests.post('http://127.0.0.1:{}/{}'.format(SERVER_PORT, command), files={file_name: f})

    if r.status_code != 200:
        print(r.text)
    else:
        print(json.dumps(json.loads(r.text), indent=4, sort_keys=True))
