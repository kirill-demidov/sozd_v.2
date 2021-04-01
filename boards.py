import requests
import json
from requests.auth import HTTPBasicAuth

startAt = 0
total = None
url = "https://alterosmart.atlassian.net/rest/agile/1.0/board"

headers = {
    "Accept": "application/json"
}
auth = HTTPBasicAuth("k.demidov@alterosmart.com", "L99Ib8xsuFJKtvTn8SpM8F3C")

response = requests.request(
    "GET",
    url ,
    headers=headers,
    auth=auth
)

boards = json.loads(response.text)
print(boards)
