import requests
import json

url = 'https://api.football-data.org/v4/matches'
headers = { 'X-Auth-Token': '98080fa6a73e4cc2a5b700b4ec5e30ca' }

# response = requests.get(uri, headers=headers)

# liste_match = []
# for match in response.json()['matches']:
#     print(match)


# url = "https://api.football-data.org/v4/competitions/ESP/standings"
# headers = { "X-Auth-Token": "VOTRE_CLE_API" }

resp = requests.get(url, headers=headers)
print(resp.json())
