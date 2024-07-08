import json

gcp_auth_key = {
  "client_id": "",
  "client_secret": "",
  "quota_project_id": "data-engineering-educativeio",
  "refresh_token": "",
  "type": "authorized_user"
}


with open('auth.json', 'w') as outfile:
    json.dump(gcp_auth_key, outfile)