import os
import json

import bottle

import requests


headers= {'Content-Type': 'application/x-www-form-urlencoded'}
payload = {'grant_type': 'client_credentials',
            'client_id': '035614f1-eae4-422e-afb0-67d8618002d0',
            #'client_secret': 'dkprmZO118]bhDTUOL70_{]',
            'client_secret': 'nwjttDODP44-(wyIXF463;|',
            'scope': 'https://graph.microsoft.com/.default'
            }
url='https://login.microsoftonline.com/common/oauth2/v2.0/token'

try:
    r = requests.post(url, data=payload, headers=headers)
except Exception as e:
    print(e)
#print(r.json()['access_token'])
token = r.json()['access_token']
auth_value='Bearer '+token
print(auth_value)
item_url='https://graph.microsoft.com/v1.0/drives/b!4pmGXDi-KkaZzoGCawwTwEAoCJYbDBhNmWwERAiudBtGIy-UIkzbRbezRR4R4jqn/'

item_header={'Authorization': auth_value}
r2 = requests.get(item_url,headers=item_header)
print(r2.text)
