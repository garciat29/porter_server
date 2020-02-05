#library that allows for REST API calls
import requests
#library for parsing JSON responses
import json

headers= {'Content-Type': 'application/x-www-form-urlencoded'}
payload = {'grant_type': 'client_credentials',
            'client_id': 'b9d58039-ca3f-4c07-8c11-c114c1fb2531',
            'client_secret': 'hicZPRLJ179_?hstwVM02+?',
            'scope': 'https://graph.microsoft.com/.default'
            }
auth_url='https://login.microsoftonline.com/common/oauth2/v2.0/token'

#Make the authorization request
r = requests.post(auth_url, data=payload, headers=headers)

token = r.json()['access_token']
auth_value='Bearer '+token
print(auth_value)


folder_url= 'https://graph.microsoft.com/v1.0/drive/root'
auth_header={'Authorization': auth_value}

#Return information about the drive b!.......
r2 = requests.get(folder_url,headers=auth_header)
print(r2.text)



#folder_url='https://graph.microsoft.com/v1.0/drives/b!SgzDxk0bYUGvR8XgBrmU1Nup1QK1EIdFiLf_p4QTPnSitF04UJu0RpmNuazf4gWw'
#folder_url= 'https://graph.microsoft.com/v1.0/drives/b!-Q-olNGAJEiPPyoVAVMVgUZ5-BmLEVlNglMP3kNt272iVly7QFQnQqvybW0cbMIt'
