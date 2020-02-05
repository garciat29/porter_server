import requests
import json
import config.sharepoint as config


def get_auth():
    headers= {'Content-Type': 'application/x-www-form-urlencoded'}
    auth_url='https://login.microsoftonline.com/common/oauth2/v2.0/authorize?'
    auth_payload= { 'client_id': config.CLIENT_ID,
                'response_type':'code',
                'redirect_uri':'https://login.microsoftonline.com/common/oauth2/nativeclient',
                'response_mode':'query',
                'scope': 'offline_access sites.readwrite.all',
                'state': '12345'
                }

    r=requests.get(auth_url, params=auth_payload)
    if r.history:
        print("Request was redirected")
        for resp in r.history:
            print(resp.status_code, resp.url)
    return r.text

if __name__ == '__main__':
    print(get_auth())
