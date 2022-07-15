import onedrivesdk
from onedrivesdk.helpers import GetAuthCodeServer
from onedrivesdk.helpers.resource_discovery import ResourceDiscoveryRequest

redirect_uri = 'http://localhost:8080/'
# client_secret_value = 'FLa8Q~34CFm-c-adYRFkXQD4ccIWcU.BtgcWKc0a'
client_secret = 'b88975d9-9116-490a-be52-19a7a8cb5472'
client_id = 'a5d0293b-7710-4c47-96f7-23529fee65dc'
scopes=['wl.signin', 'wl.offline_access', 'onedrive.readwrite']

client = onedrivesdk.get_default_client(
    client_id='your_client_id', scopes=scopes)

auth_url = client.auth_provider.get_auth_url(redirect_uri)

#this will block until we have the code
code = GetAuthCodeServer.get_auth_code(auth_url, redirect_uri)

client.auth_provider.authenticate(code, redirect_uri, client_secret)