import base64

import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from tenacity import retry, stop_after_attempt, wait_exponential
from utils import load_secrets

from constants import SPOTIFY_BASE_URL, SPOTIFY_TOKEN_URL


@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
def request_access_token() -> str:
    client_id, client_secret, refresh_token = load_secrets().values()
    auth_str = f"{client_id}:{client_secret}"
    b64_auth_str = base64.b64encode(auth_str.encode()).decode()

    token_data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    token_headers = {"Authorization": f"Basic {b64_auth_str}", "Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(SPOTIFY_TOKEN_URL, data=token_data, headers=token_headers)
    response.raise_for_status()

    token_json = response.json()
    access_token = token_json.get("access_token")

    if not access_token:
        raise ValueError("No access token found in the response")

    return access_token


def create_spotify_client() -> RESTClient:
    """
    Creates and returns a REST client for the Spotify API

    Offset pagination is used as recommended by the documentation.
    For some endpoints, cursor based pagination may be more appropriate, but offset pagination is kept for consistency.

    Even though the documentation states otherwise, no total is returned on some of the API responses, so it has to be set to None.
    """
    paginator = OffsetPaginator(limit=50, offset=0, offset_param="offset", limit_param="limit", total_path=None, stop_after_empty_page=True)
    return RESTClient(base_url=SPOTIFY_BASE_URL, auth=BearerTokenAuth(token=request_access_token()), paginator=paginator)
