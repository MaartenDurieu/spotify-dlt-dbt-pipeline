import base64
import os
import urllib.parse

import requests
from flask import Flask, redirect, request, session, url_for
from utils import load_secrets

from constants import REDIRECT_URI, REQUIRED_API_SCOPE_PERMISSIONS, SPOTIFY_AUTH_URL, SPOTIFY_BASE_URL, SPOTIFY_TOKEN_URL


app = Flask(__name__)
app.secret_key = os.urandom(24)

secrets = load_secrets()
CLIENT_ID = secrets["CLIENT_ID"]
CLIENT_SECRET = secrets["CLIENT_SECRET"]
SPOTIFY_ME_URL = f"{SPOTIFY_BASE_URL}/me"


@app.route("/")
def login():
    auth_query_parameters = {"response_type": "code", "redirect_uri": REDIRECT_URI, "scope": REQUIRED_API_SCOPE_PERMISSIONS, "client_id": CLIENT_ID}
    url_args = "&".join([f"{key}={urllib.parse.quote(val)}" for key, val in auth_query_parameters.items()])
    auth_url = f"{SPOTIFY_AUTH_URL}/?{url_args}"
    return redirect(auth_url)


@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return "Error: no code provided"

    # Encode client_id and client_secret
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth_str = base64.b64encode(auth_str.encode()).decode()

    # Exchange code for access token
    token_data = {"grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT_URI}
    token_headers = {"Authorization": f"Basic {b64_auth_str}", "Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(SPOTIFY_TOKEN_URL, data=token_data, headers=token_headers)
    response_data = response.json()

    if "access_token" not in response_data:
        return f"Error getting token: {response_data}"

    access_token = response_data["access_token"]
    refresh_token = response_data.get("refresh_token")

    # Store tokens in session
    session["access_token"] = access_token
    session["refresh_token"] = refresh_token

    # The intial access_token is valid for 3600s
    # Each new access_token should be requested using the refresh token
    # This refresh_token stays valid unless rotated
    # By using the specific refresh_token, we can refresh the access_token which will contain the required scope/permissions
    return f"""
        <h2>Spotify Authentication Successful!</h2>
        <p>Access Token: {access_token}</p>
        <p>Refresh Token: {refresh_token}</p>
        <p>Go to <a href='/me'>/me</a> to see your Spotify profile.</p>
    """


@app.route("/me")
def me():
    access_token = session.get("access_token")
    if not access_token:
        return redirect(url_for("login"))

    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(SPOTIFY_ME_URL, headers=headers)
    return response.json()


if __name__ == "__main__":
    app.run(debug=True, port=8080)
