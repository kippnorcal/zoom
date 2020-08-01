import jwt
import os
import time
from urllib.parse import quote

import requests


def encode_uuid(val):
    return quote(quote(val, safe=""), safe="")


class Client:
    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key or os.getenv("ZOOM_KEY")
        self.api_secret = api_secret or os.getenv("ZOOM_SECRET")
        self.token = self.generate_jwt()
        self.base_uri = "https://api.zoom.us/v2"
        self.headers = {"Authorization": f"Bearer {self.token}"}

    def generate_jwt(self):
        header = {"alg": "HS256", "typ": "JWT"}
        payload = {"iss": self.api_key, "exp": int(time.time() + 3600)}
        token = jwt.encode(payload, self.api_secret, algorithm="HS256", headers=header)
        return token.decode("utf-8")

    @property
    def user(self):
        return User()

    @property
    def meeting(self):
        return Meeting()

    @property
    def past_meeting(self):
        return PastMeeting()


class User(Client):
    def list(self, **kwargs):
        return requests.get(
            f"{self.base_uri}/users", params=kwargs, headers=self.headers
        )


class Meeting(Client):
    def list(self, user_id, **kwargs):
        return requests.get(
            f"{self.base_uri}/users/{user_id}/meetings",
            params=kwargs,
            headers=self.headers,
        )


class PastMeeting(Client):
    def get_participants(self, meeting_id, **kwargs):
        meeting_id = encode_uuid(meeting_id)
        return requests.get(
            f"past_meetings/{meeting_id}/participants",
            params=kwargs,
            headers=self.headers,
        )
