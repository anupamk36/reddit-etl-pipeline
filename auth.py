from typing import Dict
import requests
from secret import read_token, save_token


class RedditAuth:
    def __init__(self) -> None:
        self.config = self.refresh_token()

    def make_request(self, config: Dict[str, str], data: Dict[str, str]) -> str:
        """Function to make request to access_token endpoint

        Args:
            config (Dict[str, str]): The config with credentials
            data (Dict[str, str]): _description_

        Raises:
            requests.HTTPError: _description_

        Returns:
            str: _description_
        """
        auth = (config["client_id"], config["client_secret"])
        headers = {"User-Agent": "Data Extraction"}
        res = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=auth,
            headers=headers,
            data=data,
            timeout=10,
        )
        res.raise_for_status()
        token = res.json()
        if "error" in token:
            raise requests.HTTPError(f"Error: {token['error']}")

        # Reddit will sometimes return a null refresh token. This will use the previous
        # one if it exists.
        config["refresh_token"] = token.get("refresh_token") or data.get(
            "refresh_token"
        )
        config["access_token"] = token.get(
            "access_token") or data.get("access_token")
        save_token(config)
        return config

    def get_token(self, code: str) -> str:
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.config["redirect_url"],
        }
        return self.make_request(data)

    def refresh_token(self) -> Dict:
        config = read_token()
        refresh_token = config["refresh_token"]
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        return self.make_request(config, data)
