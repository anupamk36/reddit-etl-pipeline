from typing import Optional
import json
import logging
from google.cloud import secretmanager
from static import Reddit


def read_token() -> Optional[dict]:
    """read_token fetchs the token value.

    Returns:
        Optional[dict]: token dictionary
    """
    client = secretmanager.SecretManagerServiceClient()
    secret_detail = (
        f"projects/{Reddit.PROJECT_ID}/secrets/{Reddit.SECRET_STRING}/versions/latest"
    )
    response = client.access_secret_version(name=secret_detail)
    data = response.payload.data.decode("utf-8")
    config = json.loads(data)
    return config


def save_token(config: Optional[dict] = None) -> dict:
    """save_token saves the token data in secrets manager

    Args:
        config (Optional[dict], optional): config dictionary. Defaults to None.

    Returns:
        dict: token data
    """
    client = secretmanager.SecretManagerServiceClient()
    parent = client.secret_path(Reddit.PROJECT_ID, Reddit.SECRET_STRING)
    config = json.dumps(config, indent=4, separators=(
        ",", ": ")).encode("utf-8")
    response = client.add_secret_version(
        request={
            "parent": parent,
            "payload": {"data": config},
        }
    )
    logging.info("Added new secret version : %s", response.name)
    return config
