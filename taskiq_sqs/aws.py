import asyncio
import json
import os

import urllib3  # boto3 peer dep (v1)

ECS_CONTAINER_METADATA_URI = "http://169.254.170.2"


class InvalidEnvironment(Exception):
    pass


async def get_container_credentials() -> dict[str, str]:
    """Fetches the ECS task role credentials provided by the metadata service"""
    if not (relative_uri := os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")):
        raise InvalidEnvironment(
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI not defined. This may not be an ECS container."
        )

    http = urllib3.PoolManager()
    resp = await asyncio.to_thread(
        http.request, "GET", f"{ECS_CONTAINER_METADATA_URI}{relative_uri}"
    )
    return json.loads(resp.data)
