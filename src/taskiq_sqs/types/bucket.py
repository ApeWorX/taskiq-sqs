from typing import NotRequired, TypedDict


class S3Bucket(TypedDict):
    """
    Represents an S3 bucket configuration.

    Attributes:
        name: The name of the bucket.
        declare: Whether to create the bucket on startup if it not exists yet. Defaults to True.
    """

    name: str
    declare: NotRequired[bool]
