from collections.abc import Mapping
from typing import Any, NotRequired, TypedDict


class S3Bucket(TypedDict):
    """
    Represents an S3 bucket configuration.

    Attributes:
        name: The name of the bucket.
        is_declare: Whether to create the bucket on startup if it not exists yet. Defaults to True.
        options: Extra keyword arguments merged into the create bucket call when the bucket is declared.
    """

    name: str
    is_declare: NotRequired[bool]
    options: NotRequired[Mapping[str, Any]]
