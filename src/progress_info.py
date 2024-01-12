from typing import Literal, NotRequired, Optional, TypedDict

from pyparsing import Any


class BaseProgressInfo(TypedDict):
    status: str
    info_dict: dict[str, Any]


class ErrorProgressInfo(BaseProgressInfo):
    status: Literal["error"]


class OkProgressInfo(BaseProgressInfo):
    filename: str
    tmpfilename: NotRequired[str]
    downloaded_bytes: NotRequired[int]
    total_bytes: NotRequired[Optional[int]]
    total_bytes_estimate: NotRequired[Optional[int]]
    elapsed: NotRequired[int]
    eta: NotRequired[int]
    speed: NotRequired[Optional[float]]
    fragment_index: NotRequired[int]
    fragment_count: NotRequired[int]


class DownloadProgressInfo(OkProgressInfo):
    status: Literal["downloading"]


class FinishedProgressInfo(OkProgressInfo):
    status: Literal["finished"]


ProgressInfo = ErrorProgressInfo | DownloadProgressInfo | FinishedProgressInfo
