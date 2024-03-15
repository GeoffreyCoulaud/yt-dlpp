from argparse import ArgumentParser, Namespace
from typing import Optional, Sequence


class AbstractInterceptor(ArgumentParser):
    """Abstract class for intercepting arguments"""

    def __init__(self) -> None:
        super().__init__(add_help=False, allow_abbrev=False)

    def add_intercepted_arguments(self, *args: str | tuple[str]):
        for arg_or_argtuple in args:
            match arg_or_argtuple:
                case str():
                    self.add_argument(arg_or_argtuple)
                case tuple():
                    self.add_argument(*arg_or_argtuple)


class InputUrlInterceptorNamespace(Namespace):
    """Namespace for yt-dlp URLs interceptor"""

    batch_file: str | None
    urls: list[str]


class InputUrlsInterceptor(AbstractInterceptor):
    """Parser to intercept yt-dlp URLs"""

    def __init__(self) -> None:
        super().__init__()
        self.add_intercepted_arguments("--batch-file")
        self.add_argument("urls", nargs="*")

    def parse_known_args(
        self,
        args: Optional[Sequence[str]] = None,
        namespace: Optional[Namespace] = None,
    ) -> tuple[InputUrlInterceptorNamespace, list[str]]:
        return super().parse_known_args(args, namespace)


class _AppInterceptor(AbstractInterceptor):
    """Parser to intercept aruments that are not allowed throughout the app"""

    def __init__(self) -> None:
        super().__init__()
        self.add_intercepted_arguments(
            ("--help", "-h"),
            "--version",
            ("--update", "-U"),
            "--update-to",
            "--dump-user-agent",
            "--list-extractors",
            "--extractor-descriptions",
            "--alias",
            "--batch-file",  # This is intercepted by InputUrlInterceptor only
        )


class InfoInterceptor(_AppInterceptor):
    """Parser to intercept disallowed arguments for the info worker"""

    def __init__(self) -> None:
        super().__init__()
        self.add_intercepted_arguments(
            "--no-simulate",
            "--dump-single-json",
        )


class DlInterceptor(_AppInterceptor):
    """Parser to intercept disallowed arguments for the download worker"""

    def __init__(self) -> None:
        super().__init__()
        self.add_intercepted_arguments(
            "--no-progress",
            "--console-title",
            "--progress-template",
            ("--quiet", "-q"),
            ("--print", "-O"),
            ("--verbose", "-v"),
            "--dump-pages",
            "--print-traffic",
        )
