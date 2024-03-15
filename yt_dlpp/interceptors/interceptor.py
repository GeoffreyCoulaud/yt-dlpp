from argparse import ArgumentParser


class _BaseInterceptor(ArgumentParser):
    """Parser to intercept aruments that are not allowed throughout the app"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # TODO add more disallowed arguments
        # see https://github.com/yt-dlp/yt-dlp?tab=readme-ov-file#general-options


class UrlInterceptor(ArgumentParser):
    """Parser to intercept yt-dlp URLs"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.add_argument("urls", nargs="+")


class InfoInterceptor(_BaseInterceptor):
    """Parser to intercept disallowed arguments for the info worker"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.add_argument("--no-simulate")
        self.add_argument("--dump-json")
        self.add_argument("--dump-single-json")


class DlInterceptor(_BaseInterceptor):
    """Parser to intercept disallowed arguments for the download worker"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.add_argument("--newline")
        self.add_argument("--no-progress")
        self.add_argument("--progress")
        self.add_argument("--console-title")
        self.add_argument("--progress-template")
        self.add_argument("--quiet", "-q")
        self.add_argument("--no-quiet")
        self.add_argument("--print", "-O")
        self.add_argument("--verbose", "-v")
        self.add_argument("--dump-pages")
        self.add_argument("--print-traffic")
