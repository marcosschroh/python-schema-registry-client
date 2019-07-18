import urllib

from collections import defaultdict


class UrlManager:
    def __init__(self, base_url: str, paths: list) -> None:
        parsed_url = urllib.parse.urlparse(base_url)

        assert (
            parsed_url.scheme
        ), f"The url does not have a schema, add one. For example http://{base_url}"

        # this is the absolute url to the server
        self.base_url = base_url

        self.paths = {path.name: path for path in map(lambda path: Path(path), paths)}

    def url_for(self, func: str, **kwargs) -> tuple:
        """
        Generate a url for a given function
        """
        path = self.paths.get(func)
        url = path.generate_url(**kwargs)

        return urllib.parse.urljoin(self.base_url, url), path.method


class Path:
    def __init__(self, path: dict) -> None:
        self.func = path[0]
        self.url = path[1]
        self.method = path[2]

    @property
    def name(self) -> str:
        return self.func

    def generate_url(self, *args, **kwargs) -> str:
        parameters = {key: value for key, value in kwargs.items() if value}

        return self.url.format_map(defaultdict(str, **parameters))
