import urllib

import pytest

from schema_registry.client import urls
from schema_registry.client.paths import paths

BASE_URLS = ("http://localhost:8081", "http://localhost:8082/api/schema-registry/")


def test_fail_url_manager_creation():
    base_url = "localhost:8081"

    with pytest.raises(AssertionError):
        urls.UrlManager(base_url, [])


@pytest.mark.parametrize("base_url", BASE_URLS)
def test_url_with_path(base_url):
    paths = [("get_cars", "cars/{car_id}", "GET"), ("create_car", "cars", "POST")]

    url_manager = urls.UrlManager(base_url, paths)
    url, method = url_manager.url_for("get_cars")

    assert base_url in url


@pytest.mark.parametrize("base_url", BASE_URLS)
def test_urls_generation(base_url):
    local_paths = [("get_cars", "cars/{car_id}", "GET"), ("create_car", "cars", "POST")]

    url_manager = urls.UrlManager(base_url, local_paths)
    url, method = url_manager.url_for("get_cars")

    assert url == urllib.parse.urljoin(base_url, "cars/")
    assert method == "GET"

    url, method = url_manager.url_for("get_cars", car_id=10)
    assert url == urllib.parse.urljoin(base_url, "cars/10")
    assert method == "GET"

    url, method = url_manager.url_for("create_car")
    assert url == urllib.parse.urljoin(base_url, "cars")
    assert method == "POST"


@pytest.mark.parametrize("base_url", BASE_URLS)
def test_client_paths(base_url):
    url_manager = urls.UrlManager(base_url, paths)

    for func, path, _ in paths:
        kwargs = {"subject": "my-subject", "version": 1}
        url, method = url_manager.url_for(func, **kwargs)

        assert base_url in url
