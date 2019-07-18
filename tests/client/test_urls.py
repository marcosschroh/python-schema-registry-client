import pytest
import urllib

from schema_registry.client import urls


def test_fail_url_manager_creation():
    base_url = "localhost:8081"

    with pytest.raises(AssertionError):
        urls.UrlManager(base_url, [])


def test_urls_generation():
    base_url = "http://localhost:8081"

    paths = [("get_cars", "/cars/{car_id}", "GET"), ("create_car", "/cars", "POST")]

    url_manager = urls.UrlManager(base_url, paths)

    url, method = url_manager.url_for("get_cars")
    assert url == urllib.parse.urljoin(base_url, "/cars/")
    assert method == "GET"

    url, method = url_manager.url_for("get_cars", car_id=10)
    assert url == urllib.parse.urljoin(base_url, "/cars/10")
    assert method == "GET"

    url, method = url_manager.url_for("create_car")
    assert url == urllib.parse.urljoin(base_url, "/cars")
    assert method == "POST"
