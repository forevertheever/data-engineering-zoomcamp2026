"""dlt pipeline to ingest NYC taxi data from a REST API.

This source uses the dlt `rest_api` helper to define a paginated
endpoint that returns JSON pages of up to 1000 records. Pagination
is implemented using a page-number paginator and stops when an
empty page is returned by the API.
"""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_rest_api_source():
    """Define dlt resources for the NYC taxi REST API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resources": [
            {
                "name": "trips",
                "endpoint": {
                    # adjust the path if your API exposes a different route
                    "path": "/taxi",
                    "method": "GET",
                    "params": {
                        # page size requested from the API
                        "limit": 1000,
                    },
                    # use page-number pagination (page=1,2,3...)
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1,
                        "stop_after_empty_page": True,
                        # the API returns a plain JSON array per page and does not
                        # include a `total` field, so disable expecting `total`.
                        "total_path": None,
                    },
                    # the API returns a JSON array (or object) per page
                    # leave `data_selector` unset so the whole response is used
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    # quick pre-flight check: verify the API is reachable (prevents long hangs)
    import requests
    import sys

    base = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
    health_url = f"{base}/taxi"
    try:
        resp = requests.get(health_url, params={"page": 1, "limit": 1}, timeout=10)
        resp.raise_for_status()
        print("API reachable, status:", resp.status_code)
    except Exception as e:
        print("API preflight check failed:", e)
        sys.exit(1)

    load_info = pipeline.run(taxi_rest_api_source())
    print(load_info)
