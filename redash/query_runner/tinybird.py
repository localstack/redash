import logging

import requests

from redash.query_runner import register
from redash.query_runner.clickhouse import ClickHouse

logger = logging.getLogger(__name__)


class Tinybird(ClickHouse):
    DEFAULT_URL = "https://api.tinybird.co"

    SQL_ENDPOINT = "%s/v0/sql"
    DATASOURCES_ENDPOINT = "%s/v0/datasources"
    PIPES_ENDPOINT = "%s/v0/pipes"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "url": {"type": "string", "default": cls.DEFAULT_URL},
                "token": {"type": "string", "title": "Auth Token"},
                "timeout": {
                    "type": "number",
                    "title": "Request Timeout",
                    "default": 30,
                },
                "verify": {
                    "type": "boolean",
                    "title": "Verify SSL certificate",
                    "default": True,
                },
            },
            "order": ["url", "token"],
            "required": ["token"],
            "extra_options": ["timeout", "verify"],
            "secret": ["token"],
        }

    @classmethod
    def name(cls):
        return "Tinybird"

    @classmethod
    def type(cls):
        return "tinybird"

    def test_connection(self):
        try:
            self._send_query(
                "SELECT count() FROM tinybird.pipe_stats LIMIT 1 FORMAT JSON"
            )
            return True
        except Exception:
            return False

    def _get_tables(self, schema):
        # datasources
        response = self._get_from_tinybird(self.DATASOURCES_ENDPOINT)
        for datasource in response["datasources"]:
            schema[datasource["name"]] = {
                "name": datasource["name"],
                "columns": [column["name"] for column in datasource["columns"]],
                "size": datasource["statistics"]["row_count"],
            }

        # endpoints
        response = self._get_from_tinybird(self.PIPES_ENDPOINT)
        for pipe in response["pipes"]:
            if not pipe.get("endpoint"):
                continue

            schema[pipe["name"]] = {
                "name": pipe["name"],
                # we cannot find out the schema of a pipe given its dynamic nature. we have used
                # `SELECT * FROM <pipe> LIMIT 1` before, but that can cause too much data processed when
                # querying some pipes
                "columns": ["no_schema"],
                "size": 0,
            }
        return list(schema.values())

    def _get_tables_stats(self, tables_dict):
        # size already included by _get_tables call
        pass

    def _send_query(self, data, stream=False):
        return self._get_from_tinybird(
            self.SQL_ENDPOINT,
            stream=stream,
            params={"q": data.encode("utf-8", "ignore")},
        )

    def _get_from_tinybird(self, endpoint, stream=False, params=None):
        url = endpoint % self.configuration.get("url", self.DEFAULT_URL)
        authorization = "Bearer %s" % self.configuration.get("token")

        try:
            response = requests.get(
                url,
                stream=stream,
                timeout=self.configuration.get("timeout", 30),
                params=params,
                headers={"Authorization": authorization},
                verify=self.configuration.get("verify", True),
            )
        except requests.RequestException as e:
            if e.response:
                details = "({}, Status Code: {})".format(
                    e.__class__.__name__, e.response.status_code
                )
            else:
                details = "({})".format(e.__class__.__name__)
            raise Exception("Connection error to: {} {}.".format(url, details))

        if response.status_code >= 400:
            raise Exception(response.text)

        return response.json()


register(Tinybird)
