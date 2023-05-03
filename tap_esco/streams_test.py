"""Stream class for tap-esco."""

import logging
import requests
from http import HTTPStatus
from urllib.parse import unquote
from typing import cast, Optional, Any
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


logging.basicConfig(level=logging.INFO)


class TapEscoStream(RESTStream):
    """Generic ESCO stream class."""

    _LOG_REQUEST_METRIC_URLS: bool = True

    def validate_response(self, response):
        """Updating the "validate_response" function of the Meltano SDK as ESCO can return an error state = 500 in case a URI has issues in it.
        See: https://github.com/meltano/sdk/blob/54222bb2dc1903c0816347952c6a77c30267f30f/singer_sdk/streams/rest.py
        """
        if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
            msg = "Possible error are URI: {uri}".format(uri=unquote(str(response.url)))
            logging.error(msg)
        if (
            response.status_code in self.extra_retry_statuses
            or HTTPStatus.INTERNAL_SERVER_ERROR
            < response.status_code
            <= max(HTTPStatus)
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)

        if (
            HTTPStatus.BAD_REQUEST
            <= response.status_code
            < HTTPStatus.INTERNAL_SERVER_ERROR
        ):
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return f"https://ec.europa.eu/esco/api"


class EscoSkillsTaxonomyLevel1(TapEscoStream):
    rest_method = "GET"
    name = "skills_taxonomy_level_1"  # Stream name
    path = "/resource/concept"  # API endpoint after base_url
    primary_keys = ["uri"]
    records_jsonpath = "$._links.narrowerConcept[0:]"  # https://jsonpath.com Use requests response json to identify the json path

    schema = th.PropertiesList(th.Property("uri", th.StringType)).to_dict()

    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict = {"uri": "http://data.europa.eu/esco/skill/S"}

        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    params=params,
                )
            ),
        )
        return request

    # https://sdk.meltano.com/en/latest/parent_streams.html
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        global uri
        uri = record["uri"]
        return {"uri": uri}


class EscoSkillsTaxonomyLevel2(TapEscoStream):
    parent_stream_type = EscoSkillsTaxonomyLevel1
    rest_method = "GET"
    name = "skills_taxonomy_level_2"  # Stream name
    path = "/resource/concept"  # API endpoint after base_url
    primary_keys = ["uri"]
    records_jsonpath = "$._links.narrowerConcept[0:]"  # https://jsonpath.com Use requests response json to identify the json path

    schema = th.PropertiesList(th.Property("uri", th.StringType)).to_dict()

    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict = {"uri": {uri}}

        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    params=params,
                )
            ),
        )
        return request

    # https://sdk.meltano.com/en/latest/parent_streams.html
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        global uri
        uri = record["uri"]
        return {"uri": uri}
