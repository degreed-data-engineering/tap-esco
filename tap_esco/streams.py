"""Stream class for tap-esco."""

import logging
import requests
from typing import cast, Dict, Optional, Any
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream

logging.basicConfig(level=logging.INFO)


class TapEscoStream(RESTStream):
    """Generic ESCO stream class."""

    _LOG_REQUEST_METRIC_URLS: bool = True

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


class EscoSkillsTaxonomyLevel3(TapEscoStream):
    parent_stream_type = EscoSkillsTaxonomyLevel2
    rest_method = "GET"
    name = "skills_taxonomy_level_3"  # Stream name
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


class EscoSkillsTaxonomyLevel4(TapEscoStream):
    parent_stream_type = EscoSkillsTaxonomyLevel3
    rest_method = "GET"
    name = "skills_taxonomy_level_4"  # Stream name
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


# class EscoSkillsList(TapEscoStream):
#     parent_stream_type = EscoSkillsTaxonomyLevel4
#     rest_method = "GET"
#     name = "skills_list"  # Stream name
#     path = "/resource/concept"  # API endpoint after base_url
#     primary_keys = ["uri"]
#     records_jsonpath = "$._links.narrowerSkill[0:]"  # https://jsonpath.com Use requests response json to identify the json path

#     schema = th.PropertiesList(th.Property("uri", th.StringType)).to_dict()

#     def prepare_request(
#         self, context: Optional[dict], next_page_token: Optional[Any]
#     ) -> requests.PreparedRequest:
#         http_method = self.rest_method
#         url: str = self.get_url(context)
#         params: dict = {"uri": {uri}}
#         logging.info("###########" + str(uri))
#         request = cast(
#             requests.PreparedRequest,
#             self.requests_session.prepare_request(
#                 requests.Request(
#                     method=http_method,
#                     url=url,
#                     params=params,
#                 )
#             ),
#         )
#         return request

#     # https://sdk.meltano.com/en/latest/parent_streams.html
#     def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
#         """Return a context dictionary for child streams."""
#         global uri
#         uri = record["uri"]
#         return {"uri": uri}


# class EscoSkillsDetails(TapEscoStream):
#     parent_stream_type = EscoSkillsList
#     rest_method = "GET"
#     name = "skills_details"  # Stream name
#     path = "/resource/concept"  # API endpoint after base_url
#     primary_keys = ["uri"]
#     records_jsonpath = "$."  # https://jsonpath.com Use requests response json to identify the json path

#     schema = th.PropertiesList(
#         th.Property("uri", th.StringType), th.Property("title", th.StringType)
#     ).to_dict()

#     def prepare_request(
#         self, context: Optional[dict], next_page_token: Optional[Any]
#     ) -> requests.PreparedRequest:
#         http_method = self.rest_method
#         url: str = self.get_url(context)
#         params: dict = {"uri": {uri}}

#         request = cast(
#             requests.PreparedRequest,
#             self.requests_session.prepare_request(
#                 requests.Request(
#                     method=http_method,
#                     url=url,
#                     params=params,
#                 )
#             ),
#         )
#         return request
