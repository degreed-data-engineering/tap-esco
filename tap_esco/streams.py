"""Stream class for tap-esco."""

import logging
import requests
import re
from http import HTTPStatus
from urllib.parse import unquote
from urllib.request import urlopen
from typing import Optional, Any, Dict
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


logging.basicConfig(level=logging.INFO)

base_uri = "http://data.europa.eu/esco/skill/S"


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

    def parse_response(self, response: requests.Response):
        if response.json():
            if "logref" not in response.json():
                if response.json()["className"] == "Skill":
                    yield response.json()
                elif response.json()["className"] == "Concept":
                    if "narrowerConcept" in response.json()["_links"]:
                        for narrowerConcept in response.json()["_links"][
                            "narrowerConcept"
                        ]:
                            yield {"uri": narrowerConcept["uri"]}
                    elif "narrowerSkill" in response.json()["_links"]:
                        for narrowerSkill in response.json()["_links"]["narrowerSkill"]:
                            yield {"uri": narrowerSkill["uri"]}

    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return f"https://ec.europa.eu/esco/api"


class EscoSkillsTaxonomy(TapEscoStream):
    name = "skills_taxonomy"  # Stream name
    path = "/resource/concept?uri={base_uri}".format(
        base_uri=base_uri
    )  # API endpoint after base_url
    primary_keys = ["uri"]
    schema = th.PropertiesList(th.Property("uri", th.StringType)).to_dict()

    # https://sdk.meltano.com/en/latest/parent_streams.html
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return record


class EscoSkillsTaxonomyLevel0(TapEscoStream):
    parent_stream_type = EscoSkillsTaxonomy
    name = "skills_taxonomy_level_0"  # Stream name
    path = "/resource/concept?uri={uri}"  # API endpoint after base_url
    primary_keys = ["uri"]
    schema = th.PropertiesList(th.Property("uri", th.StringType)).to_dict()

    # https://sdk.meltano.com/en/latest/parent_streams.html
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return record


class EscoSkillsTaxonomyLevel1(TapEscoStream):
    parent_stream_type = EscoSkillsTaxonomyLevel0
    name = "skills_taxonomy_level_1"  # Stream name
    path = "/resource/concept?uri={uri}"  # API endpoint after base_url
    primary_keys = ["uri"]
    schema = th.PropertiesList(th.Property("uri", th.StringType)).to_dict()

    # https://sdk.meltano.com/en/latest/parent_streams.html
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return record


class EscoSkillsTaxonomyLevel2(TapEscoStream):
    parent_stream_type = EscoSkillsTaxonomyLevel1
    name = "skills_taxonomy_level_2"  # Stream name
    path = "/resource/concept?uri={uri}"  # API endpoint after base_url
    primary_keys = ["uri"]
    schema = th.PropertiesList(th.Property("uri", th.StringType)).to_dict()

    # https://sdk.meltano.com/en/latest/parent_streams.html
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return record


class EscoSkillsDetails(TapEscoStream):
    parent_stream_type = EscoSkillsTaxonomyLevel2
    name = "skills_details"  # Stream name
    path = "/resource/skill?uri={uri}"  # API endpoint after base_url
    primary_keys = ["uri"]

    def post_process(
        self, row: Dict[str, Any], context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        if "_embedded" in row:
            if "ancestors" in row["_embedded"]:
                for ancestor in row["_embedded"]["ancestors"]:
                    uri = ancestor["_links"]["self"]["uri"]
                    if uri == base_uri or uri == row["uri"]:
                        next
                    level_check = uri.split("/")[len(uri.split("/")) - 1].count(".")
                    if level_check == 2:
                        row["uri_level_2"] = uri
                        row["title_level_2"] = ancestor["title"]
                    if level_check == 1:
                        row["uri_level_1"] = uri
                        row["title_level_1"] = ancestor["title"]
                    if level_check == 0:
                        row["uri_level_0"] = uri
                        row["title_level_0"] = ancestor["title"]
        if "description" in row:
            if "en-us" in row["description"]:
                row["description_en"] = row["description"]["en-us"]["literal"]
        if "alternativeLabel" in row:
            if "en" in row["alternativeLabel"]:
                row["alternativeLabel_en"] = " | ".join(row["alternativeLabel"]["en"])
        return row

    schema = th.PropertiesList(
        th.Property("uri", th.StringType),
        th.Property("uri_level_0", th.StringType),
        th.Property("uri_level_1", th.StringType),
        th.Property("uri_level_2", th.StringType),
        th.Property("title", th.StringType),
        th.Property("title_level_0", th.StringType),
        th.Property("title_level_1", th.StringType),
        th.Property("title_level_2", th.StringType),
        th.Property("alternativeLabel_en", th.StringType),
        th.Property("description_en", th.StringType),
    ).to_dict()
