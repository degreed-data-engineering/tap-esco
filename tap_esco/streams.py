"""Stream class for tap-esco."""

import logging
import requests
import re

# from http import HTTPStatus
# from urllib.parse import unquote, urlparse
from urllib.request import urlopen
from typing import Optional, Any, Dict, Iterable
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream

# from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


logging.basicConfig(level=logging.INFO)

base_uri = "http://data.europa.eu/esco/skill/S"


class TapEscoStream(RESTStream):
    """Generic ESCO stream class."""

    _LOG_REQUEST_METRIC_URLS: bool = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Getting last available ESCO version scraping ESCO website
        esco_url = "https://esco.ec.europa.eu/en/classification/skill_main"
        start_string = '<div class="block-wrapper--esco_version">'
        end_string = "</div>"
        regex = "v\d\.\d\.\d"
        html = urlopen(esco_url).read().decode("utf-8")
        start_index = html.find(start_string) + len(start_string)
        end_index = start_index + html[start_index:].find(end_string)
        html_slice = html[start_index:end_index]
        self.selectedVersion = re.findall(regex, html_slice)[0]
        logging.info("ESCO latest version: " + self.selectedVersion)

    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return f"https://ec.europa.eu/esco/api"


class EscoSkillsDetails(TapEscoStream):
    name = "skills_details"  # Stream name
    path = "/resource/concept?uri={base_uri}".format(
        base_uri=base_uri
    )  # API endpoint after base_url
    primary_keys = ["uri"]
    replication_key = "selectedVersion"
    schema = th.PropertiesList(
        th.Property("uri", th.StringType), th.Property("selectedVersion", th.StringType)
    ).to_dict()

    def _get_response(self, uri, bulk_call=False):
        if bulk_call == False:
            url_addition = "/resource/concept?uri="
        if bulk_call == True:
            url_addition = "/resource/concept?uris="
        response = requests.get(self.url_base + url_addition + uri)
        return response

    def _get_skills_details(self, uris_list):
        uris = ",".join(uris_list)
        response = self._get_response(uris, True)
        if response.status_code == 200:
            logging.info("OK {len}".format(len=len(uris_list)))
        else:
            for uri in uris_list:
                response = self._get_response(uri)
                if response.status_code == 200:
                    logging.info("OK")
                else:
                    logging.info("ERROR")
                    continue

    def _get_uris(self, response):
        if "narrowerConcept" in response.json()["_links"]:
            for narrowerConcept in response.json()["_links"]["narrowerConcept"]:
                response = self._get_response((narrowerConcept["uri"]))
                if "narrowerConcept" in response.json()["_links"]:
                    for narrowerConcept in response.json()["_links"]["narrowerConcept"]:
                        response = self._get_response((narrowerConcept["uri"]))
                        if "narrowerConcept" in response.json()["_links"]:
                            for narrowerConcept in response.json()["_links"][
                                "narrowerConcept"
                            ]:
                                response = self._get_response((narrowerConcept["uri"]))
                                if "narrowerSkill" in response.json()["_links"]:
                                    uris_list = []
                                    for narrowerSkill in response.json()["_links"][
                                        "narrowerSkill"
                                    ]:
                                        uris_list.append(narrowerSkill["uri"])
                                        if len(uris_list) == 50:
                                            self._get_skills_details(uris_list)
                                            uris_list = []
                                    self._get_skills_details(uris_list)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        if response.json():
            uris_list = self._get_uris(response)

    # https://sdk.meltano.com/en/latest/parent_streams.html
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        if "replication_key_value" in self.stream_state:
            if self.selectedVersion == self.stream_state["replication_key_value"]:
                logging.info("ESCO version is up to date. Exiting.")
                exit()
            else:
                return record
        else:
            return record


# class EscoSkillsTaxonomy(TapEscoStream):

#     name = "skills_taxonomy"  # Stream name
#     path = "/resource/concept?uri={base_uri}".format(
#         base_uri=base_uri
#     )  # API endpoint after base_url
#     primary_keys = ["uri"]
#     replication_key = "selectedVersion"
#     schema = th.PropertiesList(
#         th.Property("uri", th.StringType), th.Property("selectedVersion", th.StringType)
#     ).to_dict()

#     # https://sdk.meltano.com/en/latest/parent_streams.html
#     def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
#         """Return a context dictionary for child streams."""
#         if "replication_key_value" in self.stream_state:
#             if self.selectedVersion == self.stream_state["replication_key_value"]:
#                 logging.info("ESCO version is up to date. Exiting.")
#                 exit()
#             else:
#                 return record
#         else:
#             return record


# class EscoSkillsTaxonomyLevel0(TapEscoStream):
#     parent_stream_type = EscoSkillsTaxonomy
#     name = "skills_taxonomy_level_0"  # Stream name
#     path = "/resource/concept?uri={uri}"  # API endpoint after base_url
#     primary_keys = ["uri"]
#     schema = th.PropertiesList(
#         th.Property("uri", th.StringType), th.Property("selectedVersion", th.StringType)
#     ).to_dict()

#     # https://sdk.meltano.com/en/latest/parent_streams.html
#     def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
#         """Return a context dictionary for child streams."""
#         return record


# class EscoSkillsTaxonomyLevel1(TapEscoStream):
#     parent_stream_type = EscoSkillsTaxonomyLevel0
#     name = "skills_taxonomy_level_1"  # Stream name
#     path = "/resource/concept?uri={uri}"  # API endpoint after base_url
#     primary_keys = ["uri"]
#     schema = th.PropertiesList(
#         th.Property("uri", th.StringType), th.Property("selectedVersion", th.StringType)
#     ).to_dict()

#     # https://sdk.meltano.com/en/latest/parent_streams.html
#     def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
#         """Return a context dictionary for child streams."""
#         return record


# class EscoSkillsTaxonomyLevel2(TapEscoStream):
#     parent_stream_type = EscoSkillsTaxonomyLevel1
#     name = "skills_taxonomy_level_2"  # Stream name
#     path = "/resource/concept?uri={uri}"  # API endpoint after base_url
#     primary_keys = ["uri"]
#     schema = th.PropertiesList(
#         th.Property("uri", th.StringType), th.Property("selectedVersion", th.StringType)
#     ).to_dict()

#     # https://sdk.meltano.com/en/latest/parent_streams.html
#     def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
#         """Return a context dictionary for child streams."""
#         return record


# class EscoSkillsDetails(TapEscoStream):
#     parent_stream_type = EscoSkillsTaxonomyLevel2
#     name = "skills_details"  # Stream name
#     path = "/resource/skill?uri={uri}"  # API endpoint after base_url
#     primary_keys = ["uri"]

#     schema = th.PropertiesList(
#         th.Property("uri", th.StringType),
#         th.Property("uri_level_0", th.StringType),
#         th.Property("uri_level_1", th.StringType),
#         th.Property("uri_level_2", th.StringType),
#         th.Property("title", th.StringType),
#         th.Property("title_level_0", th.StringType),
#         th.Property("title_level_1", th.StringType),
#         th.Property("title_level_2", th.StringType),
#         th.Property("alternativeLabel_en", th.StringType),
#         th.Property("description_en", th.StringType),
#         th.Property("selectedVersion", th.StringType),
#     ).to_dict()

#     def post_process(
#         self, row: Dict[str, Any], context: Optional[Dict[str, Any]] = None
#     ) -> Dict[str, Any]:
#         if "_embedded" in row:
#             if "ancestors" in row["_embedded"]:
#                 for ancestor in row["_embedded"]["ancestors"]:
#                     uri = ancestor["_links"]["self"]["uri"]
#                     if uri == base_uri or uri == row["uri"]:
#                         next
#                     level_check = uri.split("/")[len(uri.split("/")) - 1].count(".")
#                     if level_check == 2:
#                         row["uri_level_2"] = uri
#                         row["title_level_2"] = ancestor["title"]
#                     if level_check == 1:
#                         row["uri_level_1"] = uri
#                         row["title_level_1"] = ancestor["title"]
#                     if level_check == 0:
#                         row["uri_level_0"] = uri
#                         row["title_level_0"] = ancestor["title"]
#         if "description" in row:
#             if "en-us" in row["description"]:
#                 row["description_en"] = row["description"]["en-us"]["literal"]
#         if "alternativeLabel" in row:
#             if "en" in row["alternativeLabel"]:
#                 row["alternativeLabel_en"] = " | ".join(row["alternativeLabel"]["en"])
#         return row


#############


# def response_error_message(self, response: requests.Response) -> str:
#     """Build error message for invalid http statuses."""
#     full_path = urlparse(response.url).path or self.path
#     error_type = (
#         "Client"
#         if HTTPStatus.BAD_REQUEST
#         <= response.status_code
#         < HTTPStatus.INTERNAL_SERVER_ERROR
#         else "Server"
#     )

#     return (
#         f"{response.status_code} {error_type} Error: "
#         f"{response.reason} for path: {full_path}"
#     )

# def validate_response(self, response):
#     """Updating the "validate_response" function of the Meltano SDK as ESCO can return an error state = 500 in case a URI has issues in it.
#     See: https://github.com/meltano/sdk/blob/54222bb2dc1903c0816347952c6a77c30267f30f/singer_sdk/streams/rest.py
#     Status list: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status
#     """
#     if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:  # 500
#         msg = "Possible error are URI: {uri}".format(uri=unquote(str(response.url)))
#         logging.warning(msg)

#     if (
#         response.status_code == HTTPStatus.TOO_MANY_REQUESTS  # 429
#         or HTTPStatus.INTERNAL_SERVER_ERROR  # 500
#         < response.status_code
#         <= max(HTTPStatus)  # 511
#     ):
#         msg = self.response_error_message(response)
#         raise RetriableAPIError(msg, response)

#     if (
#         HTTPStatus.BAD_REQUEST  # 400
#         <= response.status_code
#         < HTTPStatus.TOO_MANY_REQUESTS  # 429
#         or HTTPStatus.REQUEST_HEADER_FIELDS_TOO_LARGE  # 431
#         <= response.status_code
#         < HTTPStatus.INTERNAL_SERVER_ERROR  # 500
#     ):
#         msg = self.response_error_message(response)
#         raise FatalAPIError(msg)

# def parse_response(self, response: requests.Response):
#     if response.json():
#         if "logref" not in response.json():
#             if response.json()["className"] == "Skill":
#                 yield response.json()
#             elif response.json()["className"] == "Concept":
#                 if "narrowerConcept" in response.json()["_links"]:
#                     for narrowerConcept in response.json()["_links"][
#                         "narrowerConcept"
#                     ]:
#                         yield {
#                             "uri": narrowerConcept["uri"],
#                             "selectedVersion": self.selectedVersion,
#                         }
#                 elif "narrowerSkill" in response.json()["_links"]:
#                     for narrowerSkill in response.json()["_links"]["narrowerSkill"]:
#                         yield {
#                             "uri": narrowerSkill["uri"],
#                             "selectedVersion": self.selectedVersion,
#                         }
