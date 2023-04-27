"""Stream class for tap-esco."""

import logging
from typing import Dict, Optional, Any
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIAuthenticatorBase, OAuthAuthenticator

logging.basicConfig(level=logging.INFO)


class EscoOAuthAuthenticator(OAuthAuthenticator):
    """Authenticate with client credentials"""

    @property
    def oauth_request_body(self) -> dict:
        return {
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "grant_type": "client_credentials",
            "scope": "emsi_open",
        }


class TapEscoStream(RESTStream):
    """Generic ESCO stream class."""

    url_base = "https://emsiservices.com/skills"

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return EscoOAuthAuthenticator(
            stream=self,
            auth_endpoint=f"https://auth.emsicloud.com/connect/token",
        )


# class SkillsListStream(RESTStream):
#     """Specific Lightcast stream class for Skills List GET call that contains a parameter (limit)."""

#     url_base = "https://emsiservices.com/skills"

#     def get_url_params(
#         self, context: Optional[dict], next_page_token: Optional[Any]
#     ) -> Dict[str, Any]:
#         """Return a dictionary of values to be used in URL parameterization."""
#         params = {}
#         if self.config["limit"] == -1:
#             params.update({"fields": "id"})
#         else:
#             params.update({"limit": self.config["limit"], "fields": "id"})
#         return params

#     @property
#     def authenticator(self) -> APIAuthenticatorBase:
#         return LightcastOAuthAuthenticator(
#             stream=self,
#             auth_endpoint=f"https://auth.emsicloud.com/connect/token",
#         )


# class SkillsLatestVersion(TapLightcastStream):
#     name = "skills_latest_version"  # Stream name
#     path = "/meta"  # API endpoint after base_url
#     primary_keys = ["latestVersion"]
#     records_jsonpath = "$.data"  # https://jsonpath.com Use requests response json to identify the json path

#     schema = th.PropertiesList(th.Property("latestVersion", th.StringType)).to_dict()

#     # https://sdk.meltano.com/en/latest/parent_streams.html
#     def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
#         """Return a context dictionary for child streams."""
#         global latestVersion
#         latestVersion = record["latestVersion"]
#         return {"latestVersion": latestVersion}


# class SkillsList(SkillsListStream):
#     name = "skills_list"  # Stream name
#     parent_stream_type = SkillsLatestVersion
#     path = "/versions/{latestVersion}/skills"  # API endpoint after base_url
#     primary_keys = ["id"]
#     records_jsonpath = "$.data[0:]"  # https://jsonpath.com Use requests response json to identify the json path

#     schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()

#     # https://sdk.meltano.com/en/latest/parent_streams.html
#     def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
#         """Return a context dictionary for child streams."""
#         return {"latestVersion": latestVersion, "id": record["id"]}


class SkillsDetails(TapEscoStream):
    name = "skills_details"  # Stream name
    path = "/versions/{latestVersion}/skills/{id}"  # API endpoint after base_url
    primary_keys = ["id"]
    records_jsonpath = "$.data[0:]"  # https://jsonpath.com Use requests response json to identify the json path

    schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()
