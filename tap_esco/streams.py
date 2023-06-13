"""Stream class for tap-esco."""

import logging
import requests
import re
import pandas as pd

from urllib.request import urlopen
from typing import Optional, Iterable, Dict, Any
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.helpers.jsonpath import extract_jsonpath

logging.basicConfig(level=logging.INFO)

base_uri = "http://data.europa.eu/esco/skill/S"


class TapEscoStream(RESTStream):
    """Generic ESCO stream class."""

    _LOG_REQUEST_METRIC_URLS: bool = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = [
            "uri",
            "title",
            "description_en",
            "alternativeLabel_en",
            "selectedVersion",
            "uri_level_0",
            "title_level_0",
            "uri_level_1",
            "title_level_1",
            "uri_level_2",
            "title_level_2",
        ]
        self.results_df = pd.DataFrame(columns=self.columns)
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
    records_jsonpath = "$[*]"  # https://jsonpath.com Use requests response json to identify the json path
    replication_key = "selectedVersion"
    schema = th.PropertiesList(
        th.Property("uri", th.StringType),
        th.Property("title", th.StringType),
        th.Property("description_en", th.StringType),
        th.Property("alternativeLabel_en", th.StringType),
        th.Property("selectedVersion", th.StringType),
        th.Property("uri_level_0", th.StringType),
        th.Property("title_level_0", th.StringType),
        th.Property("uri_level_1", th.StringType),
        th.Property("title_level_1", th.StringType),
        th.Property("uri_level_2", th.StringType),
        th.Property("title_level_2", th.StringType),
    ).to_dict()

    def _create_dataframe(self, response):
        # uri
        uri = response["uri"]
        # title
        if "title" in response:
            title = response["title"]
        else:
            title = ""
        # description_en
        if "description" in response:
            if "en-us" in response["description"]:
                description_en = response["description"]["en-us"]["literal"]
            else:
                description_en = ""
        else:
            description_en = ""
        # alternativeLabel
        if "alternativeLabel" in response:
            if "en" in response["alternativeLabel"]:
                alternativeLabel_en = " | ".join(response["alternativeLabel"]["en"])
            else:
                alternativeLabel_en = ""
        else:
            alternativeLabel_en = ""

        new_row = pd.DataFrame(
            [
                {
                    "uri": uri,
                    "title": title,
                    "description_en": description_en,
                    "alternativeLabel_en": alternativeLabel_en,
                    "selectedVersion": self.selectedVersion,
                    "uri_level_2": self.uri_level_2,
                    "title_level_2": self.title_level_2,
                    "uri_level_1": self.uri_level_1,
                    "title_level_1": self.title_level_1,
                    "uri_level_0": self.uri_level_0,
                    "title_level_0": self.title_level_0,
                }
            ]
        )
        self.results_df = pd.concat(
            [self.results_df, new_row], axis=0, ignore_index=True
        )

    def _get_skills_details(self, uris_list):
        uris = ",".join(uris_list)
        response = requests.get(self.url_base + "/resource/concept?uris=" + uris)
        if response.status_code == 200:
            logging.info("Parsed {len} skills".format(len=len(uris_list)))
            for uri in response.json()["_embedded"]:
                self._create_dataframe(response.json()["_embedded"][uri])

        else:
            logging.warning(
                "Error found while running bulk operation. Processing skills one by one"
            )
            for uri in uris_list:
                response = requests.get(self.url_base + "/resource/concept?uri=" + uri)
                if response.status_code == 200:
                    logging.info("Parsed uri: {uri}".format(uri=uri))
                    self._create_dataframe(response.json())
                else:
                    logging.warning("Error found at url: {}".format(response.url))
                    continue

    def _get_uris(self, response):
        if response.status_code == 200 and response.json():
            if "narrowerConcept" in response.json()["_links"]:
                for narrowerConcept in response.json()["_links"]["narrowerConcept"]:
                    uri = narrowerConcept["uri"]
                    if (uri.split("/")[-1]).count(".") == 0:
                        self.uri_level_0 = uri
                        self.title_level_0 = narrowerConcept["title"]
                    if (uri.split("/")[-1]).count(".") == 1:
                        self.uri_level_1 = uri
                        self.title_level_1 = narrowerConcept["title"]
                    if (uri.split("/")[-1]).count(".") == 2:
                        self.uri_level_2 = uri
                        self.title_level_2 = narrowerConcept["title"]
                    logging.info("Skills taxonomy level uri: {}".format(uri))
                    response = requests.get(
                        self.url_base + "/resource/concept?uri=" + uri
                    )
                    response = self._get_uris(response)

            elif "narrowerSkill" in response.json()["_links"]:
                uris_list = []
                narrowerSkills = response.json()["_links"]["narrowerSkill"]
                logging.info("Found {} skills".format(len(narrowerSkills)))
                for narrowerSkill in narrowerSkills:
                    uris_list.append(narrowerSkill["uri"])
                    if len(uris_list) == 50:
                        self._get_skills_details(uris_list)
                        uris_list = []
                if len(uris_list) > 0:
                    self._get_skills_details(uris_list)
        else:
            logging.warning("Error found at url: {}".format(response.url))

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        if response.json():
            self._get_uris(response)
            logging.info("Total number of skills: {}".format(len(self.results_df)))
            input = self.results_df.to_dict()
            yield from extract_jsonpath(self.records_jsonpath, input=input)
