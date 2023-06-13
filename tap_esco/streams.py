"""Stream class for tap-esco."""

import logging
import requests
import re

from urllib.request import urlopen
from typing import Optional, Iterable
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream

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

    def _get_skills_details(self, uris_list):
        uris = ",".join(uris_list)
        response = requests.get(self.url_base + "/resource/concept?uris=" + uris)
        if response.status_code == 200:
            logging.info("Parsed {len} skills".format(len=len(uris_list)))
        else:
            logging.warning(
                "Error found while running bulk operation. Processing skills one by one."
            )
            for uri in uris_list:
                response = requests.get(self.url_base + "/resource/concept?uri=" + uri)
                if response.status_code == 200:
                    logging.info("Parsed uri: {uri}".format(uri=uri))
                else:
                    logging.warning("Error found in uri: {uri}".format(uri=uri))
                    continue

    def _get_response(self, response):
        logging_text = "Skills taxonomy level {}"

        if response.status_code == 200:
            if "narrowerConcept" in response.json()["_links"]:
                for narrowerConcept in response.json()["_links"]["narrowerConcept"]:
                    logging.info(logging_text.format(narrowerConcept["uri"]))
                    response = requests.get(
                        self.url_base
                        + "/resource/concept?uri="
                        + narrowerConcept["uri"]
                    )
                    response = self._get_response(response)
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

    def _get_uris(self, response):
        level_0 = self._get_response(response)

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
