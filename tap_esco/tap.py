"""esco tap class."""

from typing import List
from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_esco.streams import (
    EscoSkillsTaxonomyLevel1,
    EscoSkillsTaxonomyLevel2,
    EscoSkillsTaxonomyLevel3,
    EscoSkillsTaxonomyLevel4,
    # EscoSkillsList,
    # EscoSkillsDetails,
)

PLUGIN_NAME = "tap-esco"

STREAM_TYPES = [
    EscoSkillsTaxonomyLevel1,
    EscoSkillsTaxonomyLevel2,
    EscoSkillsTaxonomyLevel3,
    EscoSkillsTaxonomyLevel4,
    # EscoSkillsList,
    # EscoSkillsDetails,
]


class TapEsco(Tap):
    """esco tap class."""

    name = "tap-esco"
    # config_jsonschema = th.PropertiesList().to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]
        return streams


# CLI Execution:
cli = TapEsco.cli
