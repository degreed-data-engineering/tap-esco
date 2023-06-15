"""esco tap class."""

from typing import List
from singer_sdk import Tap, Stream

from tap_esco.streams import (
    EscoSkillsDetails,
)

PLUGIN_NAME = "tap-esco"

STREAM_TYPES = [EscoSkillsDetails]


class TapEsco(Tap):
    """esco tap class."""

    name = "tap-esco"

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]
        return streams


# CLI Execution:
cli = TapEsco.cli
