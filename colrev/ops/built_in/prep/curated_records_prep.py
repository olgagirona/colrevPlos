#! /usr/bin/env python
"""Setting curated records to md_prepared as a prep operation"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import zope.interface
from dataclasses_jsonschema import JsonSchemaMixin

import colrev.env.package_manager
import colrev.ops.search_sources
import colrev.record

if TYPE_CHECKING:
    import colrev.ops.prep

# pylint: disable=too-few-public-methods


@zope.interface.implementer(colrev.env.package_manager.PrepPackageEndpointInterface)
@dataclass
class CuratedPrep(JsonSchemaMixin):
    """Prepares records by setting records with curated masterdata to md_prepared"""

    settings_class = colrev.env.package_manager.DefaultSettings

    source_correction_hint = "check with the developer"
    always_apply_changes = True

    def __init__(
        self,
        *,
        prep_operation: colrev.ops.prep.Prep,  # pylint: disable=unused-argument
        settings: dict,
    ) -> None:
        self.settings = self.settings_class.load_settings(data=settings)

    def prepare(
        self,
        prep_operation: colrev.ops.prep.Prep,  # pylint: disable=unused-argument
        record: colrev.record.PrepRecord,
    ) -> colrev.record.Record:
        """Prepare a record by setting curated ones to md_prepared"""

        if record.masterdata_is_curated():
            if colrev.record.RecordState.md_imported == record.data.get(
                "colrev_status", "NA"
            ):
                record.set_status(target_state=colrev.record.RecordState.md_prepared)

        return record


if __name__ == "__main__":
    pass
