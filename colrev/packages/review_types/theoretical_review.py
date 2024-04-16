#! /usr/bin/env python
"""Theoretical review"""
from dataclasses import dataclass

import zope.interface
from dataclasses_jsonschema import JsonSchemaMixin

import colrev.ops.search
import colrev.package_manager.interfaces
import colrev.package_manager.package_manager
import colrev.package_manager.package_settings
import colrev.record.record

# pylint: disable=unused-argument
# pylint: disable=duplicate-code
# pylint: disable=too-few-public-methods


@zope.interface.implementer(
    colrev.package_manager.interfaces.ReviewTypePackageEndpointInterface
)
@dataclass
class TheoreticalReview(JsonSchemaMixin):
    """Theoretical review"""

    settings_class = colrev.package_manager.package_settings.DefaultSettings
    ci_supported: bool = True

    def __init__(
        self, *, operation: colrev.process.operation.Operation, settings: dict
    ) -> None:
        self.settings = self.settings_class.load_settings(data=settings)

    def __str__(self) -> str:
        return "theoretical review"

    def initialize(
        self, settings: colrev.settings.Settings
    ) -> colrev.settings.Settings:
        """Initialize a theoretical review"""

        settings.data.data_package_endpoints = [
            {
                "endpoint": "colrev.obsidian",
                "version": "0.1",
                "config": {},
            },
            {
                "endpoint": "colrev.paper_md",
                "version": "1.0",
                "word_template": "APA-7.docx",
            },
        ]
        return settings
