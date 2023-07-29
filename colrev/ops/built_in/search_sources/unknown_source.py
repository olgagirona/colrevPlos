#! /usr/bin/env python
"""SearchSource: Unknown source (default for all other sources)"""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import dacite
import zope.interface
from dacite import from_dict
from dataclasses_jsonschema import JsonSchemaMixin
from thefuzz import fuzz

import colrev.env.language_service
import colrev.env.package_manager
import colrev.exceptions as colrev_exceptions
import colrev.ops.load_utils_bib
import colrev.ops.load_utils_md
import colrev.ops.load_utils_ris
import colrev.ops.search
import colrev.record
import colrev.ui_cli.cli_colors as colors

# pylint: disable=unused-argument
# pylint: disable=duplicate-code


@zope.interface.implementer(
    colrev.env.package_manager.SearchSourcePackageEndpointInterface
)
@dataclass
class UnknownSearchSource(JsonSchemaMixin):
    """SearchSource for unknown search results"""

    settings_class = colrev.env.package_manager.DefaultSourceSettings

    source_identifier = "colrev.unknown_source"
    search_type = colrev.settings.SearchType.DB
    api_search_supported = False
    ci_supported: bool = False
    heuristic_status = colrev.env.package_manager.SearchSourceHeuristicStatus.na
    short_name = "Unknown Source"
    link = (
        "https://github.com/CoLRev-Environment/colrev/blob/main/"
        + "colrev/ops/built_in/search_sources/unknown_source.md"
    )

    HTML_CLEANER = re.compile("<.*?>")
    __padding = 40

    def __init__(
        self, *, source_operation: colrev.operation.Operation, settings: dict
    ) -> None:
        converters = {Path: Path, Enum: Enum}
        self.search_source = from_dict(
            data_class=self.settings_class,
            data=settings,
            config=dacite.Config(type_hooks=converters, cast=[Enum]),  # type: ignore
        )
        self.review_manager = source_operation.review_manager
        self.language_service = colrev.env.language_service.LanguageService()

    @classmethod
    def heuristic(cls, filename: Path, data: str) -> dict:
        """Source heuristic for unknown sources"""

        result = {"confidence": 0.1}

        return result

    @classmethod
    def add_endpoint(cls, operation: colrev.ops.search.Search, params: str) -> None:
        """Add SearchSource as an endpoint (based on query provided to colrev search -a )"""
        raise NotImplementedError

    def validate_source(
        self,
        search_operation: colrev.ops.search.Search,
        source: colrev.settings.SearchSource,
    ) -> None:
        """Validate the SearchSource (parameters etc.)"""

        search_operation.review_manager.logger.debug(
            f"Validate SearchSource {source.filename}"
        )

        if "query_file" in source.search_parameters:
            if not Path(source.search_parameters["query_file"]).is_file():
                raise colrev_exceptions.InvalidQueryException(
                    f"File does not exist: query_file {source.search_parameters['query_file']} "
                    f"for ({source.filename})"
                )

        search_operation.review_manager.logger.debug(
            f"SearchSource {source.filename} validated"
        )

    def run_search(
        self, search_operation: colrev.ops.search.Search, rerun: bool
    ) -> None:
        """Run a search of Crossref"""

    def get_masterdata(
        self,
        prep_operation: colrev.ops.prep.Prep,
        record: colrev.record.Record,
        save_feed: bool = True,
        timeout: int = 10,
    ) -> colrev.record.Record:
        """Not implemented"""
        return record

    def __ris_fixes(self, *, entries: dict) -> None:
        for entry in entries:
            if "title" in entry and "primary_title" not in entry:
                entry["primary_title"] = entry.pop("title")

            if "publication_year" in entry and "year" not in entry:
                entry["year"] = entry.pop("publication_year")

    def load(self, load_operation: colrev.ops.load.Load) -> dict:
        """Load the records from the SearchSource file"""

        if not self.search_source.filename.is_file():
            return {}

        data = self.search_source.filename.read_text(encoding="utf-8")
        # # Correct the file extension if necessary
        if re.findall(
            r"^%0", data, re.MULTILINE
        ) and self.search_source.filename.suffix not in [".enl"]:
            new_filename = self.search_source.filename.with_suffix(".enl")
            self.review_manager.logger.info(
                f"{colors.GREEN}Rename to {new_filename} "
                f"(because the format is .enl){colors.END}"
            )
            self.search_source.filename.rename(new_filename)
            self.review_manager.dataset.add_changes(
                path=self.search_source.filename, remove=True
            )
            self.search_source.filename = new_filename
            self.review_manager.dataset.add_changes(path=new_filename)
            self.review_manager.create_commit(
                msg=f"Rename {self.search_source.filename}"
            )

        if re.findall(
            r"^TI ", data, re.MULTILINE
        ) and self.search_source.filename.suffix not in [".ris"]:
            new_filename = self.search_source.filename.with_suffix(".ris")
            self.review_manager.logger.info(
                f"{colors.GREEN}Rename to {new_filename} "
                f"(because the format is .ris){colors.END}"
            )
            self.search_source.filename.rename(new_filename)
            self.review_manager.dataset.add_changes(
                path=self.search_source.filename, remove=True
            )
            self.search_source.filename = new_filename
            self.review_manager.dataset.add_changes(path=new_filename)
            self.review_manager.create_commit(
                msg=f"Rename {self.search_source.filename}"
            )

        if self.search_source.filename.suffix == ".ris":
            colrev.ops.load_utils_ris.apply_ris_fixes(
                filename=self.search_source.filename
            )
            ris_entries = colrev.ops.load_utils_ris.load_ris_entries(
                filename=self.search_source.filename
            )
            self.__ris_fixes(entries=ris_entries)
            records = colrev.ops.load_utils_ris.convert_to_records(ris_entries)
            return records

        if self.search_source.filename.suffix == ".bib":
            records = colrev.ops.load_utils_bib.load_bib_file(
                load_operation=load_operation, source=self.search_source
            )
            return records

        if self.search_source.filename.suffix == ".csv":
            csv_loader = colrev.ops.load_utils_table.CSVLoader(
                load_operation=load_operation, settings=self.search_source
            )
            records = csv_loader.load()
            load_operation.review_manager.dataset.save_records_dict_to_file(
                records=records,
                save_path=self.search_source.get_corresponding_bib_file(),
            )
            return records

        if self.search_source.filename.suffix in [".xls", ".xlsx"]:
            excel_loader = colrev.ops.load_utils_table.ExcelLoader(
                load_operation=load_operation, source=self.search_source
            )
            records = excel_loader.load()
            load_operation.review_manager.dataset.save_records_dict_to_file(
                records=records,
                save_path=self.search_source.get_corresponding_bib_file(),
            )
            return records

        if self.search_source.filename.suffix == ".md":
            md_loader = colrev.ops.load_utils_md.MarkdownLoader(
                load_operation=load_operation, source=self.search_source
            )
            records = md_loader.load()
            load_operation.review_manager.dataset.save_records_dict_to_file(
                records=records,
                save_path=self.search_source.get_corresponding_bib_file(),
            )
            return records

        if self.search_source.filename.suffix in [
            ".enl",
        ]:
            records = colrev.ops.load_utils_enl.load(source=self.search_source)
            load_operation.review_manager.dataset.save_records_dict_to_file(
                records=records,
                save_path=self.search_source.get_corresponding_bib_file(),
            )
            return records

        raise NotImplementedError

    def __heuristically_fix_entrytypes(
        self, *, record: colrev.record.PrepRecord
    ) -> None:
        """Prepare the record by heuristically correcting erroneous ENTRYTYPEs"""

        # Journal articles should not have booktitles/series set.
        if record.data["ENTRYTYPE"] == "article":
            if "booktitle" in record.data and "journal" not in record.data:
                record.update_field(
                    key="journal",
                    value=record.data["booktitle"],
                    source="unkown_source_prep",
                )
                record.remove_field(key="booktitle")
            if "series" in record.data and "journal" not in record.data:
                record.update_field(
                    key="journal",
                    value=record.data["series"],
                    source="unkown_source_prep",
                )
                record.remove_field(key="series")

        if self.search_source.filename.suffix == ".md":
            if record.data["ENTRYTYPE"] == "misc" and "publisher" in record.data:
                record.update_field(
                    key="ENTRYTYPE", value="book", source="unkown_source_prep"
                )
            if record.data.get("year", "year") == record.data.get("date", "date"):
                record.remove_field(key="date")
            if (
                "inbook" == record.data["ENTRYTYPE"]
                and "chapter" not in record.data
                and "title" in record.data
            ):
                record.rename_field(key="title", new_key="chapter")

        if (
            "dissertation" in record.data.get("fulltext", "NA").lower()
            and record.data["ENTRYTYPE"] != "phdthesis"
        ):
            prior_e_type = record.data["ENTRYTYPE"]
            record.update_field(
                key="ENTRYTYPE", value="phdthesis", source="unkown_source_prep"
            )
            self.review_manager.report_logger.info(
                f' {record.data["ID"]}'.ljust(self.__padding, " ")
                + f"Set from {prior_e_type} to phdthesis "
                '("dissertation" in fulltext link)'
            )

        if (
            "thesis" in record.data.get("fulltext", "NA").lower()
            and record.data["ENTRYTYPE"] != "phdthesis"
        ):
            prior_e_type = record.data["ENTRYTYPE"]
            record.update_field(
                key="ENTRYTYPE", value="phdthesis", source="unkown_source_prep"
            )
            self.review_manager.report_logger.info(
                f' {record.data["ID"]}'.ljust(self.__padding, " ")
                + f"Set from {prior_e_type} to phdthesis "
                '("thesis" in fulltext link)'
            )

        if (
            "this thesis" in record.data.get("abstract", "NA").lower()
            and record.data["ENTRYTYPE"] != "phdthesis"
        ):
            prior_e_type = record.data["ENTRYTYPE"]
            record.update_field(
                key="ENTRYTYPE", value="phdthesis", source="unkown_source_prep"
            )
            self.review_manager.report_logger.info(
                f' {record.data["ID"]}'.ljust(self.__padding, " ")
                + f"Set from {prior_e_type} to phdthesis "
                '("thesis" in abstract)'
            )

    def __format_inproceedings(self, *, record: colrev.record.PrepRecord) -> None:
        if record.data.get("booktitle", "UNKNOWN") == "UNKNOWN":
            return

        if (
            "UNKNOWN" != record.data["booktitle"]
            and "inbook" != record.data["ENTRYTYPE"]
        ):
            record.format_if_mostly_upper(key="booktitle", case="title")

            stripped_btitle = re.sub(r"\d{4}", "", record.data["booktitle"])
            stripped_btitle = re.sub(r"\d{1,2}th", "", stripped_btitle)
            stripped_btitle = re.sub(r"\d{1,2}nd", "", stripped_btitle)
            stripped_btitle = re.sub(r"\d{1,2}rd", "", stripped_btitle)
            stripped_btitle = re.sub(r"\d{1,2}st", "", stripped_btitle)
            stripped_btitle = re.sub(r"\([A-Z]{3,6}\)", "", stripped_btitle)
            stripped_btitle = stripped_btitle.replace("Proceedings of the", "").replace(
                "Proceedings", ""
            )
            stripped_btitle = stripped_btitle.lstrip().rstrip()
            record.update_field(
                key="booktitle",
                value=stripped_btitle,
                source="unkown_source_prep",
                keep_source_if_equal=True,
            )

    def __format_article(self, record: colrev.record.PrepRecord) -> None:
        if (
            record.data.get("journal", "UNKNOWN") != "UNKNOWN"
            and len(record.data["journal"]) > 10
            and "UNKNOWN" != record.data["journal"]
        ):
            record.format_if_mostly_upper(key="journal", case="title")

        if record.data.get("volume", "UNKNOWN") != "UNKNOWN":
            record.update_field(
                key="volume",
                value=record.data["volume"].replace("Volume ", ""),
                source="unkown_source_prep",
                keep_source_if_equal=True,
            )

    def __format_fields(self, *, record: colrev.record.PrepRecord) -> None:
        """Format fields"""

        if record.data.get("ENTRYTYPE", "") == "inproceedings":
            self.__format_inproceedings(record=record)
        elif record.data.get("ENTRYTYPE", "") == "article":
            self.__format_article(record=record)

        if record.data.get("author", "UNKNOWN") != "UNKNOWN":
            # fix name format
            if (1 == len(record.data["author"].split(" ")[0])) or (
                ", " not in record.data["author"]
            ):
                record.update_field(
                    key="author",
                    value=colrev.record.PrepRecord.format_author_field(
                        input_string=record.data["author"]
                    ),
                    source="unkown_source_prep",
                    keep_source_if_equal=True,
                )
            # Replace nicknames in parentheses
            record.data["author"] = re.sub(r"\([^)]*\)", "", record.data["author"])
            record.data["author"] = record.data["author"].replace("  ", " ").rstrip()

        if record.data.get("title", "UNKNOWN") != "UNKNOWN":
            record.format_if_mostly_upper(key="title")

        if "pages" in record.data:
            record.unify_pages_field()
            if (
                not re.match(r"^\d*$", record.data["pages"])
                and not re.match(r"^\d*--\d*$", record.data["pages"])
                and not re.match(r"^[xivXIV]*--[xivXIV]*$", record.data["pages"])
            ):
                self.review_manager.report_logger.info(
                    f' {record.data["ID"]}:'.ljust(self.__padding, " ")
                    + f'Unusual pages: {record.data["pages"]}'
                )

        if (
            "url" in record.data
            and "fulltext" in record.data
            and record.data["url"] == record.data["fulltext"]
        ):
            record.remove_field(key="fulltext")

        if "language" in record.data:
            try:
                self.language_service.unify_to_iso_639_3_language_codes(record=record)
                record.update_field(
                    key="language",
                    value=record.data["language"],
                    source="unkown_source_prep",
                    keep_source_if_equal=True,
                )
            except colrev_exceptions.InvalidLanguageCodeException:
                del record.data["language"]

    def __remove_redundant_fields(self, *, record: colrev.record.PrepRecord) -> None:
        if (
            record.data["ENTRYTYPE"] == "article"
            and "journal" in record.data
            and "booktitle" in record.data
        ):
            similarity_journal_booktitle = fuzz.partial_ratio(
                record.data["journal"].lower(), record.data["booktitle"].lower()
            )
            if similarity_journal_booktitle / 100 > 0.9:
                record.remove_field(key="booktitle")

        if record.data.get("publisher", "") in ["researchgate.net"]:
            record.remove_field(key="publisher")

        if (
            record.data["ENTRYTYPE"] == "inproceedings"
            and "journal" in record.data
            and "booktitle" in record.data
        ):
            similarity_journal_booktitle = fuzz.partial_ratio(
                record.data["journal"].lower(), record.data["booktitle"].lower()
            )
            if similarity_journal_booktitle / 100 > 0.9:
                record.remove_field(key="journal")

    def __impute_missing_fields(self, *, record: colrev.record.PrepRecord) -> None:
        if "date" in record.data and "year" not in record.data:
            year = re.search(r"\d{4}", record.data["date"])
            if year:
                record.update_field(
                    key="year",
                    value=year.group(0),
                    source="unkown_source_prep",
                    keep_source_if_equal=True,
                )

    def __unify_special_characters(self, *, record: colrev.record.PrepRecord) -> None:
        # Remove html entities
        for field in list(record.data.keys()):
            # Skip dois (and their provenance), which may contain html entities
            if field in [
                "colrev_masterdata_provenance",
                "colrev_data_provenance",
                "doi",
            ]:
                continue
            if field in ["author", "title", "journal"]:
                record.data[field] = re.sub(r"\s+", " ", record.data[field])
                record.data[field] = re.sub(self.HTML_CLEANER, "", record.data[field])

    def prepare(
        self, record: colrev.record.PrepRecord, source: colrev.settings.SearchSource
    ) -> colrev.record.Record:
        """Source-specific preparation for unknown sources"""

        if not record.has_quality_defects() or record.masterdata_is_curated():
            return record

        self.__heuristically_fix_entrytypes(
            record=record,
        )

        self.__impute_missing_fields(record=record)

        self.__format_fields(record=record)

        self.__remove_redundant_fields(record=record)

        self.__unify_special_characters(record=record)

        return record
