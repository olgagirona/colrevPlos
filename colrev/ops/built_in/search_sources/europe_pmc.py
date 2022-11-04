#! /usr/bin/env python
"""SearchSource: Europe PMC"""
from __future__ import annotations

import json
import typing
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from sqlite3 import OperationalError
from typing import TYPE_CHECKING
from urllib.parse import quote
from xml.etree.ElementTree import Element

import requests
import zope.interface
from dacite import from_dict
from dataclasses_jsonschema import JsonSchemaMixin
from thefuzz import fuzz

import colrev.env.package_manager
import colrev.exceptions as colrev_exceptions
import colrev.ops.search
import colrev.record

if TYPE_CHECKING:
    import colrev.ops.prep


# pylint: disable=unused-argument
# pylint: disable=duplicate-code


@zope.interface.implementer(
    colrev.env.package_manager.SearchSourcePackageEndpointInterface
)
@dataclass
class EuropePMCSearchSource(JsonSchemaMixin):
    """SearchSource for Europe PMC"""

    # settings_class = colrev.env.package_manager.DefaultSourceSettings
    source_identifier = (
        "https://www.ebi.ac.uk/europepmc/webservices/rest/article/{{europe_pmc_id}}"
    )

    @dataclass
    class EuropePMCSearchSourceSettings(JsonSchemaMixin):
        """Settings for EuropePMCSearchSource"""

        # pylint: disable=duplicate-code
        # pylint: disable=too-many-instance-attributes
        endpoint: str
        filename: Path
        search_type: colrev.settings.SearchType
        source_identifier: str
        search_parameters: dict
        load_conversion_package_endpoint: dict
        comment: typing.Optional[str]

        _details = {
            "search_parameters": {
                "tooltip": "Currently supports a scope item "
                "with venue_key and journal_abbreviated fields."
            },
        }

    settings_class = EuropePMCSearchSourceSettings

    def __init__(
        self,
        *,
        source_operation: colrev.operation.Operation,
        settings: dict = None,
    ) -> None:

        if settings:
            if "query" not in settings["search_parameters"]:
                raise colrev_exceptions.InvalidQueryException(
                    "query required in search_parameters"
                )

            self.settings = from_dict(data_class=self.settings_class, data=settings)

    def __retrieve_and_append(
        self,
        *,
        search_operation: colrev.ops.search.Search,
        records_dict: typing.Dict[str, typing.Dict],
    ) -> typing.Dict[str, typing.Dict]:

        search_operation.review_manager.logger.info(
            f"Retrieving from Europe PMC: {self.settings.search_parameters['query']}"
        )

        available_ids = [
            x["europe_pmc_id"] for x in records_dict.values() if "europe_pmc_id" in x
        ]
        max_id = (
            max(
                [int(x["ID"]) for x in records_dict.values() if x["ID"].isdigit()] + [1]
            )
            + 1
        )

        record_input = colrev.record.Record(
            data={"title": self.settings.search_parameters["query"]}
        )

        for retrieved_record in self.europe_pcmc_query(
            review_manager=search_operation.review_manager,
            record_input=record_input,
            most_similar_only=False,
        ):

            if "colrev_data_provenance" in retrieved_record.data:
                del retrieved_record.data["colrev_data_provenance"]
            if "colrev_masterdata_provenance" in retrieved_record.data:
                del retrieved_record.data["colrev_masterdata_provenance"]

            if retrieved_record.data["europe_pmc_id"] not in available_ids:
                retrieved_record.data["ID"] = str(max_id).rjust(6, "0")
                available_ids.append(retrieved_record.data["europe_pmc_id"])

                records_dict[retrieved_record.data["ID"]] = retrieved_record.data
                max_id += 1

        return records_dict

    # @classmethod
    # def check_status(cls, *, prep_operation: colrev.ops.prep.Prep) -> None:
    # ...

    @classmethod
    def __europe_pmc_xml_to_record(cls, *, item: Element) -> colrev.record.PrepRecord:

        # pylint: disable=too-many-branches
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-statements

        retrieved_record_dict: dict = {"ENTRYTYPE": "misc"}

        author_node = item.find("authorString")
        if author_node is not None:
            if author_node.text is not None:
                authors_string = colrev.record.PrepRecord.format_author_field(
                    input_string=author_node.text
                )
                retrieved_record_dict.update(author=authors_string)

        journal_node = item.find("journalTitle")
        if journal_node is not None:
            if journal_node.text is not None:
                retrieved_record_dict.update(journal=journal_node.text)
                retrieved_record_dict.update(ENTRYTYPE="article")

        doi_node = item.find("doi")
        if doi_node is not None:
            if doi_node.text is not None:
                retrieved_record_dict.update(doi=doi_node.text)

        title_node = item.find("title")
        if title_node is not None:
            if title_node.text is not None:
                retrieved_record_dict.update(title=title_node.text)

        year_node = item.find("pubYear")
        if year_node is not None:
            if year_node.text is not None:
                retrieved_record_dict.update(year=year_node.text)

        volume_node = item.find("journalVolume")
        if volume_node is not None:
            if volume_node.text is not None:
                retrieved_record_dict.update(volume=volume_node.text)

        number_node = item.find("issue")
        if number_node is not None:
            if number_node.text is not None:
                retrieved_record_dict.update(number=number_node.text)

        pmid_node = item.find("pmid")
        if pmid_node is not None:
            if pmid_node.text is not None:
                retrieved_record_dict.update(pmid=pmid_node.text)

        source_node = item.find("source")
        if source_node is not None:
            if source_node.text is not None:
                retrieved_record_dict.update(epmc_source=source_node.text)

        epmc_id_node = item.find("id")
        if epmc_id_node is not None:
            if epmc_id_node.text is not None:
                retrieved_record_dict.update(epmc_id=epmc_id_node.text)

        retrieved_record_dict["europe_pmc_id"] = (
            retrieved_record_dict.get("epmc_source", "NO_SOURCE")
            + "/"
            + retrieved_record_dict.get("epmc_id", "NO_ID")
        )
        retrieved_record_dict["ID"] = retrieved_record_dict["europe_pmc_id"]
        del retrieved_record_dict["epmc_id"]
        del retrieved_record_dict["epmc_source"]

        record = colrev.record.PrepRecord(data=retrieved_record_dict)

        # https://www.ebi.ac.uk/europepmc/webservices/rest/article/MED/23245604
        source = (
            "https://www.ebi.ac.uk/europepmc/webservices/rest/article/"
            f"{record.data['europe_pmc_id']}"
        )

        record.add_provenance_all(source=source)
        return record

    @classmethod
    def __get_similarity(
        cls,
        *,
        record: colrev.record.Record,
        retrieved_record: colrev.record.Record,
    ) -> float:
        title_similarity = fuzz.partial_ratio(
            retrieved_record.data["title"].lower(),
            record.data.get("title", "").lower(),
        )
        container_similarity = fuzz.partial_ratio(
            retrieved_record.get_container_title().lower(),
            record.get_container_title().lower(),
        )

        weights = [0.6, 0.4]
        similarities = [title_similarity, container_similarity]

        similarity = sum(similarities[g] * weights[g] for g in range(len(similarities)))
        # logger.debug(f'record: {pp.pformat(record)}')
        # logger.debug(f'similarities: {similarities}')
        # logger.debug(f'similarity: {similarity}')
        # pp.pprint(retrieved_record_dict)
        return similarity

    @classmethod
    def europe_pcmc_query(
        cls,
        *,
        review_manager: colrev.review_manager.ReviewManager,
        record_input: colrev.record.Record,
        most_similar_only: bool = True,
        timeout: int = 10,
    ) -> list:
        """Retrieve records from Europe PMC based on a query"""

        # pylint: disable=too-many-branches
        # pylint: disable=too-many-statements
        # pylint: disable=too-many-locals

        try:

            record = record_input.copy_prep_rec()

            url = (
                "https://www.ebi.ac.uk/europepmc/webservices/rest/search?query="
                + quote(record.data["title"])
            )

            headers = {"user-agent": f"{__name__} (mailto:{review_manager.email})"}
            record_list = []
            session = review_manager.get_cached_session()

            while url != "END":
                review_manager.logger.info(url)
                ret = session.request("GET", url, headers=headers, timeout=timeout)
                ret.raise_for_status()
                if ret.status_code != 200:
                    # review_manager.logger.debug(
                    #     f"europe_pmc failed with status {ret.status_code}"
                    # )
                    return []

                most_similar, most_similar_record = 0.0, {}
                root = ET.fromstring(ret.text)
                result_list = root.findall("resultList")[0]

                for result_item in result_list.findall("result"):

                    retrieved_record = cls.__europe_pmc_xml_to_record(item=result_item)

                    if "title" not in retrieved_record.data:
                        continue

                    similarity = cls.__get_similarity(
                        record=record, retrieved_record=retrieved_record
                    )

                    source = (
                        "https://www.ebi.ac.uk/europepmc/webservices/rest/article/"
                        f"{retrieved_record.data['europe_pmc_id']}"
                    )
                    retrieved_record.set_masterdata_complete(source_identifier=source)

                    if not most_similar_only:
                        record_list.append(retrieved_record)

                    elif most_similar < similarity:
                        most_similar = similarity
                        most_similar_record = retrieved_record.get_data()

                url = "END"
                if not most_similar_only:
                    next_page_url_node = root.find("nextPageUrl")
                    if next_page_url_node is not None:
                        if next_page_url_node.text is not None:
                            url = next_page_url_node.text

        except json.decoder.JSONDecodeError:
            pass
        except requests.exceptions.RequestException:
            return []
        except OperationalError as exc:
            raise colrev_exceptions.ServiceNotAvailableException(
                "sqlite, required for requests CachedSession "
                "(possibly caused by concurrent operations)"
            ) from exc

        if most_similar_only:
            record_list = [colrev.record.PrepRecord(data=most_similar_record)]

        return record_list

    def get_masterdata_from_europe_pmc(
        self,
        *,
        prep_operation: colrev.ops.prep.Prep,
        record: colrev.record.Record,
        timeout: int = 10,  # pylint: disable=unused-argument
    ) -> colrev.record.Record:
        """Retrieve masterdata from Europe PMC based on similarity with the record provided"""

        # pylint: disable=too-many-branches
        try:

            if len(record.data.get("title", "")) > 35:

                retrieved_records = self.europe_pcmc_query(
                    review_manager=prep_operation.review_manager,
                    record_input=record,
                    timeout=timeout,
                )
                retrieved_record = retrieved_records.pop()

                retries = 0
                while (
                    not retrieved_record
                    and retries < prep_operation.max_retries_on_error
                ):
                    retries += 1

                    retrieved_records = self.europe_pcmc_query(
                        review_manager=prep_operation.review_manager,
                        record_input=record,
                        timeout=timeout,
                    )
                    retrieved_record = retrieved_records.pop()

                if 0 == len(retrieved_record.data):
                    return record

                similarity = colrev.record.PrepRecord.get_retrieval_similarity(
                    record_original=record, retrieved_record_original=retrieved_record
                )

                if similarity > prep_operation.retrieval_similarity:
                    # prep_operation.review_manager.logger.debug("Found matching record")
                    # prep_operation.review_manager.logger.debug(
                    #     f"europe_pmc similarity: {similarity} "
                    #     f"(>{prep_operation.retrieval_similarity})"
                    # )

                    # https://www.ebi.ac.uk/europepmc/webservices/rest/article/MED/23245604
                    source = (
                        "https://www.ebi.ac.uk/europepmc/webservices/rest/article/"
                        f"{retrieved_record.data.get('europe_pmc_id', 'NO_ID')}"
                    )

                    record.merge(merging_record=retrieved_record, default_source=source)

                else:
                    # prep_operation.review_manager.logger.debug(
                    #     f"europe_pmc similarity: {similarity} "
                    #     f"(<{prep_operation.retrieval_similarity})"
                    # )
                    pass

        except requests.exceptions.RequestException:
            pass
        except UnicodeEncodeError:
            prep_operation.review_manager.logger.error(
                "UnicodeEncodeError - this needs to be fixed at some time"
            )

        return record

    def run_search(self, search_operation: colrev.ops.search.Search) -> None:
        """Run a search of Europe PMC"""

        # https://europepmc.org/RestfulWebService

        search_operation.review_manager.logger.info(
            f"Retrieve Europe PMC: {self.settings.search_parameters}"
        )

        records: list = []
        if self.settings.filename.is_file():
            with open(self.settings.filename, encoding="utf8") as bibtex_file:
                feed_rd = search_operation.review_manager.dataset.load_records_dict(
                    load_str=bibtex_file.read()
                )
                records = list(feed_rd.values())

        try:

            records_dict = {r["ID"]: r for r in records}
            records_dict = self.__retrieve_and_append(
                search_operation=search_operation,
                records_dict=records_dict,
            )

            search_operation.save_feed_file(
                records=records_dict, feed_file=self.settings.filename
            )

        except UnicodeEncodeError:
            print("UnicodeEncodeError - this needs to be fixed at some time")
        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
        ):
            pass

    @classmethod
    def heuristic(cls, filename: Path, data: str) -> dict:
        """Source heuristic for Europe PMC"""

        result = {"confidence": 0.0}
        if "europe_pmc_id" in data:
            result["confidence"] = 1.0

        return result

    def load_fixes(
        self,
        load_operation: colrev.ops.load.Load,
        source: colrev.settings.SearchSource,
        records: typing.Dict,
    ) -> dict:
        """Load fixes for Europe PMC"""

        return records

    def prepare(
        self, record: colrev.record.Record, source: colrev.settings.SearchSource
    ) -> colrev.record.Record:
        """Source-specific preparation for Europe PMC"""
        record.data["author"].rstrip(".")
        record.data["title"].rstrip(".")
        return record


if __name__ == "__main__":
    pass