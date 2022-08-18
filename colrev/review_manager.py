#!/usr/bin/env python3
import importlib
import io
import json
import logging
import os
import pkgutil
import pprint
import re
import shutil
import sys
import tempfile
import typing
from collections import Counter
from contextlib import redirect_stdout
from dataclasses import asdict
from enum import Enum
from importlib.metadata import version
from pathlib import Path

import dacite
import git
import pandas as pd
import yaml
from dacite import from_dict
from git.exc import GitCommandError
from git.exc import InvalidGitRepositoryError

import colrev.cli_colors as colors
import colrev.environment
import colrev.exceptions as colrev_exceptions
import colrev.process
import colrev.record
import colrev.review_dataset
import colrev.settings


class ReviewManager:
    """
    Class for managing individual CoLRev review project (repositories)
    """

    notified_next_process = None
    """ReviewManager was notified for the upcoming process and
    will provide access to the ReviewDataset"""

    def __init__(
        self,
        *,
        path_str: str = None,
        force_mode: bool = False,
        debug_mode: bool = False,
    ) -> None:

        self.force_mode = force_mode
        """Force mode variable (bool)"""

        if path_str is not None:
            self.path = Path(path_str)
            """Path of the project repository"""
        else:
            self.path = Path.cwd()

        if debug_mode:
            self.DEBUG_MODE = True
        else:
            self.DEBUG_MODE = False

        try:
            self.paths = self.__get_file_paths(repository_dir_str=self.path)

            self.settings = self.load_settings()
        except Exception as e:
            if force_mode:
                print(e)
            else:
                raise e

        try:
            if self.DEBUG_MODE:
                self.report_logger = self.__setup_report_logger(level=logging.DEBUG)
                """Logger for the commit report"""
                self.logger = self.__setup_logger(level=logging.DEBUG)
                """Logger for processing information"""
            else:
                self.report_logger = self.__setup_report_logger(level=logging.INFO)
                self.logger = self.__setup_logger(level=logging.INFO)
        except Exception as e:
            if force_mode:
                print(e)
            else:
                raise e

        try:
            global_git_vars = (
                colrev.environment.EnvironmentManager.get_name_mail_from_git()
            )
            if 2 != len(global_git_vars):
                logging.error(
                    "Global git variables (user name and email) not available."
                )
                return
            self.COMMITTER, self.EMAIL = global_git_vars

            self.pp = pprint.PrettyPrinter(indent=4, width=140, compact=False)
            self.REVIEW_DATASET = colrev.review_dataset.ReviewDataset(
                REVIEW_MANAGER=self
            )
            """The review dataset object"""

        except Exception as e:
            if force_mode:
                print(e)
            else:
                raise e

        if self.DEBUG_MODE:
            print("\n\n")
            self.logger.debug("Created review manager instance")
            self.logger.debug(f"Settings:\n{self.settings}")

    def load_settings(self) -> colrev.settings.Configuration:

        # https://tech.preferred.jp/en/blog/working-with-configuration-in-python/

        # possible extension : integrate/merge global, default settings
        # from colrev.environment import EnvironmentManager
        # def selective_merge(base_obj, delta_obj):
        #     if not isinstance(base_obj, dict):
        #         return delta_obj
        #     common_keys = set(base_obj).intersection(delta_obj)
        #     new_keys = set(delta_obj).difference(common_keys)
        #     for k in common_keys:
        #         base_obj[k] = selective_merge(base_obj[k], delta_obj[k])
        #     for k in new_keys:
        #         base_obj[k] = delta_obj[k]
        #     return base_obj
        # print(selective_merge(default_settings, project_settings))

        if not self.paths["SETTINGS"].is_file():
            filedata = pkgutil.get_data(__name__, "template/settings.json")
            if filedata:
                settings = json.loads(filedata.decode("utf-8"))
                with open(self.paths["SETTINGS"], "w", encoding="utf8") as file:
                    json.dump(settings, file, indent=4)

        with open(self.paths["SETTINGS"], encoding="utf-8") as f:
            loaded_settings = json.load(f)

        converters = {Path: Path, Enum: Enum}

        # TODO : check validation
        # (e..g, non-float values for prep/similarity do not through errors)
        settings = from_dict(
            data_class=colrev.settings.Configuration,
            data=loaded_settings,
            config=dacite.Config(type_hooks=converters, cast=[Enum]),  # type: ignore
        )

        return settings

    def save_settings(self) -> None:
        def custom_asdict_factory(data):
            def convert_value(obj):
                if isinstance(obj, Enum):
                    return obj.value
                if isinstance(obj, Path):
                    return str(obj)
                return obj

            return {k: convert_value(v) for k, v in data}

        exported_dict = asdict(self.settings, dict_factory=custom_asdict_factory)
        with open("settings.json", "w", encoding="utf-8") as outfile:
            json.dump(exported_dict, outfile, indent=4)
        self.REVIEW_DATASET.add_changes(path="settings.json")

    def __get_file_paths(self, *, repository_dir_str: Path) -> dict:
        repository_dir = repository_dir_str
        main_refs = "records.bib"
        data = "data.csv"
        pdf_dir = "pdfs"
        paper = "paper.md"
        readme = "readme.md"
        report = "report.log"
        search_dir = "search"
        status = "status.yaml"
        corrections = ".corrections"
        settings = "settings.json"
        return {
            "REPO_DIR": repository_dir,
            "RECORDS_FILE_RELATIVE": Path(main_refs),
            "RECORDS_FILE": repository_dir.joinpath(main_refs),
            "DATA_RELATIVE": Path(data),
            "DATA": repository_dir.joinpath(data),
            "PDF_DIRECTORY_RELATIVE": Path(pdf_dir),
            "PDF_DIRECTORY": repository_dir.joinpath(pdf_dir),
            "PAPER_RELATIVE": Path(paper),
            "PAPER": repository_dir.joinpath(paper),
            "README_RELATIVE": Path(readme),
            "README": repository_dir.joinpath(readme),
            "REPORT_RELATIVE": Path(report),
            "REPORT": repository_dir.joinpath(report),
            "SEARCHDIR_RELATIVE": Path(search_dir),
            "SEARCHDIR": repository_dir.joinpath(search_dir),
            "STATUS_RELATIVE": Path(status),
            "STATUS": repository_dir.joinpath(status),
            "CORRECTIONS_PATH": repository_dir.joinpath(corrections),
            "SETTINGS": repository_dir.joinpath(settings),
            "SETTINGS_RELATIVE": Path(settings),
        }

    def get_remote_url(self):
        git_repo = self.REVIEW_DATASET.get_repo()
        for remote in git_repo.remotes:
            if remote.url:
                remote_url = remote.url.rstrip(".git")
                return remote_url

        return None

    def __setup_logger(self, *, level=logging.INFO) -> logging.Logger:
        # for logger debugging:
        # from logging_tree import printout
        # printout()
        logger = logging.getLogger(f"colrev{str(self.path).replace('/', '_')}")

        logger.setLevel(level)

        if logger.handlers:
            for handler in logger.handlers:
                logger.removeHandler(handler)

        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        handler.setLevel(level)

        logger.addHandler(handler)
        logger.propagate = False

        return logger

    def __setup_report_logger(self, *, level=logging.INFO) -> logging.Logger:
        report_logger = logging.getLogger(
            f"colrev_report{str(self.path).replace('/', '_')}"
        )

        if report_logger.handlers:
            for handler in report_logger.handlers:
                report_logger.removeHandler(handler)

        report_logger.setLevel(level)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        report_file_handler = logging.FileHandler("report.log", mode="a")
        report_file_handler.setFormatter(formatter)

        report_logger.addHandler(report_file_handler)

        if logging.DEBUG == level:
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            report_logger.addHandler(handler)
        report_logger.propagate = False

        return report_logger

    @classmethod
    def retrieve_package_file(cls, *, template_file: Path, target: Path) -> None:

        filedata = pkgutil.get_data(__name__, str(template_file))
        if filedata:
            with open(target, "w", encoding="utf8") as file:
                file.write(filedata.decode("utf-8"))

    def __get_colrev_versions(self) -> typing.List[str]:

        current_colrev_version = version("colrev")
        last_colrev_version = current_colrev_version

        last_commit_message = self.REVIEW_DATASET.get_commit_message(commit_nr=0)
        cmsg_lines = last_commit_message.split("\n")
        for cmsg_line in cmsg_lines[0:100]:
            if "colrev:" in cmsg_line and "version" in cmsg_line:
                last_colrev_version = cmsg_line[cmsg_line.find("version ") + 8 :]

        return [last_colrev_version, current_colrev_version]

    def __check_software(self) -> None:
        last_version, current_version = self.__get_colrev_versions()
        if last_version != current_version:
            raise colrev_exceptions.CoLRevUpgradeError(last_version, current_version)

    def upgrade_colrev(self) -> None:

        last_version, current_version = self.__get_colrev_versions()

        if "+" in last_version:
            last_version = last_version[: last_version.find("+")]
        if "+" in current_version:
            current_version = current_version[: current_version.find("+")]

        cur_major = current_version[: current_version.rfind(".")]
        next_minor = str(int(current_version[current_version.rfind(".") + 1 :]) + 1)
        upcoming_version = cur_major + "." + next_minor

        colrev.process.CheckProcess(REVIEW_MANAGER=self)  # to notify

        def print_release_notes(selected_version: str):

            filedata = pkgutil.get_data(__name__, "../CHANGELOG.md")
            active = False
            if filedata:
                for line in filedata.decode("utf-8").split("\n"):
                    if selected_version in line:
                        active = True
                        print(f"Release notes v{selected_version}")
                        continue
                    if "### [" in line and selected_version not in line:
                        active = False
                    if active:
                        print(line)

        def migrate_0_3_0(self) -> bool:
            records = self.REVIEW_DATASET.load_records_dict()
            if len(records.values()) > 0:
                for record in records.values():
                    if "LOCAL_INDEX" == record.get("metadata_source", ""):
                        record["metadata_source"] = "CURATED"
                    if "pdf_hash" in record:
                        record["colrev_pdf_id"] = "cpid1:" + record["pdf_hash"]
                        del record["pdf_hash"]

                self.REVIEW_DATASET.save_records_dict(records=records)
                self.REVIEW_DATASET.add_record_changes()

            self.REVIEW_DATASET.inplace_change(
                filename=self.paths["SOURCES"],
                old_string="search_type: LOCAL_PAPER_INDEX",
                new_string="PDFS",
            )
            self.REVIEW_DATASET.add_changes(path=str(self.paths["SOURCES_RELATIVE"]))

            if self.REVIEW_DATASET.has_changes():
                return True
            return False

        def migrate_0_4_0(self) -> bool:

            if not Path("settings.json").is_file():
                filedata = pkgutil.get_data(__name__, "template/settings.json")
                if not filedata:
                    print("error reading file")
                    return False
                settings = json.loads(filedata.decode("utf-8"))
            else:
                with open("settings.json", encoding="utf-8") as f:
                    settings = json.load(f)

            old_sources_path = Path("sources.yaml")
            if old_sources_path.is_file():
                if old_sources_path.is_file():
                    with open(old_sources_path, encoding="utf-8") as f:
                        sources_df = pd.json_normalize(yaml.safe_load(f))
                        sources = sources_df.to_dict("records")
                        print(sources)
                for source in sources:
                    if len(source["search_parameters"]) > 0:
                        if "dblp" == source["search_parameters"][0]["endpoint"]:
                            source["source_identifier"] = "{{dblp_key}}"
                        elif "crossref" == source["search_parameters"][0]["endpoint"]:
                            source[
                                "source_identifier"
                            ] = "https://api.crossref.org/works/{{doi}}"
                        elif (
                            "pdfs_directory"
                            == source["search_parameters"][0]["endpoint"]
                        ):
                            source["source_identifier"] = "{{file}}"
                        else:
                            source["source_identifier"] = source["search_parameters"][
                                0
                            ]["endpoint"]

                        source["search_parameters"] = source["search_parameters"][0][
                            "params"
                        ]
                    else:
                        source["search_parameters"] = ""
                        source["source_identifier"] = source.get("source_url", "")

                    if (
                        source["comment"] != source["comment"]
                        or "NA" == source["comment"]
                    ):  # NaN
                        source["comment"] = ""

                    if "source_url" in source:
                        del source["source_url"]
                    if "source_name" in source:
                        del source["source_name"]
                    if "last_sync" in source:
                        del source["last_sync"]

                settings["search"]["sources"] = sources

            if any(r["name"] == "exclusion" for r in settings["prep"]["prep_rounds"]):
                e_r = [
                    r
                    for r in settings["prep"]["prep_rounds"]
                    if r["name"] == "exclusion"
                ][0]
                if "exclude_predatory_journals" in e_r["scripts"]:
                    e_r["scripts"].remove("exclude_predatory_journals")

            for source in settings["search"]["sources"]:
                source["script"] = {"endpoint": "bib_pybtex"}

            settings["prep"]["man_prep_scripts"] = [{"endpoint": "colrev_cli_man_prep"}]
            settings["prescreen"]["scope"] = [{"LanguageScope": ["en"]}]
            if "plugin" in settings["prescreen"]:
                del settings["prescreen"]["plugin"]
            if "mode" in settings["prescreen"]:
                del settings["prescreen"]["mode"]
            settings["prescreen"]["scripts"] = [
                {"endpoint": "scope_prescreen"},
                {"endpoint": "colrev_cli_prescreen"},
            ]
            if "process" in settings["screen"]:
                del settings["screen"]["process"]
            settings["screen"]["scripts"] = [{"endpoint": "colrev_cli_screen"}]

            settings["pdf_get"]["man_pdf_get_scripts"] = [
                {"endpoint": "colrev_cli_pdf_get_man"}
            ]
            settings["pdf_get"]["scripts"] = [
                {"endpoint": "unpaywall"},
                {"endpoint": "local_index"},
            ]

            settings["pdf_prep"]["scripts"] = [
                {"endpoint": "pdf_check_ocr"},
                {"endpoint": "remove_coverpage"},
                {"endpoint": "remove_last_page"},
                {"endpoint": "validate_pdf_metadata"},
                {"endpoint": "validate_completeness"},
            ]
            settings["pdf_prep"]["man_pdf_prep_scripts"] = [
                {"endpoint": "colrev_cli_pdf_prep_man"}
            ]

            for x in settings["data"]["data_format"]:
                if "MANUSCRIPT" == x["endpoint"]:
                    if "paper_endpoint_version" not in x:
                        x["paper_endpoint_version"] = "0.1"
                if "STRUCTURED" == x["endpoint"]:
                    if "structured_data_endpoint_version" not in x:
                        x["structured_data_endpoint_version"] = "0.1"

            if "curated_metadata" in str(self.path):
                repo = git.Repo(str(self.path))
                settings["project"]["curation_url"] = repo.remote().url.replace(
                    ".git", ""
                )

            if old_sources_path.is_file():
                old_sources_path.unlink()
                self.REVIEW_DATASET.remove_file_from_git(path=str(old_sources_path))

            if Path("shared_config.ini").is_file():
                Path("shared_config.ini").unlink()
                self.REVIEW_DATASET.remove_file_from_git(path="shared_config.ini")
            if Path("private_config.ini").is_file():
                Path("private_config.ini").unlink()

            if "curated_metadata" in str(self.path):
                settings["project"]["curated_masterdata"] = True
                settings["project"]["curated_fields"] = [
                    "doi",
                    "url",
                    "dblp_key",
                ]

            settings["dedupe"]["same_source_merges"] = "prevent"

            if settings["project"]["review_type"] == "NA":
                if "curated_metadata" in str(self.path):
                    settings["project"]["review_type"] = "curated_masterdata"
                else:
                    settings["project"]["review_type"] = "literature_review"

            with open("settings.json", "w", encoding="utf-8") as outfile:
                json.dump(settings, outfile, indent=4)

            self.settings = self.load_settings()
            self.save_settings()

            self.REVIEW_DATASET.add_setting_changes()
            records = self.REVIEW_DATASET.load_records_dict()
            if len(records.values()) > 0:
                for record in records.values():
                    if "manual_duplicate" in record:
                        del record["manual_duplicate"]
                    if "manual_non_duplicate" in record:
                        del record["manual_non_duplicate"]
                    if "origin" in record:
                        record["colrev_origin"] = record["origin"]
                        del record["origin"]
                    if "status" in record:
                        record["colrev_status"] = record["status"]
                        del record["status"]
                    if "excl_criteria" in record:
                        record["exclusion_criteria"] = record["excl_criteria"]
                        del record["excl_criteria"]
                    if "metadata_source" in record:
                        del record["metadata_source"]

                    if "colrev_masterdata" in record:
                        if record["colrev_masterdata"] == "ORIGINAL":
                            del record["colrev_masterdata"]
                        else:
                            record["colrev_masterdata_provenance"] = record[
                                "colrev_masterdata"
                            ]
                            del record["colrev_masterdata"]

                    if "curated_metadata" in str(self.path):
                        if "colrev_masterdata_provenance" in record:
                            if "CURATED" == record["colrev_masterdata_provenance"]:
                                record["colrev_masterdata_provenance"] = {}
                    if "colrev_masterdata_provenance" not in record:
                        record["colrev_masterdata_provenance"] = {}
                    if "colrev_data_provenance" not in record:
                        record["colrev_data_provenance"] = {}

                    # if "source_url" in record:
                    #     record["colrev_masterdata"] = \
                    #           "CURATED:" + record["source_url"]
                    #     del record["source_url"]
                    # else:
                    #     record["colrev_masterdata"] = "ORIGINAL"
                    # Note : for curated repositories
                    # record["colrev_masterdata"] = "CURATED"

                self.REVIEW_DATASET.save_records_dict(records=records)
                self.REVIEW_DATASET.add_record_changes()

            self.retrieve_package_file(
                template_file=Path("template/.pre-commit-config.yaml"),
                target=Path(".pre-commit-config.yaml"),
            )

            self.REVIEW_DATASET.add_changes(path=".pre-commit-config.yaml")
            # Note: the order is important in this case.
            self.REVIEW_DATASET.update_colrev_ids()

            return True

        def migrate_0_5_0(self) -> None:

            with open("settings.json", encoding="utf-8") as f:
                settings = json.load(f)

            settings["pdf_get"]["scripts"] = [
                s
                for s in settings["pdf_get"]["scripts"]
                if s["endpoint"] != "website_screenshot"
            ]
            if "sources" in settings["search"]:
                for source in settings["search"]["sources"]:
                    source["script"] = {"endpoint": "bib_pybtex"}
                    if "search" not in source["filename"]:
                        source["filename"] = "search/" + source["filename"]

                if "sources" not in settings:
                    settings["sources"] = settings["search"]["sources"]
                    del settings["search"]["sources"]

                    for source in settings["sources"]:
                        source["search_script"] = source["script"]
                        del source["script"]

                        source["conversion_script"] = {"endpoint": "bibtex"}

                        source["source_prep_scripts"] = []
                        if "CROSSREF" == source["source_name"]:
                            source["search_script"] = {"endpoint": "search_crossref"}
                        if "DBLP" == source["source_name"]:
                            source["search_script"] = {"endpoint": "search_dblp"}
                        if "BACKWARD_SEARCH" == source["source_name"]:
                            source["search_script"] = {"endpoint": "backward_search"}
                        if "COLREV_PROJECT" == source["source_name"]:
                            source["search_script"] = {
                                "endpoint": "search_colrev_project"
                            }
                        if "INDEX" == source["source_name"]:
                            source["search_script"] = {"endpoint": "search_local_index"}
                        if "PDFs" == source["source_name"]:
                            source["search_script"] = {"endpoint": "search_pdfs_dir"}
                        if "bib_pybtex" == source["search_script"]["endpoint"]:
                            source["search_script"] = {}

                settings = {
                    "project": settings["project"],
                    "sources": settings["sources"],
                    "search": settings["search"],
                    "load": settings["load"],
                    "prep": settings["prep"],
                    "dedupe": settings["dedupe"],
                    "prescreen": settings["prescreen"],
                    "pdf_get": settings["pdf_get"],
                    "pdf_prep": settings["pdf_prep"],
                    "screen": settings["screen"],
                    "data": settings["data"],
                }

            for source in settings["sources"]:
                if "FEED" == source["search_type"]:
                    if "CROSSREF" == source["source_name"]:
                        source["search_type"] = "DB"
                    elif "DBLP" == source["source_name"]:
                        source["search_type"] = "DB"
                    elif "pdfs" == source["source_name"].lower():
                        source["search_type"] = "PDFS"
                    else:
                        source["search_type"] = "DB"

            for prep_round in settings["prep"]["prep_rounds"]:
                prep_round["scripts"] = [
                    s
                    for s in prep_round["scripts"]
                    if s["endpoint"]
                    not in ["get_doi_from_sem_scholar", "update_metadata_status"]
                ]

            if "retrieve_forthcoming" not in settings["search"]:
                if "colrev/curated_metadata" in str(self.path):
                    settings["search"]["retrieve_forthcoming"] = False
                else:
                    settings["search"]["retrieve_forthcoming"] = True

            if settings["project"]["review_type"] == "NA":
                if "curated_metadata" in str(self.path):
                    settings["project"]["review_type"] = "curated_masterdata"
                else:
                    settings["project"]["review_type"] = "literature_review"

            for prep_round in settings["prep"]["prep_rounds"]:
                prep_round["scripts"] = [
                    {"endpoint": s} if "endpoint" not in s and isinstance(str, s) else s
                    for s in prep_round["scripts"]
                ]
            if "explanation" not in settings["prescreen"]:
                settings["prescreen"]["explanation"] = ""
            if "scope" in settings["prescreen"]:
                scope_items = settings["prescreen"]["scope"]
                del settings["prescreen"]["scope"]

                if len(scope_items) > 0:

                    if "scope_prescreen" not in [
                        s["endpoint"] for s in settings["prescreen"]["scripts"]
                    ]:
                        settings["prescreen"].insert(0, {"endpoint": "scope_prescreen"})
                    scope_prescreen = [
                        s
                        for s in settings["prescreen"]["scripts"]
                        if s["endpoint"] == "scope_prescreen"
                    ][0]
                    for elements in scope_items:
                        for scope_key, scope_item in elements.items():
                            scope_prescreen[scope_key] = scope_item

            if settings["screen"]["criteria"] == []:
                settings["screen"]["criteria"] = {}

            if "colrev_version" not in settings["project"]:
                settings["project"]["colrev_version"] = "0.5.0"

            if "scripts" not in settings["dedupe"]:
                settings["dedupe"]["scripts"] = [
                    {"endpoint": "active_learning_training"},
                    {
                        "endpoint": "active_learning_automated",
                        "merge_threshold": 0.8,
                        "partition_threshold": 0.5,
                    },
                ]

            if "rename_pdfs" not in settings["pdf_get"]:
                settings["pdf_get"]["rename_pdfs"] = True

            settings["pdf_get"]["man_pdf_get_scripts"] = [
                {"endpoint": "colrev_cli_pdf_get_man"}
            ]
            if "pdf_required_for_screen_and_synthesis" not in settings["pdf_get"]:
                settings["pdf_get"]["pdf_required_for_screen_and_synthesis"] = True

            settings["pdf_prep"]["man_pdf_prep_scripts"] = [
                {"endpoint": "colrev_cli_pdf_prep_man"}
            ]

            if "data_format" in settings["data"]:
                data_scripts = settings["data"]["data_format"]
                del settings["data"]["data_format"]
                settings["data"]["scripts"] = data_scripts

            if "title" not in settings["project"]:
                settings["project"]["title"] = str(self.path.parents[0].stem)

            if "authors" not in settings["project"]:
                settings["project"]["authors"] = [
                    {
                        "name": self.COMMITTER,
                        "initials": "".join(
                            part[0] for part in self.COMMITTER.split(" ")
                        ),
                        "email": self.EMAIL,
                    }
                ]
            if "keywords" not in settings["project"]:
                settings["project"]["keywords"] = ["TODO"]

            with open("settings.json", "w", encoding="utf-8") as outfile:
                json.dump(settings, outfile, indent=4)

            self.settings = self.load_settings()
            self.save_settings()
            self.REVIEW_DATASET.add_setting_changes()

            records = self.REVIEW_DATASET.load_records_dict()
            if len(records.values()) > 0:
                for record in records.values():
                    if "exclusion_criteria" in record:
                        record["screening_criteria"] = (
                            record["exclusion_criteria"]
                            .replace("=no", "=in")
                            .replace("=yes", "=out")
                        )
                        del record["exclusion_criteria"]

                self.REVIEW_DATASET.save_records_dict(records=records)
                self.REVIEW_DATASET.add_record_changes()

            print("Manual steps required to rename references.bib > records.bib.")

            # git branch backup
            # git filter-branch --tree-filter 'if [ -f references.bib ];
            # then mv references.bib records.bib; fi' HEAD
            # rm -d -r .git/refs/original
            # # DO NOT REPLACE IN SETTINGS.json (or in records.bib/references.bib/...)
            # (some search sources may be named "references.bib")
            # git filter-branch --tree-filter
            # "find . \( -name **.md -o -name .pre-commit-config.yaml \)
            # -exec sed -i -e \ 's/references.bib/records.bib/g' {} \;"

        # next version should be:
        # ...
        # {'from': '0.4.0', "to": '0.5.0', 'script': migrate_0_4_0}
        # {'from': '0.5.0', "to": upcoming_version, 'script': migrate_0_5_0}
        migration_scripts: typing.List[typing.Dict[str, typing.Any]] = [
            {"from": "0.3.0", "to": "0.4.0", "script": migrate_0_3_0},
            {"from": "0.4.0", "to": "0.5.0", "script": migrate_0_4_0},
            {"from": "0.5.0", "to": upcoming_version, "script": migrate_0_5_0},
        ]

        # Start with the first step if the version is older:
        if last_version not in [x["from"] for x in migration_scripts]:
            last_version = "0.3.0"

        while current_version in [x["from"] for x in migration_scripts]:
            self.logger.info(f"Current CoLRev version: {last_version}")

            migrator = [x for x in migration_scripts if x["from"] == last_version].pop()

            migration_script = migrator["script"]

            self.logger.info(f"Migrating from {migrator['from']} to {migrator['to']}")

            updated = migration_script(self)
            if updated:
                self.logger.info(f"Updated to: {last_version}")
            else:
                self.logger.info("Nothing to do.")
                self.logger.info(
                    "If the update notification occurs again, run\n "
                    "git commit -n -m --allow-empty 'update colrev'"
                )

            # Note : the version in the commit message will be set to
            # the current_version immediately. Therefore, use the migrator['to'] field.
            last_version = migrator["to"]

            if last_version == upcoming_version:
                break

        if self.REVIEW_DATASET.has_changes():
            self.create_commit(
                msg=f"Upgrade to CoLRev {upcoming_version}",
                script_call="colrev settings -u",
            )
            print_release_notes(selected_version=upcoming_version)
        else:
            self.logger.info("Nothing to do.")
            self.logger.info(
                "If the update notification occurs again, run\n "
                "git commit -n -m --allow-empty 'update colrev'"
            )

    def check_repository_setup(self) -> None:

        # 1. git repository?
        if not self.__is_git_repo(path=self.paths["REPO_DIR"]):
            raise colrev_exceptions.RepoSetupError("no git repository. Use colrev init")

        # 2. colrev project?
        if not self.__is_colrev_project():
            raise colrev_exceptions.RepoSetupError(
                "No colrev repository."
                + "To retrieve a shared repository, use colrev init."
                + "To initalize a new repository, "
                + "execute the command in an empty directory."
            )

        installed_hooks = self.__get_installed_hooks()

        # 3. Pre-commit hooks installed?
        self.__require_hooks_installed(installed_hooks=installed_hooks)

        # 4. Pre-commit hooks up-to-date?
        try:
            if not self.__hooks_up_to_date(installed_hooks=installed_hooks):
                raise colrev_exceptions.RepoSetupError(
                    "Pre-commit hooks not up-to-date. Use\n"
                    + "colrev settings --update_hooks"
                )
                # This could also be a warning, but hooks should not change often.

        except GitCommandError:
            self.logger.warning(
                "No Internet connection, cannot check remote "
                "colrev-hooks repository for updates."
            )

    def __get_base_prefix_compat(self) -> str:
        return (
            getattr(sys, "base_prefix", None)
            or getattr(sys, "real_prefix", None)
            or sys.prefix
        )

    def in_virtualenv(self) -> bool:
        return self.__get_base_prefix_compat() != sys.prefix

    def __check_git_conflicts(self) -> None:
        # Note: when check is called directly from the command line.
        # pre-commit hooks automatically notify on merge conflicts

        git_repo = git.Repo(str(self.paths["REPO_DIR"]))
        unmerged_blobs = git_repo.index.unmerged_blobs()

        for path, list_of_blobs in unmerged_blobs.items():
            for (stage, _) in list_of_blobs:
                if stage != 0:
                    raise colrev_exceptions.GitConflictError(path)

    def __is_git_repo(self, *, path: Path) -> bool:

        try:
            _ = git.Repo(str(path)).git_dir
            return True
        except InvalidGitRepositoryError:
            return False

    def __is_colrev_project(self) -> bool:
        required_paths = [
            Path(".pre-commit-config.yaml"),
            Path(".gitignore"),
            Path("settings.json"),
        ]
        if not all((self.path / x).is_file() for x in required_paths):
            return False
        return True

    def __get_installed_hooks(self) -> dict:
        installed_hooks: dict = {"hooks": []}
        with open(".pre-commit-config.yaml", encoding="utf8") as pre_commit_y:
            pre_commit_config = yaml.load(pre_commit_y, Loader=yaml.FullLoader)
        installed_hooks[
            "remote_pv_hooks_repo"
        ] = "https://github.com/geritwagner/colrev-hooks"
        for repository in pre_commit_config["repos"]:
            if repository["repo"] == installed_hooks["remote_pv_hooks_repo"]:
                installed_hooks["local_hooks_version"] = repository["rev"]
                installed_hooks["hooks"] = [hook["id"] for hook in repository["hooks"]]
        return installed_hooks

    def __lsremote(self, *, url: str) -> dict:
        remote_refs = {}
        g = git.cmd.Git()
        for ref in g.ls_remote(url).split("\n"):
            hash_ref_list = ref.split("\t")
            remote_refs[hash_ref_list[1]] = hash_ref_list[0]
        return remote_refs

    def __hooks_up_to_date(self, *, installed_hooks: dict) -> bool:
        refs = self.__lsremote(url=installed_hooks["remote_pv_hooks_repo"])
        remote_sha = refs["HEAD"]
        if remote_sha == installed_hooks["local_hooks_version"]:
            return True
        return False

    def __require_hooks_installed(self, *, installed_hooks: dict) -> bool:
        required_hooks = ["check", "format", "report", "sharing"]
        hooks_activated = set(installed_hooks["hooks"]) == set(required_hooks)
        if not hooks_activated:
            missing_hooks = [
                x for x in required_hooks if x not in installed_hooks["hooks"]
            ]
            raise colrev_exceptions.RepoSetupError(
                f"missing hooks in .pre-commit-config.yaml ({missing_hooks})"
            )

        pch_file = Path(".git/hooks/pre-commit")
        if pch_file.is_file():
            with open(pch_file, encoding="utf8") as f:
                if "File generated by pre-commit" not in f.read(4096):
                    raise colrev_exceptions.RepoSetupError(
                        "pre-commit hooks not installed (use pre-commit install)"
                    )
        else:
            raise colrev_exceptions.RepoSetupError(
                "pre-commit hooks not installed (use pre-commit install)"
            )

        psh_file = Path(".git/hooks/pre-push")
        if psh_file.is_file():
            with open(psh_file, encoding="utf8") as f:
                if "File generated by pre-commit" not in f.read(4096):
                    raise colrev_exceptions.RepoSetupError(
                        "pre-commit push hooks not installed "
                        "(use pre-commit install --hook-type pre-push)"
                    )
        else:
            raise colrev_exceptions.RepoSetupError(
                "pre-commit push hooks not installed "
                "(use pre-commit install --hook-type pre-push)"
            )

        pcmh_file = Path(".git/hooks/prepare-commit-msg")
        if pcmh_file.is_file():
            with open(pcmh_file, encoding="utf8") as f:
                if "File generated by pre-commit" not in f.read(4096):
                    raise colrev_exceptions.RepoSetupError(
                        "pre-commit prepare-commit-msg hooks not installed "
                        "(use pre-commit install --hook-type prepare-commit-msg)"
                    )
        else:
            raise colrev_exceptions.RepoSetupError(
                "pre-commit prepare-commit-msg hooks not installed "
                "(use pre-commit install --hook-type prepare-commit-msg)"
            )

        return True

    def check_repo(self, *, DATA) -> dict:
        """Check whether the repository is in a consistent state
        Entrypoint for pre-commit hooks
        """
        # Note : we have to return status code and message
        # because printing from other packages does not work in pre-commit hook.

        # We work with exceptions because each issue may be raised in different checks.
        self.notified_next_process = colrev.process.ProcessType.check
        PASS, FAIL = 0, 1
        check_scripts: typing.List[typing.Dict[str, typing.Any]] = [
            {
                "script": colrev.environment.EnvironmentManager.check_git_installed,
                "params": [],
            },
            {
                "script": colrev.environment.EnvironmentManager.check_docker_installed,
                "params": [],
            },
            {
                "script": colrev.environment.EnvironmentManager.build_docker_images,
                "params": [],
            },
            {"script": self.__check_git_conflicts, "params": []},
            {"script": self.check_repository_setup, "params": []},
            {"script": self.__check_software, "params": []},
        ]

        self.paths["SEARCHDIR"].mkdir(exist_ok=True)

        failure_items = []
        if not self.paths["RECORDS_FILE"].is_file():
            self.logger.debug("Checks for RECORDS_FILE not activated")
        else:

            # Note : retrieving data once is more efficient than
            # reading the RECORDS_FILE multiple times (for each check)

            if self.REVIEW_DATASET.file_in_history(
                filepath=self.paths["RECORDS_FILE_RELATIVE"]
            ):
                prior = self.REVIEW_DATASET.retrieve_prior()
                self.logger.debug("prior")
                self.logger.debug(self.pp.pformat(prior))
            else:  # if RECORDS_FILE not yet in git history
                prior = {}

            data = self.REVIEW_DATASET.retrieve_data(prior=prior)
            self.logger.debug("data")
            self.logger.debug(self.pp.pformat(data))

            main_refs_checks = [
                {
                    "script": self.REVIEW_DATASET.check_persisted_ID_changes,
                    "params": {"prior": prior, "data": data},
                },
                {"script": self.REVIEW_DATASET.check_sources, "params": []},
                {
                    "script": self.REVIEW_DATASET.check_main_records_duplicates,
                    "params": {"data": data},
                },
                {
                    "script": self.REVIEW_DATASET.check_main_records_origin,
                    "params": {"prior": prior, "data": data},
                },
                {
                    "script": self.REVIEW_DATASET.check_fields,
                    "params": {"data": data},
                },
                {
                    "script": self.REVIEW_DATASET.check_status_transitions,
                    "params": {"data": data},
                },
                {
                    "script": self.REVIEW_DATASET.check_main_records_screen,
                    "params": {"data": data},
                },
            ]

            if not prior:  # Selected checks if RECORDS_FILE not yet in git history
                main_refs_checks = [
                    x
                    for x in main_refs_checks
                    if x["script"]
                    in [
                        "check_sources",
                        "check_main_records_duplicates",
                    ]
                ]

            check_scripts += main_refs_checks

            self.logger.debug("Checks for RECORDS_FILE activated")

            PAPER = self.paths["PAPER"]
            if not PAPER.is_file():
                self.logger.debug("Checks for PAPER not activated\n")
            else:

                manuscript_checks = [
                    # TODO : check the whole script
                    # {
                    #     "script": ManuscriptEndpoint.check_new_record_source_tag,
                    #     "params": [self],
                    # },
                    {
                        "script": DATA.main,
                        "params": [True],
                    },
                    {
                        "script": self.update_status_yaml,
                        "params": [],
                    },
                ]
                check_scripts += manuscript_checks
                self.logger.debug("Checks for PAPER activated\n")

            # TODO: checks for structured data
            # See functions in comments
            # if DATA.is_file():
            #     data = pd.read_csv(DATA, dtype=str)
            #     check_duplicates_data(data)
            # check_screen_data(screen, data)
            # DATA = REVIEW_MANAGER.paths['DATA']

        for check_script in check_scripts:
            try:
                if [] == check_script["params"]:
                    self.logger.debug(f'{check_script["script"].__name__}() called')
                    check_script["script"]()
                else:
                    self.logger.debug(
                        f'{check_script["script"].__name__}(params) called'
                    )
                    if isinstance(check_script["params"], list):
                        check_script["script"](*check_script["params"])
                    else:
                        check_script["script"](**check_script["params"])
                self.logger.debug(f'{check_script["script"].__name__}: passed\n')
            except (
                colrev_exceptions.MissingDependencyError,
                colrev_exceptions.GitConflictError,
                colrev_exceptions.PropagatedIDChange,
                colrev_exceptions.DuplicateIDsError,
                colrev_exceptions.OriginError,
                colrev_exceptions.FieldValueError,
                colrev_exceptions.StatusTransitionError,
                colrev_exceptions.UnstagedGitChangesError,
                colrev_exceptions.StatusFieldValueError,
            ) as e:
                failure_items.append(f"{type(e).__name__}: {e}")

        if len(failure_items) > 0:
            return {"status": FAIL, "msg": "  " + "\n  ".join(failure_items)}
        return {"status": PASS, "msg": "Everything ok."}

    def report(self, *, msg_file: Path) -> dict:
        """Append commit-message report if not already available
        Entrypoint for pre-commit hooks)
        """

        update = False
        with open(msg_file, encoding="utf8") as f:
            contents = f.read()
            if "Command" not in contents:
                update = True
            if "Properties" in contents:
                update = False
        with open(msg_file, "w", encoding="utf8") as f:
            f.write(contents)
            # Don't append if it's already there
            if update:
                report = self.__get_commit_report(script_name="MANUAL", saved_args=None)
                f.write(report)

        colrev.process.CheckProcess(REVIEW_MANAGER=self)  # to notify

        self.REVIEW_DATASET.check_corrections_of_curated_records()

        return {"msg": "TODO", "status": 0}

    def sharing(self) -> dict:
        """Check whether sharing requirements are met
        Entrypoint for pre-commit hooks)
        """

        stat = self.get_status_freq()
        collaboration_instructions = self.get_collaboration_instructions(stat=stat)

        status_code = not all(
            x["level"] in ["SUCCESS", "WARNING"]
            for x in collaboration_instructions["items"]
        )

        msgs = "\n ".join(
            [
                x["level"] + x["title"] + x.get("msg", "")
                for x in collaboration_instructions["items"]
            ]
        )
        return {"msg": msgs, "status": status_code}

    def format_records_file(self) -> dict:
        """Format the records file
        Entrypoint for pre-commit hooks)
        """

        PASS, FAIL = 0, 1
        if not self.paths["RECORDS_FILE"].is_file():
            return {"status": PASS, "msg": "Everything ok."}

        try:

            colrev.process.FormatProcess(REVIEW_MANAGER=self)  # to notify

            changed = self.REVIEW_DATASET.format_records_file()
            self.update_status_yaml()

            self.settings = self.load_settings()
            self.save_settings()

        except (
            colrev_exceptions.UnstagedGitChangesError,
            colrev_exceptions.StatusFieldValueError,
        ) as e:
            return {"status": FAIL, "msg": f"{type(e).__name__}: {e}"}

        if changed:
            return {"status": FAIL, "msg": "records file formatted"}
        return {"status": PASS, "msg": "Everything ok."}

    def notify(self, *, process: colrev.process.Process, state_transition=True) -> None:
        """Notify the REVIEW_MANAGER about the next process"""

        if state_transition:
            process.check_precondition()
        self.notified_next_process = process.type
        self.REVIEW_DATASET.reset_log_if_no_changes()

    def __get_commit_report(
        self, *, script_name: str = None, saved_args: dict = None
    ) -> str:

        report = "\n\nReport\n\n"

        if script_name is not None:
            if "MANUAL" == script_name:
                report = report + "Commit created manually or by external script\n\n"
            elif " " in script_name:
                script_name = (
                    script_name.replace("colrev", "colrev")
                    .replace("colrev cli", "colrev")
                    .replace("prescreen_cli", "prescreen")
                )
                script_name = (
                    script_name.split(" ")[0]
                    + " "
                    + script_name.split(" ")[1].replace("_", "-")
                )

                report = report + f"Command\n   {script_name}"
        if saved_args is None:
            report = report + "\n"
        else:
            report = report + " \\ \n"
            for k, v in saved_args.items():
                if isinstance(v, (bool, float, int, str)):
                    if v == "":
                        report = report + f"     --{k} \\\n"
                    else:
                        report = report + f"     --{k}={v} \\\n"
            # Replace the last backslash (for argument chaining across linebreaks)
            report = report.rstrip(" \\\n") + "\n"
            try:
                last_commit_sha = self.REVIEW_DATASET.get_last_commit_sha()
                report = report + f"   On commit {last_commit_sha}\n"
            except ValueError:
                pass

        # url = g.execut['git', 'config', '--get remote.origin.url']

        # append status
        f = io.StringIO()
        with redirect_stdout(f):
            stat = self.get_status_freq()
            self.print_review_status(status_info=stat)

        # Remove colors for commit message
        status_page = (
            f.getvalue()
            .replace("\033[91m", "")
            .replace("\033[92m", "")
            .replace("\033[93m", "")
            .replace("\033[94m", "")
            .replace("\033[0m", "")
        )
        status_page = status_page.replace("Status\n\n", "Status\n")
        report = report + status_page

        tree_hash = self.REVIEW_DATASET.get_tree_hash()
        if self.paths["RECORDS_FILE"].is_file():
            tree_info = f"Properties for tree {tree_hash}\n"  # type: ignore
            report = report + "\n\n" + tree_info
            report = report + "   - Traceability of records ".ljust(38, " ") + "YES\n"
            report = (
                report + "   - Consistency (based on hooks) ".ljust(38, " ") + "YES\n"
            )
            completeness_condition = self.get_completeness_condition()
            if completeness_condition:
                report = (
                    report + "   - Completeness of iteration ".ljust(38, " ") + "YES\n"
                )
            else:
                report = (
                    report + "   - Completeness of iteration ".ljust(38, " ") + "NO\n"
                )
            report = (
                report
                + "   To check tree_hash use".ljust(38, " ")
                + "git log --pretty=raw -1\n"
            )
            report = (
                report
                + "   To validate use".ljust(38, " ")
                + "colrev validate --properties\n"
                + "".ljust(38, " ")
                + "--commit INSERT_COMMIT_HASH"
            )
        report = report + "\n"

        report = report + "\nSoftware"
        rt_version = version("colrev")
        report = report + "\n   - colrev:".ljust(33, " ") + "version " + rt_version
        version("colrev_hooks")
        report = (
            report
            + "\n   - colrev hooks:".ljust(33, " ")
            + "version "
            + version("colrev_hooks")
        )
        sys_v = sys.version
        report = (
            report
            + "\n   - Python:".ljust(33, " ")
            + "version "
            + sys_v[: sys_v.find(" ")]
        )

        stream = os.popen("git --version")
        git_v = stream.read()
        report = (
            report
            + "\n   - Git:".ljust(33, " ")
            + git_v.replace("git ", "").replace("\n", "")
        )
        stream = os.popen("docker --version")
        docker_v = stream.read()
        report = (
            report
            + "\n   - Docker:".ljust(33, " ")
            + docker_v.replace("Docker ", "").replace("\n", "")
        )
        if script_name is not None:
            ext_script = script_name.split(" ")[0]
            if ext_script != "colrev":
                try:
                    script_version = version(ext_script)
                    report = (
                        report
                        + f"\n   - {ext_script}:".ljust(33, " ")
                        + "version "
                        + script_version
                    )
                except importlib.metadata.PackageNotFoundError:
                    pass

        if "dirty" in report:
            report = (
                report + "\n    * created with a modified version (not reproducible)"
            )
        report = report + "\n"

        return report

    def __get_version_flag(self) -> str:
        flag = ""
        if "dirty" in version("colrev"):
            flag = "*"
        return flag

    def update_status_yaml(self) -> None:

        status_freq = self.get_status_freq()
        with open(self.paths["STATUS"], "w", encoding="utf8") as f:
            yaml.dump(status_freq, f, allow_unicode=True)

        self.REVIEW_DATASET.add_changes(path=self.paths["STATUS_RELATIVE"])

    def get_status(self) -> dict:
        status_dict = {}
        with open(self.paths["STATUS"], encoding="utf8") as stream:
            try:
                status_dict = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return status_dict

    def reset_log(self) -> None:

        self.report_logger.handlers[0].stream.close()  # type: ignore
        self.report_logger.removeHandler(self.report_logger.handlers[0])

        with open("report.log", "r+", encoding="utf8") as f:
            f.truncate(0)

        file_handler = logging.FileHandler("report.log")
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        self.report_logger.addHandler(file_handler)

    def reorder_log(self, *, IDs: list, criterion=None) -> None:
        """Reorder the report.log according to an ID list (after multiprocessing)"""

        # https://docs.python.org/3/howto/logging-cookbook.html
        # #logging-to-a-single-file-from-multiple-processes

        self.report_logger.handlers[0].stream.close()  # type: ignore
        self.report_logger.removeHandler(self.report_logger.handlers[0])

        firsts = []
        ordered_items = ""
        consumed_items = []
        with open("report.log", encoding="utf8") as r:
            items = []  # type: ignore
            item = ""
            for line in r.readlines():
                if any(
                    x in line
                    for x in [
                        "[INFO] Prepare",
                        "[INFO] Completed ",
                        "[INFO] Batch size",
                        "[INFO] Summary: Prepared",
                        "[INFO] Further instructions ",
                        "[INFO] To reset the metdatata",
                        "[INFO] Summary: ",
                        "[INFO] Continuing batch ",
                        "[INFO] Load records.bib",
                        "[INFO] Calculate statistics",
                        "[INFO] ReviewManager: run ",
                        "[INFO] Retrieve PDFs",
                        "[INFO] Statistics:",
                        "[INFO] Set ",
                    ]
                ):
                    firsts.append(line)
                    continue
                if re.search(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} ", line):
                    # prep: only list "Dropped xy field" once
                    if "[INFO] Dropped " in line:
                        if any(
                            item[item.find("[INFO] Dropped ") :] in x for x in items
                        ):
                            continue
                    if item != "":
                        item = item.replace("\n\n", "\n").replace("\n\n", "\n")
                        items.append(item)
                        item = ""
                    item = line
                else:
                    item = item + line

            items.append(item.replace("\n\n", "\n").replace("\n\n", "\n"))

        if criterion is None:
            for ID in IDs:
                for item in items:
                    if f"({ID})" in item:
                        formatted_item = item
                        if "] prepare(" in formatted_item:
                            formatted_item = f"\n\n{formatted_item}"
                        ordered_items = ordered_items + formatted_item
                        consumed_items.append(item)

            for x in consumed_items:
                if x in items:
                    items.remove(x)

        if criterion == "descending_thresholds":
            item_details = []
            while items:
                item = items.pop()
                confidence_value = re.search(r"\(confidence: (\d.\d{0,3})\)", item)
                if confidence_value:
                    item_details.append([confidence_value.group(1), item])
                    consumed_items.append(item)
                else:
                    firsts.append(item)

            item_details.sort(key=lambda x: x[0])
            ordered_items = "".join([x[1] for x in item_details])

        if len(ordered_items) > 0 or len(items) > 0:
            formatted_report = (
                "".join(firsts)
                + "\nDetailed report\n"
                + ordered_items.lstrip("\n")
                + "\n\n"
                + "".join(items)
            )
        else:
            formatted_report = "".join(firsts)

        with open("report.log", "w", encoding="utf8") as f:
            f.write(formatted_report)

        file_handler = logging.FileHandler("report.log")
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        self.report_logger.addHandler(file_handler)

    def create_commit(
        self,
        *,
        msg: str,
        manual_author: bool = False,
        script_call: str = "",
        saved_args: dict = None,
        realtime_override: bool = False,
    ) -> bool:
        """Create a commit (including a commit report)"""

        # if "realtime" == self.settings.project.review_type and not realtime_override:
        #     return False

        if self.REVIEW_DATASET.has_changes():
            self.logger.info("Preparing commit: checks and updates")
            self.update_status_yaml()
            self.REVIEW_DATASET.add_changes(path=self.paths["STATUS_RELATIVE"])

            # TODO : hooks seem to fail most of the time
            hook_skipping = True

            processing_report = ""
            if self.paths["REPORT"].is_file():

                # Reformat
                prefixes = [
                    "[('change', 'author',",
                    "[('change', 'title',",
                    "[('change', 'journal',",
                    "[('change', 'booktitle',",
                ]

                with tempfile.NamedTemporaryFile(mode="r+b", delete=False) as temp:
                    with open(self.paths["REPORT"], "r+b") as f:
                        shutil.copyfileobj(f, temp)
                # self.paths["REPORT"].rename(temp.name)
                with open(temp.name, encoding="utf8") as reader, open(
                    self.paths["REPORT"], "w", encoding="utf8"
                ) as writer:
                    line = reader.readline()
                    while line:
                        if (
                            any(prefix in line for prefix in prefixes)
                            and "', '" in line[30:]
                        ):
                            split_pos = line.rfind("', '") + 2
                            indent = line.find("', (") + 3
                            writer.write(line[:split_pos] + "\n")
                            writer.write(" " * indent + line[split_pos:])
                        else:
                            writer.write(line)

                        line = reader.readline()

                with open("report.log", encoding="utf8") as f:
                    line = f.readline()
                    debug_part = False
                    while line:
                        # For more efficient debugging (loading of dict with Enum)
                        if "colrev_status" in line and "<RecordState." in line:
                            line = line.replace("<RecordState", "RecordState")
                            line = line[: line.rfind(":")] + line[line.rfind(">") + 1 :]
                        if "[DEBUG]" in line or debug_part:
                            debug_part = True
                            if any(
                                x in line
                                for x in ["[INFO]", "[ERROR]", "[WARNING]", "[CRITICAL"]
                            ):
                                debug_part = False
                        if not debug_part:
                            processing_report = processing_report + line
                        line = f.readline()

                processing_report = "\nProcessing report\n" + "".join(processing_report)

            if manual_author:
                git_author = git.Actor(self.COMMITTER, self.EMAIL)
            else:
                git_author = git.Actor(f"script:{script_call}", "")
            # TODO: test and update the following
            if "apply_correction" in script_call:
                cmsg = msg
            else:
                cmsg = (
                    msg
                    + self.__get_version_flag()
                    + self.__get_commit_report(
                        script_name=f"{script_call}", saved_args=saved_args
                    )
                    + processing_report
                )
            self.REVIEW_DATASET.create_commit(
                msg=cmsg,
                author=git_author,
                committer=git.Actor(self.COMMITTER, self.EMAIL),
                hook_skipping=hook_skipping,
            )

            self.logger.info("Created commit")
            self.reset_log()
            if self.REVIEW_DATASET.has_changes():
                raise colrev_exceptions.DirtyRepoAfterProcessingError(
                    "A clean repository is expected."
                )
            return True
        return False

    def get_status_freq(self) -> dict:
        def get_nr_search(search_dir) -> int:

            if not search_dir.is_dir():
                return 0
            bib_files = search_dir.glob("*.bib")
            number_search = 0
            for search_file in bib_files:
                number_search += self.REVIEW_DATASET.get_nr_in_bib(
                    file_path=search_file
                )
            return number_search

        record_header_list = self.REVIEW_DATASET.get_record_header_list()

        status_list = [x["colrev_status"] for x in record_header_list]
        screening_criteria = [
            x["screening_criteria"]
            for x in record_header_list
            if x["screening_criteria"] not in ["", "NA"]
        ]
        md_duplicates_removed = sum(
            (x["colrev_origin"].count(";")) for x in record_header_list
        )

        origin_list = [x["colrev_origin"] for x in record_header_list]
        record_links = 0
        for origin in origin_list:
            nr_record_links = origin.count(";")
            record_links += nr_record_links + 1

        stat: dict = {"colrev_status": {}}

        criteria = list(self.settings.screen.criteria.keys())
        screening_statistics = {crit: 0 for crit in criteria}
        for screening_case in screening_criteria:
            for criterion in screening_case.split(";"):
                criterion_name, decision = criterion.split("=")
                if "out" == decision:
                    screening_statistics[criterion_name] += 1

        stat["colrev_status"]["currently"] = {
            str(rs): 0 for rs in list(colrev.record.RecordState)
        }
        stat["colrev_status"]["overall"] = {
            str(rs): 0 for rs in list(colrev.record.RecordState)
        }

        currently_stats = dict(Counter(status_list))
        for currently_stat, val in currently_stats.items():
            stat["colrev_status"]["currently"][currently_stat] = val
            stat["colrev_status"]["overall"][currently_stat] = val

        atomic_step_number = 0
        completed_atomic_steps = 0

        self.logger.debug("Set overall colrev_status statistics (going backwards)")
        st_o = stat["colrev_status"]["overall"]
        non_completed = 0
        current_state = colrev.record.RecordState.rev_synthesized  # start with the last
        visited_states = []
        nr_incomplete = 0
        while True:
            self.logger.debug(
                f"current_state: {current_state} with {st_o[str(current_state)]}"
            )
            if colrev.record.RecordState.md_prepared == current_state:
                st_o[str(current_state)] += md_duplicates_removed

            states_to_consider = [current_state]
            predecessors: typing.List[typing.Dict[str, typing.Any]] = [
                {
                    "trigger": "init",
                    "source": colrev.record.RecordState.md_imported,
                    "dest": colrev.record.RecordState.md_imported,
                }
            ]
            # Go backward through the process model
            predecessor = None
            while predecessors:
                predecessors = [
                    t
                    for t in colrev.process.ProcessModel.transitions
                    if t["source"] in states_to_consider
                    and t["dest"] not in visited_states
                ]
                for predecessor in predecessors:
                    self.logger.debug(
                        f' add {st_o[str(predecessor["dest"])]} '
                        f'from {str(predecessor["dest"])} '
                        f'(predecessor transition: {predecessor["trigger"]})'
                    )
                    st_o[str(current_state)] = (
                        st_o[str(current_state)] + st_o[str(predecessor["dest"])]
                    )
                    visited_states.append(predecessor["dest"])
                    if predecessor["dest"] not in states_to_consider:
                        states_to_consider.append(predecessor["dest"])
                if len(predecessors) > 0:
                    if predecessors[0]["trigger"] != "init":
                        completed_atomic_steps += st_o[str(predecessor["dest"])]
            atomic_step_number += 1
            # Note : the following does not consider multiple parallel steps.
            for trans_for_completeness in [
                t
                for t in colrev.process.ProcessModel.transitions
                if current_state == t["dest"]
            ]:
                nr_incomplete += stat["colrev_status"]["currently"][
                    str(trans_for_completeness["source"])
                ]

            t_list = [
                t
                for t in colrev.process.ProcessModel.transitions
                if current_state == t["dest"]
            ]
            t: dict = t_list.pop()
            if current_state == colrev.record.RecordState.md_imported:
                break
            current_state = t["source"]  # go a step back
            non_completed += stat["colrev_status"]["currently"][str(current_state)]

        stat["colrev_status"]["currently"]["non_completed"] = non_completed

        stat["colrev_status"]["currently"]["non_processed"] = (
            stat["colrev_status"]["currently"]["md_imported"]
            + stat["colrev_status"]["currently"]["md_retrieved"]
            + stat["colrev_status"]["currently"]["md_needs_manual_preparation"]
            + stat["colrev_status"]["currently"]["md_prepared"]
        )

        stat["colrev_status"]["currently"][
            "md_duplicates_removed"
        ] = md_duplicates_removed
        stat["colrev_status"]["overall"]["md_retrieved"] = get_nr_search(
            self.paths["SEARCHDIR"]
        )
        stat["colrev_status"]["currently"]["md_retrieved"] = (
            stat["colrev_status"]["overall"]["md_retrieved"] - record_links
        )
        stat["completeness_condition"] = (0 == nr_incomplete) and (
            0 == stat["colrev_status"]["currently"]["md_retrieved"]
        )

        stat["colrev_status"]["currently"]["exclusion"] = screening_statistics

        stat["colrev_status"]["overall"]["rev_screen"] = stat["colrev_status"][
            "overall"
        ]["pdf_prepared"]
        stat["colrev_status"]["overall"]["rev_prescreen"] = stat["colrev_status"][
            "overall"
        ]["md_processed"]
        stat["colrev_status"]["currently"]["pdf_needs_retrieval"] = stat[
            "colrev_status"
        ]["currently"]["rev_prescreen_included"]

        colrev_masterdata_items = [
            x["colrev_masterdata_provenance"] for x in record_header_list
        ]
        stat["colrev_status"]["CURATED_records"] = len(
            [x for x in colrev_masterdata_items if "CURATED:" in x]
        )
        # Note : 'title' in curated_fields: simple heuristic for masterdata curation
        if self.settings.project.curated_masterdata:
            stat["colrev_status"]["CURATED_records"] = stat["colrev_status"]["overall"][
                "md_processed"
            ]

        # note: 10 steps
        stat["atomic_steps"] = (
            10 * st_o[str(colrev.record.RecordState.md_imported)]
            - 8 * stat["colrev_status"]["currently"]["md_duplicates_removed"]
            - 7 * stat["colrev_status"]["currently"]["rev_prescreen_excluded"]
            - 6 * stat["colrev_status"]["currently"]["pdf_not_available"]
            - stat["colrev_status"]["currently"]["rev_excluded"]
            - stat["colrev_status"]["currently"]["rev_synthesized"]
        )
        stat["completed_atomic_steps"] = completed_atomic_steps
        self.logger.debug(f"stat: {self.pp.pformat(stat)}")
        return stat

    def get_collaboration_instructions(self, *, stat) -> dict:

        SHARE_STAT_REQ = self.settings.project.share_stat_req
        found_a_conflict = False

        git_repo = git.Repo(str(self.paths["REPO_DIR"]))
        unmerged_blobs = git_repo.index.unmerged_blobs()
        for _, list_of_blobs in unmerged_blobs.items():
            for (stage, _) in list_of_blobs:
                if stage != 0:
                    found_a_conflict = True

        nr_commits_behind, nr_commits_ahead = 0, 0

        collaboration_instructions: dict = {"items": []}
        CONNECTED_REMOTE = 0 != len(git_repo.remotes)
        if CONNECTED_REMOTE:
            origin = git_repo.remotes.origin
            if origin.exists():
                (
                    nr_commits_behind,
                    nr_commits_ahead,
                ) = self.REVIEW_DATASET.get_remote_commit_differences()
        if CONNECTED_REMOTE:
            collaboration_instructions["title"] = "Versioning and collaboration"
            collaboration_instructions["SHARE_STAT_REQ"] = SHARE_STAT_REQ
        else:
            collaboration_instructions[
                "title"
            ] = "Versioning (not connected to shared repository)"

        if found_a_conflict:
            item = {
                "title": "Git merge conflict detected",
                "level": "WARNING",
                "msg": "To resolve:\n  1 https://docs.github.com/en/"
                + "pull-requests/collaborating-with-pull-requests/"
                + "addressing-merge-conflicts/resolving-a-merge-conflict-"
                + "using-the-command-line",
            }
            collaboration_instructions["items"].append(item)

        # Notify when changes in bib files are not staged
        # (this may raise unexpected errors)

        non_staged = [
            item.a_path
            for item in git_repo.index.diff(None)
            if ".bib" == item.a_path[-4:]
        ]
        if len(non_staged) > 0:
            item = {
                "title": f"Non-staged changes: {','.join(non_staged)}",
                "level": "WARNING",
            }
            collaboration_instructions["items"].append(item)

        elif not found_a_conflict:
            if CONNECTED_REMOTE:
                if nr_commits_behind > 0:
                    item = {
                        "title": "Remote changes available on the server",
                        "level": "WARNING",
                        "msg": "Once you have committed your changes, get the latest "
                        + "remote changes",
                        "cmd_after": "git add FILENAME \n  git commit -m 'MSG' \n  "
                        + "git pull --rebase",
                    }
                    collaboration_instructions["items"].append(item)

                if nr_commits_ahead > 0:
                    # TODO : suggest detailed commands
                    # (depending on the working directory/index)
                    item = {
                        "title": "Local changes not yet on the server",
                        "level": "WARNING",
                        "msg": "Once you have committed your changes, upload them "
                        + "to the shared repository.",
                        "cmd_after": "git push",
                    }
                    collaboration_instructions["items"].append(item)

                if SHARE_STAT_REQ == "NONE":
                    collaboration_instructions["status"] = {
                        "title": "Sharing: currently ready for sharing",
                        "level": "SUCCESS",
                        "msg": "",
                        # If consistency checks pass -
                        # if they didn't pass, the message wouldn't be displayed
                    }

                # TODO : all the following: should all search results be imported?!
                if SHARE_STAT_REQ == "PROCESSED":
                    if 0 == stat["colrev_status"]["currently"]["non_processed"]:
                        collaboration_instructions["status"] = {
                            "title": "Sharing: currently ready for sharing",
                            "level": "SUCCESS",
                            "msg": "",
                            # If consistency checks pass -
                            # if they didn't pass, the message wouldn't be displayed
                        }

                    else:
                        collaboration_instructions["status"] = {
                            "title": "Sharing: currently not ready for sharing",
                            "level": "WARNING",
                            "msg": "All records should be processed before sharing "
                            + "(see instructions above).",
                        }

                # Note: if we use all(...) in the following,
                # we do not need to distinguish whether
                # a PRE_SCREEN or INCLUSION_SCREEN is needed
                if SHARE_STAT_REQ == "SCREENED":
                    # TODO : the following condition is probably not sufficient
                    if 0 == stat["colrev_status"]["currently"]["pdf_prepared"]:
                        collaboration_instructions["status"] = {
                            "title": "Sharing: currently ready for sharing",
                            "level": "SUCCESS",
                            "msg": "",
                            # If consistency checks pass -
                            # if they didn't pass, the message wouldn't be displayed
                        }

                    else:
                        collaboration_instructions["status"] = {
                            "title": "Sharing: currently not ready for sharing",
                            "level": "WARNING",
                            "msg": "All records should be screened before sharing "
                            + "(see instructions above).",
                        }

                if SHARE_STAT_REQ == "COMPLETED":
                    if 0 == stat["colrev_status"]["currently"]["non_completed"]:
                        collaboration_instructions["status"] = {
                            "title": "Sharing: currently ready for sharing",
                            "level": "SUCCESS",
                            "msg": "",
                            # If consistency checks pass -
                            # if they didn't pass, the message wouldn't be displayed
                        }
                    else:
                        collaboration_instructions["status"] = {
                            "title": "Sharing: currently not ready for sharing",
                            "level": "WARNING",
                            "msg": "All records should be completed before sharing "
                            + "(see instructions above).",
                        }

        else:
            if CONNECTED_REMOTE:
                collaboration_instructions["status"] = {
                    "title": "Sharing: currently not ready for sharing",
                    "level": "WARNING",
                    "msg": "Merge conflicts need to be resolved first.",
                }

        if 0 == len(collaboration_instructions["items"]):
            item = {
                "title": "Up-to-date",
                "level": "SUCCESS",
            }
            collaboration_instructions["items"].append(item)

        return collaboration_instructions

    def print_review_status(self, *, status_info: dict) -> None:

        print("")
        print("Status")
        print("")

        # NOTE: the first figure should always
        # refer to the nr of records that completed this step

        stat = status_info["colrev_status"]

        perc_curated = 0
        denominator = (
            stat["overall"]["md_prepared"]
            + stat["currently"]["md_needs_manual_preparation"]
            - stat["currently"]["md_duplicates_removed"]
        )
        if denominator > 0:

            perc_curated = (stat["CURATED_records"] / (denominator)) * 100

        rjust_padd = 7
        search_info = (
            "  Search        "
            + f'{str(stat["overall"]["md_retrieved"]).rjust(rjust_padd, " ")} retrieved'
        )
        search_add_info = []
        if stat["overall"]["md_prepared"] > 0:
            # search_add_info.append(f"{str(int(perc_curated))}% curated")
            # Note: do not print percentages becaus
            # - the other figures are all absolute numbers
            # - the denominator changes (particularly confusing in the prep when
            #   the number of curated records remains the same but the percentage
            #   decreases)
            if perc_curated < 30:
                search_add_info.append(
                    f"only {colors.RED}{str(stat['CURATED_records'])} "
                    f"curated{colors.END}"
                )
            elif perc_curated > 60:
                search_add_info.append(
                    f"{colors.GREEN}{str(stat['CURATED_records'])} curated{colors.END}"
                )
            else:
                search_add_info.append(f"{str(stat['CURATED_records'])} curated")
        if stat["currently"]["md_retrieved"] > 0:
            search_add_info.append(
                f'{colors.ORANGE}{stat["currently"]["md_retrieved"]}'
                f" to load{colors.END}"
            )
        if len(search_add_info) > 0:
            search_info += f'    ({", ".join(search_add_info)})'
        print(search_info)

        metadata_info = (
            "  Metadata      "
            + f'{str(stat["overall"]["md_processed"]).rjust(rjust_padd, " ")} processed'
        )
        metadata_add_info = []
        if stat["currently"]["md_duplicates_removed"] > 0:
            metadata_add_info.append(
                f'{stat["currently"]["md_duplicates_removed"]} duplicates removed'
            )

        if stat["currently"]["md_imported"] > 0:
            metadata_add_info.append(
                f'{colors.ORANGE}{stat["currently"]["md_imported"]}'
                f" to prepare{colors.END}"
            )

        if stat["currently"]["md_needs_manual_preparation"] > 0:
            metadata_add_info.append(
                f'{colors.ORANGE}{stat["currently"]["md_needs_manual_preparation"]} '
                f"to prepare manually{colors.END}"
            )

        if stat["currently"]["md_prepared"] > 0:
            metadata_add_info.append(
                f'{colors.ORANGE}{stat["currently"]["md_prepared"]}'
                f" to deduplicate{colors.END}"
            )

        if len(metadata_add_info) > 0:
            metadata_info += f"    ({', '.join(metadata_add_info)})"
        print(metadata_info)

        prescreen_info = (
            "  Prescreen     "
            + f'{str(stat["overall"]["rev_prescreen_included"]).rjust(rjust_padd, " ")}'
            " included"
        )
        prescreen_add_info = []
        if stat["currently"]["rev_prescreen_excluded"] > 0:
            prescreen_add_info.append(
                f'{stat["currently"]["rev_prescreen_excluded"]} excluded'
            )
        if stat["currently"]["md_processed"] > 0:
            prescreen_add_info.append(
                f'{colors.ORANGE}{stat["currently"]["md_processed"]}'
                f" to prescreen{colors.END}"
            )
        if len(prescreen_add_info) > 0:
            prescreen_info += f"     ({', '.join(prescreen_add_info)})"
        print(prescreen_info)

        pdfs_info = (
            "  PDFs          "
            + f'{str(stat["overall"]["pdf_prepared"]).rjust(rjust_padd, " ")} prepared'
        )
        pdf_add_info = []
        if stat["currently"]["rev_prescreen_included"] > 0:
            pdf_add_info.append(
                f'{colors.ORANGE}{stat["currently"]["rev_prescreen_included"]}'
                f" to retrieve{colors.END}"
            )
        if stat["currently"]["pdf_needs_manual_retrieval"] > 0:
            pdf_add_info.append(
                f'{colors.ORANGE}{stat["currently"]["pdf_needs_manual_retrieval"]}'
                f" to retrieve manually{colors.END}"
            )
        if stat["currently"]["pdf_not_available"] > 0:
            pdf_add_info.append(
                f'{stat["currently"]["pdf_not_available"]} not available'
            )
        if stat["currently"]["pdf_imported"] > 0:
            pdf_add_info.append(
                f'{colors.ORANGE}{stat["currently"]["pdf_imported"]}'
                f" to prepare{colors.END}"
            )
        if stat["currently"]["pdf_needs_manual_preparation"] > 0:
            pdf_add_info.append(
                f'{colors.ORANGE}{stat["currently"]["pdf_needs_manual_preparation"]}'
                f" to prepare manually{colors.END}"
            )
        if len(pdf_add_info) > 0:
            pdfs_info += f"     ({', '.join(pdf_add_info)})"
        print(pdfs_info)

        screen_info = (
            "  Screen        "
            + f'{str(stat["overall"]["rev_included"]).rjust(rjust_padd, " ")} included'
        )
        screen_add_info = []
        if stat["currently"]["pdf_prepared"] > 0:
            screen_add_info.append(
                f'{colors.ORANGE}{stat["currently"]["pdf_prepared"]}'
                f" to screen{colors.END}"
            )
        if stat["currently"]["rev_excluded"] > 0:
            screen_add_info.append(f'{stat["currently"]["rev_excluded"]} excluded')
        if len(screen_add_info) > 0:
            screen_info += f"     ({', '.join(screen_add_info)})"
        print(screen_info)

        data_info = (
            "  Data          "
            + f'{str(stat["overall"]["rev_synthesized"]).rjust(rjust_padd, " ")} '
            "synthesized"
        )
        data_add_info = []
        if stat["currently"]["rev_included"] > 0:
            data_add_info.append(
                f'{colors.ORANGE}{stat["currently"]["rev_included"]}'
                f" to synthesize{colors.END}"
            )
        if len(data_add_info) > 0:
            data_info += f'  ({", ".join(data_add_info)})'
        print(data_info)

    def get_completeness_condition(self) -> bool:
        stat = self.get_status_freq()
        return stat["completeness_condition"]

    @classmethod
    def get_environment_service(cls, *, service_identifier):
        # pylint: disable=too-many-return-statements

        if "AdapterManager" == service_identifier:
            return colrev.environment.AdapterManager
        if "GrobidService" == service_identifier:
            return colrev.environment.GrobidService
        if "TEIParser" == service_identifier:
            return colrev.environment.TEIParser
        if "EnvironmentManager" == service_identifier:
            return colrev.environment.EnvironmentManager
        if "ZoteroTranslationService" == service_identifier:
            return colrev.environment.ZoteroTranslationService
        if "ScreenshotService" == service_identifier:
            return colrev.environment.ScreenshotService
        if "LocalIndex" == service_identifier:
            return colrev.environment.LocalIndex
        if "Resources" == service_identifier:
            return colrev.environment.Resources

        raise colrev_exceptions.ServiceNotAvailableException(service_identifier)


if __name__ == "__main__":
    pass