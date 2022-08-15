#! /usr/bin/env python
import logging
import multiprocessing as mp
import pkgutil
import time
import typing
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

import git
import requests_cache
import timeout_decorator
from pathos.multiprocessing import ProcessPool

import colrev.built_in.database_connectors as db_connectors
import colrev.built_in.prep as built_in_prep
import colrev.cli_colors as colors
import colrev.process
import colrev.record
import colrev.settings


logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests_cache").setLevel(logging.ERROR)


class Preparation(colrev.process.Process):

    PAD = 0
    TIMEOUT = 10
    MAX_RETRIES_ON_ERROR = 3

    requests_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    }

    # pylint: disable=duplicate-code
    fields_to_keep = [
        "ID",
        "ENTRYTYPE",
        "colrev_status",
        "colrev_origin",
        "colrev_masterdata_provenance",
        "colrev_data_provenance",
        "colrev_pid",
        "colrev_id",
        "author",
        "year",
        "title",
        "journal",
        "booktitle",
        "chapter",
        "series",
        "volume",
        "number",
        "pages",
        "doi",
        "abstract",
        "school",
        "editor",
        "book-group-author",
        "book-author",
        "keywords",
        "file",
        "fulltext",
        "publisher",
        "dblp_key",
        "sem_scholar_id",
        "url",
        "isbn",
        "address",
        "edition",
        "warning",
        "crossref",
        "date",
        "wos_accession_number",
        "link",
        "url",
        "crossmark",
        "warning",
        "note",
        "issn",
        "language",
        "howpublished",
        "cited_by",
        "cited_by_file",
    ]

    built_in_scripts: typing.Dict[str, typing.Dict[str, typing.Any]] = {
        "load_fixes": {
            "endpoint": built_in_prep.LoadFixesPrep,
        },
        "exclude_non_latin_alphabets": {
            "endpoint": built_in_prep.ExcludeNonLatinAlphabetsPrep,
        },
        "exclude_languages": {
            "endpoint": built_in_prep.ExcludeLanguagesPrep,
        },
        "exclude_collections": {
            "endpoint": built_in_prep.ExcludeCollectionsPrep,
        },
        "remove_urls_with_500_errors": {
            "endpoint": built_in_prep.RemoveError500URLsPrep,
        },
        "remove_broken_IDs": {
            "endpoint": built_in_prep.RemoveBrokenIDPrep,
        },
        "global_ids_consistency_check": {
            "endpoint": built_in_prep.GlobalIDConsistencyPrep,
        },
        "prep_curated": {
            "endpoint": built_in_prep.CuratedPrep,
        },
        "format": {
            "endpoint": built_in_prep.FormatPrep,
        },
        "resolve_crossrefs": {
            "endpoint": built_in_prep.BibTexCrossrefResolutionPrep,
        },
        "get_doi_from_sem_scholar": {
            "endpoint": built_in_prep.SemanticScholarPrep,
        },
        "get_doi_from_urls": {"endpoint": built_in_prep.DOIFromURLsPrep},
        "get_masterdata_from_doi": {
            "endpoint": built_in_prep.DOIMetadataPrep,
        },
        "get_masterdata_from_crossref": {
            "endpoint": built_in_prep.CrossrefMetadataPrep,
        },
        "get_masterdata_from_dblp": {
            "endpoint": built_in_prep.DBLPMetadataPrep,
        },
        "get_masterdata_from_open_library": {
            "endpoint": built_in_prep.OpenLibraryMetadataPrep,
        },
        "get_masterdata_from_citeas": {
            "endpoint": built_in_prep.CiteAsPrep,
        },
        "get_year_from_vol_iss_jour_crossref": {
            "endpoint": built_in_prep.CrossrefYearVolIssPrep,
        },
        "get_record_from_local_index": {
            "endpoint": built_in_prep.LocalIndexPrep,
        },
        "remove_nicknames": {
            "endpoint": built_in_prep.RemoveNicknamesPrep,
        },
        "format_minor": {
            "endpoint": built_in_prep.FormatMinorPrep,
        },
        "drop_fields": {
            "endpoint": built_in_prep.DropFieldsPrep,
        },
        "remove_redundant_fields": {
            "endpoint": built_in_prep.RemoveRedundantFieldPrep,
        },
        "correct_recordtype": {
            "endpoint": built_in_prep.CorrectRecordTypePrep,
        },
        "update_metadata_status": {
            "endpoint": built_in_prep.UpdateMetadataStatusPrep,
        },
    }

    def __init__(
        self,
        *,
        REVIEW_MANAGER,
        force=False,
        similarity: float = 0.9,
        notify_state_transition_process: bool = True,
        debug: str = "NA",
    ):
        super().__init__(
            REVIEW_MANAGER=REVIEW_MANAGER,
            process_type=colrev.process.ProcessType.prep,
            notify_state_transition_process=notify_state_transition_process,
            debug=(debug != "NA"),
        )
        self.notify_state_transition_process = notify_state_transition_process

        self.RETRIEVAL_SIMILARITY = similarity

        self.fields_to_keep += self.REVIEW_MANAGER.settings.prep.fields_to_keep

        # if similarity == 0.0:  # if it has not been set use default
        # saved_args["RETRIEVAL_SIMILARITY"] = self.RETRIEVAL_SIMILARITY
        # RETRIEVAL_SIMILARITY = self.RETRIEVAL_SIMILARITY
        # saved_args["RETRIEVAL_SIMILARITY"] = similarity

        self.CPUS: int = self.CPUS * 4

        EnvironmentManager = self.REVIEW_MANAGER.get_environment_service(
            service_identifier="GrobidService"
        )
        cache_path = EnvironmentManager.colrev_path / Path("prep_requests_cache")
        self.session = requests_cache.CachedSession(
            str(cache_path), backend="sqlite", expire_after=timedelta(days=30)
        )

    def check_DBs_availability(self) -> None:

        # TODO : check_status as a default method for the PreparationInterface
        # and iterate over it?

        self.REVIEW_MANAGER.logger.info("Check availability of connectors...")
        db_connectors.CrossrefConnector.check_status(PREPARATION=self)
        self.REVIEW_MANAGER.logger.info("CrossrefConnector available")
        db_connectors.DBLPConnector.check_status(PREPARATION=self)
        self.REVIEW_MANAGER.logger.info("DBLPConnector available")
        db_connectors.OpenLibraryConnector.check_status(PREPARATION=self)
        self.REVIEW_MANAGER.logger.info("OpenLibraryConnector available")

        print()

    def __print_diffs_for_debug(self, PRIOR, PREPARATION_RECORD, prep_script):
        diffs = PRIOR.get_diff(OTHER_RECORD=PREPARATION_RECORD)
        if diffs:
            change_report = (
                f"{prep_script}"
                f'({PREPARATION_RECORD.data["ID"]})'
                f" changed:\n{self.REVIEW_MANAGER.pp.pformat(diffs)}\n"
            )
            if self.REVIEW_MANAGER.DEBUG_MODE:
                self.REVIEW_MANAGER.logger.info(change_report)
                self.REVIEW_MANAGER.logger.info(
                    "To correct errors in the script,"
                    " open an issue at "
                    "https://github.com/geritwagner/colrev/issues"
                )
                self.REVIEW_MANAGER.logger.info(
                    "To correct potential errors at source,"
                    f" {prep_script.source_correction_hint}"
                )
                input("Press Enter to continue")
                print("\n")
        else:
            self.REVIEW_MANAGER.logger.debug(f"{prep_script.prepare} changed: -")
            if self.REVIEW_MANAGER.DEBUG_MODE:
                print("\n")
                time.sleep(0.3)

    # Note : no named arguments for multiprocessing
    def prepare(self, item: dict) -> dict:

        RECORD = item["record"]

        if not RECORD.status_to_prepare():
            return RECORD.get_data()

        self.REVIEW_MANAGER.logger.info(" prep " + RECORD.data["ID"])

        # PREPARATION_RECORD changes with each script and
        # eventually replaces record (if md_prepared or endpoint.always_apply_changes)
        PREPARATION_RECORD = RECORD.copy_prep_rec()

        # UNPREPARED_RECORD will not change (for diffs)
        UNPREPARED_RECORD = RECORD.copy_prep_rec()

        for prep_round_script in deepcopy(item["prep_round_scripts"]):

            try:
                PREP_SCRIPT = self.prep_scripts[prep_round_script["endpoint"]]

                if self.REVIEW_MANAGER.DEBUG_MODE:
                    self.REVIEW_MANAGER.logger.info(
                        f"{PREP_SCRIPT.SETTINGS.name}(...) called"
                    )

                PRIOR = PREPARATION_RECORD.copy_prep_rec()

                PREPARATION_RECORD = PREP_SCRIPT.prepare(self, PREPARATION_RECORD)

                self.__print_diffs_for_debug(
                    PRIOR=PRIOR,
                    PREPARATION_RECORD=PREPARATION_RECORD,
                    prep_script=PREP_SCRIPT,
                )

                if PREP_SCRIPT.always_apply_changes:
                    RECORD.update_by_record(UPDATE=PREPARATION_RECORD)

                if PREPARATION_RECORD.preparation_save_condition():
                    RECORD.update_by_record(UPDATE=PREPARATION_RECORD)
                    RECORD.update_masterdata_provenance(
                        UNPREPARED_RECORD=UNPREPARED_RECORD,
                        REVIEW_MANAGER=self.REVIEW_MANAGER,
                    )

                if PREPARATION_RECORD.preparation_break_condition():
                    RECORD.update_by_record(UPDATE=PREPARATION_RECORD)
                    break
            except timeout_decorator.timeout_decorator.TimeoutError:
                self.REVIEW_MANAGER.logger.error(
                    f"{colors.RED}{PREP_SCRIPT.SETTINGS.name}(...) timed out{colors.END}"
                )

        if self.LAST_ROUND:
            if RECORD.status_to_prepare():
                RECORD.update_by_record(UPDATE=PREPARATION_RECORD)
                # Note: update_masterdata_provenance sets to md_needs_manual_preparation
                RECORD.update_masterdata_provenance(
                    UNPREPARED_RECORD=UNPREPARED_RECORD,
                    REVIEW_MANAGER=self.REVIEW_MANAGER,
                )

        return RECORD.get_data()

    def reset(self, *, record_list: typing.List[dict]):

        record_list = [
            r
            for r in record_list
            if str(r["colrev_status"])
            in [
                str(colrev.record.RecordState.md_prepared),
                str(colrev.record.RecordState.md_needs_manual_preparation),
            ]
        ]

        for r in [
            r
            for r in record_list
            if str(r["colrev_status"])
            not in [
                str(colrev.record.RecordState.md_prepared),
                str(colrev.record.RecordState.md_needs_manual_preparation),
            ]
        ]:
            msg = (
                f"{r['ID']}: status must be md_prepared/md_needs_manual_preparation "
                + f'(is {r["colrev_status"]})'
            )
            self.REVIEW_MANAGER.logger.error(msg)
            self.REVIEW_MANAGER.report_logger.error(msg)

        record_reset_list = [[record, deepcopy(record)] for record in record_list]

        RECORDS_FILE_RELATIVE = self.REVIEW_MANAGER.paths["RECORDS_FILE_RELATIVE"]
        git_repo = git.Repo(str(self.REVIEW_MANAGER.paths["REPO_DIR"]))
        revlist = (
            (
                commit.hexsha,
                commit.message,
                (commit.tree / str(RECORDS_FILE_RELATIVE)).data_stream.read(),
            )
            for commit in git_repo.iter_commits(paths=str(RECORDS_FILE_RELATIVE))
        )

        for commit_id, cmsg, filecontents in list(revlist):
            cmsg_l1 = str(cmsg).split("\n", maxsplit=1)[0]
            if "colrev load" not in cmsg:
                print(f"Skip {str(commit_id)} (non-load commit) - {str(cmsg_l1)}")
                continue
            print(f"Check {str(commit_id)} - {str(cmsg_l1)}")

            prior_records_dict = self.REVIEW_MANAGER.REVIEW_DATASET.load_records(
                load_str=filecontents.decode("utf-8")
            )
            for prior_record in prior_records_dict.values():
                if str(prior_record["colrev_status"]) != str(
                    colrev.record.RecordState.md_imported
                ):
                    continue
                for record_to_unmerge, record in record_reset_list:

                    if any(
                        o in prior_record["colrev_origin"]
                        for o in record["colrev_origin"].split(";")
                    ):
                        self.REVIEW_MANAGER.report_logger.info(
                            f'reset({record["ID"]}) to'
                            f"\n{self.REVIEW_MANAGER.pp.pformat(prior_record)}\n\n"
                        )
                        # Note : we don't want to restore the old ID...
                        current_id = record_to_unmerge["ID"]
                        record_to_unmerge.clear()
                        for k, v in prior_record.items():
                            record_to_unmerge[k] = v
                        record_to_unmerge["ID"] = current_id
                        break
                # Stop if all original records have been found
                if (
                    len(
                        [
                            x["colrev_status"] != "md_imported"
                            for x, y in record_reset_list
                        ]
                    )
                    == 0
                ):
                    break

        # TODO : double-check! resetting the prep does not necessarily mean
        # that wrong records were merged...
        # TODO : if any record_to_unmerge['status'] != RecordState.md_imported:
        # retrieve the original record from the search/source file
        for record_to_unmerge, record in record_reset_list:
            record_to_unmerge.update(
                colrev_status=colrev.record.RecordState.md_needs_manual_preparation
            )

    def reset_records(self, *, reset_ids: list) -> None:
        # Note: entrypoint for CLI

        records = self.REVIEW_MANAGER.REVIEW_DATASET.load_records_dict()
        records_to_reset = []
        for reset_id in reset_ids:
            if reset_id in records:
                records_to_reset.append(records[reset_id])
            else:
                print(f"Error: record not found (ID={reset_id})")

        self.reset(record_list=records_to_reset)

        saved_args = {"reset_records": ",".join(reset_ids)}
        self.REVIEW_MANAGER.REVIEW_DATASET.save_records_dict(records=records)
        self.REVIEW_MANAGER.REVIEW_DATASET.add_record_changes()
        self.REVIEW_MANAGER.create_commit(
            msg="Reset metadata for manual preparation",
            script_call="colrev prep",
            saved_args=saved_args,
        )

    def reset_ids(self) -> None:
        # Note: entrypoint for CLI

        records = self.REVIEW_MANAGER.REVIEW_DATASET.load_records_dict()

        git_repo = self.REVIEW_MANAGER.REVIEW_DATASET.get_repo()
        RECORDS_FILE_RELATIVE = self.REVIEW_MANAGER.paths["RECORDS_FILE_RELATIVE"]
        revlist = (
            ((commit.tree / str(RECORDS_FILE_RELATIVE)).data_stream.read())
            for commit in git_repo.iter_commits(paths=str(RECORDS_FILE_RELATIVE))
        )
        filecontents = next(revlist)  # noqa
        prior_records_dict = self.REVIEW_MANAGER.REVIEW_DATASET.load_records(
            load_str=filecontents.decode("utf-8")
        )
        for record in records.values():
            prior_record_l = [
                x
                for x in prior_records_dict.values()
                if x["colrev_origin"] == record["colrev_origin"]
            ]
            if len(prior_record_l) != 1:
                continue
            prior_record = prior_record_l[0]
            record["ID"] = prior_record["ID"]

        self.REVIEW_MANAGER.REVIEW_DATASET.save_records_dict(records=records)

    def setup_custom_script(self) -> None:

        filedata = pkgutil.get_data(__name__, "template/custom_prep_script.py")
        if filedata:
            with open("custom_prep_script.py", "w", encoding="utf-8") as file:
                file.write(filedata.decode("utf-8"))

        self.REVIEW_MANAGER.REVIEW_DATASET.add_changes(path="custom_prep_script.py")

        prep_round = self.REVIEW_MANAGER.settings.prep.prep_rounds[-1]
        prep_round.scripts.append({"endpoint": "custom_prep_script"})
        self.REVIEW_MANAGER.save_settings()

    def main(
        self,
        *,
        keep_ids: bool = False,
        debug_ids: str = "NA",
        debug_file: str = "NA",
    ) -> None:
        """Preparation of records"""

        saved_args = locals()

        self.check_DBs_availability()

        if self.REVIEW_MANAGER.DEBUG_MODE:
            print("\n\n\n")
            self.REVIEW_MANAGER.logger.info("Start debug prep\n")
            self.REVIEW_MANAGER.logger.info(
                "The script will replay the preparation procedures"
                " step-by-step, allow you to identify potential errors, trace them to "
                "their colrev_origin and correct them."
            )
            input("\nPress Enter to continue")
            print("\n\n")

        if not keep_ids:
            del saved_args["keep_ids"]

        def load_prep_data():

            record_state_list = (
                self.REVIEW_MANAGER.REVIEW_DATASET.get_record_state_list()
            )
            nr_tasks = len(
                [
                    x
                    for x in record_state_list
                    if str(colrev.record.RecordState.md_imported) == x["colrev_status"]
                ]
            )

            if 0 == len(record_state_list):
                PAD = 35
            else:
                PAD = min((max(len(x["ID"]) for x in record_state_list) + 2), 35)

            r_states_to_prepare = [
                colrev.record.RecordState.md_imported,
                colrev.record.RecordState.md_prepared,
                colrev.record.RecordState.md_needs_manual_preparation,
            ]
            items = self.REVIEW_MANAGER.REVIEW_DATASET.read_next_record(
                conditions=[{"colrev_status": s} for s in r_states_to_prepare]
            )

            prior_ids = [
                x["ID"]
                for x in record_state_list
                if str(colrev.record.RecordState.md_imported) == x["colrev_status"]
            ]

            prep_data = {
                "nr_tasks": nr_tasks,
                "PAD": PAD,
                "items": list(items),
                "prior_ids": prior_ids,
            }
            self.REVIEW_MANAGER.logger.debug(self.REVIEW_MANAGER.pp.pformat(prep_data))
            return prep_data

        def get_preparation_data(*, prep_round: colrev.settings.PrepRound):
            if self.REVIEW_MANAGER.DEBUG_MODE:
                prepare_data = load_prep_data_for_debug(
                    debug_ids=debug_ids, debug_file=debug_file
                )
                if prepare_data["nr_tasks"] == 0:
                    print("ID not found in history.")
            else:
                prepare_data = load_prep_data()

            if self.REVIEW_MANAGER.DEBUG_MODE:
                self.REVIEW_MANAGER.logger.info(
                    "In this round, we set the similarity "
                    f"threshold ({self.RETRIEVAL_SIMILARITY})"
                )
                input("Press Enter to continue")
                print("\n\n")
                self.REVIEW_MANAGER.logger.info(
                    f"prepare_data: " f"{self.REVIEW_MANAGER.pp.pformat(prepare_data)}"
                )
            self.PAD = prepare_data["PAD"]
            items = prepare_data["items"]
            prep_data = []
            for item in items:
                prep_data.append(
                    {
                        "record": colrev.record.PrepRecord(data=item),
                        # Note : we cannot load scripts here
                        # because pathos/multiprocessing
                        # does not support functions as parameters
                        "prep_round_scripts": prep_round.scripts,
                        "prep_round": prep_round.name,
                    }
                )
            return prep_data

        def load_prep_data_for_debug(
            *, debug_ids: str, debug_file: str = "NA"
        ) -> typing.Dict:

            self.REVIEW_MANAGER.logger.info("Data passed to the scripts")
            if debug_file is None:
                debug_file = "NA"
            if "NA" != debug_file:
                with open(debug_file, encoding="utf8") as target_db:
                    records_dict = self.REVIEW_MANAGER.REVIEW_DATASET.load_records_dict(
                        load_str=target_db.read()
                    )

                for record in records_dict.values():
                    if colrev.record.RecordState.md_imported != record.get("state", ""):
                        self.REVIEW_MANAGER.logger.info(
                            f"Setting colrev_status to md_imported {record['ID']}"
                        )
                        record["colrev_status"] = colrev.record.RecordState.md_imported
                debug_ids_list = list(records_dict.keys())
                debug_ids = ",".join(debug_ids_list)
                self.REVIEW_MANAGER.logger.info("Imported record (retrieved from file)")

            else:
                records = []
                debug_ids_list = debug_ids.split(",")
                REVIEW_DATASET = self.REVIEW_MANAGER.REVIEW_DATASET
                original_records = list(
                    REVIEW_DATASET.read_next_record(
                        conditions=[{"ID": ID} for ID in debug_ids_list]
                    )
                )
                # self.REVIEW_MANAGER.logger.info("Current record")
                # self.REVIEW_MANAGER.pp.pprint(original_records)
                records = REVIEW_DATASET.retrieve_records_from_history(
                    original_records=original_records,
                    condition_state=colrev.record.RecordState.md_imported,
                )
                self.REVIEW_MANAGER.logger.info(
                    "Imported record (retrieved from history)"
                )

            if len(records) == 0:
                prep_data = {"nr_tasks": 0, "PAD": 0, "items": [], "prior_ids": []}
            else:
                print(colrev.record.PrepRecord(data=records[0]))
                input("Press Enter to continue")
                print("\n\n")
                prep_data = {
                    "nr_tasks": len(debug_ids_list),
                    "PAD": len(debug_ids),
                    "items": records,
                    "prior_ids": [debug_ids_list],
                }
            return prep_data

        def setup_prep_round(*, i, prep_round):

            if i == 0:
                self.FIRST_ROUND = True

            else:
                self.FIRST_ROUND = False

            if i == len(self.REVIEW_MANAGER.settings.prep.prep_rounds) - 1:
                self.LAST_ROUND = True
            else:
                self.LAST_ROUND = False

            # Note : we add the script automatically (not as part of the settings.json)
            # because it must always be executed at the end
            if prep_round.name not in ["load_fixes", "exclusion"]:
                prep_round.scripts.append({"endpoint": "update_metadata_status"})

            self.REVIEW_MANAGER.logger.info(f"Prepare ({prep_round.name})")

            self.RETRIEVAL_SIMILARITY = prep_round.similarity  # type: ignore
            saved_args["similarity"] = self.RETRIEVAL_SIMILARITY
            self.REVIEW_MANAGER.report_logger.debug(
                f"Set RETRIEVAL_SIMILARITY={self.RETRIEVAL_SIMILARITY}"
            )

            required_prep_scripts = list(prep_round.scripts)

            required_prep_scripts.append({"endpoint": "update_metadata_status"})

            AdapterManager = self.REVIEW_MANAGER.get_environment_service(
                service_identifier="AdapterManager"
            )
            self.prep_scripts: typing.Dict[
                str, typing.Any
            ] = AdapterManager.load_scripts(
                PROCESS=self,
                scripts=required_prep_scripts,
            )

        def log_details(*, prepared_records: list) -> None:
            nr_recs = len(
                [
                    record
                    for record in prepared_records
                    if record["colrev_status"] == colrev.record.RecordState.md_prepared
                ]
            )

            self.REVIEW_MANAGER.logger.info(
                "Records prepared:".ljust(35) + f"{colors.GREEN}{nr_recs}{colors.END}"
            )

            nr_recs = len(
                [
                    record
                    for record in prepared_records
                    if record["colrev_status"]
                    == colrev.record.RecordState.md_needs_manual_preparation
                ]
            )
            if nr_recs > 0:
                self.REVIEW_MANAGER.report_logger.info(
                    f"Statistics: {nr_recs} records not prepared"
                )
                self.REVIEW_MANAGER.logger.info(
                    "Records to prepare manually:".ljust(35)
                    + f"{colors.ORANGE}{nr_recs}{colors.END}"
                )
            else:
                self.REVIEW_MANAGER.logger.info(
                    "Records to prepare manually:".ljust(35) + f"{nr_recs}"
                )

            nr_recs = len(
                [
                    record
                    for record in prepared_records
                    if record["colrev_status"]
                    == colrev.record.RecordState.rev_prescreen_excluded
                ]
            )
            if nr_recs > 0:
                self.REVIEW_MANAGER.report_logger.info(
                    f"Statistics: {nr_recs} records (prescreen) excluded "
                    "(non-latin alphabet)"
                )
                self.REVIEW_MANAGER.logger.info(
                    "Records prescreen-excluded:".ljust(35)
                    + f"{colors.GREEN}{nr_recs}{colors.END}"
                )

        if "NA" != debug_ids:
            self.REVIEW_MANAGER.DEBUG_MODE = True

        for i, prep_round in enumerate(self.REVIEW_MANAGER.settings.prep.prep_rounds):

            setup_prep_round(i=i, prep_round=prep_round)

            preparation_data = get_preparation_data(prep_round=prep_round)

            if len(preparation_data) == 0:
                print("No records to prepare.")
                return

            if self.REVIEW_MANAGER.DEBUG_MODE:
                # Note: preparation_data is not turned into a list of records.
                prepared_records = []
                for item in preparation_data:
                    record = self.prepare(item)
                    prepared_records.append(record)
            else:
                # Note : p_map shows the progress (tqdm) but it is inefficient
                # https://github.com/swansonk14/p_tqdm/issues/34
                # from p_tqdm import p_map
                # preparation_data = p_map(self.prepare, preparation_data)

                script_names = [r["endpoint"] for r in prep_round.scripts]
                if "exclude_languages" in script_names:  # type: ignore
                    self.REVIEW_MANAGER.logger.info(
                        f"{colors.ORANGE}The language detector may take "
                        f"longer and require RAM{colors.END}"
                    )
                    pool = ProcessPool(nodes=mp.cpu_count() // 2)
                else:
                    pool = ProcessPool(nodes=self.CPUS)
                prepared_records = pool.map(self.prepare, preparation_data)

                pool.close()
                pool.join()
                pool.clear()

            if not self.REVIEW_MANAGER.DEBUG_MODE:
                # prepared_records = [x.get_data() for x in prepared_records]
                self.REVIEW_MANAGER.REVIEW_DATASET.save_record_list_by_ID(
                    record_list=prepared_records
                )

                log_details(prepared_records=prepared_records)

                # Multiprocessing mixes logs of different records.
                # For better readability:
                prepared_records_IDs = [x["ID"] for x in prepared_records]
                self.REVIEW_MANAGER.reorder_log(IDs=prepared_records_IDs)

                self.REVIEW_MANAGER.create_commit(
                    msg=f"Prepare records ({prep_round.name})",
                    script_call="colrev prep",
                    saved_args=saved_args,
                )
                self.REVIEW_MANAGER.reset_log()
                print()

        if not keep_ids and not self.REVIEW_MANAGER.DEBUG_MODE:
            self.REVIEW_MANAGER.REVIEW_DATASET.set_IDs()
            self.REVIEW_MANAGER.create_commit(
                msg="Set IDs", script_call="colrev prep", saved_args=saved_args
            )

        return


if __name__ == "__main__":
    pass
