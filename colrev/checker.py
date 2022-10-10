#! /usr/bin/env python
"""Checkers for CoLRev repositories"""
from __future__ import annotations

import itertools
import os
import re
import sys
import typing
from importlib.metadata import version
from pathlib import Path
from subprocess import check_call
from subprocess import DEVNULL
from subprocess import STDOUT
from typing import TYPE_CHECKING

import git
import yaml
from git.exc import GitCommandError
from git.exc import InvalidGitRepositoryError

import colrev.exceptions as colrev_exceptions
import colrev.operation

if TYPE_CHECKING:
    import colrev.review_manager


PASS, FAIL = 0, 1


class Checker:
    """The CoLRev checker makes sure the project setup is ok"""

    __COLREV_HOOKS_URL = "https://github.com/geritwagner/colrev-hooks"

    def __init__(
        self,
        *,
        review_manager: colrev.review_manager.ReviewManager,
    ) -> None:

        self.review_manager = review_manager

    def get_colrev_versions(self) -> list[str]:
        """Get the colrev version as a list: (last-version, current-version)"""
        current_colrev_version = version("colrev")
        last_colrev_version = current_colrev_version
        try:
            last_commit_message = self.review_manager.dataset.get_commit_message(
                commit_nr=0
            )
            cmsg_lines = last_commit_message.split("\n")
            for cmsg_line in cmsg_lines[0:100]:
                if "colrev:" in cmsg_line and "version" in cmsg_line:
                    last_colrev_version = cmsg_line[cmsg_line.find("version ") + 8 :]
        except ValueError:
            pass
        return [last_colrev_version, current_colrev_version]

    def __check_software(self) -> None:
        last_version, current_version = self.get_colrev_versions()
        if last_version != current_version:
            raise colrev_exceptions.CoLRevUpgradeError(last_version, current_version)
        if not sys.version_info > (2, 7):
            raise colrev_exceptions.CoLRevException("CoLRev does not support Python 2.")
        if sys.version_info < (3, 5):
            self.review_manager.logger.warning(
                "CoLRev uses Python 3.8 features (currently, %s is installed). Please upgrade.",
                sys.version_info,
            )

    def __lsremote(self, *, url: str) -> dict:
        remote_refs = {}
        git_repo = git.cmd.Git()
        for ref in git_repo.ls_remote(url).split("\n"):
            hash_ref_list = ref.split("\t")
            remote_refs[hash_ref_list[1]] = hash_ref_list[0]
        return remote_refs

    def __colrev_hook_up_to_date(self) -> bool:

        with open(".pre-commit-config.yaml", encoding="utf8") as pre_commit_y:
            pre_commit_config = yaml.load(pre_commit_y, Loader=yaml.FullLoader)

        local_hooks_version = ""
        for repository in pre_commit_config["repos"]:
            if repository["repo"] == self.__COLREV_HOOKS_URL:
                local_hooks_version = repository["rev"]

        refs = self.__lsremote(url=self.__COLREV_HOOKS_URL)
        remote_sha = refs["HEAD"]
        if remote_sha == local_hooks_version:
            return True
        return False

    def __update_colrev_hooks(self) -> None:
        if self.__COLREV_HOOKS_URL not in self.__get_installed_repos():
            return
        try:
            if not self.__colrev_hook_up_to_date():
                self.review_manager.logger.info("Updating pre-commit hooks")
                check_call(["pre-commit", "autoupdate"], stdout=DEVNULL, stderr=STDOUT)
                self.review_manager.dataset.add_changes(
                    path=Path(".pre-commit-config.yaml")
                )
        except GitCommandError:
            self.review_manager.logger.warning(
                "No Internet connection, cannot check remote "
                "colrev-hooks repository for updates."
            )
        return

    def check_repository_setup(self) -> None:
        """Check the repository setup"""

        # 1. git repository?
        if not self.__is_git_repo():
            raise colrev_exceptions.RepoSetupError("no git repository. Use colrev init")

        # 2. colrev project?
        if not self.__is_colrev_project():
            raise colrev_exceptions.RepoSetupError(
                "No colrev repository."
                + "To retrieve a shared repository, use colrev init."
                + "To initalize a new repository, "
                + "execute the command in an empty directory."
            )

        # 3. Pre-commit hooks installed?
        self.__require_colrev_hooks_installed()

        # 4. Pre-commit hooks up-to-date?
        self.__update_colrev_hooks()

    def in_virtualenv(self) -> bool:
        """Check whether CoLRev operates in a virtual environment"""

        def get_base_prefix_compat() -> str:
            return (
                getattr(sys, "base_prefix", None)
                or getattr(sys, "real_prefix", None)
                or sys.prefix
            )

        return get_base_prefix_compat() != sys.prefix

    def __check_git_conflicts(self) -> None:
        # Note: when check is called directly from the command line.
        # pre-commit hooks automatically notify on merge conflicts

        git_repo = self.review_manager.dataset.get_repo()
        unmerged_blobs = git_repo.index.unmerged_blobs()

        for path, list_of_blobs in unmerged_blobs.items():
            for (stage, _) in list_of_blobs:
                if stage != 0:
                    raise colrev_exceptions.GitConflictError(Path(path))

    def __is_git_repo(self) -> bool:
        try:
            _ = self.review_manager.dataset.get_repo().git_dir
            return True
        except InvalidGitRepositoryError:
            return False

    def __is_colrev_project(self) -> bool:
        required_paths = [
            Path(".pre-commit-config.yaml"),
            Path(".gitignore"),
            Path("settings.json"),
        ]
        if not all((self.review_manager.path / x).is_file() for x in required_paths):
            return False
        return True

    def __get_installed_hooks(self) -> list:
        installed_hooks = []
        with open(".pre-commit-config.yaml", encoding="utf8") as pre_commit_y:
            pre_commit_config = yaml.load(pre_commit_y, Loader=yaml.FullLoader)
        for repository in pre_commit_config["repos"]:
            installed_hooks.extend([hook["id"] for hook in repository["hooks"]])
        return installed_hooks

    def __get_installed_repos(self) -> list:
        installed_repos = []
        with open(".pre-commit-config.yaml", encoding="utf8") as pre_commit_y:
            pre_commit_config = yaml.load(pre_commit_y, Loader=yaml.FullLoader)
        for repository in pre_commit_config["repos"]:
            installed_repos.append(repository["repo"])
        return installed_repos

    def __require_colrev_hooks_installed(self) -> bool:
        required_hooks = [
            "colrev-hooks-check",
            "colrev-hooks-format",
            "colrev-hooks-report",
            "colrev-hooks-share",
        ]
        installed_hooks = self.__get_installed_hooks()
        hooks_activated = set(required_hooks).issubset(set(installed_hooks))
        if not hooks_activated:
            missing_hooks = [x for x in required_hooks if x not in installed_hooks]
            raise colrev_exceptions.RepoSetupError(
                f"missing hooks in .pre-commit-config.yaml ({', '.join(missing_hooks)})"
            )

        pch_file = Path(".git/hooks/pre-commit")
        if pch_file.is_file():
            with open(pch_file, encoding="utf8") as file:
                if "File generated by pre-commit" not in file.read(4096):
                    raise colrev_exceptions.RepoSetupError(
                        "pre-commit hooks not installed (use pre-commit install)"
                    )
        else:
            raise colrev_exceptions.RepoSetupError(
                "pre-commit hooks not installed (use pre-commit install)"
            )

        psh_file = Path(".git/hooks/pre-push")
        if psh_file.is_file():
            with open(psh_file, encoding="utf8") as file:
                if "File generated by pre-commit" not in file.read(4096):
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
            with open(pcmh_file, encoding="utf8") as file:
                if "File generated by pre-commit" not in file.read(4096):
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

    def __retrieve_ids_from_bib(self, *, file_path: Path) -> list:
        assert file_path.suffix == ".bib"
        record_ids = []
        with open(file_path, encoding="utf8") as file:
            line = file.readline()
            while line:
                if "@" in line[:5]:
                    record_id = line[line.find("{") + 1 : line.rfind(",")]
                    record_ids.append(record_id.lstrip())
                line = file.readline()
        return record_ids

    def __check_colrev_origins(self, *, status_data: dict) -> None:
        """Check colrev_origins"""

        # Check whether each record has an origin
        if not len(status_data["entries_without_origin"]) == 0:
            raise colrev_exceptions.OriginError(
                f"Entries without origin: {', '.join(status_data['entries_without_origin'])}"
            )

        # Check for broken origins
        all_record_links = []
        for bib_file in self.review_manager.search_dir.glob("*.bib"):
            search_ids = self.__retrieve_ids_from_bib(file_path=bib_file)
            for search_id in search_ids:
                all_record_links.append(bib_file.name + "/" + search_id)
        delta = set(status_data["record_links_in_bib"]) - set(all_record_links)
        if len(delta) > 0:
            raise colrev_exceptions.OriginError(f"broken origins: {delta}")

        # Check for non-unique origins
        origins = list(itertools.chain(*status_data["origin_list"]))
        non_unique_origins = []
        for org in origins:
            if origins.count(org) > 1:
                non_unique_origins.append(org)
        if non_unique_origins:
            for _, org in status_data["origin_list"]:
                if org in non_unique_origins:
                    raise colrev_exceptions.OriginError(
                        f'Non-unique origin: origin="{org}"'
                    )

    def check_fields(self, *, status_data: dict) -> None:
        """Check field values"""

        # Check status fields
        status_schema = colrev.record.RecordState
        stat_diff = set(status_data["status_fields"]).difference(status_schema)
        if stat_diff:
            raise colrev_exceptions.FieldValueError(
                f"status field(s) {stat_diff} not in {status_schema}"
            )

    def check_status_transitions(self, *, status_data: dict) -> None:
        """Check for invalid state transitions"""
        # Note : currently, we do not prevent particular transitions.
        # We may decide to provide settings parameters to apply
        # more restrictive rules related to valid transitions.

        # We allow particular combinations of multiple transitions
        # if len(set(status_data["start_states"])) > 1:
        #     raise colrev_exceptions.StatusTransitionError(
        #         "multiple transitions from different "
        #         f'start states ({set(status_data["start_states"])})'
        #     )

        # We may apply more restrictive criteria to prevent invalid_state_transitions
        # E.g., setting a record from rev_synthesized to rev_included should be ok.
        # if len(set(status_data["invalid_state_transitions"])) > 0:
        #     raise colrev_exceptions.StatusTransitionError(
        #         "invalid state transitions: \n    "
        #         + "\n    ".join(status_data["invalid_state_transitions"])
        #     )

    def __check_records_screen(self, *, status_data: dict) -> None:
        """Check consistency of screening criteria and status"""

        # pylint: disable=too-many-branches

        # Check screen
        # Note: consistency of inclusion_2=yes -> inclusion_1=yes
        # is implicitly ensured through status
        # (screen2-included/excluded implies prescreen included!)

        field_errors = []

        if status_data["screening_criteria_list"]:
            screening_criteria = self.review_manager.settings.screen.criteria
            if not screening_criteria:
                criteria = ["NA"]
                pattern = "^NA$"
                pattern_inclusion = "^NA$"
            else:
                pattern = "=(in|out);".join(screening_criteria.keys()) + "=(in|out)"
                pattern_inclusion = "=in;".join(screening_criteria.keys()) + "=in"

            for [record_id, status, screen_crit] in status_data[
                "screening_criteria_list"
            ]:

                if status not in colrev.record.RecordState.get_post_x_states(
                    state=colrev.record.RecordState.rev_included
                ):
                    assert "NA" == screen_crit
                    continue

                # print([record_id, status, screen_crit])
                if not re.match(pattern, screen_crit):
                    # Note: this should also catch cases of missing
                    # screening criteria
                    field_errors.append(
                        "Screening criteria field not matching "
                        f"pattern: {screen_crit} ({record_id}; criteria: {criteria})"
                    )

                elif str(colrev.record.RecordState.rev_excluded) == status:
                    if ["NA"] == criteria:
                        if "NA" == screen_crit:
                            continue
                        field_errors.append(f"screen_crit field not NA: {screen_crit}")

                    if "=out" not in screen_crit:
                        self.review_manager.logger.error("criteria: %s", criteria)
                        field_errors.append(
                            "Excluded record with no screening_criterion violated: "
                            f"{record_id}, {status}, {screen_crit}"
                        )

                # Note: we don't have to consider the cases of
                # status=retrieved/prescreen_included/prescreen_excluded
                # because they would not have screening_criteria.
                elif status in [
                    str(colrev.record.RecordState.rev_included),
                    str(colrev.record.RecordState.rev_synthesized),
                ]:
                    if not re.match(pattern_inclusion, screen_crit):
                        field_errors.append(
                            "Included record with screening_criterion satisfied: "
                            f"{record_id}, {status}, {screen_crit}"
                        )
                else:
                    if status == colrev.record.RecordState.rev_excluded:
                        continue
                    if not re.match(pattern_inclusion, screen_crit):
                        field_errors.append(
                            "Record with screening_criterion but before "
                            f"screen: {record_id}, {status}"
                        )
        if len(field_errors) > 0:
            raise colrev_exceptions.FieldValueError(
                "\n    " + "\n    ".join(field_errors)
            )

    def check_change_in_propagated_id(
        self, *, prior_id: str, new_id: str = "TBD", project_context: Path
    ) -> list:
        """Check whether propagated IDs were changed

        A propagated ID is a record ID that is stored outside the records.bib.
        Propagated IDs should not be changed in the records.bib
        because this would break the link between the propagated ID and its metadata.
        """
        # pylint: disable=too-many-branches

        ignore_patterns = [
            ".git",
            ".report.log",
            ".pre-commit-config.yaml",
        ]

        text_formats = [".txt", ".csv", ".md", ".bib", ".yaml"]
        notifications = []
        for root, dirs, files in os.walk(project_context, topdown=False):
            for name in files:
                if any((x in name) or (x in root) for x in ignore_patterns):
                    continue
                if prior_id in name:
                    msg = (
                        f"Old ID ({prior_id}, changed to {new_id} in the "
                        + f"RECORDS_FILE) found in filepath: {name}"
                    )
                    if msg not in notifications:
                        notifications.append(msg)

                if not any(name.endswith(x) for x in text_formats):
                    self.review_manager.logger.debug("Skipping %s", name)
                    continue
                self.review_manager.logger.debug("Checking %s", name)
                if name.endswith(".bib"):
                    retrieved_ids = self.__retrieve_ids_from_bib(
                        file_path=Path(os.path.join(root, name))
                    )
                    if prior_id in retrieved_ids:
                        msg = (
                            f"Old ID ({prior_id}, changed to {new_id} in "
                            + f"the RECORDS_FILE) found in file: {name}"
                        )
                        if msg not in notifications:
                            notifications.append(msg)
                else:
                    with open(os.path.join(root, name), encoding="utf8") as file:
                        line = file.readline()
                        while line:
                            if name.endswith(".bib") and "@" in line[:5]:
                                line = file.readline()
                            if prior_id in line:
                                msg = (
                                    f"Old ID ({prior_id}, to {new_id} in "
                                    + f"the RECORDS_FILE) found in file: {name}"
                                )
                                if msg not in notifications:
                                    notifications.append(msg)
                            line = file.readline()
            for name in dirs:
                if any((x in name) or (x in root) for x in ignore_patterns):
                    continue
                if prior_id in name:
                    notifications.append(
                        f"Old ID ({prior_id}, changed to {new_id} in the "
                        f"RECORDS_FILE) found in filepath: {name}"
                    )
        return notifications

    def __check_change_in_propagated_ids(
        self, *, prior: dict, status_data: dict
    ) -> None:
        """Check for changes in propagated IDs"""

        if "persisted_IDs" not in prior:
            return
        for prior_origin, prior_id in prior["persisted_IDs"]:
            if prior_origin not in [x[1] for x in status_data["origin_list"]]:
                # Note: this does not catch origins removed before md_processed
                raise colrev_exceptions.OriginError(f"origin removed: {prior_origin}")
            for new_id, new_origin in status_data["origin_list"]:
                if new_origin == prior_origin:
                    if new_id != prior_id:
                        notifications = self.check_change_in_propagated_id(
                            prior_id=prior_id,
                            new_id=new_id,
                            project_context=self.review_manager.path,
                        )
                        notifications.append(
                            "ID of processed record changed from "
                            f"{prior_id} to {new_id}"
                        )
                        raise colrev_exceptions.PropagatedIDChange(notifications)

    def check_sources(self) -> None:
        """Check the sources"""
        for source in self.review_manager.settings.sources:

            if not source.filename.is_file():
                self.review_manager.logger.debug(
                    f"Search details without file: {source.filename}"
                )
            if not str(source.filename)[:12].startswith("data/search/"):
                self.review_manager.logger.debug(
                    f"Source filename does not start with 'data/search/: {source.filename}"
                )

            # date_regex = r"^\d{4}-\d{2}-\d{2}$"
            # if "completion_date" in source:
            #     if not re.search(date_regex, source["completion_date"]):
            #         raise SearchSettingsError(
            #             "completion date not matching YYYY-MM-DD format: "
            #             f'{source["completion_date"]}'
            #         )
            # if "start_date" in source:
            #     if not re.search(date_regex, source["start_date"]):
            #         raise SearchSettingsError(
            #             "start_date date not matchin YYYY-MM-DD format: "
            #             f'{source["start_date"]}'
            #         )

    def __retrieve_prior(self) -> dict:
        prior: dict = {"colrev_status": [], "persisted_IDs": []}
        prior_records = next(self.review_manager.dataset.load_from_git_history())
        for prior_record in prior_records.values():
            for orig in prior_record["colrev_origin"]:
                prior["colrev_status"].append([orig, prior_record["colrev_status"]])
                if prior_record[
                    "colrev_status"
                ] in colrev.record.RecordState.get_post_x_states(
                    state=colrev.record.RecordState.md_processed
                ):
                    prior["persisted_IDs"].append([orig, prior_record["ID"]])
        return prior

    def __get_status_transitions(
        self,
        *,
        record_id: str,
        origin: list,
        prior: dict,
        status: colrev.record.RecordState,
        status_data: dict,
    ) -> dict:

        prior_status = []
        if "colrev_status" in prior:
            prior_status = [
                stat for (org, stat) in prior["colrev_status"] if org in origin
            ]

        status_transition = {}
        if len(prior_status) == 0:
            status_transition[record_id] = "load"
        else:
            proc_transition_list: list = [
                x["trigger"]
                for x in colrev.record.RecordStateModel.transitions
                if str(x["source"]) == prior_status[0] and str(x["dest"]) == status
            ]
            if len(proc_transition_list) == 0 and prior_status[0] != status:
                status_data["start_states"].append(prior_status[0])
                if prior_status[0] not in colrev.record.RecordState:
                    raise colrev_exceptions.StatusFieldValueError(
                        record_id, "colrev_status", prior_status[0]
                    )
                if status not in colrev.record.RecordState:
                    raise colrev_exceptions.StatusFieldValueError(
                        record_id, "colrev_status", str(status)
                    )

                status_data["invalid_state_transitions"].append(
                    f"{record_id}: {prior_status[0]} to {status}"
                )
            if 0 == len(proc_transition_list):
                status_transition[record_id] = "load"
            else:
                proc_transition = proc_transition_list.pop()
                status_transition[record_id] = proc_transition
        return status_transition

    def __retrieve_status_data(self, *, prior: dict) -> dict:

        status_data: dict = {
            "pdf_not_exists": [],
            "status_fields": [],
            "status_transitions": [],
            "start_states": [],
            "screening_criteria_list": [],
            "IDs": [],
            "entries_without_origin": [],
            "record_links_in_bib": [],
            "persisted_IDs": [],
            "origin_list": [],
            "invalid_state_transitions": [],
        }

        for record_dict in self.review_manager.dataset.load_records_dict(
            header_only=True
        ).values():
            status_data["IDs"].append(record_dict["ID"])

            for org in record_dict["colrev_origin"]:
                status_data["origin_list"].append([record_dict["ID"], org])

            post_md_processed_states = colrev.record.RecordState.get_post_x_states(
                state=colrev.record.RecordState.md_processed
            )
            if record_dict["colrev_status"] in post_md_processed_states:
                for origin_part in record_dict["colrev_origin"]:
                    status_data["persisted_IDs"].append(
                        [origin_part, record_dict["ID"]]
                    )

            if "file" in record_dict:
                if record_dict["file"].is_file():
                    status_data["pdf_not_exists"].append(record_dict["ID"])

            if [] != record_dict.get("colrev_origin", []):
                for org in record_dict["colrev_origin"]:
                    status_data["record_links_in_bib"].append(org)
            else:
                status_data["entries_without_origin"].append(record_dict["ID"])

            status_data["status_fields"].append(record_dict["colrev_status"])

            if "screening_criteria" in record_dict:
                ec_case = [
                    record_dict["ID"],
                    record_dict["colrev_status"],
                    record_dict["screening_criteria"],
                ]
                status_data["screening_criteria_list"].append(ec_case)

            status_transition = self.__get_status_transitions(
                record_id=record_dict["ID"],
                origin=record_dict["colrev_origin"],
                prior=prior,
                status=record_dict["colrev_status"],
                status_data=status_data,
            )

            status_data["status_transitions"].append(status_transition)

        return status_data

    def check_repo(self) -> dict:
        """Check whether the repository is in a consistent state
        Entrypoint for pre-commit hooks
        """

        # pylint: disable=not-a-mapping

        self.review_manager.notified_next_operation = (
            colrev.operation.OperationsType.check
        )

        # We work with exceptions because each issue may be raised in different checks.
        # Currently, linting is limited for the scripts.

        environment_manager = self.review_manager.get_environment_manager()
        check_scripts: list[dict[str, typing.Any]] = [
            {
                "script": environment_manager.check_git_installed,
                "params": [],
            },
            {
                "script": environment_manager.check_docker_installed,
                "params": [],
            },
            {"script": self.__check_git_conflicts, "params": []},
            {"script": self.check_repository_setup, "params": []},
            {"script": self.__check_software, "params": []},
        ]

        if self.review_manager.dataset.records_file.is_file():
            if self.review_manager.dataset.records_file_in_history():
                prior = self.__retrieve_prior()
                self.review_manager.logger.debug("prior")
                self.review_manager.logger.debug(
                    self.review_manager.p_printer.pformat(prior)
                )
            else:  # if RECORDS_FILE not yet in git history
                prior = {}

            status_data = self.__retrieve_status_data(prior=prior)

            main_refs_checks = [
                {"script": self.check_sources, "params": []},
            ]
            # Note : duplicate record IDs are already prevented by pybtex...

            if prior:  # if RECORDS_FILE in git history
                main_refs_checks.extend(
                    [
                        {
                            "script": self.__check_change_in_propagated_ids,
                            "params": {"prior": prior, "status_data": status_data},
                        },
                        {
                            "script": self.__check_colrev_origins,
                            "params": {"status_data": status_data},
                        },
                        {
                            "script": self.check_fields,
                            "params": {"status_data": status_data},
                        },
                        {
                            "script": self.check_status_transitions,
                            "params": {"status_data": status_data},
                        },
                        {
                            "script": self.__check_records_screen,
                            "params": {"status_data": status_data},
                        },
                    ]
                )

            check_scripts.extend(main_refs_checks)

        data_operation = self.review_manager.get_data_operation(
            notify_state_transition_operation=False
        )
        data_checks = [
            {
                "script": data_operation.main,
                "params": [],
            },
            {
                "script": self.review_manager.update_status_yaml,
                "params": [],
            },
        ]

        check_scripts.extend(data_checks)

        failure_items = []
        for check_script in check_scripts:
            try:
                if not check_script["params"]:
                    self.review_manager.logger.debug(
                        "%s() called", check_script["script"].__name__
                    )
                    check_script["script"]()
                else:
                    self.review_manager.logger.debug(
                        "%s(params) called", check_script["script"].__name__
                    )
                    if isinstance(check_script["params"], list):
                        check_script["script"](*check_script["params"])
                    else:
                        check_script["script"](**check_script["params"])
                self.review_manager.logger.debug(
                    "%s: passed\n", check_script["script"].__name__
                )
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
            ) as exc:
                failure_items.append(f"{type(exc).__name__}: {exc}")

        if len(failure_items) > 0:
            return {"status": FAIL, "msg": "  " + "\n  ".join(failure_items)}
        return {"status": PASS, "msg": "Everything ok."}


if __name__ == "__main__":
    pass