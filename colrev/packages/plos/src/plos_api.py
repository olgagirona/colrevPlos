#! /usr/bin/env python
"""Plos API"""
import contextlib
import re
import typing
import urllib
from datetime import timedelta
from importlib.metadata import version
from pathlib import Path
from time import sleep

import requests
import requests_cache
from rapidfuzz import fuzz

import colrev.env.environment_manager
import colrev.exceptions as colrev_exceptions
import colrev.record.record_prep
from colrev.constants import Colors
from colrev.constants import Fields
from colrev.constants import Filepaths
from colrev.packages.crossref.src import record_transformer

LIMIT = 100 #Number max of elements returned
#MAXOFFSET = 1000

#Creates a session with cache
SESSION = requests_cache.CachedSession(
    str(Filepaths.LOCAL_ENVIRONMENT_DIR / Path("crossref_cache.sqlite")),
    backend="sqlite",
    expire_after=timedelta(days=30),
)

class PlosAPIError(Exception):
    """Plos API Error"""


class MaxOffsetError(PlosAPIError):
    """Max Offset Error"""


class HTTPRequest:
    "HTTP Resquest"

class Enpoint:
    "Endpoint"

    CURSOR_AS_ITER_METHOD = False

    def __init__(
            self,
            request_url: str,
            *,
            email: str = "",
            plos_plus_token: str = "",
    ) -> None:
        
        #self.retrieve = HTTPRequest(timeout=60).retrieve

        #List of http headers
        self.headers = {
            "user-agent": f"colrev/{version('colrev')} "
            + f"(https://github.com/CoLRev-Environment/colrev; mailto:{email})"
        }

        self.plos_plus_token = plos_plus_token
        if plos_plus_token:
            self.headers["Plos-Plus-API-Token"] = self.plos_plus_token
            self.request_url = request_url
            self.request_params: typing.Dict[str, str] = {}
            self.timeout = 60


class PlosAPI:
    "Plos Api"

    ISSN_REGEX = r"^\d{4}-?\d{3}[\dxX]$"
    YEAR_SCOPE_REGEX = r"^\d{4}-\d{4}$"  

    # https://github.com/Plos/rest-api-doc
    _api_url = "https://api.plos.org/"

    last_updated: str = ""

    _availability_exception_message = (
        f"Plos ({Colors.ORANGE}check https://status.plos.org/{Colors.END})"
    )

    def __init__(
            self,
            *,
            params: dict,
            rerun: bool = False,
    ):
        self.params = params

        _, self.email = (
            colrev.env.environment_manager.EnvironmentManager.get_name_mail_from_git()
        )
        self.rerun = rerun

    
    def get_url(self) -> str:
        "Get the url from the Plos API"

        if "url" not in self.params:
            raise ValueError("No url in params")
        
        url = self.params["url"]
        if not self.rerun and self.last_updated:
            #Changes the last updated date

            #see https://api.plos.org/solr/search-fields/
            #Publication_date format:
            #   [2009-12-07T00:00:00Z TO 2013-02-20T23:59:59Z]

            last_updated = self.last_updated.split(" ", maxsplit=1)[0]
            date_filter = f"fq=publication_date:[{last_updated} TO NOW]"
            url = f"{url}?{date_filter}"

        return url

    def get_len_total(self) -> int:
        "Get the total number of records from Plos based on the parameters"

        endpoint = Enpoint(self.params["url"], email=self.email)
        return endpoint.get_nr() #TO DO IN ENDPOINT CLASS
    
