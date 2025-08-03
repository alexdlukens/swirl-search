"""
@author:     Sid Probstein
@contact:    sid@swirl.today
"""

from os import environ
from sys import path

import django
from bs4 import BeautifulSoup

from swirl.utils import swirl_setdir

path.append(swirl_setdir())  # path to settings.py file
environ.setdefault("DJANGO_SETTINGS_MODULE", "swirl_server.settings")
django.setup()

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

from http import HTTPStatus

import requests
from celery.utils.log import get_task_logger
from requests.auth import HTTPBasicAuth, HTTPDigestAuth, HTTPProxyAuth
from requests.exceptions import ConnectionError
from urllib3.exceptions import NewConnectionError

from swirl.connectors.requests import Requests

########################################
########################################


class BeautifulSoupRTDExtractor(Requests):
    type = "BeautifulSoupRTDExtractor"

    def __init__(self, provider_id, search_id, update, request_id=""):
        super().__init__(provider_id, search_id, update, request_id)

    ########################################

    def get_method(self):
        return "get"

    def send_request(self, url, params=None, query=None, **kwargs):
        return requests.get(url, params=params, **kwargs)

    def execute_search(self, session=None):
        logger.debug(f"{self}: execute_search()")

        # determine if paging is required
        pages = 1
        if "PAGE" in self.query_mappings:
            if self.provider.results_per_query > 10:
                # yes, gather multiple pages
                pages = int(int(self.provider.results_per_query) / 10)
                # handle remainder
                if (int(self.provider.results_per_query) % 10) > 0:
                    pages = pages + 1

        # issue the query
        start = 1
        mapped_responses = []

        for page in range(0, pages):
            if "PAGE" in self.query_mappings:
                page_query = self.query_to_provider[: self.query_to_provider.rfind("&")]
                page_spec = None
                if "RESULT_INDEX" in self.query_mappings["PAGE"]:
                    page_spec = self.query_mappings["PAGE"].replace(
                        "RESULT_INDEX", str(start)
                    )
                if "RESULT_ZERO_INDEX" in self.query_mappings["PAGE"]:
                    page_spec = self.query_mappings["PAGE"].replace(
                        "RESULT_ZERO_INDEX", str(start - 1)
                    )
                if "PAGE_INDEX" in self.query_mappings["PAGE"]:
                    page_spec = self.query_mappings["PAGE"].replace(
                        "PAGE_INDEX", page + 1
                    )
                if page_spec:
                    page_query = (
                        page_query
                        + "&"
                        + page_spec
                        + self.query_to_provider[self.query_to_provider.rfind("&") :]
                    )
                else:
                    self.warning(
                        f"failed to resolve PAGE query mapping: {self.query_mappings['PAGE']}"
                    )
                    page_query = self.query_to_provider
            else:
                page_query = self.query_to_provider

            # check the query
            if page_query == "":
                self.error("page_query is blank")
                return

            # dictionary of authentication types permitted in the upcoming eval
            http_auth_dispatch = {
                "HTTPBasicAuth": HTTPBasicAuth,
                "HTTPDigestAuth": HTTPDigestAuth,
                "HTTProxyAuth": HTTPProxyAuth,
            }

            response = None
            # issue the query
            try:
                if self.provider.credentials:
                    if (
                        session
                        and self.provider.eval_credentials
                        and "{credentials}" in self.provider.credentials
                    ):
                        credentials = session[self.provider.eval_credentials]
                        self.provider.credentials = self.provider.credentials.replace(
                            "{credentials}", credentials
                        )
                    if self.provider.credentials.startswith("HTTP"):
                        # handle HTTPBasicAuth('user', 'pass') etc
                        http_auth = http_auth_parse(self.provider.credentials)

                        response = self.send_request(
                            page_query,
                            auth=http_auth_dispatch.get(http_auth[0])(*http_auth[1]),
                            query=self.query_string_to_provider,
                            headers=self._put_configured_headers(),
                        )
                    else:
                        if self.provider.credentials.startswith("bearer="):
                            # populate with bearer token
                            (username, password, verify_certs, ca_certs, bearer) = (
                                self.get_creds(def_verify_certs=True)
                            )
                            headers = {"Authorization": f"Bearer {bearer}"}
                            if ca_certs and os.path.exists(ca_certs):
                                response = self.send_request(
                                    page_query,
                                    headers=self._put_configured_headers(headers),
                                    query=self.query_string_to_provider,
                                    verify=ca_certs,
                                )
                            else:
                                response = self.send_request(
                                    page_query,
                                    headers=self._put_configured_headers(headers),
                                    query=self.query_string_to_provider,
                                    verify=verify_certs,
                                )
                        elif self.provider.credentials.startswith("X-Api-Key="):
                            headers = {
                                "X-Api-Key": f"{self.provider.credentials.split('X-Api-Key=')[1]}"
                            }
                            logger.debug(
                                f"{self}: sending request with auth header X-Api-Key"
                            )
                            response = self.send_request(
                                page_query,
                                headers=self._put_configured_headers(headers),
                                query=self.query_string_to_provider,
                            )
                            # all others
                        else:
                            response = self.send_request(
                                page_query,
                                query=self.query_string_to_provider,
                                headers=self._put_configured_headers(),
                            )
                        # end if
                    # end if
                else:
                    # response = requests.get(page_query)
                    response = self.send_request(
                        page_query,
                        query=self.query_string_to_provider,
                        headers=self._put_configured_headers(),
                    )
            except NewConnectionError as err:
                self.error(
                    f"requests.{self.get_method()} reports {err} from: {self.provider.connector} -> {page_query}",
                    NewConnectionError,
                )
                return
            except ConnectionError as err:
                self.error(
                    f"requests.{self.get_method()} reports {err} from: {self.provider.connector} -> {page_query}"
                )
                return
            except requests.exceptions.InvalidURL as err:
                self.error(
                    f"requests.{self.get_method()} reports {err} from: {self.provider.connector} -> {page_query}"
                )
                return
            if response.status_code != HTTPStatus.OK:
                self.error(
                    f"request.{self.get_method()} returned: {response.status_code} {response.reason} from: {self.provider.name} for: {page_query}"
                )
                return
            # end if

            # normalize the response
            content_type = response.headers["Content-Type"]
            html_tree = None

            # get html response
            if "text/html" in content_type:
                try:
                    html_tree = BeautifulSoup(response.content, "html.parser")
                except Exception as err:
                    self.error(
                        f"BeautifulSoup failed to parse HTML response: {err} from: {self.provider.connector} -> {page_query}"
                    )
                    return

            if not html_tree:
                self.error(
                    f"BeautifulSoup did not return a valid HTML tree from: {self.provider.connector} -> {page_query}"
                )
                return

            if "KEY" not in self.response_mappings:
                raise Exception(
                    "KEY query mapping is required for BeautifulSoupRTDExtractor"
                )
            # get items by class
            items = html_tree.find_all(class_=self.response_mappings.get("KEY"))
            logger.debug(
                f"BeautifulSoupRTDExtractor found {len(items)} items with class: {self.query_mappings.get('KEY')} in: {page_query}"
            )
            logger.debug("items: " + str(items))
            if not items:
                self.warning(
                    f"BeautifulSoupRTDExtractor did not find any items with class: {self.query_mappings.get('KEY')} in: {page_query}"
                )
                return
            
            found = len(items)

            for item in items:
                # create a response dict
                mapped_response = {}

                link_item = item.find("h5").find("a")
                mapped_response["url"] = link_item["href"] if link_item else None
                mapped_response["title"] = link_item.text.strip() if link_item else None
                mapped_response["body"] = item.find("p").text.strip() if item.find("p") else None

                # add the item to the list of responses
                mapped_responses.append(mapped_response)
            
            
        self.found = found
        self.retrieved = len(mapped_responses)
        self.response = mapped_responses

        return
