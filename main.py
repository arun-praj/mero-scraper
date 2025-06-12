# @@AUTHOR - Arun Praj
# @@DESC - Scraps merolagani's floorsheet data


import re
import requests
import urllib.parse
import time
import random
import sqlite3

import pandas as pd

from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime


class MeroScraper:

    CURR_REQ_LAST_PG_NO: int = 0
    SCRAPING_DELAY_MAX_TIME = 1  # seconds
    SCRAPING_DELAY_MIN_TIME = 1
    _FLARE_SOLVRR_PROXY_URI: str = (
        "http://0.0.0.0:8191/v1"  # flaresolverr for proxying requests, helps bypass bot detection
    )
    _SCRAPEE_URI: str = "https://merolagani.com/Floorsheet.aspx"
    _request_form_data = {
        "__EVENTTARGET": "",  # EVENTTARGET is only genereated when form sheet input fields are changed , remains empty if pagination is clicked
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": "",  # important to change for every request
        "__EVENTVALIDATION": "",  # important to change for every request
        "__VIEWSTATEGENERATOR": "1F15F17F",
        "ctl00$ASCompany$hdnAutoSuggest": "0",
        "ctl00$ASCompany$txtAutoSuggest": "",
        "ctl00$txtNews": "",
        "ctl00$AutoSuggest1$hdnAutoSuggest": "0",
        "ctl00$AutoSuggest1$txtAutoSuggest": "",
        "ctl00$ContentPlaceHolder1$ASCompanyFilter$hdnAutoSuggest": "0",
        "ctl00$ContentPlaceHolder1$ASCompanyFilter$txtAutoSuggest": "",
        "ctl00$ContentPlaceHolder1$txtBuyerBrokerCodeFilter": "",
        "ctl00$ContentPlaceHolder1$txtSellerBrokerCodeFilter": "",
        "ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter": "",  # empty for current date
        "ctl00$ContentPlaceHolder1$PagerControl1$hdnPCID": "PC1",
        "ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage": "1",  # page number
        "ctl00$ContentPlaceHolder1$PagerControl1$btnPaging": "",  # delete this when the inputs are present and add this when pagination is used
        "ctl00$ContentPlaceHolder1$PagerControl2$hdnPCID": "PC2",
        "ctl00$ContentPlaceHolder1$PagerControl2$hdnCurrentPage": "0",
    }

    def _scrape_hidden_fields(self, html: str):

        VIEW_STATE = "__VIEWSTATE"
        EVENT_VALIDATION = "__EVENTVALIDATION"

        soup = BeautifulSoup(html, "html.parser")

        view_state = soup.find(
            name="input", attrs={"name": VIEW_STATE, "id": VIEW_STATE}
        ).get("value")
        event_validation = soup.find(
            name="input", attrs={"name": EVENT_VALIDATION, "id": EVENT_VALIDATION}
        ).get("value")

        if not view_state and not event_validation:
            raise Exception(f"{VIEW_STATE} and {EVENT_VALIDATION} extraction failed")

        self._request_form_data["__VIEWSTATE"] = view_state
        self._request_form_data["__EVENTVALIDATION"] = event_validation

    def _scrape_last_page_number(self, html) -> int:
        soup = BeautifulSoup(html, "html.parser")

        pagination_text = soup.find(
            name="span",
            attrs={"id": "ctl00_ContentPlaceHolder1_PagerControl1_litRecords"},
        ).get_text(strip=True)
        match = re.search(r"Total pages: (\d+)", pagination_text)

        if match:
            self.CURR_REQ_LAST_PG_NO = int(match.group(1))
        else:
            raise Exception("Error scraping Total page number from html.")

    def _scrape_table_data_to_db(
        self,
        html: str,
        date: str = "",
        symbol: str = "",
        buyer: str = "",
        seller: str = "",
        db_write_mode="",
    ):
        # soup = BeautifulSoup(html, "html.parser")
        # table = soup.find("table")
        # headers = [th.span.get_text(strip=True) for th in table.find_all("th")]
        today = datetime.today()
        dfs = pd.read_html(StringIO(html))[0]
        conn = sqlite3.connect("floorsheet.db")  # creates file if doesn't exist
        str_build = []

        if date:
            str_build.append(date)
        if symbol:
            str_build.append(symbol)

        if buyer:
            str_build.append(buyer)
        if seller:
            str_build.append(seller)

        table_name = (
            "-".join(str_build) if len(str_build) > 0 else today.strftime("%m/%d/%Y")
        )
        safe_table_name = re.sub(r"\W+", "_", table_name)
        dfs.to_sql(
            f"floorsheet_{safe_table_name}", conn, if_exists=db_write_mode, index=False
        )

    def initial_request(self) -> str:
        """
        Initial GET request does not require the hidden fields __VIEWSTATE and __EVENTVALIDATION,
        """

        headers = {"Content-Type": "application/json"}
        data = {
            "cmd": "request.get",
            "url": self._SCRAPEE_URI,
            "maxTimeout": 60000,
        }
        response = requests.post(
            self._FLARE_SOLVRR_PROXY_URI, headers=headers, json=data
        )
        result = response.json()
        self._scrape_hidden_fields(result["solution"]["response"])

    def subsequent_request(
        self,
        date: str = "",
        page: int = 1,
        symbol: str = "",
        buyer: str = "",
        seller: str = "",
        persist=True,
        db_write_mode="append",
    ) -> str:

        self._request_form_data[
            "ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage"
        ] = page

        self._request_form_data["ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter"] = (
            date
        )
        self._request_form_data[
            "ctl00$ContentPlaceHolder1$ASCompanyFilter$txtAutoSuggest"
        ] = symbol

        self._request_form_data[
            "ctl00$ContentPlaceHolder1$txtBuyerBrokerCodeFilter"
        ] = buyer
        self._request_form_data[
            "ctl00$ContentPlaceHolder1$txtSellerBrokerCodeFilter"
        ] = seller

        if date != "":
            self._request_form_data["__EVENTTARGET"] = (
                "ctl00$ContentPlaceHolder1$lbtnSearchFloorsheet"
            )

        post_data = urllib.parse.urlencode(self._request_form_data)

        USER_AGENTS = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0",
        ]
        headers = {
            "Content-Type": "application/json",
            "User-Agent": random.choice(USER_AGENTS),
        }
        data = {
            "cmd": "request.post",
            "url": self._SCRAPEE_URI,
            "maxTimeout": 60000,
            "postData": post_data,
        }
        response = requests.post(
            self._FLARE_SOLVRR_PROXY_URI, headers=headers, json=data
        )
        result = response.json()
        html = result["solution"]["response"]
        self._scrape_hidden_fields(html)

        if persist:
            self._scrape_table_data_to_db(
                html=html,
                date=date,
                buyer=buyer,
                seller=seller,
                symbol=symbol,
                db_write_mode=db_write_mode,
            )
        else:
            self._scrape_last_page_number(html)


if __name__ == "__main__":

    print(
        "\n\nNone of the fields are mandatory. If date is not specified then by default today's date is used.**\n\n"
    )
    date = input("Enter the date to scrape the floorsheet (MM/DD/YYYY) : ")
    symbol = input("Enter the Stock symbol: ")
    buyer = input("Enter the buyer code: ")
    seller = input("Enter the seller code: ")

    print("\n\nScraping Started... \n")

    mero_scraper = MeroScraper()
    mero_scraper.initial_request()
    mero_scraper.subsequent_request(date=date, page=1, persist=False)

    START = 1
    LAST_PAGE = mero_scraper.CURR_REQ_LAST_PG_NO

    if mero_scraper.CURR_REQ_LAST_PG_NO > 0:
        db_write_mode = "replace"
        for i in range(START, LAST_PAGE + 1):
            delay = random.randint(
                mero_scraper.SCRAPING_DELAY_MIN_TIME,
                mero_scraper.SCRAPING_DELAY_MAX_TIME,
            )
            # Long break after every 10 request
            # if i % 10 == 0:
            #     delay = 300
            print(
                f"Scraping floorsheet page no. {i} of {LAST_PAGE} with random {delay}s delay .... "
            )

            # long break after 10 requests

            time.sleep(delay)
            mero_scraper.subsequent_request(
                date=date,
                page=i,
                symbol=symbol,
                seller=seller,
                buyer=buyer,
                db_write_mode=db_write_mode,
            )
            db_write_mode = "append"
            print(f"Successfully saved page no. {i} into database.\n\n")
        print("\n\n** Completed **\n\n")
