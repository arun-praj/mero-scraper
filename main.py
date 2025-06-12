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
from datetime import datetime, timedelta


class MeroScraper:

    CURR_REQ_LAST_PG_NO: int = 0
    SCRAPING_DELAY_MAX_TIME = 60  # seconds
    SCRAPING_DELAY_MIN_TIME = 10
    _FLARE_SOLVRR_PROXY_URI: str = (
        "http://0.0.0.0:8192/v1"  # flaresolverr for proxying requests, helps bypass bot detection
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

    def get_curr_req_last_pg_no(self):
        return self.CURR_REQ_LAST_PG_NO

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

    def _verify_date_exists(self, html: str) -> bool:
        soup = BeautifulSoup(html, "html.parser")
        if soup.find(name="div", attrs={"id": "ctl00_ContentPlaceHolder1_divNoData"}):
            self.CURR_REQ_LAST_PG_NO = 0
            return False
        return True

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
        date="",
        db_write_mode="",
    ):

        col_mapping = {
            "#": "id",
            "Transact. No.": "contract_id",
            "Symbol": "symbol",
            "Buyer": "buyer_id",
            "Seller": "seller_id",
            "Quantity": "quantity",
            "Rate": "rate",
            "Amount": "amount",
            "date": date,
        }
        today = datetime.today()
        dfs = pd.read_html(StringIO(html))[0]

        dfs = dfs.rename(columns=col_mapping)
        dfs["date"] = date
        conn = sqlite3.connect("floorsheet.db")  # creates file if doesn't exist

        safe_table_name = re.sub(r"\W+", "_", str(today.strftime("%m/%d/%Y")))
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
        response = requests.get(self._SCRAPEE_URI, headers=headers, timeout=60)
        print(response)
        if response.status_code != 200:
            raise Exception()
        self._scrape_hidden_fields(response.text)

    def subsequent_request(
        self,
        date: str = "",
        page: int = 1,
        persist=True,
        db_write_mode="append",
    ) -> str:

        self._request_form_data[
            "ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage"
        ] = page

        self._request_form_data["ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter"] = (
            date
        )

        # if date != "":
        #     self._request_form_data["__EVENTTARGET"] = (
        #         "ctl00$ContentPlaceHolder1$lbtnSearchFloorsheet"
        #     )

        post_data = urllib.parse.urlencode(self._request_form_data)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.5735.199 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        response = requests.post(
            self._SCRAPEE_URI,
            headers=headers,
            data=self._request_form_data,  # Directly passing dict
            timeout=60,
        )

        if response.status_code != 200:
            raise Exception()

        html = response.text

        if not self._verify_date_exists(html=html):
            print(f"Skipping date {date} as date is unavailable.")
            return "DO_NOT_EXIST"

        self._scrape_last_page_number(html)
        self._scrape_hidden_fields(html)

        if persist:
            self._scrape_table_data_to_db(
                html=html,
                date=date,
                db_write_mode=db_write_mode,
            )


if __name__ == "__main__":

    print(
        "\n\n** If the start date and end date are not specified then default start date is 08/20/2014 and end date is today's date **\n\n"
    )
    start_date_str = (
        input("(Optional) Enter the start date (MM/DD/YYYY) :  ") or "08/20/2014"
    )
    end_date_str = input("(Optional) Enter the end date (MM/DD/YYYY) : ") or str()

    start_date = datetime.strptime(start_date_str, "%m/%d/%Y").date()
    end_date = datetime.today().date()

    if end_date_str:
        end_date = datetime.strptime(end_date_str, "%m/%d/%Y").date()

    current_date = start_date

    print(f"\n\nInitializing...\n")

    mero_scraper = MeroScraper()
    mero_scraper.initial_request()
    mero_scraper.subsequent_request(date=start_date_str, page=1, persist=False)

    print(f"\n\nScraping Started from {start_date} to {end_date}... \n\n")

    START = 1
    LAST_PAGE = mero_scraper.get_curr_req_last_pg_no()

    if mero_scraper.CURR_REQ_LAST_PG_NO > 0:
        db_write_mode = "append"

        while current_date < end_date:

            # This request is to get last page number
            exist_message = mero_scraper.subsequent_request(
                date=current_date.strftime("%m/%d/%Y"),
                page=1,
                db_write_mode=db_write_mode,
                persist=False,
            )
            if exist_message == "DO_NOT_EXIST":
                current_date += timedelta(days=1)
                continue

            LAST_PAGE = mero_scraper.get_curr_req_last_pg_no()

            for page_number in range(START, LAST_PAGE + 1):

                print(f"[{current_date}] Scraping page {page_number}/{LAST_PAGE} .... ")
                # time.sleep(delay)

                exist_message = mero_scraper.subsequent_request(
                    date=current_date.strftime("%m/%d/%Y"),
                    page=page_number,
                    db_write_mode=db_write_mode,
                )

                db_write_mode = "append"
                print(f"Successfully saved page no. {page_number} into database.\n\n")

            with open("saved_dates.log", "a") as f:
                f.write(
                    f"({current_date}): {LAST_PAGE}/{LAST_PAGE} : Save successful\n"
                )
            current_date += timedelta(days=1)

        print("\n\n** Completed **\n\n")

# 09/01/2014
# 09/05/2014
