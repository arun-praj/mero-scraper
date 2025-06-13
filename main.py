import re
import requests
import time
import sqlite3
import logging

import pandas as pd

from io import StringIO
from bs4 import BeautifulSoup as BS
from datetime import datetime, timedelta

logging.basicConfig(
    filename="floorsheet.log",  # Specify the log file name
    level=logging.INFO,  # Set the logging level to INFO
    format="%(levelname)s - %(message)s",  # Define the log message format
)


class MeroScraper:

    CURR_REQ_LAST_PG_NO: int = 0
    _SCRAPEE_URI: str = "https://merolagani.com/Floorsheet.aspx"
    DB_NAME = "floorsheet_new.db"

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

        soup = BS(html, "html.parser")

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
        soup = BS(html, "html.parser")

        pagination_text = soup.find(
            name="span",
            attrs={"id": "ctl00_ContentPlaceHolder1_PagerControl1_litRecords"},
        ).get_text(strip=True)
        match = re.search(r"Total pages: (\d+)", pagination_text)

        if match:
            self.CURR_REQ_LAST_PG_NO = int(match.group(1))
        else:
            raise Exception("Error scraping Total page number from html.")

    def _verify_date_exists(self, html: str) -> bool:
        soup = BS(html, "html.parser")
        if soup.find(name="div", attrs={"id": "ctl00_ContentPlaceHolder1_divNoData"}):
            self.CURR_REQ_LAST_PG_NO = 0
            return False
        return True

    def _scrape_table_data_to_db(
        self,
        html: str,
        date="",
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

        dfs = pd.read_html(StringIO(html))[0]
        dfs = dfs.rename(columns=col_mapping)
        dfs["date"] = date

        conn = sqlite3.connect(self.DB_NAME)  # creates file if doesn't exist
        dfs.to_sql(f"floorsheet", conn, if_exists="append", index=False)

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
    ) -> str:

        self._request_form_data[
            "ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage"
        ] = page

        self._request_form_data["ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter"] = (
            date
        )

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
            print(f"\nSkipping date {date} as date is unavailable.\n")
            return "DO_NOT_EXIST"

        self._scrape_last_page_number(html)
        self._scrape_hidden_fields(html)

        if persist:
            self._scrape_table_data_to_db(
                html=html,
                date=date,
            )
        return "EXIST"


if __name__ == "__main__":

    start_date_str = (
        input("(Optional) Enter the start date (MM/DD/YYYY) :  ") or "08/20/2014"
    )
    end_date_str = input("(Optional) Enter the end date (MM/DD/YYYY) : ") or str()

    start_date = datetime.strptime(start_date_str, "%m/%d/%Y").date()
    end_date = datetime.today().date()

    if end_date_str:
        end_date = datetime.strptime(end_date_str, "%m/%d/%Y").date()

    current_date = start_date

    mero_scraper = MeroScraper()

    mero_scraper.initial_request()

    START = 1

    logging.info(f"Scraping Started from {start_date} to {end_date}.")

    while current_date < end_date:
        try:
            # This will set the current date's last page number
            exist_message = mero_scraper.subsequent_request(
                date=current_date.strftime("%m/%d/%Y"),
                page=1,
                persist=False,
            )

        except Exception as e:
            logging.error(f"Exception: {str(e)}. Trying again in 120s")
            print(f"Exception :{e}. Trying again in {120}s")
            time.sleep(120)

        else:

            if exist_message == "EXIST":
                last_page = mero_scraper.get_curr_req_last_pg_no()
                current_page = 1
                while current_page <= last_page:
                    print(
                        f"[{current_date}] Scraping page {current_page}/{last_page} ...."
                    )

                    try:
                        mero_scraper.subsequent_request(
                            date=current_date.strftime("%m/%d/%Y"),
                            page=current_page,
                        )
                        print(
                            f"Succesfully saved {current_page}/{last_page} page into database.\n\n"
                        )
                        current_page += 1

                    except Exception as e:
                        logging.error(f"Exception: {str(e)}. Trying again in 120s")
                        print(
                            f"Exception :{e}. Trying again in {mero_scraper.count_down(120)}s"
                        )
                        time.sleep(120)
                logging.info(
                    (current_date.strftime("%m/%d/%Y"))
                    + f": {mero_scraper.get_curr_req_last_pg_no()}/{mero_scraper.get_curr_req_last_pg_no()} : Scraped and saved into database"
                )
            else:
                logging.warning(f"{current_date}: No data available. Skipping")

            current_date += timedelta(days=1)
    logging.info("SCRAPING COMPLETE")
