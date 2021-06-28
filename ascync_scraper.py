import asyncio
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
from aiohttp import ClientSession
from bs4 import BeautifulSoup

from scrape_covid19_msia import Scraper

default_url = "https://kpkesihatan.com/{format1}/kenyataan-akhbar-kpk-{format2}-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/"

CSV_DIR = "test_data"

# column names for the DataFrame
column_names = ["Date", "Recovered", "Cumulative Recovered", "Imported Case",
                "Local Case", "Active Case", "New Case",
                "Cumulative Case", "ICU", "Ventilator",
                "Death", "Cumulative Death", "URL"]


class AsyncScraper(Scraper):
    def __init__(self, start_date, end_date):
        super().__init__(start_date, end_date)
        self.verbose = 0
        self.current_response_dict = None

    @staticmethod
    async def fetch(session, current_date, url):
        async with session.get(url) as response:
            assert response.status == 200, f"Error accessing page on {current_date}\n{url}"
            html_body = await response.read()
            soup = BeautifulSoup(html_body, "lxml")
            state_df = pd.read_html(html_body,
                                    match='JUMLAH KESELURUHAN',
                                    header=0)[-1]
            return {"date": current_date,
                    "soup": soup,
                    "state_df": state_df,
                    "url": url}

    async def fetch_with_sem(self, sem, session, current_date, url):
        async with sem:
            return await self.fetch(session, current_date, url)

    async def async_scrape(self):
        print(f"[INFO] Total days: {self.total_days}")
        tasks = []

        sem = asyncio.Semaphore(10)
        async with ClientSession() as session:
            for i in range(self.total_days):
                self.current_url = default_url.format(
                    **self.create_date_dict(self.current_date))
                if self.verbose:
                    print(f"{self.current_date = }")
                    print(f"{self.current_url = }")
                tasks.append(
                    asyncio.create_task(
                        self.fetch_with_sem(
                            sem, session,
                            current_date=self.current_date,
                            url=self.current_url
                        )
                    )
                )
                self.current_date += timedelta(days=1)

            start_time = time.perf_counter()
            pages_content = await asyncio.gather(*tasks)
            # [{"body": "...", "current_date": datetime(2020, 1, 21)}]
            total_time = time.perf_counter() - start_time
            print(
                f"\nTime elapsed for scraping responses {total_time:.4f} seconds\n")
            return pages_content

    async def get_response_dict(self):
        # need to add this to avoid RuntimeError in Windows
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        scrape_coroutine = self.async_scrape()
        try:
            # to check whether running in IPython mode
            get_ipython
        except:
            results = asyncio.run(scrape_coroutine)
        else:
            # running in IPython mode
            results = await scrape_coroutine
        return results

    def get_soup(self):
        return self.current_response_dict['soup']

    async def scrape_all(self, verbose=0):
        self.verbose = verbose
        df = pd.DataFrame(columns=column_names)

        start_time = time.perf_counter()
        # get the page contents using asyncio
        results = await self.get_response_dict()

        print("\n[INFO] Extracting data from the responses ...")
        for result in results:
            self.current_response_dict = result
            self.current_date = result['date']
            self.current_url = result['url']
            print(f"\nCurrent date: {self.current_date.date()}\n")
            try:
                if self.current_date >= self.new_format_date \
                        and not self.new_format_flag:
                    print("[ATTENTION] USING NEW text format "
                          f"starting from {self.current_date.date()}.")
                    self.new_format_flag = True

                if self.new_format_flag:
                    # using new text scraping format method
                    data_dict = self.scrape_data_new(verbose=verbose)
                else:
                    # still in old text format, scrape using old method
                    data_dict = self.scrape_data(verbose=verbose)

                data_dict["Date"] = self.current_date
                data_dict["URL"] = self.current_url
                df = df.append(data_dict, ignore_index=True)

                self.current_date += timedelta(days=1)
                self.current_date_dict = self.create_date_dict(
                    self.current_date)
            except:
                # save a csv file to check
                filename = f"{self.start_date.date()}_{self.current_date.date()}.csv"
                df.to_csv(os.path.join(
                    CSV_DIR, filename), index=False)
                print("[ERROR] Problem with", self.current_url)
                raise Exception(f"Error on {self.current_date.date()}")

        filename = f"{self.start_date.date()}_{self.end_date.date()}.csv"
        df.to_csv(os.path.join(CSV_DIR, filename), index=False)
        print(f"\n[INFO] {filename} created in {CSV_DIR}.")
        print("\n[INFO] CONGRATS! You have scraped until the end date!")
        total_time = time.perf_counter() - start_time
        # 285 seconds
        print(f"Total time elapsed: {total_time:.2f} seconds")


if __name__ == '__main__':
    start_date = datetime(2021, 1, 21)
    end_date = datetime(2021, 4, 20)
    scraper = AsyncScraper(start_date, end_date)
    verbose = 0

    try:
        # check whether running in IPython mode
        get_ipython
    except:
        scraper.scrape_all(verbose=verbose)
    else:
        await scraper.scrape_all(verbose=verbose)
