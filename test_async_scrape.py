import asyncio
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from aiohttp import ClientSession
from bs4 import BeautifulSoup

# translate the months from English to Malay
month_translation = {"January": "januari",
                     "February": "februari",
                     "March": "mac",
                     "April": "april",
                     "May": "mei",
                     "June": "jun",
                     "July": "julai",
                     "August": "ogos",
                     "September": "september",
                     "October": "oktober",
                     "November": "november",
                     "December": "disember"}

default_url = "https://kpkesihatan.com/{format1}/kenyataan-akhbar-kpk-{format2}-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/"

new_format_date = datetime(2021, 1, 20)


def create_datetime(day, month, year):
    data_date = '-'.join([str(day).zfill(2),
                          str(month).zfill(2), str(year)])
    data_datetime = datetime.strptime(data_date, '%d-%m-%Y')
    return data_datetime


def create_date_dict(dt):
    """to be passed to the default_url to create the URL for the date"""
    month_full = month_translation[dt.strftime('%B')]
    date_dict = {'format1': dt.strftime(
        '%Y/%m/%d'), 'format2': f'{dt.day}-{month_full}-{dt.year}'}
    return date_dict


def create_datetime_and_dict(day, month, year):
    data_datetime = create_datetime(day, month, year)
    date_dict = create_date_dict(data_datetime)
    return data_datetime, date_dict


async def fetch(url, session, current_date=None):
    async with session.get(url) as response:
        assert response.status == 200, f"Error accessing page on {current_date}\n{url}"
        html_body = await response.read()
        soup = BeautifulSoup(html_body, "lxml")
        df = pd.read_html(html_body,
                          match='JUMLAH KESELURUHAN',
                          header=0)[-1]
        return {"soup": soup, "date": current_date, "df": df}


async def fetch_with_sem(sem, url, session, current_date=None):
    async with sem:
        return await fetch(url, session, current_date)


async def async_scrape(start_date=datetime(2021, 1, 21), end_date=None, verbose=0):
    assert isinstance(end_date, datetime)
    current_date = start_date
    total_days = (end_date - start_date).days + 1
    print(f"[INFO] Total days: {total_days}")
    tasks = []

    sem = asyncio.Semaphore(10)
    async with ClientSession() as session:
        for i in range(total_days):
            url = default_url.format(**create_date_dict(current_date))
            if verbose:
                print(f"{current_date = }")
                print(f"{url = }")
            tasks.append(
                asyncio.create_task(
                    fetch_with_sem(sem, url, session, current_date))
            )
            current_date += timedelta(days=1)
        start_time = time.perf_counter()
        pages_content = await asyncio.gather(*tasks)
        # [{"body": "...", "current_date": datetime(2020, 1, 21)}]
        total_time = time.perf_counter() - start_time
        print(f"{total_time = :.4f} seconds")
        return pages_content

# need to add this to avoid RuntimeError in Windows
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
start_date = datetime(2021, 1, 21)
end_date = datetime(2021, 4, 30)
scrape_coroutine = async_scrape(start_date=start_date,
                                end_date=end_date,
                                verbose=0)
# results = asyncio.run(scrape_coroutine)
results = await scrape_coroutine
print("\n[INFO] Results for last 5 days:")
for result in results[-5:]:
    print(f"{result['date'] = }")
