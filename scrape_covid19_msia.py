import os
import re
import sys
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from IPython.display import display

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

cases_to_extract_old = ['(pulih|sembuh)', "kumulatif kes (yang telah pulih|sembuh)", "kes baharu",
                        "jumlah kes positif", "Unit Rawatan Rapi",
                        "pernafasan", "kes kematian", "kumulatif kes kematian"]

cases_to_extract_new = ["sembuh", "kumulatif_sembuh", "baharu",
                        "kumulatif_baharu", "import", "tempatan",
                        "aktif", "Unit Rawatan Rapi",
                        "pernafasan", "kematian", "kumulatif_kematian"]

column_names = ["Date", "Recovered", "Cumulative Recovered", "Imported Case",
                "Local Case", "Active Case", "New Case",
                "Cumulative Case", "ICU", "Ventilator",
                "Death", "Cumulative Death", "URL"]

case_name_mapping = {'(pulih|sembuh)': "Recovered",
                     "kumulatif kes (yang telah pulih|sembuh)": "Cumulative Recovered",
                     "kes baharu": "New Case", "jumlah kes positif": "Cumulative Case",
                     "Unit Rawatan Rapi": "ICU", "pernafasan": "Ventilator",
                     "kes kematian": "Death", "kumulatif kes kematian": "Cumulative Death",
                     "jumlah kumulatif kes positif": "Cumulative Case",
                     "Jumlah kes positif": "Cumulative Case",
                     # new text format mapping
                     "sembuh": "Recovered", "baharu": "New Case",
                     "kumulatif_sembuh": "Cumulative Recovered",
                     "kumulatif_baharu": "Cumulative Case", "import": "Imported Case",
                     "tempatan": "Local Case", "aktif": "Active Case",
                     "kematian": "Death", "kumulatif_kematian": "Cumulative Death"}

default_url = "https://kpkesihatan.com/{format1}/kenyataan-akhbar-kpk-{format2}-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/"

special_dates = ['16-Apr-2020', '13-May-2020', '28-May-2020',
                 '18-Jun-2020', '16-Jul-2020', '26-Jul-2020',
                 '10-Dec-2020']
special_dt = [datetime.strptime(i, '%d-%b-%Y')
              for i in special_dates]
special_urls = ["https://kpkesihatan.com/2020/04/16/kenyataan-akhbar-16-april-2020-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/",
                "https://kpkesihatan.com/2020/05/13/kenyataan-akhbar-kpk-13-may-2020-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/",
                "https://kpkesihatan.com/2020/05/28/situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/",
                "https://kpkesihatan.com/2020/06/18/kenyataan-akhbar-kpk-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/",
                "https://kpkesihatan.com/2020/07/16/kenyataan-akhbar-kkm-16-julai-2020-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/",
                "https://kpkesihatan.com/2020/07/26/kenyataan-akhbar-kementerian-kesihatan-malaysia-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/",
                "https://kpkesihatan.com/2020/12/10/kenyataan-akhbar-kpk-9-disember-2020-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia-2/"]


class Scraper:
    def __init__(self, start_date, end_date):
        assert isinstance(start_date, datetime)
        assert isinstance(end_date, datetime)

        self.start_date = start_date
        self.end_date = end_date

        self.start_date_dict = self.create_date_dict(self.start_date)
        self.end_date_dict = self.create_date_dict(self.end_date)
        # inclusive of final date
        self.total_days = (self.end_date - self.start_date).days + 1

        self.current_date, self.current_date_dict = self.start_date, self.start_date_dict

        self.df = pd.DataFrame(columns=column_names)

        self.new_format_flag = False
        self.new_format_date = datetime(2021, 1, 20)

    def get_matched_number(self, txt_found, numbers_found,
                           text_pos='first', number_pos='first',
                           verbose=0):

        # text_pos & number_pos are the locating positions to find the nearest digits
        assert text_pos in ('first', 'end')
        assert number_pos in ('first', 'end')

        distance_list = []

        text_span_idx = 0 if text_pos == 'first' else 1
        number_span_idx = 0 if number_pos == 'first' else 1

        for number in numbers_found:
            distance = abs(txt_found.span()[
                           text_span_idx] - number.span()[number_span_idx])

            distance_list.append(distance)

        min_dist = min(distance_list)
        min_index = distance_list.index(min_dist)
        if self.current_txt == 'jumlah kes positif':
            # skipping once to get the correct value for cumulative
            min_index += 1
        matched_number = int(numbers_found[min_index].group())

        if verbose:
            print(f'Numbers found so far: \
                {[number.group() for number in numbers_found[:len(distance_list)]]}')
            print(f'Distance list: {distance_list}\n')

        return matched_number

    def find_text_and_numbers(self, txt, all_text):
        if self.current_txt == 'Unit Rawatan Rapi':
            # to avoid taking the numbers for the 'pernafasan'
            regex_text = rf"([^.,\n]*(?:Unit Rawatan Rapi))"
        else:
            regex_text = rf"([^.,\n]*{txt}[^.,]*[,.]+)"

        if self.current_txt == 'kes kematian':
            sentence_list = list(re.finditer(regex_text, all_text))

            # to avoid sentence with strings like "ke-1234"
            first_sentence = sentence_list[0].group()
            if re.search(r'ke-\d+', first_sentence):
                # take the next sentence instead
                sentence_idx = 1
            else:
                sentence_idx = 0
            sentenceObj = sentence_list[sentence_idx]
        else:
            # straight away take the first found sentence using `re.search`
            sentenceObj = re.search(regex_text, all_text)

        if not sentenceObj:
            # raise Exception(f"[ERROR] {txt} not found!")
            print(f"[ERROR] {txt} not found! Set to 0 for now.")
            print("Saving to 'txt_error.txt' to check later.\n")
            error_text = f"{txt} - {self.current_date.date()}\n"
            with open('txt_error.txt', 'a') as f:
                f.write(error_text)
            return None, None
        else:
            # get the proper `span` (location) of the text in the sentence to
            #  calculate the correct distance with numbers in the sentence
            sentence = sentenceObj.group()
            txt_found = re.search(txt, sentence)
            numbers_found = list(re.finditer('\d+', sentence))

            if 'tiada' in sentence.lower() and not numbers_found:
                return None, None

            return txt_found, numbers_found

    @staticmethod
    def replace_comma_sep_digits(matchObj):
        comma_sep_digits = matchObj.group()
        new_number = comma_sep_digits.replace(',', '').replace(' ', '')
        return new_number

    def find_number_new(self, soup, txt, number_idx):
        all_case_ul = soup.find_all("ul")[1]
        for li_tag in all_case_ul:
            if txt in li_tag.text:
                sentence = li_tag.text

                if 'tiada' in sentence.lower():
                    # 0 case found
                    return 0

                sentence_comma_removed = re.sub(
                    r"\d+,\s*\d+", self.replace_comma_sep_digits, sentence)
                numbers_found = re.findall(r'\d+', sentence_comma_removed)
                matched_number = int(numbers_found[number_idx])
                return matched_number
        return 'error'

    def scrape_data(self, verbose=0):
        r = requests.get(self.current_url)
        if r.status_code == 404:
            raise Exception("Error 404 accessing page!!")
        soup = BeautifulSoup(r.content, "lxml")
        all_text = soup.get_text()
        # Remove all COVID-19 words to avoid getting number 19 accidentally
        all_text = all_text.replace('COVID-19', '')\
            .replace('covid19', '')
        all_text = re.sub(
            r"\d+,\s*\d+", self.replace_comma_sep_digits, all_text)

        data_dict = {}
        txt_to_skip = []

        for txt in cases_to_extract_old:
            self.current_txt = txt

            if txt_to_skip:
                if txt in txt_to_skip:
                    continue

            try:
                if verbose:
                    print(f"[INFO] Finding {txt} ...")

                # if txt == '(pulih|sembuh)':
                #     if 'Kes sembuh :' in all_text:
                #         print(self.current_url)
                #         print("\n[WARNING] Proceeding to scraping "
                #               "using new text format ...\n")
                #         # not the same text anymore, proceed to new format
                #         return None
                #     else:
                #         txt_found, numbers_found = self.find_text_and_numbers(
                #             txt, all_text)

                if txt in ('kes baharu', 'jumlah kes positif'):
                    txt_to_search = "JUMLAH KESELURUHAN"
                    # the correct index for the number
                    number_idx = 1 if txt == "kes baharu" else 2
                    # find all table rows
                    sentence_tag = [tags for tags in soup.find_all(
                        "tr") if txt_to_search in tags.text][-1]
                    # find the table data with the correct index
                    matched_number_str = sentence_tag.find_all("td")[
                        number_idx].text
                    # replace any comma separating the digits
                    matched_number_str = matched_number_str.replace(',', '')\
                        .replace(' ', '')
                    # replace any number surrounded by parenthesis
                    matched_number_str = re.sub(
                        r'\(.+\)', ' ', matched_number_str)
                else:
                    txt_found, numbers_found = self.find_text_and_numbers(
                        txt, all_text)

                if txt in ('kes baharu', 'jumlah kes positif'):
                    matched_number = int(matched_number_str)
                elif not txt_found and not numbers_found:
                    print("[INFO] 'tiada' and no digit found "
                          f"in the sentence with '{txt}'")

                    matched_number = 0

                    if txt == 'kes kematian':
                        txt_to_skip.append('kumulatif kes kematian')
                        correct_col_name = case_name_mapping['kumulatif kes kematian']
                        data_dict[correct_col_name] = prev_cumu_death
                    elif txt == 'pernafasan':
                        # set to same with ICU number
                        matched_number = data_dict['ICU']

                elif txt_found and not numbers_found:
                    print("[WARNING] 'tiada' is not found but no digit "
                          f"is found in the sentence with '{txt}'\n")
                    matched_number = 0
                else:
                    matched_number = self.get_matched_number(txt_found, numbers_found,
                                                             verbose=verbose)

                # save the cumulative death to use it for the next row
                #  where there is no new death case
                if txt == 'kumulatif kes kematian':
                    prev_cumu_death = matched_number

                if verbose:
                    print(f"Text found: {txt_found}\n")

            except Exception as e:
                print(f"\nError obtaining {txt} !!")
                raise Exception(f"{e.__class__} occurred.")

            correct_col_name = case_name_mapping[txt]
            data_dict[correct_col_name] = matched_number

        for col_name in ("Imported Case", "Local Case", "Active Case",):
            data_dict[col_name] = np.nan

        return data_dict

    def scrape_data_new(self, verbose=0):
        r = requests.get(self.current_url)
        if r.status_code == 404:
            raise Exception("Error 404 accessing page!!")
        soup = BeautifulSoup(r.content, "lxml")

        data_dict = {}

        for txt in cases_to_extract_new:
            if verbose:
                print(f"[INFO] Finding {txt} ...")

            try:
                if 'kumulatif' in txt:
                    number_idx = 1
                    # get the name to search: one of ('sembuh', 'baharu', 'kematian')
                    txt_to_search = txt.split("_")[1]
                    matched_number = self.find_number_new(
                        soup, txt_to_search, number_idx)
                else:
                    number_idx = 0
                    matched_number = self.find_number_new(
                        soup, txt, number_idx)
                    # li_text_list = [li_tag.text for li_tag in all_case_ul if txt in li_tag.text]
            except Exception as e:
                print(f"\nError obtaining {txt} !!")
                raise Exception(f"{e.__class__} occurred.")

            if not matched_number:
                print(f"[INFO] 'tiada' is found, {txt} = 0")
            elif matched_number == 'error':
                raise Exception(f"[ERROR] Cannot find '{txt}'!")

            correct_col_name = case_name_mapping[txt]
            data_dict[correct_col_name] = matched_number

        return data_dict

    @staticmethod
    def create_datetime(day, month, year):
        data_date = '-'.join([str(day).zfill(2),
                             str(month).zfill(2), str(year)])
        data_datetime = datetime.strptime(data_date, '%d-%m-%Y')
        return data_datetime

    def create_date_dict(self, dt):
        month_full = month_translation[dt.strftime('%B')]
        date_dict = {'format1': dt.strftime(
            '%Y/%m/%d'), 'format2': f'{dt.day}-{month_full}-{dt.year}'}
        return date_dict

    @staticmethod
    def create_datetime_and_dict(self, day, month, year):
        data_datetime = self.create_datetime(day, month, year)
        date_dict = self.create_date_dict(data_datetime)
        return data_datetime, date_dict

    def setup_current_url(self, day_number):
        print(f"[INFO] Scraping data for {self.current_date.date()} "
              f"({day_number + 1}/{self.total_days}) ...")
        # print(self.current_url)
        if self.current_date in special_dt:
            self.current_url = special_urls[special_dt.index(
                self.current_date)]
        else:
            self.current_url = default_url.format(**self.current_date_dict)

    def scrape(self, verbose=0):
        start_time = time.time()
        for day_number in range(self.total_days):
            self.setup_current_url(day_number)

            try:
                # This were used to detect which date the new text format started
                # if not self.new_format_flag:
                #     # still in old text format, scrape using old method
                #     data_dict = self.scrape_data(verbose=0)

                # if not data_dict:
                #     print("[ATTENTION] USING NEW text format "
                #           f"on {self.current_date.date()}.")
                #     sys.exit("Stopping to modify the new format for now")
                #     # using new text format method
                #     self.new_format_flag = True
                #     data_dict = self.scrape_data_new(verbose=verbose)

                if self.current_date == self.new_format_date:
                    print("[ATTENTION] USING NEW text format "
                          f"starting from {self.current_date.date()}.")
                    self.new_format_flag = True

                if self.new_format_flag:
                    # using new text scraping format method
                    data_dict = self.scrape_data_new(verbose=0)
                else:
                    # still in old text format, scrape using old method
                    data_dict = self.scrape_data(verbose=verbose)

                # print(data_dict)

                # df.loc[current_date] = data_dict
                data_dict["Date"] = self.current_date
                data_dict["URL"] = self.current_url
                self.df = self.df.append(data_dict, ignore_index=True)

                self.current_date += timedelta(days=1)
                self.current_date_dict = self.create_date_dict(
                    self.current_date)
            except:
                # save a csv file to check
                filename = f"{self.start_date.date()}_{self.current_date.date()}.csv"
                self.df.to_csv(os.path.join(
                    "csv_files", filename), index=False)
                print("[ERROR] Problem with", self.current_url)
                raise Exception(f"Error on {self.current_date.date()}")

        filename = f"{self.start_date.date()}_{self.end_date.date()}.csv"
        self.df.to_csv(os.path.join("csv_files", filename), index=False)
        print("\n[INFO] CONGRATS! You have scraped until the end date!")
        total_time = time.time() - start_time
        print(f"Total time elapsed: {total_time:.2f} seconds")

    def tables_to_csv(self):
        filename = (f"state_new_{self.start_date.date()}"
                    f"_{self.current_date.date()}.csv")
        self.df_all_new.to_csv(os.path.join("csv_files", filename))

        filename = (f"state_cumu_{self.start_date.date()}"
                    f"_{self.current_date.date()}.csv")
        self.df_all_cumu.to_csv(os.path.join("csv_files", filename))

    def scrape_table(self):
        start_time = time.time()
        self.df_all_new = pd.DataFrame()
        self.df_all_cumu = pd.DataFrame()

        # def replace_unk_name(matchObj):
        #     # a function used for `re.sub()` to rename column names
        #     return ' '.join(['WP', matchObj.group(3)]).strip()

        for day_number in range(self.total_days):
            self.setup_current_url(day_number)

            r = requests.get(self.current_url)
            if r.status_code == 404:
                raise Exception("Error 404 accessing page!!")

            try:
                # extract the last table containing JUMLAH KESELURUHAN to be exact
                df = pd.read_html(r.content,
                                  match='JUMLAH KESELURUHAN',
                                  header=0)[-1]

                # Setup the df into the proper format with one row for each date
                df = df.set_index('NEGERI')
                df = df.T
                df['Date'] = self.current_date
                df["URL"] = self.current_url
                df.set_index('Date', inplace=True)

                # to fix weird column names containing "\xa0"
                #  or extra spaces in between "WP" and state name
                # new_col_names = []
                # for col_name in df.columns:
                #     corrected_name = re.sub(r'(WP|W.P.)(\xa0|[\s]+)(\w+)',
                #                             replace_unk_name,
                #                             col_name)
                #     new_col_names.append(corrected_name)
                # df.columns = new_col_names

                # simpler method
                df.columns = df.columns.str.replace(
                    '\xa0', ' ').str.replace('.', '', regex=False)

                # save the order of the column names to make sure they align
                if day_number == 0:
                    col_name_order = df.columns.values

                df = df[col_name_order]

                df_new_case = df.iloc[[0], :]
                df_cumul_case = df.iloc[[1], :]

                self.df_all_new = self.df_all_new.append(df_new_case)
                self.df_all_cumu = self.df_all_cumu.append(df_cumul_case)

                self.current_date += timedelta(days=1)
                self.current_date_dict = self.create_date_dict(
                    self.current_date)

            except:
                # save a csv file to check
                self.tables_to_csv()

                print("[ERROR] Problem with", self.current_url)
                raise Exception(f"Error on {self.current_date.date()}")

        self.tables_to_csv()
        print("\n[INFO] CONGRATS! You have scraped until the end date!")
        total_time = time.time() - start_time
        print(f"Total time elapsed: {total_time:.2f} seconds")

    def scrape_table_2(self):
        """
        Second method to scrape table faster
        and process them in the end, 
        but cannot add URL column easily.
        """

        start_time = time.time()

        def finalize_df():
            # Replace wrong names
            self.state_df.State = self.state_df.State.str.replace(
                '\xa0', ' ').str.replace('.', '', regex=False)

            new_df = self.state_df.pivot_table(index='Date',
                                               columns='State',
                                               values='New Case',
                                               aggfunc='max')

            cumu_df = self.state_df.pivot_table(index='Date',
                                                columns='State',
                                                values='Cumulative Case',
                                                aggfunc='max')

            filename = (f"2_state_new_{self.start_date.date()}"
                        f"_{self.current_date.date()}.csv")
            new_df.to_csv(os.path.join("csv_files", filename))

            filename = (f"2_state_cumu_{self.start_date.date()}"
                        f"_{self.current_date.date()}.csv")
            cumu_df.to_csv(os.path.join("csv_files", filename))

        state_column_names = ['State', 'New Case', 'Cumulative Case']
        self.state_df = pd.DataFrame(columns=state_column_names)

        for day_number in range(self.total_days):
            self.setup_current_url(day_number)

            r = requests.get(self.current_url)
            if r.status_code == 404:
                raise Exception("Error 404 accessing page!!")

            try:
                # extract the last table containing JUMLAH KESELURUHAN to be exact
                df = pd.read_html(r.content,
                                  match='JUMLAH KESELURUHAN',
                                  header=0)[-1]
                df.columns = state_column_names
                df['Date'] = self.current_date

                self.state_df = self.state_df.append(df)

                self.current_date += timedelta(days=1)
                self.current_date_dict = self.create_date_dict(
                    self.current_date)
            except:
                # save a csv file to check
                # self.tables_to_csv()
                print("[ERROR] Problem with", self.current_url)
                raise Exception(f"Error on {self.current_date.date()}")

        # handle and save the df
        finalize_df()
        print("\n[INFO] CONGRATS! You have scraped until the end date!")
        total_time = time.time() - start_time
        # 270 seconds
        print(f"Total time elapsed: {total_time:.2f} seconds")

    def test_scrape_first_day(self, new_format=0, verbose=0):
        self.current_url = default_url.format(**self.start_date_dict)
        r = requests.get(self.current_url)
        if r.status_code == 404:
            raise Exception("Error 404 accessing page!!")

        if not new_format:
            data_dict = self.scrape_data(verbose=verbose)
        else:
            data_dict = self.scrape_data_new(verbose=verbose)
        print(self.current_url)
        display(data_dict)
        return data_dict

    def continue_scraping(self, verbose=0):
        self.start_date = self.current_date
        self.start_date_dict = self.current_date
        self.total_days = (self.end_date - self.current_date).days + 1
        self.scrape(verbose=verbose)


# FIRST DATE TO SCRAPE
first_date = Scraper.create_datetime(day=27, month=3, year=2020)

# NEW FORMAT ON 2021-01-20
# start_date = Scraper.create_datetime(day=20, month=1, year=2021)

# FINAL DATE TO SCRAPE SO FAR
final_date = Scraper.create_datetime(day=15, month=4, year=2021)

# testing misc dates
# start_date = Scraper.create_datetime(day=31, month=3, year=2021)
end_date = Scraper.create_datetime(day=4, month=4, year=2020)

# scraper = Scraper(first_date, end_date)
# scraper = Scraper(start_date, end_date)

# scrape all days
scraper = Scraper(first_date, final_date)

verbose = 0
# scraper.scrape(verbose=verbose)
# scraper.scrape_table()
scraper.scrape_table_2()

# data_dict = scraper.test_scrape_first_day(verbose=verbose)
# data_dict = scraper.test_scrape_first_day(new_format=1, verbose=verbose)

# scraper.continue_scraping(verbose=verbose)

test_scrape = 0
if test_scrape:
    # current_url = "https://kpkesihatan.com/2021/04/17/kenyataan-akhbar-kpk-17-april-2021-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/"
    current_url = "https://kpkesihatan.com/2020/04/01/kenyataan-akhbar-kpk-1-april-2020-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/"
    # current_url = "https://kpkesihatan.com/2020/03/27/kenyataan-akhbar-kpk-27-mac-2020-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/"
    r = requests.get(current_url)
    if r.status_code == 404:
        raise Exception("Error 404 accessing page!!")
    soup = BeautifulSoup(r.content, "lxml")

    all_text = soup.get_text()
    all_text = all_text.replace('COVID-19', '')\
        .replace('covid19', '')

    def replace_comma_sep_digits(matchObj):
        comma_sep_digits = matchObj.group()
        new_number = comma_sep_digits.replace(',', '').replace(' ', '')
        return new_number

    test_text = 'Sehingga kini, terdapat 12,913\xa0kes\xa0positif  yang sedang dirawat di Unit Rawatan Rapi (ICU),\xa0di mana 30 kes memerlukan bantuan pernafasan.'

    test_text = re.sub(
        r"\d+,\s*\d+", repl=replace_comma_sep_digits, string=test_text)

    # txt = 'Unit Rawatan Rapi'
    # # sentence = re.search(rf"([^.\n]*{txt}[^.]*[.]+)", test_text).group()
    # sentence = re.search(rf"([^.,\n]*{txt}[^.,]*[.,]+)", test_text).group()
    # txt_found = re.search(txt, sentence)
    # numbers_found = list(re.finditer(r"\d+,*\d+", sentence))

    txt_to_search = "JUMLAH KESELURUHAN"
    # txt = "kes baharu"
    test_txt = "jumlah kes positif"
    # the correct index for the number
    number_idx = 1 if test_txt == "kes baharu" else 2
    sentence_tag = [tags for tags in soup.find_all(
        "tr") if txt_to_search in tags.text][-1]
    matched_number_str = sentence_tag.find_all("td")[number_idx].text
    matched_number_str = matched_number_str.replace(',', '').replace(' ', '')
    matched_number_str = re.sub(r'\(.+\)', ' ', matched_number_str)
    matched_number = int(matched_number_str)

test_scrape_new = 0
if test_scrape_new:
    # current_url = "https://kpkesihatan.com/2021/01/20/kenyataan-akhbar-kpk-20-januari-2021-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/"
    current_url = "https://kpkesihatan.com/2021/01/20/kenyataan-akhbar-kpk-20-januari-2021-situasi-semasa-jangkitan-penyakit-coronavirus-2019-covid-19-di-malaysia/"
    r = requests.get(current_url)
    if r.status_code == 404:
        raise Exception("Error 404 accessing page!!")
    soup = BeautifulSoup(r.content, "lxml")

    def replace_comma_sep_digits(matchObj):
        comma_sep_digits = matchObj.group()
        new_number = comma_sep_digits.replace(',', '').replace(' ', '')
        return new_number

    def find_number_new(soup, txt, number_idx):
        all_case_ul = soup.find_all("ul")[1]
        for li_tag in all_case_ul:
            if txt in li_tag.text:
                sentence = li_tag.text
                sentence_comma_removed = re.sub(
                    r"\d+,\s*\d+", replace_comma_sep_digits, sentence)
                numbers_found = re.findall(r'\d+', sentence_comma_removed)
                matched_number = int(numbers_found[number_idx])
                return matched_number
        return None

    data_dict = {}

    for txt in cases_to_extract_new:
        if verbose:
            print(f"[INFO] Finding {txt} ...")

        try:
            if 'kumulatif' in txt:
                number_idx = 1
                # get the name to search: one of ('sembuh', 'baharu', 'kematian')
                txt_to_search = txt.split("_")[1]
                matched_number = find_number_new(
                    soup, txt_to_search, number_idx)
            else:
                number_idx = 0
                matched_number = find_number_new(soup, txt, number_idx)
                # li_text_list = [li_tag.text for li_tag in all_case_ul if txt in li_tag.text]
        except Exception as e:
            print(f"\nError obtaining {txt} !!")
            raise Exception(f"{e.__class__} occurred.")

        if not matched_number:
            raise Exception(f"[ERROR] No number found for {txt}")

        correct_col_name = case_name_mapping[txt]
        data_dict[correct_col_name] = matched_number

    print(current_url)
    display(data_dict)
