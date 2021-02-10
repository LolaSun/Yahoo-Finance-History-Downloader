from datetime import datetime
from time import sleep
from bs4 import BeautifulSoup, Tag
from requests import Response
import arrow
import requests
import os
import json



class HistoryDownloader:
    base_url = 'https://finance.yahoo.com'
    option_url_template = base_url + '/quote/{comp}/options?p={comp}&straddle=true'
    date_pattern = '%a, %d %b %Y %H:%M:%S %Z'

    headers = {
        "accept": "text/html,"
                  "application/xhtml+xml,"
                  "application/xml;q=0.9,"
                  "image/webp,"
                  "image/apng,"
                  "*/*;q=0.8,"
                  "application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "ru,en;q=0.9,uk;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    }

    def __init__(self, companies: list, data_dir: str):
        self.companies = companies

        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

        self._session = None
        self._make_session()

    def _make_session(self):
        self._session = requests.Session()
        self._session.headers.update(self.headers)
        self._get(self.base_url)
        print('Session initialized.')

    def _get(self, url: str) -> Response:
        with self._session as s:
            r = s.get(url)
        if r.status_code != 200:
            raise ValueError('ERROR: answer is not 200!')

        return r

    def download(self):
        comp_response_tuples = []
        for comp_name in self.companies:
            r = self._get(self.option_url_template.format(comp=comp_name))
            comp_response_tuples.append((comp_name, r))

        self._save_to_json(comp_response_tuples)


    def _parse_table(self, table: Tag) -> dict:
        table_contents = {"Calls": {"Last Price": [],
                                    "Change": [],
                                    "% Change": [],
                                    "Volume": [],
                                    "Open Interest": []
                                    },
                          "Strike": [],
                          "Puts": {"Last Price": [],
                                   "Change": [],
                                   "% Change": [],
                                   "Volume": [],
                                   "Open Interest": []
                                   }
                          }

        columns = [
            table_contents["Calls"]["Last Price"],
            table_contents["Calls"]["Change"],
            table_contents["Calls"]["% Change"],
            table_contents["Calls"]["Volume"],
            table_contents["Calls"]["Open Interest"],
            table_contents["Strike"],
            table_contents["Puts"]["Last Price"],
            table_contents["Puts"]["Change"],
            table_contents["Puts"]["% Change"],
            table_contents["Puts"]["Volume"],
            table_contents["Puts"]["Open Interest"]
        ]

        for tr in table.find_all('tr')[1:]:
            td_contents = []
            for td in tr.find_all('td'):
                td_contents.append(td.text.strip())

            print(td_contents)

            for col, td in zip(columns, td_contents):
                col.append(td)

        print(table_contents)
        return table_contents

    def _save_to_json(self, comp_response_tuples: list):
        z_date = arrow.get(datetime.strptime(comp_response_tuples[0][1].headers['Date'], self.date_pattern))
        json_name = '{d}_{m}_{y}_{H}_{M}.json'.format(d=z_date.day,
                                                      m=z_date.month,
                                                      y=z_date.year,
                                                      H=z_date.hour,
                                                      M=z_date.minute)

        parse_html_contents = {}
        for comp_name, response in comp_response_tuples:
            company_name = '{comp}'.format(comp=comp_name)
            table = BeautifulSoup(response.text, "html.parser").find("table")
            table_contents = self._parse_table(table)
            parse_html_contents[company_name] = table_contents

        with open(json_name, 'w') as f:
            json.dump(parse_html_contents, f, indent=2)


def main():
    companies = ['SPY', 'QQQ', 'DIA', 'GLD']
    data_dir = 'downloaded_data'

    downloader = None

    while True:
        try:
            if not downloader:
                downloader = HistoryDownloader(companies, data_dir)

            downloader.download()
        except Exception as error:
            print('{}: ERROR: "{}"!'.format(arrow.now(), error))
            downloader = None
            sleep(60)
            continue

        sleep(300)


if __name__ == '__main__':
    main()
