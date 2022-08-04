import time
import os
import urllib.request
import argparse
import csv
import requests
from bs4 import BeautifulSoup
import concurrent.futures

QB_URL = "https://www.fantasypros.com/nfl/matchups/qb.php"
RB_URL = "https://www.fantasypros.com/nfl/matchups/rb.php"
WR_URL = "https://www.fantasypros.com/nfl/matchups/wr.php"
TE_URL = "https://www.fantasypros.com/nfl/matchups/te.php"
K_URL = "https://www.fantasypros.com/nfl/matchups/k.php"

BASE_URL = "https://www.fantasypros.com"

URLS = [QB_URL, RB_URL, WR_URL, TE_URL, K_URL]
DEFAULT_OUTPUT_FILE = 'players.csv'


def download_image(url, filename):
    try:
        urllib.request.urlretrieve(url, f"images/{filename}")
        return filename
    except Exception as err:
        return None


def get_player_data(url):
    try:
        res = requests.get(url)
        if res.ok:
            soup = BeautifulSoup(res.text, 'html.parser')
            parent_div = soup.find('div', attrs={'class': 'primary-heading-subheading'})
            if parent_div is not None:
                name = parent_div.h1.text
                position = parent_div.h2.text
                spans = soup.find_all('span', attrs={'class': 'bio-detail'})
                team = ""
                for span in spans:
                    if span.text.startswith("College:"):
                        team = span.text.replace("College: ", "")

                ecr_div = soup.find_all('div', attrs={'class': 'clearfix detail'})
                try:
                    erc_span = ecr_div[0].find('span', attrs={'class': 'pull-right'})
                except:
                    erc_span = None
                erc = ""
                if erc_span is not None:
                    erc = erc_span.text.replace('#', "")

                image_tag = soup.find('img', attrs={'class': 'side-nav-player-photo-radius-8'})
                dt = {
                    "name": name.strip(),
                    "team": team.strip(),
                    "position": position.strip(),
                    "ecr": erc.strip()
                }
                image_link = image_tag['src']
                filename = "{team}_{position}_{ecr}_{name}.png".format(**dt)
                file = download_image(image_link, filename)
                dt['image'] = file

                return dt

            else:
                return None
    except Exception as err:
        return None


def parse_website(url):
    res = requests.get(url)
    if res.ok:
        soup = BeautifulSoup(res.text, 'html.parser')
        players = soup.find_all('a', attrs={'class': 'player-name'})
        data = []
        for player in players:
            if player['href'] is not None and player['href'] != "#":
                link = f"{BASE_URL}{player['href']}"
                dt = get_player_data(link)
                if dt is not None:
                    data.append(dt)
        return data
    else:
        return None


def export_csv(output, data_all):
    with open(output, 'w', newline='') as file:
        playerwriter = csv.writer(file)
        playerwriter.writerow([
            'Position', 'ECR', 'Name', 'Team', 'Photo'
        ])
        for data in data_all:
            for dt in data:
                playerwriter.writerow([
                    dt['position'], dt['ecr'], dt['name'], dt['team'], dt['image']
                ])


def main(output):
    print("Data extracting ...")
    os.makedirs('images', exist_ok=True)
    start = time.perf_counter()
    written_data = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(parse_website, url): url for url in URLS}
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
                if data is not None and data not in written_data:
                    written_data.append(data)
            except Exception as err:
                print(err, "main")
    if len(written_data) > 0:
        export_csv(output, written_data)
    print("time : ", time.perf_counter() - start)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', help='Output file', default=DEFAULT_OUTPUT_FILE)
    args = parser.parse_args()
    main(args.o)
