import urllib.request
from pathlib import Path
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd


class Crawler:
    def __init__(self):
        self.image_output_dir = Path("./images")
        self.data_dir = Path('./data')
        self.image_output_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def get_soup(self, url):
        with urllib.request.urlopen(url) as response:
            html = response.read()
        soup = BeautifulSoup(html, 'html.parser')
        return soup

    def get_info(self, soup):
        info = soup.body.find_next(id="v-details-list").find_all('p')
        res = {}
        for i in info:
            name, value = i.text.split('：', 1)
            res[name.strip()] = value.strip()
        return res

    def get_poster(self, soup):
        image_output_dir = self.image_output_dir
        poster = soup.body.find_next(id="v-poster").find_next('img')
        link = poster.get('src', '')
        if len(link) > 0:
            link = "https:" + link
            image_path = image_output_dir.joinpath(Path(link).name)
        else:
            link = ""
            image_path = ""
        return link, image_path

    def save_poster(self, link, image_path):
        if link != "":
            urllib.request.urlretrieve(link, image_path)

    def download_image(self, args):
        return self.save_poster(**args)

    def get_star_list(self, url):
        soup = self.get_soup(url)
        page_list = soup.body.find_next(id="list_stars").find_all("li")
        res = []
        for p in page_list:
            a = p.find_next('a')
            page_url = "https://www.ijq.tv"+a.get('href', '')
            avatar_url = "https:" + a.img.get('src', '')
            name = a.img.get("alt")
            res += [{'name': name, 'page_url': page_url, 'avatar_url': avatar_url}]
        return res

    def crawl_page(self, url, save_img=False):
        if len(url) > 0:
            soup = self.get_soup(url)
            info = self.get_info(soup)
            poster, image_local_path = self.get_poster(soup)
            info['image_url'] = poster
            info['image_path'] = image_local_path.as_posix()
            if save_img:
                self.save_poster(poster, image_local_path)
        else:
            info = {}
        return info

    def crawl_list(self, url):
        page_list = self.get_star_list(url)
        res = []
        for p in tqdm(page_list):
            try:
                info = self.crawl_page(p.get('page_url', ''))
            except:
                print('error: ' + p.get('page_url'))
                info = {}
            info.update(p)
            res += [info]

        # save result
        list_name = Path(url).name.replace('html', 'csv')
        res = pd.DataFrame(res)
        res.columns = [i.replace('\xa0', '').replace('\u3000', '')
                       for i in list(res.columns)]
        res.to_csv(self.data_dir.joinpath(list_name), index=False)
