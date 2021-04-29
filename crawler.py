import urllib.request
from pathlib import Path
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd


class Crawler:

    def __init__(self, image_dir, data_dir, page_dir, list_dir):
        self.IMAGE_DIR = Path(image_dir)
        self.DATA_DIR = Path(data_dir)
        self.PAGE_DIR = Path(page_dir)
        self.LIST_DIR = Path(list_dir)
        self.LIST_FILE = self.LIST_DIR.parent.joinpath('list.csv')
        self.DATA_FILE = self.DATA_DIR.parent.joinpath('data.csv')
        output_dirs = [self.IMAGE_DIR, self.DATA_DIR,
                       self.PAGE_DIR, self.LIST_DIR]
        for d in output_dirs:
            d.mkdir(parents=True, exist_ok=True)

    def _list_urls(self, pat, pages):
        pat = urllib.parse.quote(pat)
        urls = [f"https://www.ijq.tv/mingxing/{pat}_{page_name}.html"
                for page_name in range(1, pages + 1)]
        return urls

    def check_pending_lists(self, test=False):

        urls_saved = [i.name.replace('.csv', '.html')
                      for i in self.LIST_DIR.glob("*.csv")]

        urls = []
        # Mainland
        urls += self._list_urls("list_内地", 213)
        # Hong Kong
        urls += self._list_urls("list_香港", 8)
        # Taiwan
        urls += self._list_urls("list_台湾", 9)
        # Korea
        urls += self._list_urls("list_韩国", 20)
        # Japan
        urls += self._list_urls("list_日本", 12)
        # US
        urls += self._list_urls(f"list_美国", 27)

        if test:
            urls = urls[:2]

        urls_pending = [i for i in urls if Path(i).name not in urls_saved]

        return urls_pending, urls_saved

    def crawl_list(self, url, save=True):
        with urllib.request.urlopen(url) as response:
            html = response.read()
        soup = BeautifulSoup(html, 'html.parser')
        page_list = soup.body.find_next(id="list_stars").find_all("li")
        res = []
        for p in page_list:
            a = p.find_next('a')
            page_url = "https://www.ijq.tv"+a.get('href', '')
            avatar_url = "https:" + a.img.get('src', '')
            name = a.img.get("alt")
            res += [{'name': name, 'page_url': page_url}]

        if save:
            filename = Path(url).name.replace('html', 'csv')
            pd.DataFrame(res).to_csv(
                self.LIST_DIR.joinpath(filename), index=False)
        return res

    def combine_list(self, test):
        urls_pending, urls_saved = self.check_pending_lists(test)
        if len(urls_pending) == 0:
            print('='*60)
            print('COMBINE LIST')
            print('='*60)

            rows = []
            for p in tqdm(urls_saved):
                p = self.LIST_DIR.joinpath(p.replace('.html', '.csv'))
                rows += [pd.read_csv(p)]
            data = pd.concat(rows)
            data.to_csv(self.LIST_FILE, index=False)
            print(f'combined list is saved [{self.LIST_FILE.as_posix()}]')
        else:
            print('there is still pending lists')

    def check_pending_pages(self, test):
        pages = pd.read_csv(self.LIST_FILE)
        if test:
            pages = pages[:4]
        urls = pages['page_url'].tolist()
        saved_pages = [i.name for i in self.PAGE_DIR.glob('*.html')]
        urls_pending = [i for i in urls if Path(i).name not in saved_pages]
        return urls_pending, saved_pages

    def crawl_page(self, url, save=True):
        with urllib.request.urlopen(url) as response:
            html = response.read()
        filename = Path(url).name
        if save:
            with open(self.PAGE_DIR.joinpath(filename), 'wb') as f:
                f.write(html)
        return html

    def check_pending_parsed_pages(self, test):
        pages = [i.name for i in self.PAGE_DIR.glob("*.html")]
        if test:
            pages = pages[:4]
        pages_parsed = [i.name.replace('.csv', '.html')
                        for i in self.DATA_DIR.glob("*.csv")]
        pages_pending = [i for i in pages if i not in pages_parsed]
        return pages_pending, pages_parsed

    def parse_page(self, filename, save=True):

        with open(self.PAGE_DIR.joinpath(filename), 'r') as f:
            html = f.read()
        soup = BeautifulSoup(html, 'html.parser')
        info = self._get_info(soup)
        info['avatar'] = self._get_avatar(soup)
        info['bio'] = self._get_bio(soup)
        info['poster'] = self._get_poster(soup)

        if save:
            filename = filename.replace('.html', '.csv')
            pd.DataFrame([info]).to_csv(
                self.DATA_DIR.joinpath(filename), index=False)
        return info

    def combine_parsed_page(self, test):
        pages_pending, pages_parsed = self.check_pending_parsed_pages(test)
        if len(pages_pending) == 0:
            rows = []
            for d in tqdm(pages_parsed):
                d = d.replace('.html', '.csv')
                rows += [pd.read_csv(self.DATA_DIR.joinpath(d))]
            data = pd.concat(rows)
            data.to_csv(self.DATA_FILE, index=False)
            print(
                f"combined parsed page is saved [{self.DATA_FILE.as_posix()}]")
        else:
            print("there's pending parsing page")
            data = None
        return data

    def check_pending_images(self, test):
        data = pd.read_csv('./chinese-celebrity-faces.csv')
        images = data[['AVATAR', 'AVATAR_ID']].rename(columns={
            'AVATAR': 'url',
            'AVATAR_ID': 'filename'
        }).to_dict(orient='records')
        if test:
            images = images[:4]
        images_saved = [i.name for i in Path(self.IMAGE_DIR).glob('*')]
        images_pending = [
            i for i in images if i['filename'] not in images_saved]
        return images_pending, images_saved

    def _get_info(self, soup):
        info = soup.body.find_next(id="v-details-list").find_all('p')
        res = {}
        for i in info:
            text = i.text
            name, value = i.text.split('：', 1)
            res[name.strip()] = value.strip()
        try:
            res['weibo'] = (info[14]).a.get('href', '')
        except:
            res['weibo'] = ''
        return res

    def _get_bio(self, soup):
        try:
            return soup.find(id="v-summary").find("div").text
        except:
            return ''

    def _get_poster(self, soup):
        try:
            res = soup.find(id="v-summary")
            res = res.find("div", {"class": "content",
                           "class": "textindent2em"})
            res = res.find('img').get('src', '')
            return res
        except:
            return ''

    def _get_avatar(self, soup):
        poster = soup.body.find_next(id="v-poster").find_next('img')
        link = poster.get('src', '')
        if len(link) > 0:
            link = "https:" + link
        else:
            link = ""
        return link

    def _save_image(self, url, filename):
        image_path = self.IMAGE_DIR.joinpath(filename)
        if url != "":
            urllib.request.urlretrieve(url, image_path)

    def save_image(self, args):
        return self._save_image(**args)
