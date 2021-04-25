import pandas as pd
from pathlib import Path
from multiprocessing import Pool
from tqdm import tqdm
import click

from crawler import Crawler
crawler = Crawler()


@click.command()
@click.option('--thread', default=-1, help='number of multi-process',  type=int)
def main(thread):
    data = pd.read_csv('chinese-celebrity-faces.csv')
    images = data['IMAGE_PATH'].unique()
    downloaded_images = list(Path('./images').glob("*"))

    print('DOWNLOAD FACE IMAGES')
    print('=' * 60)
    print(f'* total {len(images)} images')
    print(f'* downloaded {len(downloaded_images)} images')
    print('=' * 60)

    pending = data.loc[~data['IMAGE_PATH'].isin(downloaded_images), [
        'IMAGE_URL', 'IMAGE_PATH']]
    pending = pending.rename(
        columns={'IMAGE_URL': "link", "IMAGE_PATH": "image_path"})
    urls = pending.to_dict(orient='records')

    if thread > 1:
        with Pool(thread) as p:
            _ = list(tqdm(p.imap_unordered(
                crawler.download_image, urls), total=len(urls)))
    else:
        for url in tqdm(urls):
            _ = crawler.download_image(url)


if __name__ == '__main__':
    main()
