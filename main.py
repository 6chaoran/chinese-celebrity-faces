from pathlib import Path
import click
from multiprocessing import Pool
from tqdm import tqdm
from crawler import Crawler
import pandas as pd

crawler = Crawler(image_dir='./images',
                  data_dir='./data',
                  list_dir='./lists',
                  page_dir='./pages')


def crawl_page(thread, test):
    urls_pending, saved_pages = crawler.check_pending_pages(test)
    print("CRAWLING PAGE")
    print("=" * 60)
    print(f"downloaded {len(saved_pages)} pages")
    print(f"downloading remaining {len(urls_pending)} pages")
    print("=" * 60)

    # crawling
    if thread > 1:
        with Pool(thread) as p:
            _ = list(tqdm(p.imap_unordered(
                crawler.crawl_page, urls_pending), total=len(urls_pending)))
    else:
        for url in tqdm(urls_pending):
            _ = crawler.crawl_page(url)


def parse_page(thread, test):
    urls_pending, saved_pages = crawler.check_pending_parsed_pages(test)
    print("PARSING PAGE")
    print("=" * 60)
    print(f"parsed {len(saved_pages)} pages")
    print(f"parsing remaining {len(urls_pending)} pages")
    print("=" * 60)

    # crawling
    if thread > 1:
        with Pool(thread) as p:
            _ = list(tqdm(p.imap_unordered(
                crawler.parse_page, urls_pending), total=len(urls_pending)))
    else:
        for url in tqdm(urls_pending):
            _ = crawler.parse_page(url)

    crawler.combine_parsed_page(test)


def download_images(thread, test):
    images_pending, images_saved = crawler.check_pending_images(test)
    print("DOWNLOADING IMAGES")
    print("=" * 60)
    print(f"downloaded {len(images_saved)} images")
    print(f"downloading remaining {len(images_pending)} images")
    print("=" * 60)

    if thread > 1:
        with Pool(thread) as p:
            _ = list(tqdm(p.imap_unordered(
                crawler.save_image, images_pending), total=len(images_pending)))
    else:
        for url in tqdm(images_pending):
            _ = crawler.save_image(url)


def crawl_list(thread, test):
    urls_pending, urls_saved = crawler.check_pending_lists(test)

    print("CRAWLING LIST")
    print("=" * 60)
    print(f"downloaded {len(urls_saved)} lists")
    print(f"downloading remaining {len(urls_pending)} lists")
    print("=" * 60)

    # crawling
    if thread > 1:
        with Pool(thread) as p:
            _ = list(tqdm(p.imap_unordered(
                crawler.crawl_list, urls_pending), total=len(urls_pending)))
    else:
        for url in tqdm(urls_pending):
            _ = crawler.crawl_list(url)

    # combine
    crawler.combine_list(test)


@click.command()
@click.option('--target', default='list',  help='which target to crawl')
@click.option('--thread', default=-1, help='number of multi-process',  type=int)
@click.option('--test', is_flag=True, help='test mode')
def main(target, thread, test):

    if target == 'list':
        crawl_list(thread, test)
    elif target == 'page':
        crawl_page(thread, test)
    elif target == 'parse':
        parse_page(thread, test)
    elif target == 'image':
        download_images(thread, test)
    else:
        pass

if __name__ == '__main__':
    main()
