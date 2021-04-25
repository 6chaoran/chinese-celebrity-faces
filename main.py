from time import sleep
import random
from pathlib import Path
import click
from crawler import Crawler
crawler = Crawler()


@click.command()
@click.option('--start', default=-1, help='start page number',  type=int)
@click.option('--end', default=214, help='end page number', type=int)
def main(start, end):

    saved_list = list(Path('./data').glob("*.csv"))
    saved_list = [i.as_posix() for i in saved_list]
    saved_page_num = [int(i.split('_', 2)[-1].replace('.csv', ''))
                      for i in saved_list]

    if start < 0:
        start = max(saved_page_num) + 1

    for page_name in range(start, end):

        url = f"https://www.ijq.tv/mingxing/list_%E5%86%85%E5%9C%B0_{page_name}.html"
        print(f'crawling page {page_name}')
        _ = crawler.crawl_list(url)
        # rest for a random interval
        rest_interval = random.randint(1, 6)
        print(f"rest {rest_interval}s...")
        sleep(rest_interval)


if __name__ == '__main__':
    main()
