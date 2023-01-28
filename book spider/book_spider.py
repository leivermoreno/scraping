import asyncio
import re
from itertools import chain
from typing import Dict, List
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup
from httpx import AsyncClient, AsyncHTTPTransport, Timeout
from utils import execute_batch_with_interval, execute_with_interval, make_request

URL = "http://books.toscrape.com/catalogue/page-{}.html"
SLEEP_TIME = 2
BATCH_SIZE = 10


def scrape_book_details(html: str) -> Dict:
    """Scrape book data from the html.

    Args:
        html (str): html to be scraped

    Returns:
        Dict: book data
    """
    book = {}
    soup = BeautifulSoup(html, "lxml")
    book["genre"] = str(soup.select_one(".breadcrumb > li:nth-child(3) a").string)
    book["title"] = str(soup.find("h1").string)
    price = str(soup.find(class_="price_color").string)
    book["price"] = re.search(r"\d+\.\d{2}", price).group()
    stock = str(soup.find(class_="instock").text)
    book["stock"] = re.search(r"\d+", stock).group()
    book["upc"] = str(soup.select_one("table  tr:nth-child(1) > td").string)

    return book


async def get_book_data(client: AsyncClient, url: str) -> Dict:
    """Make the request to the book page and scrape the data, with a random
    delay before sending the request.

    Args:
        client (httpx.AsyncClient): client to send the request
        url (str): url to request

    Returns:
        Dict: book details
    """
    response = await make_request(client, url, delay=True, seconds=SLEEP_TIME)
    data = scrape_book_details(response)

    return data


def scrape_book_links(html: str) -> List:
    """Scrape book links from the html.

    Args:
        html (str): html to be scraped

    Returns:
        List: book links
    """
    soup = BeautifulSoup(html, "lxml")
    books = soup.find_all(class_="product_pod")
    links = []

    for book in books:
        link = book.find("a")["href"]
        link = urljoin(URL, str(link))
        links.append(link)

    return links


async def get_books(client: AsyncClient, url: str) -> List[Dict]:
    """Get book data for all book listed in the page at url

    Args:
        client (httpx.AsyncClient): client to send the request
        url (str): url to request

    Returns:
        List[Dict]: a dict per book data
    """
    response = await make_request(client, url)
    links = scrape_book_links(response)
    results = await execute_batch_with_interval(
        (get_book_data(client, link) for link in links),
        seconds=SLEEP_TIME,
        batch_size=BATCH_SIZE,
    )

    return results


async def main() -> None:
    async with AsyncClient(
        timeout=Timeout(10), transport=AsyncHTTPTransport(retries=1)
    ) as client:

        # send request to pages 1 to 50 and returns book data for listed
        # books on each page
        results = await execute_with_interval(
            (get_books(client, URL.format(i)) for i in range(1, 51)), seconds=SLEEP_TIME
        )

    pd.DataFrame(chain.from_iterable(results)).to_csv("books.csv")


asyncio.run(main())
