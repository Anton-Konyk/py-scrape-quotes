from dataclasses import dataclass
from dataclasses import dataclass, fields, astuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag


BASE_URL = "https://quotes.toscrape.com/"


@dataclass
class Quote:
    text: str
    author: str
    tags: list[str]


def parse_single_product(quote: Tag) -> Quote:
    return Quote(
        text=quote.select_one(".text").text,  # text
        author=quote.select_one(".author").text,
        tags=quote.select_one(".keywords")["content"].split(","),
    )


def get_single_page_quotes(pages_soup: Tag) -> [Quote]:
    quote = pages_soup.select(".quote")
    return [parse_single_product(product) for product in quote]


def get_all_page_quotes() -> [Quote]:
    logging.info("Starting parsing first page")
    text = requests.get(BASE_URL).content
    first_page_soup = BeautifulSoup(text, "html.parser")
    all_quotes = get_single_page_quotes(first_page_soup)
    next_page = first_page_soup.select("li.next a")
    while next_page:
        logging.info(f"Starting parsing first page {next_page[0]["href"].strip('/').split('/')[1]}")
        next_page_url = urljoin(BASE_URL, next_page[0]["href"])
        text = requests.get(next_page_url).content
        next_page_soup = BeautifulSoup(text, "html.parser")
        all_quotes.extend(get_single_page_quotes(next_page_soup))
        next_page = next_page_soup.select("li.next a")

    return all_quotes

def main(output_csv_path: str) -> None:
    write_quotes_to_csv(output_csv_path, get_all_page_quotes())


if __name__ == "__main__":
    main("quotes.csv")
