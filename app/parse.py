import csv
import json
import logging
import sys
from dataclasses import dataclass, fields, astuple
from urllib.parse import urljoin

import requests
import redis
from bs4 import BeautifulSoup, Tag


BASE_URL = "https://quotes.toscrape.com/"


@dataclass
class Quote:
    text: str
    author: str
    tags: list[str]


QUOTE_FIELDS = [fild.name for fild in fields(Quote)]


@dataclass
class Author:
    author: str
    bio: str


AUTHOR_FIELDS = [fild.name for fild in fields(Author)]


logging.basicConfig(
    level=logging.DEBUG,
    format="[%(levelname)8s]: %(message)s)",
    handlers=[
        logging.FileHandler("parser.log"),
        logging.StreamHandler(sys.stdout),
    ]
)


redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)


def get_author_bio(author, author_bio_path):
    cached_bio = redis_client.get(author)
    if cached_bio:
        print(f"Using cached data for {author}")
        return json.loads(cached_bio)

    bio = fetch_biography_from_source(author_bio_path)
    redis_client.setex(author, 86400, json.dumps(bio))
    return bio


def fetch_biography_from_source(author_bio_path):
    text = requests.get(author_bio_path).content
    bio_page_soup = BeautifulSoup(text, "html.parser")
    bio = ""

    born_element = bio_page_soup.find('strong', string="Born: ")
    if born_element:
        bio = born_element.get_text()

    date_element = bio_page_soup.select_one(".author-born-date")
    if date_element:
        bio += date_element.text

    born_element = bio_page_soup.select_one(".author-born-location")
    if born_element:
        bio += " " + born_element.text

    description_element = bio_page_soup.find('strong', string="Description:")
    if description_element:
        bio += "\n" + description_element.get_text()

    author_element = bio_page_soup.select_one(".author-description")
    if author_element:
        bio += " " + author_element.text

    return bio


def parse_single_product(quote: Tag) -> Quote:
    tags_element = quote.select_one(".keywords")
    author = quote.select_one(".author").text

    if tags_element and tags_element["content"]:
        tags = tags_element["content"].split(",")
    else:
        tags = []

    return Quote(
        text=quote.select_one(".text").text,  # text
        author=author,
        tags=tags,
    )


def parse_single_author_bio(quote: Tag) -> Author:
    author = quote.select_one(".author").text
    bio_path = quote.select_one('a[href*="/author/"]')['href']

    if bio_path:
        author_bio_path = urljoin(BASE_URL, bio_path)
        bio = get_author_bio(author, author_bio_path)
    else:
        bio = ""

    return Author(
        author=author,
        bio=bio,
    )


def get_single_page_quotes(pages_soup: Tag) -> [Quote]:
    quotes = pages_soup.select(".quote")
    return [parse_single_product(quote) for quote in quotes]


def get_author_page_bio(pages_soup: Tag) -> [Author]:
    quotes = pages_soup.select(".quote")
    return [parse_single_author_bio(quote) for quote in quotes]

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


def write_quotes_to_csv(output_csv_path: str, quotes: [Quote]) -> None:
    with open(output_csv_path, "w", newline="", encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(QUOTE_FIELDS)
        writer.writerows([astuple(quote) for quote in quotes])


def main(output_csv_path: str) -> None:
    write_quotes_to_csv(output_csv_path, get_all_page_quotes())


if __name__ == "__main__":
    main("quotes.csv")
