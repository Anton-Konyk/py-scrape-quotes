from dataclasses import dataclass
from bs4 import BeautifulSoup, Tag


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


def main(output_csv_path: str) -> None:
    pass


if __name__ == "__main__":
    main("quotes.csv")
