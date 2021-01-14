# Core Library modules
from typing import Set
from urllib.request import urljoin, urlparse

# Third party modules
import requests
import sqlalchemy.exc
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# First party modules
from data_finder import data
from data_finder.data import EMail, File, Page, RasterImage


def main(initial_url=None):
    engine = create_engine("sqlite:///foo.db")
    data.initialize(engine)
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    if initial_url is not None:
        page = Page(url=initial_url)
        session.add(page)
        print(page.url)
        try:
            session.commit()
        except sqlalchemy.exc.IntegrityError:
            session.rollback()

    page = data.get_next_page(session)
    while page:
        parse_page = True
        print(page.url)
        try:
            response = requests.get(page.url)
        except (
            requests.exceptions.SSLError,
            requests.exceptions.TooManyRedirects,
            requests.exceptions.ConnectionError,
        ):
            page.was_parsed()
            session.commit()
            page = data.get_next_page(session)
            continue
        page.size = len(response.content)
        page.was_parsed()
        if "content-type" not in response.headers:
            parse_page = False
            print(f"No content type found for {page.url}!")
            print(response.headers)
        else:
            page.content_type = response.headers["content-type"]
        if page.content_type is None or "text" not in page.content_type:
            parse_page = False
            file_obj = data.create_file(page, response.content)
            session.add(file_obj)
            session.commit()
            if "image" in file_obj.mime_type and "svg" not in file_obj.mime_type:
                image_obj = data.create_image(file_obj, response.content)
                session.add(image_obj)
                session.commit()
            elif "pdf" in file_obj.mime_type:
                pdf = data.create_pdf(file_obj, response.content)
                session.add(pdf)
                session.commit()
        if response.status_code == 200 and parse_page:
            urls = get_all_urls(page.url, response.content)
        else:
            parse_page = False
        print(f"{page.last_checked}: {page.url} was parsed")
        session.commit()
        session.flush()
        response.headers
        if parse_page:
            for url in urls:
                if not data.is_in_db(session, url):
                    if url.startswith("mailto"):
                        page = EMail(address=url)
                    elif url.startswith("http"):
                        page = Page(url=url)
                    else:
                        with open("unknown-shema.txt", "w+") as fp:
                            fp.write(url)
                        continue
                    session.add(page)
                    session.commit()
        page = data.get_next_page(session)
    session.close()


def is_valid(url):
    """Checks whether `url` is a valid URL."""
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


def get_all_urls(url: str, content: str) -> Set[str]:
    urls = set()
    domain_name = urlparse(url).netloc
    soup = BeautifulSoup(content, "html.parser")
    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            continue
        href = urljoin(url, href)

        # Remove GET parameters
        parsed_href = urlparse(href)
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path

        if not is_valid(href):
            continue
        urls.add(href)
    return urls
