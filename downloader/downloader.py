import os

import requests
from multiprocessing.pool import Pool
from lxml import html as htmlRenderer
import pandas as pd


def extract_pdf_book_url(args):
    name, url = args
    xpath_query = "//div[@class='cta-button-container__item']//a[@title='Download this book in PDF format']"
    url_prefix = "http://link.springer.com"
    response = requests.get(url)
    parsed = htmlRenderer.fromstring(response.text)
    elements_list = parsed.xpath(xpath_query)
    book_url = "{prefix}{sufix}".format(prefix=url_prefix,
                                        sufix=elements_list[0].get('href')) if elements_list else None
    return name, book_url


def download_book(args):
    name, url = args
    print("[{name}] -> downloading...".format(name=name))
    book_file_name = "{name}.pdf".format(name=name.replace("/", "-"))
    book_store_path = os.path.join(books_directory, book_file_name)
    response = requests.get(url)
    with open(book_store_path, 'wb') as f:
        f.write(response.content)
    print("[{name}] -> downloaded in: {path}".format(name=name, path=book_store_path))
    return True


if __name__ == "__main__":

    url_file = "../resources/free_springer_books_ebook_list.csv"
    books_directory = "../books"
    csv = pd.read_csv(url_file)
    names_urls = zip(csv["Book Title"], csv["OpenURL"])
    bases_url = [pair for pair in names_urls]
    print("total of possible books: {total}".format(total=len(bases_url)))
    with Pool(processes=20) as pool:
        books_urls = pool.map(func=extract_pdf_book_url, iterable=iter(bases_url))

    found = [(name, url) for name, url in books_urls if url]
    print("total of found books: {total}".format(total=len(found)))
    print(found)

    with Pool(processes=30) as pool:
        downloaded_books = pool.map(func=download_book, iterable=iter(found))

    if all(downloaded_books):
        print("all books have been downloaded")
    else:
        print("some books have not been downloaded")
