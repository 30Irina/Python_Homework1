import scrapy
from scrapy.crawler import CrawlerProcess
import re
from scrapy.utils.reactor import install_reactor
import threading
from fake_useragent import UserAgent
class WikiSpider(scrapy.Spider):
    name = 'wiki'
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = ["https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"]
    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")

    '''
    def start_requests(self):
        ua = UserAgent()
        for url in self.start_urls:
            headers = {'User-Agent': ua.random}
            yield scrapy.Request(url=url, headers=headers, callback=self.parse)
    '''

    def parse(self, response):
        rows = response.xpath('//*[@id="mw-content-text"]/div[1]/table/tbody/tr')
        for row in rows:
            cells = row.xpath('./td')
            for cell in cells:
                letter_link = cell.xpath('./a/@href').get()
                if letter_link:
                    yield response.follow(url=letter_link, callback=self.parse_films_page)

    def parse_films_page(self, response):
        film_names = response.xpath('//*[@id="mw-pages"]/div/div/div/ul/li')
        for film_name in film_names:
            name = film_name.xpath('./a/text()').get()
            film_link = film_name.xpath('./a/@href').get()
            yield response.follow(url=film_link, callback=self.parse_film_info, meta={'name': name})
        #next_page = response.xpath('//*[@id="mw-pages"]/a[contains(text(), "следующая страница")]/@href').get()
        next_page = response.xpath('//*[@id="mw-pages"]/a[2]/@href').get()
        if next_page and 'Cледующая страница' in next_page:
            yield response.follow(url=next_page, callback=self.parse_films_page)

    def parse_film_info(self, response):
        def process_table(j):
            rows = response.xpath(f'//*[@id="mw-content-text"]/div[1]/table[{j}]/tbody/tr')
            genre = None
            director = None
            country = None
            year = None
            IMDb_id = None
            for i in range(0, len(rows) + 1):
                header = response.xpath(
                    f'//*[@id="mw-content-text"]/div[1]/table[{j}]/tbody/tr[{i}]/th/text()').get()
                header2 = response.xpath(
                    f'//*[@id="mw-content-text"]/div[1]/table[{j}]/tbody/tr[{i}]/th/a/text()').get()
                values = response.xpath(
                    f'//*[@id="mw-content-text"]/div[1]/table[{j}]/tbody/tr[{i}]/td//text()').getall()

                clean_values = [re.sub(r'\[.*?\]', '', val.strip()) for val in values if val.strip()]
                clean_values = [re.sub(r'\s*,{2}\s*', ',', val) for val in clean_values]
                clean_values = [val.replace(',', '') for val in clean_values]
                clean_values = [val.replace('(', '') for val in clean_values]
                clean_values = [val.replace(')', '') for val in clean_values]
                clean_values = [val.replace('/', '') for val in clean_values]
                clean_values = [re.sub(r'\bи\b', '', val) for val in clean_values]
                #clean_values = [re.sub(r'\b-\b', '', val) for val in clean_values]
                clean_values = [val.strip() for val in clean_values if val.strip()]
                clean_values = [val for val in clean_values if val]

                if (header2 is not None and re.search(r'Жанр.*', header2)) or (
                        header is not None and re.search(r'Жанр.*', header)):
                    genre = ', '.join([val.lower() for val in clean_values])
                elif header == 'Режиссёр' or header == 'Режиссёры':
                    director = ', '.join(clean_values)
                elif header == 'Страна' or header == 'Страны':
                    country = ', '.join(clean_values)
                elif header == 'Год' or header == 'Первый показ':
                    year = ', '.join(clean_values)
                elif header == 'IMDb' or header2 == 'IMDb':
                    IMDb_id = ', '.join(clean_values)
            return genre, director, country, year, IMDb_id

        name = response.meta['name']
        for j in range(1, 3):
            genre, director, country, year, IMDb_id = process_table(j)
            if any((genre, director, country, year, IMDb_id)):
                yield {
                    'Название': name,
                    'Жанр': genre,
                    'Режиссер': director,
                    'Страна': country,
                    'Год': year,
                    'IMDb': IMDb_id
                }
                break

process = CrawlerProcess(settings={
    'FEED_FORMAT': 'csv',
    'FEED_URI': 'films.csv'
})


process.crawl(WikiSpider)
process.start()




