import scrapy
import json


class BikesSpider(scrapy.Spider):
    name = "motor"
    allowed_domains = ["https://halfords.com", "www.halfords.com"]
    start_urls = ["https://www.halfords.com/motoring/"]

    def parse(self, response):

        outer_page = response.xpath(
            '//*[@class="b-type7 mb-sm-40"]/div/a/@href'
        ).extract()

        for page in outer_page:
            self.logger.info(f"scraping section : {page}")
            yield scrapy.Request(page, callback=self.parse_cats)

    def parse_cats(self, response):

        cat_page = response.xpath(
            '//*[@class="b-type7__wrapper row bg-white justify-content-start"]/a/@href'
        ).extract()

        for each_page in cat_page:
            self.logger.info(f"starting next category:{each_page}")
            yield scrapy.Request(each_page, callback=self.parse_category)

    def parse_category(self, response):
        self.logger.info("starting each sub page")
        script_json = response.xpath('//*[@class="js-tile-model"]/text()').extract()

        for j in script_json:
            yield json.loads(j)

        next_page = response.xpath(
            '//*[@class="b-search__footer"]/div/a/@href'
        ).extract_first()
        try:
            yield scrapy.Request(next_page, callback=self.parse_cats)
        except TypeError:
            self.logger.info(f"Reached end of page.")
