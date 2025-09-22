import scrapy

class VietnamworksSpider(scrapy.Spider):
    name = "vietnamworks"
    allowed_domains = ["vietnamworks.com"]
    start_urls = ["https://www.vietnamworks.com/"]

    async def start(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        }
        for url in self.start_urls:
            yield scrapy.Request(url, headers=headers, callback=self.parse)

    def parse(self, response):
        self.log("Page title: " + response.css("title::text").get())
