import scrapy
import json


class VietnamworksSpider(scrapy.Spider):
    name = "vietnamworks"
    allowed_domains = ["ms.vietnamworks.com"]
    api_url = "https://ms.vietnamworks.com/job-search/v1.0/search"

    def start_requests(self):
        # đọc file groupJobs.json (danh sách ngành nghề)
        with open("../data/raw/groupJobs.json", encoding="utf-8") as f:
            data = json.load(f)

        groupJobs = data.get("data", [])
        jobs_filters = []

        for groupJob in groupJobs:
            attrs = groupJob.get("attributes", {})
            groupJob_id = attrs.get("groupJobFunctionId")
            if groupJob_id:
                filter_obj = {
                    "field": "jobFunction",
                    "value": f'[{{"parentId":{groupJob_id},"childrenIds":[-1]}}]'
                }
                jobs_filters.append((groupJob_id, filter_obj))

        headers = {
            "Content-Type": "application/json",
            "Origin": "https://www.vietnamworks.com",
            "Referer": "https://www.vietnamworks.com/",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/138.0.0.0 Safari/537.36",
        }

        # chạy lần đầu cho từng ngành nghề
        for job_id, filter_obj in jobs_filters:
            payload = {
                "userId": 0,
                "query": "",
                "filter": [filter_obj],
                "ranges": [],
                "order": [],
                "hitsPerPage": 50,
                "page": 0,   # API bắt đầu từ 0
                "retrieveFields": [
                    "jobTitle", "companyName", "jobUrl", "workingLocations"
                ],
                "summaryVersion": ""
            }

            yield scrapy.Request(
                url=self.api_url,
                method="POST",
                headers=headers,
                body=json.dumps(payload),
                meta={
                    "job_id": job_id,
                    "filter_obj": filter_obj,
                    "page": 0,
                    "hits_per_page": 50
                },
                callback=self.parse_api,
            )

    def parse_api(self, response):
        job_id = response.meta["job_id"]
        page = response.meta["page"]
        filter_obj = response.meta["filter_obj"]
        hits_per_page = response.meta["hits_per_page"]

        try:
            data = json.loads(response.text)
        except Exception as e:
            self.logger.error(f"❌ Lỗi parse JSON: {e}")
            return

        jobs = data.get("data", [])

        self.logger.info(
            f"JobID {job_id} - Page {page} - {len(jobs)} jobs"
        )

        for job in jobs:
            yield {
                "job_id": job_id,
                "title": job.get("jobTitle"),
                "url": job.get("jobUrl"),
                "company": job.get("companyName"),
                "location": job.get("workingLocations"),
            }

        # Nếu số job == hits_per_page thì crawl tiếp trang sau
        if len(jobs) == hits_per_page:
            next_page = page + 1
            new_payload = {
                "userId": 0,
                "query": "",
                "filter": [filter_obj],
                "ranges": [],
                "order": [],
                "hitsPerPage": hits_per_page,
                "page": next_page,
                "retrieveFields": [
                    "jobTitle", "companyName", "jobUrl", "workingLocations"
                ],
                "summaryVersion": ""
            }

            yield scrapy.Request(
                url=self.api_url,
                method="POST",
                headers=response.request.headers,
                body=json.dumps(new_payload),
                meta={
                    "job_id": job_id,
                    "page": next_page,
                    "filter_obj": filter_obj,
                    "hits_per_page": hits_per_page
                },
                callback=self.parse_api,
            )
