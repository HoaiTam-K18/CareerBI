import scrapy
import json
from ..items import VietnamworksItem


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
            groupJob_name = attrs.get("groupJobFunctionName")
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
        for groupJob_id, filter_obj in jobs_filters:
            payload = {
                "userId": 0,
                "query": "",
                "filter": [filter_obj],
                "ranges": [],
                "order": [],
                "hitsPerPage": 50,
                "page": 0,
                "retrieveFields": [
                    "jobId", "jobTitle", "companyName", "jobUrl", "workingLocations",
                    "address", "createdOn", "approvedOn", "expiredOn",
                    "jobDescription", "jobRequirement",
                    "salaryMax", "salaryMin", "salaryCurrency",
                    "companyId",
                    "skills", "benefits", "industriesV3", "jobFunctionsV3", "groupJobFunctionsV3"
                ],
                "summaryVersion": ""
            }

            yield scrapy.Request(
                url=self.api_url,
                method="POST",
                headers=headers,
                body=json.dumps(payload),
                meta={
                    "groupJob_id": groupJob_id,
                    "filter_obj": filter_obj,
                    "page": 0,
                    "hits_per_page": 50
                },
                callback=self.parse_api,
            )

    def parse_api(self, response):
        groupJob_id = response.meta["groupJob_id"]
        page = response.meta["page"]
        filter_obj = response.meta["filter_obj"]
        hits_per_page = response.meta["hits_per_page"]

        try:
            data = json.loads(response.text)
        except Exception as e:
            self.logger.error(f"Lỗi parse JSON: {e}")
            return

        jobs = data.get("data", [])

        self.logger.info(
            f"groupJob_id: {groupJob_id} - Page {page} - {len(jobs)} jobs"
        )

        for job in jobs:

            item = VietnamworksItem()

            item["skills"] = job.get("skills", [])
            item["benefits"] = job.get("benefits", [])
            item["industriesV3"] = job.get("industriesV3", [])
            item["jobFunctionsV3"] = job.get("jobFunctionsV3")


            item["groupJob_id"] = groupJob_id
            item["job_id"] = job.get("jobId")
            item["title"] = job.get("jobTitle")
            item["createdOn"] = job.get("createdOn")
            item["approvedOn"] = job.get("approvedOn")
            item["expiredOn"] = job.get("expiredOn")
            item["address"] = job.get("address")
            item["jobDescription"] = job.get("jobDescription")
            item["jobRequirement"] = job.get("jobRequirement")
            item["salaryMax"] = job.get("salaryMax")
            item["salaryMin"] = job.get("salaryMin")
            item["salaryCurrency"] = job.get("salaryCurrency")
            item["companyId"] = job.get("companyId")
            item["companyName"] = job.get("companyName")
            item["location"] = job.get("workingLocations")
            item["job_url"] = job.get("jobUrl")

            yield item

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
                    "groupJob_id": groupJob_id,
                    "page": next_page,
                    "filter_obj": filter_obj,
                    "hits_per_page": hits_per_page
                },
                callback=self.parse_api,
            )
