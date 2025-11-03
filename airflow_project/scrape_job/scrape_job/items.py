import scrapy

class VietnamworksItem(scrapy.Item):
    groupJob_id = scrapy.Field()
    groupJob_name = scrapy.Field()

    skills = scrapy.Field()
    benefits = scrapy.Field()
    industriesV3 = scrapy.Field()
    jobFunctionsV3 = scrapy.Field()
    groupJobFunctions = scrapy.Field()
    
    job_id = scrapy.Field()
    title = scrapy.Field()
    createdOn = scrapy.Field()
    approvedOn = scrapy.Field()
    expiredOn = scrapy.Field()
    address = scrapy.Field()
    
    jobDescription = scrapy.Field()
    jobRequirement = scrapy.Field()
    
    salaryMax = scrapy.Field()
    salaryMin = scrapy.Field()
    salaryCurrency = scrapy.Field()

    companyId = scrapy.Field()
    companyName = scrapy.Field()
    location = scrapy.Field()
    job_url = scrapy.Field()