import json, csv
import os, re, html
from datetime import datetime # <<< PHẢI CÓ DÒNG NÀY

# ==================================
# CÁC HÀM HỖ TRỢ 
# ==================================
def convert_to_datekey(iso_date_string):
    """
    (HÀM BỊ THIẾU)
    Chuyển đổi chuỗi ISO 8601 (ví dụ: '2025-10-20T10:18:07+07:00') 
    thành một DateKey (int, ví dụ: 20251020).
    """
    if not iso_date_string:
        return None
    try:
        # Tách phần ngày tháng trước chữ 'T'
        date_part = iso_date_string.split('T')[0]
        # Chuyển đổi thành đối tượng datetime (chỉ cần phần ngày)
        dt = datetime.strptime(date_part, '%Y-%m-%d')
        # Format thành YYYYMMDD và chuyển sang int
        return int(dt.strftime('%Y%m%d'))
    except (ValueError, TypeError):
        return None # Trả về None nếu có lỗi

def get_output_dir(spider):
    """
    (HÀM BỊ THIẾU)
    Hàm này lấy hoặc tạo thư mục output theo ngày
    """
    # 1. Lấy hoặc tạo ngày chạy duy nhất cho lần crawl này
    if not hasattr(spider, 'crawl_date_str'):
        spider.crawl_date_str = datetime.now().strftime('%d-%m-%Y') # e.g., "20-10-2025"
    
    # 2. Tạo đường dẫn thư mục
    output_dir = os.path.join("../data/clean", spider.crawl_date_str)
    
    # 3. Tạo thư mục nếu chưa có
    os.makedirs(output_dir, exist_ok=True)
    
    return output_dir

# ==================================
# PIPELINE LÀM SẠCH (Đã fix lỗi xuống dòng)
# ==================================
class JobCleanPipeline:
    def process_item(self, item, spider):
        if item.get("jobDescription"):
            desc = html.unescape(item["jobDescription"])
            desc = re.sub(r"<.*?>", "", desc); desc = re.sub(r"[\n\r]+", " ", desc)
            item["jobDescription"] = desc.strip()
        if item.get("jobRequirement"):
            req = html.unescape(item["jobRequirement"])
            req = re.sub(r"<.*?>", "", req); req = re.sub(r"[\n\r]+", " ", req)
            item["jobRequirement"] = req.strip()
        return item

# ==================================
# CÁC PIPELINES BẢNG CHIỀU VÀ BẢNG NỐI
# (Đây là phiên bản đầy đủ từ file upload của bạn + các file bị thiếu)
# ==================================

class CompanyPipeline:
    """Ghi file companies.csv (đã bao gồm address) vào thư mục theo ngày"""
    def __init__(self):
        self.companies = {} 
        self.fieldnames = ["companyId", "companyName", "address"]
        self.file_handle = None; self.writer = None
        
    def open_spider(self, spider):
        output_dir = get_output_dir(spider)
        file_path = os.path.join(output_dir, "companies.csv")
        self.file_handle = open(file_path, "w", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(self.file_handle, fieldnames=self.fieldnames)
        self.writer.writeheader()

    def process_item(self, item, spider):
        company_id = item.get("companyId"); company_name = item.get("companyName"); address = item.get("address")
        if company_id is not None:
            try:
                company_id_int = int(company_id)
                if company_id_int not in self.companies:
                    self.companies[company_id_int] = {"companyName": company_name, "address": address}
            except (ValueError, TypeError): pass
        return item

    def close_spider(self, spider):
        for c_id, data in self.companies.items():
            self.writer.writerow({"companyId": c_id, "companyName": data["companyName"], "address": data["address"]})
        if self.file_handle: self.file_handle.close()

class CityPipeline:
    """Tạo cityId SẠCH, DUY NHẤT và file mapping, ghi theo ngày"""
    def __init__(self):
        self.clean_cities_by_name = {}; self.clean_cities_by_id = {}; self.next_city_id = 1
        self.api_id_to_clean_id = {}
        self.job_cities = []; self.seen_job_cities = set()
        self.cities_writer = None; self.cities_file_handle = None
        self.mapping_writer = None; self.mapping_file_handle = None
        self.job_cities_writer = None; self.job_cities_file_handle = None

    def open_spider(self, spider):
        output_dir = get_output_dir(spider)
        cities_path = os.path.join(output_dir, "cities.csv"); self.cities_file_handle = open(cities_path, "w", encoding="utf-8", newline=""); self.cities_writer = csv.DictWriter(self.cities_file_handle, fieldnames=["cityId", "cityName"]); self.cities_writer.writeheader()
        mapping_path = os.path.join(output_dir, "api_city_mapping.csv"); self.mapping_file_handle = open(mapping_path, "w", encoding="utf-8", newline=""); self.mapping_writer = csv.DictWriter(self.mapping_file_handle, fieldnames=["apiCityId", "cityId"]); self.mapping_writer.writeheader()
        job_cities_path = os.path.join(output_dir, "job_cities.csv"); self.job_cities_file_handle = open(job_cities_path, "w", encoding="utf-8", newline=""); self.job_cities_writer = csv.DictWriter(self.job_cities_file_handle, fieldnames=["jobId", "cityId"]); self.job_cities_writer.writeheader()

    def process_item(self, item, spider):
        working_locations = item.get("location", []); job_id = item.get("job_id")
        if working_locations and job_id is not None:
            try:
                job_id_int = int(job_id)
                for loc in working_locations:
                    api_city_id = loc.get("workingLocationId"); city_name = loc.get("cityName")
                    if not api_city_id or not city_name: continue
                    try:
                        api_city_id_int = int(api_city_id); city_name = city_name.strip()
                        if city_name not in self.clean_cities_by_name:
                            clean_id = self.next_city_id; self.clean_cities_by_name[city_name] = clean_id; self.clean_cities_by_id[clean_id] = city_name; self.next_city_id += 1
                        else: clean_id = self.clean_cities_by_name[city_name]
                        if api_city_id_int not in self.api_id_to_clean_id: self.api_id_to_clean_id[api_city_id_int] = clean_id
                        if (job_id_int, clean_id) not in self.seen_job_cities: self.job_cities.append({"jobId": job_id_int, "cityId": clean_id}); self.seen_job_cities.add((job_id_int, clean_id))
                    except (ValueError, TypeError): continue
            except (ValueError, TypeError): pass
        return item

    def close_spider(self, spider):
        sorted_cities = sorted(self.clean_cities_by_id.items()); [self.cities_writer.writerow({"cityId": c_id, "cityName": c_name}) for c_id, c_name in sorted_cities]; [h.close() for h in [self.cities_file_handle] if h]
        [self.mapping_writer.writerow({"apiCityId": api_id, "cityId": clean_id}) for api_id, clean_id in self.api_id_to_clean_id.items()]; [h.close() for h in [self.mapping_file_handle] if h]
        [self.job_cities_writer.writerow(m) for m in self.job_cities]; [h.close() for h in [self.job_cities_file_handle] if h]

class SkillPipeline:
    """Ghi 2 file (skills, job_skills) vào thư mục theo ngày"""
    def __init__(self):
        self.skills = {}; self.job_skills = []; self.seen_job_skills = set()
        self.skills_writer = None; self.skills_file_handle = None
        self.job_skills_writer = None; self.job_skills_file_handle = None

    def open_spider(self, spider):
        output_dir = get_output_dir(spider)
        skills_path = os.path.join(output_dir, "skills.csv"); self.skills_file_handle = open(skills_path, "w", encoding="utf-8", newline=""); self.skills_writer = csv.DictWriter(self.skills_file_handle, fieldnames=["skillId", "skillName"]); self.skills_writer.writeheader()
        job_skills_path = os.path.join(output_dir, "job_skills.csv"); self.job_skills_file_handle = open(job_skills_path, "w", encoding="utf-8", newline=""); self.job_skills_writer = csv.DictWriter(self.job_skills_file_handle, fieldnames=["jobId", "skillId"]); self.job_skills_writer.writeheader()

    def process_item(self, item, spider):
        skills = item.get("skills", []); job_id = item.get("job_id")
        if skills and job_id is not None:
            try:
                job_id_int = int(job_id)
                for skill in skills:
                    skill_id = skill.get("skillId"); skill_name = skill.get("skillName")
                    if skill_id is None or skill_name is None: continue
                    try:
                        skill_id_int = int(skill_id)
                        if skill_id_int not in self.skills: self.skills[skill_id_int] = skill_name
                        if (job_id_int, skill_id_int) not in self.seen_job_skills: self.job_skills.append({"jobId": job_id_int, "skillId": skill_id_int}); self.seen_job_skills.add((job_id_int, skill_id_int))
                    except (ValueError, TypeError): continue
            except (ValueError, TypeError): pass
        return item

    def close_spider(self, spider):
        [self.skills_writer.writerow({"skillId": s_id, "skillName": s_name}) for s_id, s_name in self.skills.items()]; [h.close() for h in [self.skills_file_handle] if h]
        [self.job_skills_writer.writerow(m) for m in self.job_skills]; [h.close() for h in [self.job_skills_file_handle] if h]

class BenefitPipeline:
    """Ghi 2 file (benefits, job_benefits) vào thư mục theo ngày"""
    def __init__(self):
        self.benefits = {}; self.job_benefits = []; self.seen_job_benefits = set()
        self.benefits_writer = None; self.benefits_file_handle = None
        self.job_benefits_writer = None; self.job_benefits_file_handle = None

    def open_spider(self, spider):
        output_dir = get_output_dir(spider)
        benefits_path = os.path.join(output_dir, "benefits.csv"); self.benefits_file_handle = open(benefits_path, "w", encoding="utf-8", newline=""); self.benefits_writer = csv.DictWriter(self.benefits_file_handle, fieldnames=["benefitId", "benefitName", "benefitValue"]); self.benefits_writer.writeheader()
        job_benefits_path = os.path.join(output_dir, "job_benefits.csv"); self.job_benefits_file_handle = open(job_benefits_path, "w", encoding="utf-8", newline=""); self.job_benefits_writer = csv.DictWriter(self.job_benefits_file_handle, fieldnames=["jobId", "benefitId"]); self.job_benefits_writer.writeheader()

    def process_item(self, item, spider):
        benefits = item.get("benefits", []); job_id = item.get("job_id")
        if benefits and job_id is not None:
            try:
                job_id_int = int(job_id)
                for b in benefits:
                    b_id = b.get("benefitId"); b_name = b.get("benefitNameVI") or b.get("benefitName"); b_value = b.get("benefitValue", "")
                    if b_id is None: continue
                    b_value = re.sub(r"[\r\n]+", " ", b_value).strip()
                    try:
                        b_id_int = int(b_id)
                        if b_id_int not in self.benefits: self.benefits[b_id_int] = {"benefitName": b_name, "benefitValue": b_value}
                        if (job_id_int, b_id_int) not in self.seen_job_benefits: self.job_benefits.append({"jobId": job_id_int, "benefitId": b_id_int}); self.seen_job_benefits.add((job_id_int, b_id_int))
                    except (ValueError, TypeError): continue
            except (ValueError, TypeError): pass
        return item

    def close_spider(self, spider):
        [self.benefits_writer.writerow({"benefitId": b_id, "benefitName": b_data["benefitName"], "benefitValue": b_data["benefitValue"]}) for b_id, b_data in self.benefits.items()]; [h.close() for h in [self.benefits_file_handle] if h]
        [self.job_benefits_writer.writerow(m) for m in self.job_benefits]; [h.close() for h in [self.job_benefits_file_handle] if h]

class IndustryPipeline:
    """Ghi 2 file (industries, job_industries) vào thư mục theo ngày"""
    def __init__(self):
        self.industries = {}; self.job_industries = []; self.seen_job_industries = set()
        self.industries_writer = None; self.industries_file_handle = None
        self.job_industries_writer = None; self.job_industries_file_handle = None

    def open_spider(self, spider):
        output_dir = get_output_dir(spider)
        industries_path = os.path.join(output_dir, "industries.csv"); self.industries_file_handle = open(industries_path, "w", encoding="utf-8", newline=""); self.industries_writer = csv.DictWriter(self.industries_file_handle, fieldnames=["industryId", "industryName"]); self.industries_writer.writeheader()
        job_industries_path = os.path.join(output_dir, "job_industries.csv"); self.job_industries_file_handle = open(job_industries_path, "w", encoding="utf-8", newline=""); self.job_industries_writer = csv.DictWriter(self.job_industries_file_handle, fieldnames=["jobId", "industryId"]); self.job_industries_writer.writeheader()

    def process_item(self, item, spider):
        industries = item.get("industriesV3", []); job_id = item.get("job_id")
        if industries and job_id is not None:
            try:
                job_id_int = int(job_id)
                for ind in industries:
                    ind_id = ind.get("industryV3Id"); ind_name = ind.get("industryV3Name")
                    if ind_id is None or ind_name is None: continue
                    try:
                        ind_id_int = int(ind_id)
                        if ind_id_int not in self.industries: self.industries[ind_id_int] = {"industryName": ind_name}
                        if (job_id_int, ind_id_int) not in self.seen_job_industries: self.job_industries.append({"jobId": job_id_int, "industryId": ind_id_int}); self.seen_job_industries.add((job_id_int, ind_id_int))
                    except (ValueError, TypeError): continue
            except (ValueError, TypeError): pass
        return item

    def close_spider(self, spider):
        [self.industries_writer.writerow({"industryId": i_id, "industryName": i_data["industryName"]}) for i_id, i_data in self.industries.items()]; [h.close() for h in [self.industries_file_handle] if h]
        [self.job_industries_writer.writerow(m) for m in self.job_industries]; [h.close() for h in [self.job_industries_file_handle] if h]

class JobFunctionPipeline:
    """Ghi 2 file (job_functions, job_jobFunctions) vào thư mục theo ngày"""
    def __init__(self):
        self.job_functions = {}; self.job_jobfunctions = []; self.seen_job_functions = set()
        self.job_functions_writer = None; self.job_functions_file_handle = None
        self.job_jobfunctions_writer = None; self.job_jobfunctions_file_handle = None

    def open_spider(self, spider):
        output_dir = get_output_dir(spider)
        func_path = os.path.join(output_dir, "job_functions.csv"); self.job_functions_file_handle = open(func_path, "w", encoding="utf-8", newline=""); self.job_functions_writer = csv.DictWriter(self.job_functions_file_handle, fieldnames=["jobFunctionId", "jobFunctionName"]); self.job_functions_writer.writeheader()
        job_func_path = os.path.join(output_dir, "job_jobFunctions.csv"); self.job_jobfunctions_file_handle = open(job_func_path, "w", encoding="utf-8", newline=""); self.job_jobfunctions_writer = csv.DictWriter(self.job_jobfunctions_file_handle, fieldnames=["jobId", "jobFunctionId"]); self.job_jobfunctions_writer.writeheader()

    def process_item(self, item, spider):
        job_func = item.get("jobFunctionsV3"); job_id = item.get("job_id")
        if job_func and job_id is not None:
            try:
                job_id_int = int(job_id)
                func_id = job_func.get("jobFunctionV3Id"); func_name = job_func.get("jobFunctionV3Name")
                if func_id is None or func_name is None: return item
                try:
                    func_id_int = int(func_id)
                    if func_id_int not in self.job_functions: self.job_functions[func_id_int] = {"jobFunctionName": func_name}
                    if (job_id_int, func_id_int) not in self.seen_job_functions: self.job_jobfunctions.append({"jobId": job_id_int, "jobFunctionId": func_id_int}); self.seen_job_functions.add((job_id_int, func_id_int))
                except (ValueError, TypeError): pass
            except (ValueError, TypeError): pass
        return item

    def close_spider(self, spider):
        [self.job_functions_writer.writerow({"jobFunctionId": f_id, "jobFunctionName": f_data["jobFunctionName"]}) for f_id, f_data in self.job_functions.items()]; [h.close() for h in [self.job_functions_file_handle] if h]
        [self.job_jobfunctions_writer.writerow(m) for m in self.job_jobfunctions]; [h.close() for h in [self.job_jobfunctions_file_handle] if h]

class GroupJobFunctionPipeline:
    """Ghi 2 file (group_job_functions, job_group_jobFunctions) vào thư mục theo ngày"""
    def __init__(self):
        self.group_job_functions = {}; self.job_group_jobfunctions = []; self.seen_job_group_functions = set()
        self.group_func_writer = None; self.group_func_file_handle = None
        self.job_group_func_writer = None; self.job_group_func_file_handle = None

    def open_spider(self, spider):
        output_dir = get_output_dir(spider)
        group_func_path = os.path.join(output_dir, "group_job_functions.csv"); self.group_func_file_handle = open(group_func_path, "w", encoding="utf-8", newline=""); self.group_func_writer = csv.DictWriter(self.group_func_file_handle, fieldnames=["groupJobFunctionId", "groupJobFunctionName"]); self.group_func_writer.writeheader()
        job_group_func_path = os.path.join(output_dir, "job_group_jobFunctions.csv"); self.job_group_func_file_handle = open(job_group_func_path, "w", encoding="utf-8", newline=""); self.job_group_func_writer = csv.DictWriter(self.job_group_func_file_handle, fieldnames=["jobId", "groupJobFunctionId"]); self.job_group_func_writer.writeheader()

    def process_item(self, item, spider):
        group_func = item.get("groupJobFunctions"); job_id = item.get("job_id")
        
        if group_func and job_id is not None:
            try:
                job_id_int = int(job_id)
                g_id = group_func.get("groupJobFunctionId"); g_name = group_func.get("groupJobFunctionName")
                if g_id is None or g_name is None: return item
                try:
                    g_id_int = int(g_id)
                    # Ghi bảng chiều (danh sách group)
                    if g_id_int not in self.group_job_functions: 
                        self.group_job_functions[g_id_int] = {"groupJobFunctionName": g_name}
                    # Ghi bảng nối (mapping)
                    if (job_id_int, g_id_int) not in self.seen_job_group_functions: 
                        self.job_group_jobfunctions.append({"jobId": job_id_int, "groupJobFunctionId": g_id_int}); self.seen_job_group_functions.add((job_id_int, g_id_int))
                except (ValueError, TypeError): pass
            except (ValueError, TypeError): pass
        return item

    def close_spider(self, spider):
        [self.group_func_writer.writerow({"groupJobFunctionId": g_id, "groupJobFunctionName": g_data["groupJobFunctionName"]}) for g_id, g_data in self.group_job_functions.items()]; [h.close() for h in [self.group_func_file_handle] if h]
        [self.job_group_func_writer.writerow(m) for m in self.job_group_jobfunctions]; [h.close() for h in [self.job_group_func_file_handle] if h]

# ==================================
# PIPELINE BẢNG SỰ KIỆN THỜI GIAN (MỚI)
# ==================================
class JobTimePipeline:
    """Tạo bảng riêng cho Vòng đời Job (job_timelife.csv)"""
    def __init__(self):
        self.job_times = {}
        self.fieldnames = ["job_id", "createdOnDateKey", "approvedOnDateKey", "expiredOnDateKey"]
        self.file_handle = None
        self.writer = None

    def open_spider(self, spider):
        output_dir = get_output_dir(spider)
        file_path = os.path.join(output_dir, "job_timelife.csv")
        self.file_handle = open(file_path, "w", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(self.file_handle, fieldnames=self.fieldnames)
        self.writer.writeheader()

    def process_item(self, item, spider):
        job_id = item.get("job_id")
        if job_id is not None:
            try:
                job_id_int = int(job_id)
                if job_id_int not in self.job_times:
                    self.job_times[job_id_int] = {
                        "job_id": job_id_int,
                        "createdOnDateKey": convert_to_datekey(item.get("createdOn")),
                        "approvedOnDateKey": convert_to_datekey(item.get("approvedOn")),
                        "expiredOnDateKey": convert_to_datekey(item.get("expiredOn")),
                    }
            except (ValueError, TypeError): pass
        return item 

    def close_spider(self, spider):
        for time_data in self.job_times.values():
            self.writer.writerow(time_data)
        if self.file_handle:
            self.file_handle.close()

# ==================================
# PIPELINE BẢNG FACT (ĐÃ SỬA)
# ==================================
class JobPostingPipeline:
    """(ĐÃ SỬA) Bảng fact Job (đã tách riêng ngày tháng)"""
    def __init__(self):
        self.jobs = {} 
        self.fieldnames = [
            "job_id", "groupJob_id", "title", 
            "jobDescription", "jobRequirement", "salaryMax", "salaryMin",
            "salaryCurrency", "companyId", "job_url"
        ]
        self.file_handle = None
        self.writer = None

    def open_spider(self, spider):
        output_dir = get_output_dir(spider)
        file_path = os.path.join(output_dir, "job_postings.csv")
        self.file_handle = open(file_path, "w", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(self.file_handle, fieldnames=self.fieldnames)
        self.writer.writeheader()

    def process_item(self, item, spider):
        job_id = item.get("job_id")
        if job_id is not None:
            try:
                job_id_int = int(job_id)
                job_data = {key: item.get(key) for key in self.fieldnames}
                job_data['job_id'] = job_id_int 
                self.jobs[job_id_int] = job_data
            except (ValueError, TypeError): pass
        return item 

    def close_spider(self, spider):
        for job_data in self.jobs.values():
            self.writer.writerow(job_data)
        if self.file_handle:
            self.file_handle.close()