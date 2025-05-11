import urllib.request
import urllib.parse
import urllib.error
from lxml import etree
import pandas as pd
import time
import random
import chardet
import json
import jieba
import requests
import re
import os
import csv
from typing import Dict, List, Optional
from volcenginesdkarkruntime import Ark


class Config:
    """配置类"""
    GPT_API_KEY = "your_deepseek_api_key"
    BOCHAAI_API_KEY = "your_bochaai_api_key"
    
    INPUT_EXCEL = "./results/内蒙古领导爬取.csv"
    OUTPUT_EXCEL = "./results/内蒙古领导百度百科爬取.csv"
    FAILED_LOG = "./results/baike_failed_records.txt"
    
    BOCHAAI_API_URL = "https://api.bochaai.com/v1/web-search"
    MODEL = "deepseek-v3-241226"

    # 添加快代理配置
    PROXY_SECRET_ID = "your_kuaidaili_secret_id"
    PROXY_SIGNATURE = "your_kuaidaili_signature"
    PROXY_USERNAME = "your_kuaidaili_username"
    PROXY_PASSWORD = "your_kuaidaili_password"
    PROXY_API_URL = f"https://dps.kdlapi.com/api/getdps/?secret_id={PROXY_SECRET_ID}&signature={PROXY_SIGNATURE}&num=1&pt=1&sep=1"
    
    MAX_RETRIES = 3
    DELAY_MIN = 0.5
    DELAY_MAX = 2

    HEADERS = {}
    

class PersonInfo:
    """人物信息类"""
    def __init__(self, name: str, position: str, resume: str, province: str, department: str):
        self.name = name
        self.position = position
        self.resume = resume
        self.province = province
        self.department = department
        
        # Excel输出字段
        self.gender = ""
        self.birth_date = ""
        self.native_place = ""
        self.education = ""
        self.ethnicity = ""
        self.positions = {str(year): {"position": "", "level": "", "location": ""} 
                         for year in range(2016, 2025)}
    
    def update_positions(self, positions_list: List[Dict]):
        """更新职位信息
        Args:
            positions_list: 职位信息列表，每个元素是包含year/position/level/location的字典
        """
        for position_data in positions_list:
            year = str(position_data['year'])
            if year in self.positions:
                self.positions[year].update({
                    'position': position_data.get('position', ''),
                    'level': position_data.get('level', ''),
                    'location': position_data.get('location', '')
                })

    def to_dict(self) -> Dict:
        """转换为字典格式"""
        result = {
            "省份": self.province,
            "部门": self.department,
            "姓名": self.name,
            "性别": self.gender,
            "出生年月": self.birth_date,
            "籍贯": self.native_place,
            "学历": self.education,
            "民族": self.ethnicity
        }
        
        for year in range(2016, 2025):
            year_str = str(year)
            result.update({
                f"{year}职位": self.positions[year_str]["position"],
                f"{year}职级": self.positions[year_str]["level"],
                f"{year}地点": self.positions[year_str]["location"]
            })
        
        return result

class FakeChromeUA:
    """随机生成User-Agent"""
    os_type = [
        '(Windows NT 6.1; WOW64)',
        '(Windows NT 10.0; WOW64)',
        '(X11; Linux x86_64)',
        '(Macintosh; Intel Mac OS X 10_13_6)'
    ]
    
    @classmethod
    def get_ua(cls):
        chrome_version = f"Chrome/{random.randint(55, 69)}.0.{random.randint(0, 3500)}.{random.randint(0, 140)}"
        return ' '.join(['Mozilla/5.0', random.choice(cls.os_type),
                        'AppleWebKit/537.36',
                        '(KHTML, like Gecko)',
                        chrome_version,
                        'Safari/537.36'])

class BaiduSpider:
    """百度百科爬虫类"""
    def __init__(self):
        self.base_headers = Config.HEADERS
        self.proxy_ip = None
        self.opener = None
        self.update_proxy()
    
    def update_proxy(self):
        """获取新的代理IP"""
        try:
            response = requests.get(Config.PROXY_API_URL)
            self.proxy_ip = response.text.strip()
            # 配置代理
            proxy_url = f"http://{Config.PROXY_USERNAME}:{Config.PROXY_PASSWORD}@{self.proxy_ip}"
            proxy_handler = urllib.request.ProxyHandler({
                'http': proxy_url,
                'https': proxy_url
            })
            # 创建认证处理器
            auth_handler = urllib.request.HTTPBasicAuthHandler()
            self.opener = urllib.request.build_opener(proxy_handler, auth_handler)
            urllib.request.install_opener(self.opener)
            print(f"更新代理IP: {self.proxy_ip}")
        except Exception as e:
            print(f"更新代理IP失败: {e}")

    def query(self, url: str, max_retries: int = Config.MAX_RETRIES) -> Optional[str]:
        prefix = 'https://baike.baidu.com/item/'
        if not url.startswith(prefix):
            url = prefix + urllib.parse.quote(url)
        
        for retry in range(max_retries):
            try:
                # 每次重试前更新代理IP
                if retry > 0:
                    self.update_proxy()
                # self.update_proxy()
                headers = self.base_headers.copy()
                headers['User-Agent'] = FakeChromeUA.get_ua()
                headers['Referer'] = 'https://baike.baidu.com'
                req = urllib.request.Request(url=url, headers=headers, method='GET')
                response = self.opener.open(req, timeout=10)
                content = response.read()
                
                charset = chardet.detect(content)['encoding'] or 'utf-8'
                text = content.decode(charset, errors='replace')
                html = etree.HTML(text)
                
                # 提取履历信息
                # sen_list = html.xpath('''
                #     //div[@class='paraTitle_c7Isv level-1_gngtl' and h2='人物履历']/following-sibling::div[
                #         contains(@class, 'para_WzwJ3') and 
                #         count(. | //div[@class='paraTitle_c7Isv level-1_gngtl'][h2!='人物履历'][1]/preceding-sibling::div) = 
                #         count(//div[@class='paraTitle_c7Isv level-1_gngtl'][h2!='人物履历'][1]/preceding-sibling::div)
                #     ]
                #     //span[@class='text_tJaKK']/text()
                #     | (//div[contains(@class, 'basicInfo_Gvg0x J-basic-info')]//dt[@class='basicInfoItem_teWTJ itemName_J9fIC']
                #     /text())
                #     | (//div[contains(@class, 'basicInfo_Gvg0x J-basic-info')]//dd[@class='basicInfoItem_teWTJ itemValue_AEGp2']
                #     /span[@class='text_tJaKK']/text())
                #     ''')
                sen_list = html.xpath('''
                    //div[@class='paraTitle_WslP_ level-1_Ep022' and h2='人物履历']/following-sibling::div[
                        contains(@class, 'para_fT72O') and 
                        count(. | //div[@class='paraTitle_WslP_ level-1_Ep022'][h2!='人物履历'][1]/preceding-sibling::div) = 
                        count(//div[@class='paraTitle_WslP_ level-1_Ep022'][h2!='人物履历'][1]/preceding-sibling::div)
                    ]
                    //span[@class='text_H18Us']/text()
                    | (//div[contains(@class, 'basicInfo_Dxt9K')]//dt[@class='basicInfoItem_zB304 itemName_LS0Jv']
                    /text())
                    | (//div[contains(@class, 'basicInfo_Dxt9K')]//dd[@class='basicInfoItem_zB304 itemValue_AYbkR']
                    /span[@class='text_H18Us']/text())
                ''')

                sen_list_after_filter = [re.sub(r'\s+', ' ', item).strip() for item in sen_list if item.strip()]
                return '\n'.join(sen_list_after_filter)
                
            except Exception as e:
                print(f"查询 {url} 时发生错误 (重试 {retry + 1}/{max_retries}): {e}")
                if retry == max_retries - 1:
                    print(f"爬取 {url} 失败，已达到最大重试次数")
                    return None
                time.sleep(random.uniform(Config.DELAY_MIN, Config.DELAY_MAX))

class ContentValidator:
    """内容验证类"""
    def __init__(self):
        jieba.initialize()
    
    def validate_by_keywords(self, baidu_content: str, person_info: PersonInfo) -> bool:
        if not baidu_content:
            return False
            
        # 分词处理
        content_words = set(jieba.cut(baidu_content))
        keywords = set(jieba.cut(f"{person_info.province} {person_info.department} {person_info.position}"))
        
        # 计算关键词匹配度
        matched = keywords & content_words
        match_ratio = len(matched) / len(keywords)
        
        return match_ratio >= 0.6  # 匹配度阈值

class GPTHelper:
    """GPT交互类"""
    def __init__(self, api_key: str = Config.GPT_API_KEY, model: str = Config.MODEL):
        self.api_key = api_key
        self.model = model
        self.client = Ark(
            api_key=api_key
        )
        
    def validate_person(self, baidu_content: str, person_info: PersonInfo) -> bool:
        prompt = f"""
        请判断以下百科内容是否描述的是同一个人：
        
        人物信息：
        - 姓名：{person_info.name}
        - 职务：{person_info.position}
        - 省份：{person_info.province}
        - 部门：{person_info.department}
        
        百科内容：
        {baidu_content}
        
        优先判断省份与部门；个别人物职务可能为空
        请仅返回 true 或 false
        """
        
        try:
            response = self.call_gpt(prompt)
            print(f"GPT验证 {person_info.name} 为 {response.lower()}")
            return 'true' in response.lower()
        except Exception as e:
            print(f"GPT验证失败: {e}")
            return False
    
    def extract_info(self, baidu_content: str, person: PersonInfo) -> Dict:
        prompt = f"""
        请从以下履历文本中提取人物信息，请仅返回JSON格式的结果，不要有任何其他文字。注意以下要点：
        1. 文本中的履历按时间顺序排列，如果某个时间段横跨多年(如2018-2020)，则这期间每年都使用与开始年份（这里是2018年）相同的职务信息。
        2. 同一年份如有多次职务变动，只取时间最新的一条。
        3. 职级判断请使用以下标准进行判断，如果难以确定职级则根据你的知识库判断，如果还无法判断返回空值：
            - 正国级（党和国家领导人核心层）：中共中央政治局常务委员会委员、中华人民共和国主席、全国人民代表大会常务委员会委员长、国务院总理、全国政协主席、中央军事委员会主席、中央纪律检查委员会书记。
            - 副国级（党和国家领导人）：中共中央政治局委员（非常委）、国家副主席、国务院副总理、国务委员、全国人大常委会副委员长、全国政协副主席、中央军委副主席、最高人民法院院长、最高人民检察院检察长。
            - 正部级（省部级正职）：党中央直属机构正职（中央办公厅主任、中央政法委秘书长、中央政策研究室主任、非政治局委员的中组部/中宣部/统战部部长）；国务院组成部门正职（各部部长（如外交部、国防部）、央行行长、审计署审计长）；地方党政正职（省委书记、省长、自治区主席、直辖市市长）；全国性机构正职（全国政协秘书长、全国人大专委会主任委员（正部级））。
            - 副部级（省部级副职）：国务院部委副职（副部长、央行副行长、海关总署副署长）；地方党政副职（省委副书记、副省长、自治区副主席）；副省级城市四大班子正职（如武汉市市长、南京市人大常委会主任）；中央直属单位副职（中央纪委副书记（部分高配正部级）、中央党校副校长）。
            - 正厅级（地厅级正职）：省级党政机关正职（如省教育厅厅长、省公安厅厅长、省委组织部常务副部长（主持工作））；地级市四大班子正职（市委书记、市长、市人大常委会主任、市政协主席）；中央驻地方机构正职；副部级单位常务副职（如国家发改委社会发展司司长（副部级国家发改委下属正厅级岗位））。
            - 副厅级（地厅级副职）：省级机关副职；地级市四大班子副职（市委常委、副市长（如非常务副职））；中央驻地方机构副职。
            - 正处级（县处级正职）：省级机关内设机构正职（如省发改委国民经济综合处处长、省公安厅治安管理总队总队长）；县级行政区正职（县长、县委书记（普通县）、市辖区区长、区委书记（地级市下属区））；地级市直部门正职。
            - 副处级（县处级副职）：省级机关内设机构副职；县级行政区副职（副县长、县委常委、市辖区副区长）；地级市直部门副职。
            - 正科级（乡科级正职）：县级机关正职；乡镇/街道正职；地级市直部门内设科室正职。
            - 副科级（乡科级副职）：县级机关副职；乡镇/街道副职；地级市直部门内设科室副职。
            - 科员级：一级科员、二级科员（基层公务员主体）；专业技术岗（如工程师、医师等事业单位人员）。
            - 办事员级：基层辅助岗位（如乡镇政府办事员、社区工作人员、新入职公务员试用期人员）。
        4. 2024 年的职务信息如果履历文本中不包含以下内容则补充（从输入文件中读取）：
            - 职务：{person.position}

        履历文本：
        {baidu_content}
        
        需要提取的字段：
        - gender: 性别
        - birth_date: 出生年月
        - native_place: 籍贯
        - education: 学历
        - ethnicity: 民族
        - positions: 职位信息（2016-2024年），数组格式，每个元素包含：
        * year: 年份
        * position: 职位
        * level: 职级（根据知识库标准判断，难以确定时返回空）
        * location: 地点（保留省/市/区全称）
        
        返回格式示例：
        {{
            "gender": "男",
            "birth_date": "1968年3月",
            "native_place": "河北唐山",
            "education": "大学本科",
            "ethnicity": "汉族",
            "positions": [
                {{
                    "year": 2016,
                    "position": "内蒙古自治区纪委常委、秘书长",
                    "level": "正厅级",
                    "location": "内蒙古自治区"
                }},
                ...
            ]
        }}
        """
        
        try:
            response = self.call_gpt(prompt)
        except Exception as e:
            print(f"GPT提取信息失败: {e}")
            return {}
        
        try:
            # 尝试直接解析JSON
            return json.loads(response)
        except json.JSONDecodeError:
            # 如果直接解析失败，尝试清理后再解析
            # 1. 移除可能的markdown代码块标记和推理过程
            cleaned_result = re.sub(r'<thinking>.*?</thinking>', '', response, flags=re.DOTALL)
            cleaned_result = re.sub(r'<think>.*?</think>', '', cleaned_result, flags=re.DOTALL)
            cleaned_result = re.sub(r'^```json\s*|\s*```$', '', cleaned_result)
            cleaned_result = re.sub(r',\s*([}\]])', r'\1', cleaned_result)  # 修复多余逗号
            cleaned_result = re.sub(r"'(?=\s*:)", '"', cleaned_result)  # 替换单引号为双引号
            # 2. 查找第一个 { 和最后一个 } 之间的内容
            json_match = re.search(r'\{.*\}', cleaned_result, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group())
                except json.JSONDecodeError:
                    # 如果失败，尝试匹配列表或字典模式
                    try:
                        # 匹配 [...] 或 {...} 模式
                        pattern = r'(\[.*\]|\{.*\})'
                        match = re.search(pattern, cleaned_result, re.DOTALL)
                        if match:
                            matched_content = match.group()
                            # 尝试解析匹配到的内容
                            return json.loads(matched_content)
                    except json.JSONDecodeError as je:
                        print(f"JSON解析错误: {str(je)}")
                        print(f"清理后的内容: {cleaned_result}")
                        return {}
            else:
                print("未找到有效的JSON内容")
                print(f"清理后的内容: {cleaned_result}")
                return {}
    
    def call_gpt(self, prompt: str) -> str:
        try:
            response = self.client.chat.completions.create(
                model= self.model,  # 指定模型
                messages=[{"role": "user", "content": prompt}],
                stream=False
            )
            result = response.choices[0].message.content.strip()
            return result
        except requests.exceptions.RequestException as e:
            print(f"请求异常: {str(e)}")
            return {}
        except Exception as e:
            print(f"其他错误: {str(e)}")
            return {}

class WebSearcher:
    """网页搜索类"""
    def __init__(self, api_key: str = Config.BOCHAAI_API_KEY):
        self.api_key = api_key
        
    def search_baidu_pages(self, person_info: PersonInfo) -> List[str]:
        query = f"{person_info.province} {person_info.department} {person_info.name} 百度百科"
        
        payload = json.dumps({
            "query": query,
            "summary": True,
            "count": 10,
            "page": 1
        })
        
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.post(Config.BOCHAAI_API_URL, headers=headers, data=payload)
            data = response.json()
            
            # 筛选百度百科链接
            baidu_pages = []
            for item in data['data']['webPages']['value']:
                if 'baike.baidu.com' in item['url']:
                    baidu_pages.append(item['url'])
            
            return baidu_pages[:3]  # 返回前三条结果
        except Exception as e:
            print(f"搜索失败: {e}")
            return []

class DataProcessor:
    """数据处理类"""
    def __init__(self):
        self.spider = BaiduSpider()
        self.validator = ContentValidator()
        self.gpt = GPTHelper()
        self.searcher = WebSearcher()
        
    def process_file(self, input_file: str = Config.INPUT_EXCEL):
        # 读取输入文件
        df = pd.read_csv(input_file)
        
        # 处理每个人物
        for _, row in df.iterrows():
            person = PersonInfo(
                name=row['姓名'],
                position=row['职务'],
                resume=row['简历'],
                province=row['省份'],
                department=row['部门']
            )
            
            if not self.check_duplicate(person):
                print(f"正在爬取 {person.name}...")
                self.process_person(person)
            else:
                print(f"{person.name} 已存在，跳过处理")
    
    def process_person(self, person: PersonInfo):
        # 爬取百度百科
        content = self.spider.query(person.name)
        
        if not content:
            self.try_alternative_sources(person)
            return
        
        # 验证身份
        if self.validator.validate_by_keywords(content, person):
            self.extract_and_save(content, person)
        elif self.gpt.validate_person(content, person):
            self.extract_and_save(content, person)
        else:
            self.try_alternative_sources(person)
    
    def try_alternative_sources(self, person: PersonInfo):
        # 搜索其他来源
        baidu_pages = self.searcher.search_baidu_pages(person)
        
        for page in baidu_pages:
            content = self.spider.query(page)
            if not content:
                continue
                
            if self.validator.validate_by_keywords(content, person) or \
               self.gpt.validate_person(content, person):
                self.extract_and_save(content, person)
                return
        
        # 所有尝试都失败，记录到失败日志
        self.log_failed_person(person)
    
    def extract_and_save(self, content: str, person: PersonInfo):
        # 提取信息
        info = self.gpt.extract_info(content, person)
        
        # 更新人物信息
        if info:
            person.gender = info.get('gender', '')
            person.birth_date = info.get('birth_date', '')
            person.native_place = info.get('native_place', '')
            person.education = info.get('education', '')
            person.ethnicity = info.get('ethnicity', '')
            
            positions = info.get('positions', {})
            person.update_positions(positions)
        
        # 保存到Excel
        headers = ['省份', '部门', '姓名', '性别', '出生年月', '籍贯', '学历', '民族', 
                   '2016职位', '2016职级', '2016地点', '2017职位', '2017职级', '2017地点',
                   '2018职位', '2018职级', '2018地点', '2019职位', '2019职级', '2019地点',
                   '2020职位', '2020职级', '2020地点', '2021职位', '2021职级', '2021地点',
                   '2022职位', '2022职级', '2022地点', '2023职位', '2023职级', '2023地点',
                   '2024职位', '2024职级', '2024地点']
        self.save_to_excel(person, headers)
    
    def check_duplicate(self, person: PersonInfo) -> bool:
        if not os.path.exists(Config.OUTPUT_EXCEL):
            return False
        
        df = pd.read_csv(Config.OUTPUT_EXCEL)
        return person.name in df['姓名'].values
    
    def save_to_excel(self, person: PersonInfo, headers):
        file_exists = os.path.exists(Config.OUTPUT_EXCEL)
        
        with open(Config.OUTPUT_EXCEL, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            writer.writerow(person.to_dict())
    
    def log_failed_person(self, person: PersonInfo):
        # 检查文件是否存在
        if not os.path.exists(Config.FAILED_LOG):
            # 如果文件不存在，直接写入
            with open(Config.FAILED_LOG, 'w', encoding='utf-8') as f:
                f.write("姓名\t职务\t省份\t部门\n")
        
        # 检查是否已经记录过该人物
        with open(Config.FAILED_LOG, 'r', encoding='utf-8') as f:
            existing_records = f.readlines()
            for record in existing_records:
                if person.name in record:
                    print(f"{person.name} 已存在于失败日志中，跳过记录")
                    return
        
        # 如果未记录过，则追加写入
        with open(Config.FAILED_LOG, 'a', encoding='utf-8') as f:
            f.write(f"{person.name}\t{person.position}\t{person.province}\t{person.department}\n")
            print(f"记录 {person.name} 到失败日志")
    
# 主程序
if __name__ == '__main__':
    processor = DataProcessor()
    processor.process_file()
