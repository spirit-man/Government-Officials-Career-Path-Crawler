import requests
import json
import time
import os
import csv
from bs4 import BeautifulSoup, Comment
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import re
from urllib.parse import urljoin, urlparse
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from volcenginesdkarkruntime import Ark


class ContentCleaner:
    def __init__(self, base_url):
        # 政府网站特征标签库
        self.gov_patterns = {
            'header_tags': ['header', 'nav', 'topbar', 'banner'],
            'footer_classes': ['footer', 'bottom', 'copyright'],
            'ad_classes': ['ad-container', 'banner-ad', 'popup'],
            'menu_classes': ['navbar', 'menu', 'sidebar'],
            'interactive_classes': ['comment-section', 'survey'],
            'non_content_tags': ['svg', 'button', 'form']
        }
        self.base_url = base_url

    def clean_html_content(self, content):
        """智能清洗政府网站HTML内容"""
        try:
            soup = BeautifulSoup(content, 'html.parser')
            # self._print_step("解析完成", soup)
            
            # 第一阶段：基础清理
            self._basic_clean(soup)
            # self._print_step("基础清理后", soup)
            
            # 第二阶段：语义特征清理
            self._semantic_clean(soup)
            # self._print_step("语义清理后", soup)

            # 第三阶段：深度文本处理
            final_content = self._deep_text_clean(str(soup))
            # self._print_step("最终结果", final_content)
            
            return final_content
        
        except Exception as e:
            print(f"内容清洗失败: {str(e)}")
            return content

    def _print_step(self, step_name, content):
        with open(f"debug_{step_name}.html", 'w', encoding='utf-8') as f:
            f.write(str(content))

    def _basic_clean(self, soup):
        """执行基础标签清理"""
        # 移除脚本类标签
        for tag in soup.find_all(['script', 'style', 'link', 'meta', 'noscript']):
            tag.decompose()
        
        # 移除注释
        for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
            comment.extract()
        
        # 清理空链接
        for a in soup.find_all('a', href=lambda x: x in ['#', 'javascript:void(0);']):
            a.decompose()

    def _semantic_clean(self, soup):
        """基于政府网站特征清理"""
        for cls in self.gov_patterns['ad_classes']:
            for elem in soup.find_all(class_=re.compile(cls)):
                elem.decompose()
    
        for cls in self.gov_patterns['interactive_classes']:
            for elem in soup.find_all(class_=cls):
                elem.decompose()

        for cls in self.gov_patterns['non_content_tags']:
            for elem in soup.find_all(class_=cls):
                elem.decompose()

    def _deep_text_clean(self, html_str):
        """深度文本处理"""
        # 移除HTML命名空间
        html_str = re.sub(r'<\?xml[^>]+\?>', '', html_str)
        
        # 压缩政府网站典型冗余信息
        patterns = [
            r'<div class="share-title[^>]*>.*?</div>',  # 分享按钮
            r'<a[^>]*class="[^"]*more[^"]*"[^>]*>.*?</a>',  # "更多"链接
            r'<span class="date">.*?</span>',  # 重复日期
            r'<img[^>]*alt="[^"]*logo[^"]*"[^>]*>',  # logo图片
            r'<div class="clear"></div>'  # 布局用空div
        ]
        
        for pattern in patterns:
            html_str = re.sub(pattern, '', html_str, flags=re.DOTALL)
        
        # 压缩空白字符（保留换行）
        html_str = re.sub(r'[ \t]+', ' ', html_str)
        html_str = re.sub(r'\n{3,}', '\n\n', html_str)
        html_str = re.sub(r'^\s+|\s+$', '', html_str, flags=re.MULTILINE)

        # 只保留网页链接和中文字符
        # 1. 先匹配并临时保护URL
        url_pattern = r'''
            (?:https?://[^\s<>"']+) |    # 绝对路径
            (?:\./[^\s<>"']*)            # 相对路径，仅匹配 ./开头
        '''

        normalized_urls = []
        def _replace_url(match):
            url = match.group(0)
            # 处理URL
            if url.startswith('./'):
                url = url[2:]
                
            if not url.startswith('http'):
                url = urljoin(self.base_url, url)
            
            normalized_urls.append(url)
            return f'__URL{len(normalized_urls)-1}__'

        # 每匹配到一个就立即替换
        html_str = re.sub(url_pattern, _replace_url, html_str, flags=re.VERBOSE)

        # 2. 只保留中文、数字、U/R/L字母以及连续双下划线
        html_str = re.sub(
            r'(__)|([^\u4e00-\u9fff0-9ULR])',  # 匹配组1：双下划线 | 组2：非保留字符
            lambda m: m.group(1) or ' ',       # 保留组1内容，其他替换为空格
            html_str
        )

        # 3. 恢复URL
        for i, url in enumerate(normalized_urls):
            html_str = html_str.replace(f'__URL{i}__', url)
        
        # 4. 最终清理空白
        html_str = re.sub(r'\s+', ' ', html_str)
        
        return html_str.strip()


class GovInfoCrawler:
    def __init__(self, api_key, model, chunk_size, max_depth, initial_url, target_provinces, folder):
        self.api_key = api_key
        self.model = model
        self.chunk_size = chunk_size
        self.max_depth = max_depth
        self.initial_url = initial_url
        self.target_provinces = target_provinces
        self.folder = folder
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.client = Ark(
            api_key=api_key
        )
        self.setup_selenium()

    def setup_selenium(self):
        """设置 Selenium WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--ignore-ssl-errors')
        chrome_options.add_argument('--allow-insecure-localhost')
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), 
                                     options=chrome_options)

    def ask_gpt(self, content, task, base_url):
        """调用 GPT-4o API 分析内容"""
        clean = ContentCleaner(base_url)
        cleaned_content = clean.clean_html_content(content)

        prompt = f"""任务：{task}
        网页内容：{cleaned_content}
        请仅返回JSON格式的结果，不要有任何其他文字。"""
        
        try:
            response = self.client.chat.completions.create(
                model= self.model,  # 指定模型
                messages=[{"role": "user", "content": prompt}],
                stream=False
            )
            result = response.choices[0].message.content.strip()
            
            # 尝试直接解析JSON
            try:
                # 首先尝试直接解析
                return json.loads(result)
            except json.JSONDecodeError:
                # 如果直接解析失败，尝试清理后再解析
                # 1. 移除可能的markdown代码块标记和推理过程
                cleaned_result = re.sub(r'<thinking>.*?</thinking>', '', result, flags=re.DOTALL)
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
                
        except requests.exceptions.RequestException as e:
            print(f"请求异常: {str(e)}")
            return {}
        except Exception as e:
            print(f"其他错误: {str(e)}")
            return {}
        
    def get_content_request(self, url):
        session = requests.Session()
        session.verify = False  # 禁用证书验证
        response = session.get(url, headers=self.headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        content = str(soup)
        return content

    def find_safe_split_point(self, content, target_position):
        """
        在目标位置附近寻找安全的分割点
        优先寻找HTML标签的结束位置，特别是</a>标签
        """
        # 搜索范围：向前后2000个字符范围内寻找安全分割点
        search_range = 2000
        start = max(0, target_position - search_range)
        end = min(len(content), target_position + search_range)
        
        # 在目标位置附近的内容
        nearby_content = content[start:end]
        
        # 寻找最接近目标位置的</a>标签
        splits = []
        # 添加常见的HTML闭合标签作为分割点
        for tag in ['</a>', '</div>', '</p>', '</li>', '</tr>']:
            # 找到附近所有的闭合标签位置
            pos = -1
            while True:
                pos = nearby_content.find(tag, pos + 1)
                if pos == -1:
                    break
                # 将相对位置转换为绝对位置
                absolute_pos = start + pos + len(tag)
                splits.append(absolute_pos)
        
        if not splits:
            # 如果没找到合适的标签，退化到寻找空白字符
            pos = target_position
            while pos < end:
                if content[pos].isspace():
                    return pos
                pos += 1
            return target_position
        
        # 返回最接近目标位置的分割点
        return min(splits, key=lambda x: abs(x - target_position))

    def process_large_content(self, content, task, chunk_size, base_url):
        """
        处理大型内容，将内容智能分块并循环调用GPT API
        返回合并后的结果
        """
        results = []
        current_pos = 0
        chunks = []
        
        while current_pos < len(content):
            if current_pos + chunk_size >= len(content):
                # 最后一块，直接到结尾
                chunks.append(content[current_pos:])
                break
            
            # 寻找安全的分割点
            split_point = self.find_safe_split_point(content, current_pos + chunk_size)
            chunks.append(content[current_pos:split_point])
            current_pos = split_point
        
        for i, chunk in enumerate(chunks):
            try:
                # 添加块号信息到任务描述中
                chunk_task = f"{task} (第{i+1}块，共{len(chunks)}块)"
                chunk_result = self.ask_gpt(chunk, chunk_task, base_url)
                if isinstance(chunk_result, dict):
                    results.append(chunk_result)
                elif isinstance(chunk_result, list):
                    results.extend(item for item in chunk_result if isinstance(item, dict))
            except Exception as e:
                print(f"处理第{i+1}块时出错: {str(e)}")
        
        return results

    def get_province_codes(self):
        """返回省份的拼音和简称字典"""
        return {
            "北京": ["beijing", "bj"],
            "天津": ["tianjin", "tj"],
            "河北": ["hebei", "hb"],
            "山西": ["shanxi", "sx"],
            "内蒙古": ["neimenggu", "nmg"],
            "辽宁": ["liaoning", "ln"],
            "吉林": ["jilin", "jl"],
            "黑龙江": ["heilongjiang", "hlj"],
            "上海": ["shanghai", "sh"],
            "江苏": ["jiangsu", "js"],
            "浙江": ["zhejiang", "zj"],
            "安徽": ["anhui", "ah"],
            "福建": ["fujian", "fj"],
            "江西": ["jiangxi", "jx"],
            "山东": ["shandong", "sd"],
            "河南": ["henan", "hn"],
            "湖北": ["hubei", "hb"],
            "湖南": ["hunan", "hn"],
            "广东": ["guangdong", "gd"],
            "广西": ["guangxi", "gx"],
            "海南": ["hainan", "hn"],
            "重庆": ["chongqing", "cq"],
            "四川": ["sichuan", "sc"],
            "贵州": ["guizhou", "gz"],
            "云南": ["yunnan", "yn"],
            "西藏": ["xizang", "xz"],
            "陕西": ["shaanxi", "sx"],
            "甘肃": ["gansu", "gs"],
            "青海": ["qinghai", "qh"],
            "宁夏": ["ningxia", "nx"],
            "新疆": ["xinjiang", "xj"],
            "香港": ["hongkong", "hk", "xiang gang"],
            "澳门": ["macao", "mo", "aomen"],
            "台湾": ["taiwan", "tw"],
            "新疆生产建设兵团": ["bingtuan", "bt", "xjbt", "xj"]
        }

    def get_province_links(self, url):
        """获取指定省份的政府网站链接"""
        try:
            content = self.get_content_request(url)
            parsed_url = urlparse(url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

            # 修改任务提示，只获取目标省份的链接
            task = f"从页面内容中仅提取以下省份的政府网站链接：{', '.join(self.target_provinces)}，返回格式为：{{'省份名': '网站链接'}}"
            province_links = self.ask_gpt(content, task, base_url)
            
            # 过滤结果，确保只返回目标省份的链接
            filtered_links = {k: v for k, v in province_links.items() if k in self.target_provinces}
            return filtered_links
        except Exception as e:
            print(f"获取省份链接时出错: {str(e)}")
            return {}

    def get_department_links(self, province_url, province_name):
        """获取省级部门链接"""
        try:
            content = self.get_content_request(province_url)
            
            # 获取省份编码字典
            province_codes = self.get_province_codes()
            province_keywords = province_codes.get(province_name, [])
            
            # 从URL中提取当前使用的省份代码和域名
            parsed_url = urlparse(province_url)
            province_domain = parsed_url.netloc
            current_code = province_domain.split('.')[1]
            if current_code not in province_keywords:
                province_keywords.append(current_code)
            
            # 构建基础URL（移除路径部分）
            base_url = f"{parsed_url.scheme}://{province_domain}"
            
            # 构建关键词字符串
            keywords_str = "', '".join(province_keywords)
            
            # 提取政府部门和市级链接
            extract_task = (
                f"从{province_name}政府网站中提取各级政府部门和盟市的链接，包括：\n"
                "1. 省级部门：厅、局、委员会、办公室等\n"
                "2. 市级政府网站：各盟市的政府网站，如呼和浩特市、阿拉善盟等\n"
                "3. URL格式：xxx.gov.cn\n"
                "4. 排除非部门或盟市的链接，如：通知公告、政务服务、新闻中心等\n"
                "5. 部门名称应该是标准的政府部门称谓\n\n"
                f"返回格式为：{{'部门名': '链接'}}。\n\n"
                f"请优先选择包含以下关键词的URL：'{keywords_str}'和'gov'，这些URL更可能是官方部门链接\n"
                "同时关注包含'市''盟'字的政府网站链接，以及与市级同级别的政府网站链接。\n\n"
                "示例：\n"
                "✓ 省级部门：'教育厅'、'公安厅'、'发展改革委'\n"
                "✓ 省级部门网址：'http://kjt.nmg.gov.cn'、'http://mzt.nmg.gov.cn'、'http://jyt.nmg.gov.cn'\n"
                "✓ 市级网站：'呼和浩特市'、'阿拉善盟'\n"
                "✓ 市级网址：'http://www.huhhot.gov.cn'、'http://www.als.gov.cn/'\n"
                "✗ 排除：'通知公告'、'政务服务'、'信息公开'\n\n"
            )
            
            extract_links = self.ask_gpt(content, extract_task, base_url)
            
            # 处理所有链接，确保是完整的URL
            normalized_links = {}
            for dept, link in extract_links.items():
                if isinstance(link, list):
                    link = link[0]
                if link.startswith('./'):
                    link = link[2:]
                if not link.startswith('http'):
                    link = urljoin(base_url, link)
                normalized_links[dept] = link
            
            return normalized_links
        except Exception as e:
            print(f"获取部门链接时出错: {str(e)}")
            return {}

    def find_section_links(self, content, base_url):
        """查找页面中的相关板块链接"""
        task = (
            "请分析网页内容，提取与政府机构、领导信息相关的链接。将你认为最可能包含领导信息的链接优先返回，放在返回内容的前面\n\n"
        
            "【必须包含的板块】\n"
            "1. 领导信息相关：\n"
            "   - 领导信息/领导之窗/领导班子/领导简介\n"
            "   - 负责人信息/主要领导/班子成员\n"
            
            "2. 机构信息相关：\n"
            "   - 政务公开/信息公开/公开信息\n"
            "   - 组织机构/机构设置/部门设置\n"
            "   - 机构职能/部门职责/职能介绍\n"
            "   - 机关简介/单位介绍\n\n"
            
            "【严格排除的板块】\n"
            "1. 业务工作类：\n"
            "   - 预决算公开/财务公开\n"
            "   - 政策法规/规章制度\n"
            "   - 业务动态/工作动态\n"
            
            "2. 互动交流类：\n"
            "   - 领导信箱/公众互动\n"
            "   - 在线咨询/建议提案\n"
            
            "3. 信息公示类：\n"
            "   - 任何结果公示/中标公示\n"
            "   - 行政许可/行政处罚\n"
            
            "4. 其他无关内容：\n"
            "   - 新闻/通知/公告\n"
            "   - 科普宣传/知识园地\n"
            "   - 其他部门的链接\n"
            "   - 年度报表/工作报告\n"
            "   - 依申请公开\n"
            "   - 政策解读\n"
            "   - 权责清单\n\n"
            "   - 具体工作通知、公告\n"
            "   - 业务办理指南、流程\n"
            "   - 规章制度、管理办法\n\n"
            
            "【链接判断原则】\n"
            "1. 仅保留极有可能包含领导信息的板块\n"
            "2. 链接必须属于当前部门\n"
            "3. 不要求严格匹配关键词，注重语义相关性\n"
            "4. 优先选择直接包含'领导'关键词的链接\n\n"
            
            "示例参考：\n"
            "✓ 政务公开->负责人信息\n"
            "✓ 政府信息公开->法定主动公开内容->机关简介\n"
            "✓ 政务公开->领导之窗\n"
            "✗ 通知公告\n"
            "✗ 便民服务\n\n"
            
            f"返回格式：{{'板块名称': '链接URL'}}\n"
        )
        
        section_links = self.ask_gpt(content, task, base_url)
        
        # 处理链接，确保都是完整的URL
        normalized_links = {}
        for section, link in section_links.items():
            if isinstance(link, list):
                link = link[0]
            if link.startswith('./'):
                link = link[2:]
            if not link.startswith('http'):
                link = urljoin(base_url, link)
            normalized_links[section] = link
            
        return normalized_links

    def find_leadership_info(self, content, base_url):
        """在页面中查找领导信息"""
        task = (
            "请从页面中提取领导班子信息。注意以下要点：\n"
            "1. 优先查找以下位置：\n"
            "政府信息公开/法定主动公开内容栏目，特别关注URL包含 'zfxxgk'、'fdzdgknr' 的页面\n"
            "包含以下关键词的内容：'分工'、'简历'、'领导'\n"
            "2. 寻找包含领导姓名、职务、简历的内容\n"
            "3. 关注关键词，保留正厅级及以上职务的领导（“自治区”等同于省级，如自治区高级人民法院副职等于省高级人民法院副职，也是厅级），如果一个人同时有多个职务，依据最高等级职务判断：\n"
            "正国级：中共中央总书记，中共中央政治局常委，国家主席，全国人大常委会委员长，国务院总理，全国政协主席，中华人民共和国中央军事委员会主席。\n"
            "副国级：中央政治局委员，国家副主席，全国人大副委员长，全国政协副主席，国务院副总理，法院检察院，国务院国务委员，中央军委副主席，中央军委委员、中央书记处书记\n"
            "每个省级行政区，省长（自治区主席、直辖市市长）、党委书记、人大主任、政协主席四套班子一把手均为正部级职务，副职对应副部级。\n"
            "省级检察院检察长、高级法院院长，纪委书记、政法委书记职务均为副部级。\n"
            "最高法的常务副院长和最高检的常务副检察长为正部级岗位，不带“常务”的为副部级岗位。\n"
            "党中央各部委、办、处、司、局正职领导，如各处室办公室的负责人\n"
            "国务院各组成部门的厅局司正职，以及内设机构的高级领导\n"
            "国务院直属事业单位和国家局副职，以及关键部门的正职\n"
            "中央部委在地方的直属机构负责人（如生态环境部华北督察局督察专员）\n"
            "中央驻省/自治区事业单位正职（如中国气象局内蒙古气象局局长）\n"
            "群团组织中央部局委办中心的正职，如全国人大各处室主任\n"
            "全国政协各专门委员会委员和办公室正职\n"
            "中央纪委国家监察委员会纪检监察室主任\n"
            "最高人民法院各审判庭庭长\n"
            "最高人民检察院各检察厅厅长\n"
            "省级党委各部委办局的副职\n"
            "省级政府组成部门和直属事业单位的正职\n"
            "省级人大各专门委员会主任\n"
            "省级政协各专门委员会主任\n"
            "省人民检察院副职和重要纪检监察室主任\n"
            "省高级人民法院副职\n"
            "省纪委监委副职和重要岗位\n"
            "副省级省会城市的重要职位，如市委专职副书记、市委常委、副市长等，副省级省会城市有：广州、长春、济南、杭州、大连、青岛、武汉、哈尔滨、沈阳、成都、南京、西安、深圳、厦门、宁波\n"
            "地市级领导，只包括：市委书记、市长、市人大常委会主任、市政协主席的正职\n"
            "省政府督查专员、重大项目办主任等\n"
            "4. 排除：企业、高校、科研院所的领导\n"
            "5. 注意可能存在的人物简介或详细介绍\n\n"
            
            f"返回格式：[{{'姓名': '', '职务': '', '简历': ''}}]\n"
            "如果没有找到相关信息，返回空列表。\n"
        )
        
        return self.process_large_content(content, task, self.chunk_size, base_url)

    def _click_special_links(self):
        """点击特定特征的导航链接"""
        # 扩展xpath选择器，增加多种可能的导航模式
        xpaths = [
            # 原始xpath
            "//div[contains(@class,'firstList')]/a[contains(@class,'muluShow')]",
            # 通用的信息公开目录链接
            "//a[contains(text(), '信息公开目录') or contains(text(), '机构职能')]",
            # 领导信息相关链接
            "//a[contains(text(), '领导') or contains(text(), '班子')]",
            # 机构信息相关链接
            "//a[contains(text(), '机构') or contains(text(), '简介')]"
        ]
        
        try:
            for xpath in xpaths:
                try:
                    elements = WebDriverWait(self.driver, 3).until(
                        EC.presence_of_all_elements_located((By.XPATH, xpath))
                    )
                    
                    for element in elements:
                        try:
                            # 检查元素是否可见和可点击
                            if element.is_displayed() and element.is_enabled():
                                # 使用JavaScript点击，避免元素被遮挡的问题
                                self.driver.execute_script("arguments[0].click();", element)
                                time.sleep(1)  # 短暂等待
                        except Exception as click_error:
                            print(f"点击元素时出错: {click_error}")
                            continue
                            
                except TimeoutException:
                    continue  # 如果当前xpath没找到元素，尝试下一个
                    
        except Exception as e:
            print(f"展开导航链接时出错: {str(e)}")

    def _expand_hidden_contents(self):
        """展开所有隐藏的内容区块"""
        # 扩展选择器以匹配更多可能的隐藏内容
        js_code = """
        // 展开所有display:none的元素
        document.querySelectorAll('div[style*="display:none"], div[style*="display: none"]').forEach(el => {
            el.style.display = 'block';
        });
        
        // 移除各种可能的折叠类
        document.querySelectorAll('.collapsed, .hide, .hidden, .collapse:not(.in)').forEach(el => {
            el.classList.remove('collapsed', 'hide', 'hidden');
            el.classList.add('show', 'in');
        });
        
        // 激活所有标签页
        document.querySelectorAll('.tab-pane').forEach(el => {
            el.classList.add('active', 'show');
        });
        
        // 展开所有详情元素
        document.querySelectorAll('details').forEach(el => {
            el.setAttribute('open', 'true');
        });
        """
        
        try:
            # 执行JavaScript代码
            self.driver.execute_script(js_code)
            time.sleep(2)  # 等待DOM更新
            
        except Exception as e:
            print(f"展开隐藏内容时出错: {str(e)}")

    def expand_content_with_selenium(self, url):
        """针对政府网站结构的精准展开函数"""
        print(f"深度展开内容: {url}")
        max_retries = 3  # 最大重试次数
        
        for attempt in range(max_retries):
            try:
                # 设置页面加载超时
                self.driver.set_page_load_timeout(20)
                
                # 访问页面
                self.driver.get(url)
                
                # 等待页面基本加载完成
                WebDriverWait(self.driver, 15).until(
                    lambda driver: driver.execute_script('return document.readyState') == 'complete'
                )
                
                # 检查页面是否成功加载
                if "404" in self.driver.title or "错误" in self.driver.title:
                    print(f"页面可能不存在或发生错误: {self.driver.title}")
                    return None
                    
                # 展开内容
                self._click_special_links()
                self._expand_hidden_contents()
                
                # 再次等待以确保内容加载
                time.sleep(3)
                
                # 获取展开后的页面内容
                page_source = self.driver.page_source
                
                # 验证内容是否成功获取
                if len(page_source) < 1000:  # 页面内容过少可能表示加载失败
                    print("警告: 页面内容可能未完全加载")
                    if attempt < max_retries - 1:
                        continue
                        
                return page_source
                
            except Exception as e:
                print(f"第{attempt + 1}次尝试失败: {str(e)}")
                if attempt < max_retries - 1:
                    print("正在重试...")
                    time.sleep(3)  # 重试前等待
                    continue
                else:
                    print("深度展开失败，已达到最大重试次数")
                    return None
                    
            finally:
                try:
                    # 重置页面加载超时
                    self.driver.set_page_load_timeout(30)
                except Exception:
                    pass
                    
        return None

    def get_leadership_info(self, department_url, province_name, department_name, visited_urls=None, max_depth=3, all_leadership_info = None):
        """获取部门领导信息（递归查找）"""
        if visited_urls is None:
            visited_urls = set()
        if all_leadership_info is None:
            all_leadership_info = []
        
        if max_depth <= 0 or department_url in visited_urls:
            return []
        
        try:
            # 获取当前页面内容
            content = self.get_content_request(department_url)
            base_url = department_url
            
            # 1. 首先在当前页面查找领导信息
            leadership_info = self.find_leadership_info(content, base_url)
            visited_urls.add(department_url)
            if leadership_info:
                # 添加省份和部门信息
                for info in leadership_info:
                    info['省份'] = province_name
                    info['部门'] = department_name
                all_leadership_info.extend(leadership_info)
                # 如果已经找到3个或更多领导,直接返回
                if len(all_leadership_info) >= 3:
                    return all_leadership_info
            
            # 2. 查找相关板块链接
            section_links = self.find_section_links(content, base_url)
            
            # 3. 如果找到相关板块链接，递归查找
            for section_name, section_url in section_links.items():
                if section_url not in visited_urls:
                    print(f"正在查找 {section_name} 板块...")
                    self.get_leadership_info(
                        section_url, 
                        province_name, 
                        department_name, 
                        visited_urls, 
                        max_depth - 1,
                        all_leadership_info
                    )
                    visited_urls.add(section_url)
                    
                # 避免请求过快
                time.sleep(1)
                
        except Exception as e:
            print(f"处理链接 {department_url} 时出错: {str(e)}")

    def deep_search_leadership(self, visited_urls, province_name, department_name):
        """深度搜索（不再遍历子链接）"""
        for url in visited_urls:
            if any(keyword in url for keyword in ['xxgk', 'zwgk']):
                print(f"启动深度搜索: {url}")
                try:
                    # 获取展开后的内容
                    expanded_content = self.expand_content_with_selenium(url)
                    if not expanded_content:
                        continue
                    
                    # 直接解析当前页面的扩展内容
                    leadership_info = self.find_leadership_info(expanded_content, url)
                    
                    for info in leadership_info:
                        info.update({
                            '省份': province_name,
                            '部门': department_name,
                        })
                        
                    return leadership_info
                    
                except Exception as e:
                    print(f"深度搜索失败: {str(e)}")
                    continue

            else:
                print(f"没有找到包含【信息公开】的链接")
                return []
        return []

    def merge_people_with_gpt(self, result_list):
        """将整个人员列表发送给GPT进行语义判断和合并"""
        if not result_list:
            return []
        
        # 添加数据验证，过滤掉不完整的数据
        valid_results = []
        for item in result_list:
            if all(key in item for key in ['姓名', '职务', '简历']):
                valid_results.append(item)
            else:
                print(f"发现无效数据条目: {item}")
        
        if not valid_results:
            print("没有有效的人员信息可以处理")
            return []
    
        task = """请分析以下人员列表，识别并合并重复人员信息，同时验证简历内容。
        合并规则：
        1. 综合考虑姓名、职务时间线连续性、简历信息互补性等因素判断是否为同一人
        2. 若姓名相同，极大概率为同一人；此时再判断职务若语义相近，则为同一人
        3. 合并时保留所有不重复的职务信息（根据语义判断是否为同一职务），用顿号分隔
        4. 合并简历时保持时间顺序，确保语义信息完整且不重复
        5. 保留最新的省份和部门信息

        验证简历内容：检查简历内容是否为真正的个人履历描述，有效简历应包含：教育背景、工作经历、任职经历等。
        如果简历内容无效，将简历字段置为空。

        请返回JSON格式的处理后列表，每个人员包含：姓名、职务、简历、省份、部门字段，不要包含其他任何字段。
        示例格式：
        [
            {
                "姓名": "张三",
                "职务": "职务1、职务2",
                "简历": "完整简历",
                "省份": "XX",
                "部门": "XX"
            }
            {
                "姓名": "李四",
                "职务": "职务1、职务2",
                "简历": "完整简历",
                "省份": "XX",
                "部门": "XX"
            }
        ]
        """
        
        # 构造人员列表的字符串表示
        people_info = "\n\n".join([
            f"人员信息{i+1}：\n姓名：{p.get('姓名', '未知')}\n职务：{p.get('职务', '未知')}\n简历：{p.get('简历', '未知')}\n省份：{p.get('省份', '')}\n部门：{p.get('部门', '')}"
            for i, p in enumerate(valid_results)
        ])
        
        try:
            # 调用GPT进行处理
            result_list_merged = self.ask_gpt(people_info, task, "")
            
            # 验证返回的数据格式
            if not isinstance(result_list_merged, list):
                print("GPT返回格式不是列表，使用原始数据")
                return result_list
                
            return result_list_merged
            
        except Exception as e:
            print(f"处理过程出现错误: {str(e)}")
            return result_list

    def process_department(self, department_url, province_name, department_name):
        """处理单个部门的数据并保存到CSV"""
        # 保存到CSV
        os.makedirs(self.folder, exist_ok=True)
        csv_filename = f"{province_name}领导爬取.csv"
        headers = ['姓名', '职务', '简历', '省份', '部门']
        full_path = os.path.join(self.folder, csv_filename)
        file_exists = os.path.exists(full_path)

        # 检查该部门是否已经爬取
        if file_exists:
            with open(full_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row['部门'] == department_name:
                        print(f"【已存在】 {department_name}已经爬取过，跳过处理")
                        return []
        
        # 获取部门数据
        visited_urls = set()
        result_list = []
        self.get_leadership_info(department_url, province_name, department_name, visited_urls=visited_urls, max_depth=self.max_depth, all_leadership_info=result_list)
        if not result_list:
            print(f"【深度触发】{department_name}未找到常规信息")
            deep_results = self.deep_search_leadership(
                visited_urls,
                province_name,
                department_name
            )
            result_list.extend(deep_results)

        # 使用GPT合并处理整个列表
        merged_results = self.merge_people_with_gpt(result_list)

        # 记录没有找到领导信息的部门
        if not merged_results:
            no_leader_file = os.path.join(folder, 'no_leader_departments.txt')
            department_info = f"{province_name}-{department_name}\n"
            # 检查是否需要写入
            need_write = True
            if os.path.exists(no_leader_file):
                # 如果文件存在，检查内容是否已存在
                with open(no_leader_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if department_info in content:
                        need_write = False
            
            # 如果需要写入（文件不存在或内容不重复），则写入
            if need_write:
                with open(no_leader_file, 'a', encoding='utf-8') as f:
                    f.write(department_info)
    
            print(f"【未找到】 {department_name}未找到任何领导信息")
            return []
        
        with open(full_path, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            for result in merged_results:
                writer.writerow(result)
        
        return merged_results


    def main(self):
        """主程序流程"""
        initial_url = self.initial_url
        all_results = []
        
        # 1. 获取省份链接
        province_links = self.get_province_links(initial_url)
        
        # 2. 遍历每个省份
        for province_name, province_url in province_links.items():
            print(f"正在处理 {province_name}...")
            department_links = self.get_department_links(province_url, province_name)
            
            # 3. 遍历每个部门
            for dept_name, dept_url in department_links.items():
                print(f"正在处理 {province_name} {dept_name}...")
                # 处理单个部门并获取结果
                department_results = self.process_department(dept_url, province_name, dept_name)
                # 将结果添加到全局列表
                all_results.extend(department_results)
                print(f"完成处理 {province_name} {dept_name}")
                time.sleep(2)
            
            time.sleep(5)
        
        return all_results

    def __del__(self):
        """清理资源"""
        if hasattr(self, 'driver'):
            self.driver.quit()


if __name__ == "__main__":
    api_key = "your_deepseek_api_key" # 模型api
    model = "deepseek-v3-241226" # 模型id
    chunk_size = 50000 # process_large_content的分块大小
    max_depth = 4 # 递归查找网页深度

    initial_url = "https://www.gov.cn/home/2023-03/29/content_5748954.htm" # 地方政府网站
    target_provinces = ["内蒙古"] # 目标省份
    folder = './results' # 存储结果文件夹

    crawler = GovInfoCrawler(api_key, model, chunk_size, max_depth, initial_url, target_provinces, folder)
    results = crawler.main()
    print("爬取完成！")
