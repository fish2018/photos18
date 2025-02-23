import os
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import json
import re
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from PIL import Image
from io import BytesIO
import asyncio

class PhotoScraper:
    def __init__(self, base_url, start_page=1, end_page=None, max_workers=10, max_retries=3, save_interval=20, max_concurrent_downloads=50):
        self.base_url = base_url
        self.total_articles = 100  # 总文章数
        self.start_page = start_page
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.save_interval = save_interval
        self.max_concurrent_downloads = max_concurrent_downloads
        self.results = []
        self.category_name = self.get_category_name()
        self.output_dir = os.path.join("downloads", self.category_name)
        os.makedirs(self.output_dir, exist_ok=True)
        self.completed_articles = 0  # 已完成文章数
        self.end_page = end_page if end_page else int(self.total_articles / 100)

    def get_random_headers(self):
        USER_AGENTS = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        ]
        return {
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://www.photos18.com/'
        }

    def get_category_name(self):
        numbers = 100
        response = requests.get(self.base_url, headers=self.get_random_headers(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        path_url = urlparse(self.base_url).path.replace('/likes', '')
        category_tag = soup.find('a', href=f'{path_url}').text
        if category_tag:
            numbers = re.findall(r'\d+', category_tag)
        self.total_articles = int(numbers[0]) if numbers else 100
        return category_tag if category_tag else "未知分类"

    def get_article_links(self, page):
        url = f"{self.base_url}/likes&page={page}"
        response = requests.get(url, headers=self.get_random_headers(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        articles = []
        for card in soup.select('div.card'):
            link_tag = card.select_one('a.visited')
            if link_tag:
                title = link_tag.get_text(strip=True)
                path = link_tag['href']
                url = urljoin(self.base_url, path)
                articles.append({'title': title, 'url': url})
        return articles

    def download_image(self, url, save_path, retries=10):
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=self.get_random_headers(), timeout=30)  # 增加超时时间
                if response.status_code == 200:
                    content = response.content
                    # 将图片从 WebP 转换为 JPG
                    img = Image.open(BytesIO(content))
                    if img.mode != 'RGB':
                        img = img.convert('RGB')
                    img.save(save_path, 'JPEG')
                    return True
                elif response.status_code == 404:
                    print(f"图片不存在: {url}")
                    return 404
                else:
                    print(f"下载失败: {url} - HTTP 状态码: {response.status_code}")
                    return False
            except Exception as e:
                # print(f"下载失败: {url} - 尝试 {attempt + 1}/{retries} - {str(e)}")
                if attempt + 1 < retries:
                    time.sleep(2)  # 等待 2 秒后重试
                else:
                    return False
        return False

    def download_images_batch(self, images, article_dir):
        image_list = []
        with ThreadPoolExecutor(max_workers=self.max_concurrent_downloads) as executor:
            futures = []
            for index, image_url in enumerate(images):
                image_name = f"{index}.jpg"
                save_path = os.path.join(article_dir, image_name)
                if not os.path.exists(save_path):
                    futures.append(executor.submit(self.download_image, image_url, save_path))
                    image_list.append({
                        "url": image_url,
                        "filename": image_name,
                        "status": None
                    })
                else:
                    image_list.append({
                        "url": image_url,
                        "filename": image_name,
                        "status": "already_exists"
                    })
            for future, img_info in zip(futures, [i for i in image_list if i["status"] is None]):
                try:
                    success = future.result()
                    img_info["status"] = "success" if success else "failed"
                except Exception as e:
                    img_info["status"] = f"failed ({str(e)})"
        return image_list

    def get_article_data(self, article):
        for attempt in range(self.max_retries):
            try:
                response = requests.get(article['url'], headers=self.get_random_headers(), timeout=10)
                soup = BeautifulSoup(response.text, 'html.parser')

                title_tag = soup.select_one('h1.title.py-1')
                title = title_tag.get_text(strip=True) if title_tag else "未知标题"
                article_dir = os.path.join(self.output_dir, title)
                os.makedirs(article_dir, exist_ok=True)

                # images = []
                # pattern = re.compile(r'https://img\.photos18\.com/images/image/\d+/\d+\.webp\?.*')
                # for img in soup.select('img'):
                #     src = img.get('src') or img.get('data-src') or img.get('data-original')
                #     if src and pattern.match(src):
                #         full_url = urljoin(article['url'], src)
                #         full_url = full_url.split('?')[0]  # 去掉 .webp 后面的部分
                #         images.append(full_url)
                # 提取图片链接
                pattern = re.compile(r'https://img\.photos18\.com/images/image/\d+/\d+\.webp\?.*')
                images = [
                    urljoin(article['url'], img['src'].split('?')[0])  # 去掉 .webp 后面的部分
                    for img in soup.find_all('img')
                    if img.get('src') and pattern.match(img['src'])
                ]

                return {
                    'article_title': title,
                    'article_url': article['url'],
                    'images': images,
                    'article_dir': article_dir
                }
            except Exception as e:
                print(f"抓取失败: {article['url']} - 第 {attempt + 1} 次重试 - {str(e)}")
                time.sleep(2)
        print(f"抓取失败: {article['url']} - 已达到最大重试次数 {self.max_retries}")
        return {
            'article_title': "抓取失败",
            'article_url': article['url'],
            'images': [],
            'article_dir': None
        }

    def process_page_batch(self, start_page, end_page):
        articles = []
        for page in range(start_page, end_page + 1):
            print(f"正在抓取第 {page} 页的文章列表...")
            articles.extend(self.get_article_links(page))
        return articles

    def save_results(self, results, filename):
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"数据已保存到 {filepath}")

    def load_existing_results(self, filename="photos18_data_final.json"):
        filepath = os.path.join(self.output_dir, filename)
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        return []

    def process_batch(self, start_page, end_page, existing_urls, results):
        print(f"处理第 {start_page} 页到第 {end_page} 页的文章数据和图片...")
        articles = self.process_page_batch(start_page, end_page)
        new_articles = [article for article in articles if article['url'] not in existing_urls]
        print(f"本批共 {len(articles)} 篇文章，其中 {len(new_articles)} 篇需要抓取")

        article_data = []
        if new_articles:
            print("开始抓取本批文章数据...")
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(self.get_article_data, article): article for article in new_articles}
                for future in as_completed(futures):
                    article = futures[future]
                    try:
                        result = future.result(timeout=30)
                        article_data.append(result)
                        print(f"完成抓取文章数据: {article['url']} - {result['article_title']}")
                    except Exception as e:
                        print(f"任务失败: {article['url']} - {str(e)}")

        print(f"本批完成 {len(article_data)} 篇新文章数据的抓取，开始下载图片...")

        for data in article_data:
            if data['article_dir']:
                image_list = self.download_images_batch(data['images'], data['article_dir'])
                # 检查是否有图片下载失败（非 404 错误）
                if any(img['status'] == 'failed' for img in image_list):
                    print(f"文章 {data['article_title']} 有图片下载失败，跳过标记为完成")
                    continue
                # 检查是否有图片下载成功
                if any(img['status'] in ['success','already_exists'] for img in image_list):
                    results.append({
                        'article_title': data['article_title'],
                        'article_url': data['article_url'],
                        'images': image_list
                    })
                    self.completed_articles += 1
                    print(f"已抓取: {data['article_title']} ({len(image_list)}张图片) 【{self.completed_articles}/{self.total_articles}】")
            else:
                print(f"文章 {data['article_title']} 无有效图片目录。文章链接: {data['article_url']}")

        return results

    async def run(self):
        existing_results = self.load_existing_results()
        existing_urls = {result['article_url'] for result in existing_results}
        results = existing_results
        self.completed_articles = len(existing_results)

        for batch_start in range(self.start_page, self.end_page + 1, self.save_interval):
            batch_end = min(batch_start + self.save_interval - 1, self.end_page)
            results = self.process_batch(batch_start, batch_end, existing_urls, results)
            self.save_results(results, f"photos18_data_partial_{batch_start // self.save_interval + 1}.json")

        self.save_results(results, "photos18_data_final.json")
        print("所有任务完成！")

if __name__ == "__main__":
    base_url = "https://www.photos18.com/cat/3/likes?per-page=100"
    start_page = 1
    end_page = None
    max_workers = 20
    max_retries = 3
    save_interval = 1  # 每 1 页处理一批
    max_concurrent_downloads = 50

    scraper = PhotoScraper(
        base_url,
        start_page=start_page,
        end_page=end_page,
        max_workers=max_workers,
        max_retries=max_retries,
        save_interval=save_interval,
        max_concurrent_downloads=max_concurrent_downloads
    )
    asyncio.run(scraper.run())
