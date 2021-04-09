import requests
from bs4 import BeautifulSoup
import pandas as pd 
from pandas import DataFrame
import re
import requests.exceptions
from urllib.parse import urlsplit
from urllib.parse import urlparse
from collections import deque
import csv

data_path = 'zingnews_example.csv'

def crawNewsData(response, url):
		try:
			soup = BeautifulSoup(response.content, "html.parser")
		except:
			return
		try:
			body = soup.find('div', class_="the-article-body")
			content = ' '.join([text.text for text in body.findChildren("p", recursive=False)])
		except:
			return
		try:
			title = soup.find('h1', class_='the-article-title').text
		except:
			title = None
		try:
			summary = soup.find('p', class_='the-article-summary').text 
		except:
			summary = None
		try:
			date = soup.find('li', class_='the-article-publish').text
		except:
			date = None
		try:
			head = soup.find('p', class_='the-article-category')
			categorys = [category.attrs['title'] for category in head.find_all('a')]
		except: 
			categorys = None

		return [title, summary, date, categorys, content, url]


def craw_url(base_url):
  
  # a queue of urls to be crawled next
	# a set of urls that we have already processed 
	try:
		with open(data_path, mode='r', encoding='utf-8', newline='') as file:
			reader = csv.reader(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			urls = [row[-1]for row in reader]
			processed_urls = set(urls)
			new_urls = deque(urls+[base_url])
	except:
		new_urls = deque([base_url])
		processed_urls = set()
	# a set of domains inside the target website
	local_urls = set()
	# a set of domains outside the target website
	foreign_urls = set()
	# a set of broken urls
	broken_urls = set()
	# process urls one by one until we exhaust the queue
	while len(new_urls):  
		# move url from the queue to processed url set    
		url = new_urls.popleft()
		# print the current url  
		print("Processing %s" % url)  
		try:    
			response = requests.get(url)
		except:    
			# add broken urls to it’s own set, then continue    
			continue
		# print the current url 
		if url not in processed_urls:
			row = crawNewsData(response, url)
			try:
				with open(data_path, mode='a', encoding='utf-8', newline='') as file:
					writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
					writer.writerow(row)
					#print([title, summary, date, categorys, content, url])
					print('done')
			except:
				pass
		else:
			print('already crawled')
		processed_urls.add(url)
		# extract base url to resolve relative links
		parts = urlsplit(url)
		base = "{0.netloc}".format(parts)
		strip_base = base.replace("www.", "")
		base_url = "{0.scheme}://{0.netloc}".format(parts)
		path = url[:url.rfind('/')+1] if '/' in parts.path else url
		soup = BeautifulSoup(response.text, "lxml")
		for link in soup.find_all('a'):    
			# extract link url from the anchor 
			anchor = link.attrs["href"] if "href" in link.attrs else ''
			if anchor.startswith('/'):        
				local_link = base_url + anchor        
				#local_urls.add(local_link)    
			elif strip_base in anchor: 
				local_link = anchor       
				#local_urls.add(anchor)    
			elif not anchor.startswith('http'):        
				local_link = path + anchor        
				#local_urls.add(local_link)   

			local_urls.add(local_link)
			if not local_link in new_urls and not local_link in processed_urls:        
				new_urls.append(local_link)

if __name__ == '__main__':
	base_url = 'https://zingnews.vn/'
	with open(data_path, mode='w', encoding='utf-8', newline='') as file:
		writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		writer.writerow(['title', 'summary', 'date', 'categorys', 'content','url'])
	craw_url(base_url)
	# url = 'https://zingnews.vn/yeu-cau-cac-tinh-mien-trung-chu-dong-ung-pho-sieu-bao-goni-post1148193.html'
	# response = requests.get(url)
	# crawNewsData(response, url)