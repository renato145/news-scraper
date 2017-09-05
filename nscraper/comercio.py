import operator, requests, os, multiprocessing
import numpy as np
import pandas as pd
from .utils import get_bs, url_join, get_links
from bs4 import BeautifulSoup
from time import time
from datetime import date, datetime, timedelta
from functools import partial
from collections import Counter
from urllib.parse import urlsplit
from concurrent.futures import ThreadPoolExecutor

def scrape(link, out_file):
	# article object
	url = link[1]
	base_path = '://'.join(urlsplit(url)[:2])
	link_id = link[0]
	try:
		bs = get_bs(url).select('#article-default')
	except Exception as e:
		return f'skipped-{str(e)} ({url}).'
	
	if len(bs) == 0:
		return f'skipped-No element #article-default found ({url}).'
	
	# get title & summary
	bs = bs[0]
	title = bs.find(class_='news-title')
	title = '' if title is None else f'{title.get_text()!r}'
	title = title.replace('"', "'")
	summary = bs.find(class_='news-summary')
	summary = '' if summary is None else f'{summary.get_text()!r}'
	summary = summary.replace('"', "'")

	errs = []
	# get date
	try:
		date = bs.find(class_='news-date').get_text().strip().split(' ')[0]
		date = datetime.strptime(date, '%d.%m.%Y').strftime('%Y-%m-%d')
	except Exception as e:
		date = ''
		errs.append('article date: '+str(e))

	# get content
	try:
		text = [p.get_text() for p in bs.find(class_='article-content').findAll(
				class_=['parrafo first-parrafo', 'parrafo first-parrafo '])]
		if text[-1].startswith('MIRA TAMBI'): text.pop()
		text = '\n'.join(text)
		text = text.replace('"', "'")
	except Exception as e:
		text = ''
		errs.append('article text: '+str(e))

	# get list of related news
	try:
		related_news = set([url_join(base_path, n.get('href')) for n in bs.find(
			class_='article-content').find(class_='news-related').findAll('a')])
		related_news = ' '.join(related_news)
	except Exception as e:
		related_news = ''
		errs.append('related news: '+str(e))

	# get tags
	try:
		tags = set([t.get_text().lower().replace(' ', '-')
				   for t in bs.find(class_='news-tags').findAll('h2')])
		tags = ' '.join(tags | link[2])
	except Exception as e:
		tags = ''
		errs.append('tags: '+str(e))

	# save file
	line = f'comercio,{link_id},{url},{date},"{title}","{summary}","{text}",{related_news},{tags}\n'
	with open(out_file, 'a') as f:
		f.writelines(line)

	if len(errs) == 0:
		return '1'
	else:
		errs = ', '.join(errs)
		return errs + f' ({url})'

class ComercioSource(object):
	"""docstring for Comercio"""
	def __init__(self, out_file, url='http://elcomercio.pe', sub_url='archivo', tags=[], n_days=1,
			     read_mode='a', n_threads=-1, auto_mode=True):
		super(ComercioSource, self).__init__()
		assert read_mode in ['a', 'w'], f"{read_mode} not in ('a','w')."
		assert n_days > 0, f'{n_days} invalid number of days.'
		if isinstance(tags, str):
			tags = [tags]
		self.url = url
		self.tags = tags
		self.n_days = n_days
		self.sub_url = 'archivo/todas' if sub_url == 'archivo' else sub_url
		self.out_file = out_file
		self.read_mode = read_mode
		self.n_threads = multiprocessing.cpu_count() if n_threads == -1 else n_threads
		self.check_file()
		self.load_links()
		if auto_mode:
			self.filter_links()
			self.start()

	def check_file(self):
		if self.read_mode == 'w' or not os.path.exists(self.out_file):
			with open(self.out_file, self.read_mode) as f:
				f.writelines('source,link_id,url,date,title,summary,text,related_news,tags\n')

	def load_links(self, verbose=True):
		t0 = time()
		link = url_join(self.url, self.sub_url)
		links = [link]
		days_minus = lambda x: (date.today() - timedelta(days=x)).strftime('%Y-%m-%d')
		if self.n_days > 1:
			links += [url_join(link, days_minus(i)) for i in range(1, self.n_days)]

		if verbose:
			print(f'Gettings links from {len(links)} pages ({self.n_threads} threads)...')

		with ThreadPoolExecutor(self.n_threads) as executor:
			res = executor.map(get_links, links)
		
		res = [j for i in res for j in i]
		self.links = [(l.split('-')[-1], url_join(self.url, l), set(l.split('/')[1:-1]))
					  for l in res if l.startswith('/')
					  if l.split('-')[-1].isdigit()]

		if verbose:
			print('Elapsed: %.2fs' % (time() - t0))
			print(f'Got {len(self.links)} links.')
	
	def list_link_tags(self, return_counts=True):
		tags = [ll for l in self.links for ll in l[2]]
		c = Counter(tags)
		
		if return_counts:
			return sorted(c.items(), key=operator.itemgetter(1), reverse=True)
		else:
			return set(c)

	def filter_links(self, tags=[], verbose=True):
		links = self.links
		if isinstance(tags, str):
			tags = [tags]
		if len(tags) == 0:
			tags = self.tags
		# filter tags
		if len(tags) > 0:
			tags = set(tags)
			links = [l for l in links if len(l[2] & tags) > 0]
		# check repeated entries on file
		if len(links) > 0:
			df = pd.read_csv(self.out_file)
			ids, _, _ = zip(*links)
			mask = np.in1d(ids, df['link_id'].astype(str), assume_unique=True, invert=True)
			links = [i for i,j in zip(links, mask) if j]

		if verbose: 
			print(f'{len(links)} links to process.')
		self.links = links

	def start(self, verbose=True):
		t0 = time()
		if verbose:
			print(f'Scraping {len(self.links)} links on {self.n_threads} threads...')

		fun = partial(scrape, out_file=self.out_file)

		with ThreadPoolExecutor(self.n_threads) as executor:
			res = executor.map(fun, self.links)
		
		self.last_result = list(res)

		if verbose:
			n_rows = np.sum([0 if r.startswith('skipped') else 1 for r in self.last_result])
			print('Elapsed: %.2fs' % (time() - t0))
			print(f'{n_rows} rows added.')

