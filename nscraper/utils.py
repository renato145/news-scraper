import requests
from bs4 import BeautifulSoup

def get_bs(url):
	req = requests.get(url)
	assert req.status_code == 200, f'Request status {req.status_code} on {url}.'
	return BeautifulSoup(req.content, 'html5lib')

def url_join(*args):
	args = [i if i[-1] == '/' else i+'/' for i in args]
	args = [i[1:] if i[0] == '/' else i for i in args]
	return ''.join(args)[:-1]

def get_links(url):
	bs = get_bs(url)
	return set([l.get('href') for l in bs.find_all('a') if l.get('href') is not None])