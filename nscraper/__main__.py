import click
from . import ComercioSource

@click.command()
@click.option('--filename', '-f', prompt='Output file')
@click.option('--suburl', default='archivo')
@click.option('--tags', default=[], help='tags separated by comma (t1,t2,t3)')
@click.option('--days', default=1, help='number of days to look back')
@click.option('--threads', default=-1)
@click.option('--readmode', default='a', help="'a' or 'w'")
@click.option('--ignoretoday/--no-ignoretoday', default=True,
			  help='ignore data from current day')
@click.option('--chunksize', '-cs', default=64)
def main(filename, suburl, tags, days, threads, readmode, ignoretoday, chunksize):
	if isinstance(tags, str):
		tags = tags.split(',')
	source = ComercioSource(filename, sub_url=suburl, tags=tags, n_days=days,
						    n_threads=threads, read_mode=readmode,
						    ignore_today=ignoretoday, chunk_size=chunksize)

if __name__ == '__main__':
	main()