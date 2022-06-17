import pandas as pd
import requests
import urllib.request
from urllib.parse import urlparse
from bs4 import BeautifulSoup

def get_sitemap(url):
    response = urllib.request.urlopen(url)
    xml = BeautifulSoup(response,
                        'lxml-xml',
                        from_encoding=response.info().get_param('charset'))
    return xml

def get_sitemap_type(xml):
    sitemapindex = xml.find_all('sitemapindex')
    sitemap = xml.find_all('urlset')

    if sitemapindex:
        return 'sitemapindex'
    elif sitemap:
        return 'urlset'
    else:
        return

def get_child_sitemaps(xml):
    sitemaps = xml.find_all("sitemap")
    output = []

    for sitemap in sitemaps:
        output.append(sitemap.findNext("loc").text)
    return output


def sitemap_to_dataframe(xml, name=None, data=None, verbose=False):
    df = pd.DataFrame(columns=['loc', 'changefreq', 'priority', 'domain', 'sitemap_name'])
    urls = xml.find_all("url")
    for url in urls:
        if xml.find("loc"):
            loc = url.findNext("loc").text
            parsed_uri = urlparse(loc)
            domain = '{uri.netloc}'.format(uri=parsed_uri)
        else:
            loc = ''
            domain = ''

        if xml.find("changefreq"):
            changefreq = url.findNext("changefreq").text
        else:
            changefreq = ''

        if xml.find("priority"):
            priority = url.findNext("priority").text
        else:
            priority = ''

        if name:
            sitemap_name = name
        else:
            sitemap_name = ''

        row = {
            'domain': domain,
            'loc': loc,
            'changefreq': changefreq,
            'priority': priority,
            'sitemap_name': sitemap_name,
        }

        if verbose:
            print(row)

        df = df.append(row, ignore_index=True)
    return df

def get_all_urls(url):
    xml = get_sitemap(url)
    sitemap_type = get_sitemap_type(xml)

    if sitemap_type =='sitemapindex':
        sitemaps = get_child_sitemaps(xml)
    else:
        sitemaps = [url]

    df = pd.DataFrame(columns=['loc', 'changefreq', 'priority', 'domain', 'sitemap_name'])

    for sitemap in sitemaps:
        sitemap_xml = get_sitemap(sitemap)
        df_sitemap = sitemap_to_dataframe(sitemap_xml, name=sitemap)

        df = pd.concat([df, df_sitemap], ignore_index=True)

    return df

if __name__ == '__main__':
    url_finished = "https://www.ql2.com/sitemap.xml"
    xml_finished = get_sitemap(url_finished)

    df = get_all_urls(url_finished)
    df_urls = df[['loc']]

    df_pages = pd.DataFrame(columns=['url', 'status_code', 'history'])

    for index, row in df.iterrows():
        request = requests.get(row['loc'])
        status_code = request.status_code
        history = request.history

        page = {
            'url': row['loc'],
            'status_code': status_code,
            'history': history
        }

        df_pages = df_pages.append(page, ignore_index=True)

    df_pages.to_csv('ql2_checks.csv', index=False)
    badurls = df_pages.url[df_pages.status_code != 200]
    print(badurls)