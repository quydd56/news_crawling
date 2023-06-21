# import lib
import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import feedparser
import re
import pyodbc
import pandas as pd
import dateutil.parser
import time


# def check url
def is_valid(url):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


# def crawl url
def get_all_rss_links(url):
    # initialize the set of links (unique links)
    internal_urls = set()
    external_urls = set()
    category = set()
    rss_links_cols = ['website', 'category', 'link']
    lst = []
    table_link = ()
    category_name = ()
    """
    Returns all URLs that is found on `url` in which it belongs to the same website
    """
    # all URLs of `url`
    urls = set()
    # domain name of the URL without the protocol
    domain_name = urlparse(url).netloc
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    for a_tag in soup.findAll('a'):
        href = a_tag.attrs.get("href")
        if not a_tag.attrs.get("title") is None:
            category_name = a_tag.attrs.get("title")
        elif not a_tag.text is None:
            category_name = a_tag.text
        if href == "" or href is None:
            # href empty tag
            continue
        # join the URL if it's relative (not absolute link)
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        # remove URL GET parameters, URL fragments, etc.
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if not is_valid(href):
            # not a valid URL
            continue
        if href in internal_urls:
            # already in the set
            continue
        if href[0:6] == "mailto":
            # mailto exception
            continue
        if not href.endswith('rss'):
            # mailto exception
            continue
        if domain_name not in href:
            # external link
            if href not in external_urls:
                # print(f"{GRAY}[!] External link: {href}{RESET}")
                external_urls.add(href)
            continue
        # print(f"{GREEN}[*] Internal link: {href}{RESET}")
        urls.add(href)
        internal_urls.add(href)
        category.add(category_name)
        lst.append([url, category_name, href])
        table_link = pd.DataFrame(lst, columns=rss_links_cols)
    return table_link


# get_all_rss_links('https://dantri.com.vn/rss.htm')


# def parse news
def parse_news(bm_mom_feeds_link):
    d = feedparser.parse(bm_mom_feeds_link)
    # Declare storage
    cols = ['link', 'date', 'title', 'summarize']
    lst = []
    # parse news
    for entry in d.entries:
        # print(entry.title + "\n" + entry.description + "\n" + str(entry.published))
        # if not entry.description.startswith('<'):
        lst.append([bm_mom_feeds_link, entry.get('published'), re.sub("<.*?>", "", entry.get('title')),
                    re.sub("<.*?>", "", entry.get('description'))])
    df1 = pd.DataFrame(lst, columns=cols)
    return df1


# measure
start_time = time.time()
print("Crawling RSS link...")

# Get multiple source
source_news = ('https://thanhnien.vn/rss.html',
               'https://dantri.com.vn/rss.htm',
               'https://cafef.vn/rss.chn',
               'https://vnexpress.net/rss',
               'https://tuoitre.vn/rss.htm',
               'https://nld.com.vn/rss.htm'
               )
table_cols = ['website', 'category', 'link']
all_link = pd.DataFrame([], columns=table_cols)
table = pd.DataFrame([], columns=table_cols)
for source in source_news:
    table = get_all_rss_links(source)
    all_link = all_link.append(table, ignore_index=True)

# export data to excel
# all_link.to_excel(r"E:\project\20210914 stock data\Pycharm\output_all_rss_links.xlsx")

# measure
print("Crawling RSS link done! {0:.3f}s".format(time.time() - start_time))
print("Crawling RSS detail...")
# parse_news('https://dantri.com.vn/kinh-doanh/doanh-nghiep.rss')

news_table = pd.DataFrame([], columns=['link', 'date', 'title', 'summarize'])
for link in all_link['link'].tolist():
    parsed_table = parse_news(link)
    news_table = news_table.append(parsed_table)

# export to excel
# news_table.to_excel(r"E:\project\20210914 stock data\Pycharm\output_rss_parsed.xlsx", engine='xlsxwriter')

# measure
print("Crawling RSS detail done! {0:.3f}s".format(time.time() - start_time))
print("updating database...")
start_time = time.time()

# connect to SQL server
server = 'DESKTOP-Q21C3LU\SQLEXPRESS'
database = 'stockdata_qdd'
username = 'sa'
password = 'abc@1234'
cnxn = pyodbc.connect(
    'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()
# Insert all_link into SQL Server:
cursor.execute("truncate table Stockdata_qdd.dbo.all_link_tmp")

for index, row in all_link.iterrows():
    cursor.execute("INSERT INTO Stockdata_qdd.dbo.all_link_tmp (website,category,link) values(?,?,?)", row.website,
                   row.category, row.link)

cursor.execute("insert into Stockdata_qdd.dbo.news_crawl_log (time, table_name, row)"
               "      values"
               "     ("
               "         SYSDATETIME(),"
               "         'all_link',"
               "         (select count(1)"
               "          from Stockdata_qdd.dbo.[all_link_tmp] b"
               "          where not exists"
               "            (select a.link"
               "            from Stockdata_qdd.dbo.[all_link] a"
               "            where a.link = b.link"
               "            )"
               "         )"
               "     )")

cursor.execute(" insert into Stockdata_qdd.dbo.[all_link]"
               " select * from Stockdata_qdd.dbo.[all_link_tmp] b  "
               "where not exists "
               "	(select a.link"
               "     from Stockdata_qdd.dbo.[all_link] a"
               "     where a.link = b.link)")
# Insert news_parsed into SQL Server:
cursor.execute("truncate table Stockdata_qdd.dbo.news_parsed_tmp")

for index, row in news_table.iterrows():
    cursor.execute("INSERT INTO Stockdata_qdd.dbo.news_parsed_tmp (link,date,title, summarize) values(?,?,?,?)",
                   row.link, dateutil.parser.parse(row.date), row.title, row.summarize)

# Delete duplicate row from trang chu
cursor.execute("with CTE as ("
               "    select *"
               "    from Stockdata_qdd.dbo.[news_parsed_tmp]"
               "   where title+link in"
               "    ("
               "        select c.title+c.link "
               "        from"
               "        ("
               "            SELECT a.[date],a.link, a.summarize, a.title, b.category, b.website,"
               "                    count(a.title) OVER ("
               "                        partition by a.title"
               "                    ) as cnt,"
               "                    ROW_NUMBER() OVER ("
               "                        partition by a.title"
               "                        order by b.category"
               "                    ) as stt"
               "            from  Stockdata_qdd.dbo.[news_parsed_tmp] a"
               "            left join Stockdata_qdd.dbo.all_link b on a.link = b.link"
               "        ) c"
               "        where c.cnt > 1 and c.stt > 1 "
               "        and rtrim(ltrim(upper(c.category))) in (rtrim(ltrim(upper(N'Trang Chủ'))),rtrim(ltrim(upper(N'Tin mới nhất')))) "
               "    )"
               ")"
               "delete from CTE where 1=1"
               )
# delete remain double
cursor.execute("with CTE as ("
               "    select *"
               "    from Stockdata_qdd.dbo.[news_parsed_tmp]"
               "   where title+link in"
               "    ("
               "        select c.title+c.link "
               "        from"
               "        ("
               "            SELECT a.[date],a.link, a.summarize, a.title, b.category, b.website,"
               "                    count(a.title) OVER ("
               "                        partition by a.title"
               "                    ) as cnt,"
               "                    ROW_NUMBER() OVER ("
               "                        partition by a.title"
               "                        order by b.category"
               "                    ) as stt"
               "            from  Stockdata_qdd.dbo.[news_parsed_tmp] a"
               "            left join Stockdata_qdd.dbo.all_link b on a.link = b.link"
               "        ) c"
               "        where c.cnt > 1 and c.stt > 1 "
               "    )"
               ")"
               "delete from CTE where 1=1"
               )

cursor.execute("insert into Stockdata_qdd.dbo.news_crawl_log (time, table_name, row)"
               "      values"
               "     ("
               "         SYSDATETIME(),"
               "         'news_parsed',"
               "         (select count(1)"
               "          from Stockdata_qdd.dbo.[news_parsed_tmp] b"
               "          where not exists"
               "            (select a.summarize"
               "            from Stockdata_qdd.dbo.[news_parsed] a"
               "            where a.summarize = b.summarize"
               "            )"
               "         )"
               "     )")

cursor.execute(" insert into Stockdata_qdd.dbo.[news_parsed]"
               " select * from Stockdata_qdd.dbo.[news_parsed_tmp] b  "
               "where not exists "
               "	(select a.summarize"
               "     from Stockdata_qdd.dbo.[news_parsed] a"
               "     where a.summarize = b.summarize)")
# Close cursor
cnxn.commit()
cursor.close()

# measure
print("Done ! {0:.3f}s".format(time.time() - start_time))
print("Sending telegram message...")
start_time = time.time()

# %%
import requests
import pyodbc
import pandas as pd
import itertools
from datetime import datetime

# insert data from csv file into dataframe.
server = 'DESKTOP-Q21C3LU\SQLEXPRESS'
database = 'stockdata_qdd'
username = 'sa'
password = 'abc@1234'
cnxn = pyodbc.connect(
    'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()
title = cursor.execute("SELECT title "
                       "FROM Stockdata_qdd.dbo.[news_parsed_tmp] "
                       "where dateadd(hour, datediff(hour, 0, date), 0) = dateadd(hour, datediff(hour, 0, dateadd(hour, -1, SYSDATETIME())), 0)")


# Insert all_link into SQL Server:
def telegram_bot_sendtext(bot_message):
    bot_token = 
    bot_chatID = 
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    return response.json()


title_list = list(itertools.chain(*title))
telegram_bot_sendtext('-------------------------------------------------------')
telegram_bot_sendtext('Time: ' + str(datetime.now().strftime("%m/%d/%Y, %H:%M")))
telegram_bot_sendtext('Total: ' + str(len(title_list)) + ' titles')
telegram_bot_sendtext('----Start----')
for text in title_list:
    telegram_bot_sendtext(text)
telegram_bot_sendtext('----End----')
cursor.close()

# measure
print("Message sent! {0:.3f}s".format(time.time() - start_time))
