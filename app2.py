import requests
from bs4 import BeautifulSoup as bs
from payload import headers, proxies, user_agents
from time import sleep, time
import functools
import random
from loguru import logger
import sys
import csv
from datetime import datetime

logger.remove(0)
logger.add(sys.stderr, format="{time:D MMMM YYYY > HH:mm:ss} | {level} | {message}")
logger.add(".\\log\\runtime.log", rotation="10 MB", compression="zip")


def get_session(proxies=proxies, user_agents=user_agents, prev_proxy=None):
    session = requests.Session()
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'ru-RU,ru;q=0.9',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'Dnt': '1',
        }
    session.headers.update(headers)
    while True:
        proxy = random.choice(proxies)
        if proxy != prev_proxy:
            prev_proxy = proxy
            break
    session.proxies = {"http": proxy, "https": proxy}
    logger.info(f"Используем прокси: {session.proxies['http']}")
    return session


def get_html(url, session):
    try:
        html = session.get(url=url)
        return html.text
    except requests.Timeout:
        logger.warning('Timeout occurred')
        session = get_session()
        html = session.get(url=url, headers=headers)
        return html.text

    except requests.ConnectionError:
        logger.warning(f"Connection error occurred")
        session = get_session()
        html = session.get(url=url, headers=headers)
        return html.text

    except requests.TooManyRedirects:
        logger.warning("Too many redirects")
        session = get_session()
        html = session.get(url=url, headers=headers)
        return html.text

    except requests.RequestException as error:
        logger.warning(f"An error occurred while fetching {error}")
        session = get_session()
        html = session.get(url=url, headers=headers)
        return html.text
    

def get_soup(html):
    soup = bs(html, 'html.parser')
    return soup


def get_catalog(session):
    response = session.get('https://www.samsonopt.ru/zakaz')
    soup = bs(response.text, 'html.parser')
    catalog_tags = soup.find_all(class_='CatalogMenu__list CatalogMenu__list--sub')

    for item in catalog_tags:
        subcategory_tags = item.find_all(class_='CatalogMenu__rubric')
        for tag in subcategory_tags:
            for item in tag:
                yield f"https://www.samsonopt.ru{item.get('href')}"
    

def get_catalog_groups(url, session):
    response = session.get(url=url)
    soup = bs(response.text, 'html.parser')
    catalog_urls = {}
    if soup.find('div', class_='CatalogBubble m-b-1'):
        catalog_bubble = soup.find('div', class_='CatalogBubble m-b-1').find_all('a')
        for item in catalog_bubble:
            catalog_urls[item.text.strip()] = f"https://www.samsonopt.ru{item.get('href')}"
            # catalog_urls.append(f"https://www.samsonopt.ru{item.get('href')}")
    elif soup.find('div', class_='bigPhoto'):
        catalog_groups = soup.find('div', class_='bigPhoto').find_all('a')
        for item in catalog_groups:
            catalog_urls[item.text.strip()] = f"https://www.samsonopt.ru{item.get('href')}"
            # catalog_urls.append(f"https://www.samsonopt.ru{item.get('href')}")
    elif soup.find('div', class_='subCatalog'):
        subcatalog = soup.find('div', class_='subCatalog').find_all('a')
        for item in subcatalog:
            catalog_urls[item.text.strip()] = f"https://www.samsonopt.ru{item.get('href')}"
            # catalog_urls.append(f"https://www.samsonopt.ru{item.get('href')}")
    else:
        catalog_urls.append(url)
    return catalog_urls


def check_subcategory(soup):
    if soup.find('div', class_='CatalogBubble m-b-1'):
        return False
    else:
        return True
    

def get_pagination_urls(url, soup):
    result = []
    result.append(url+'&SHOW_MORE=Y')
    paginator_box = soup.find('div', class_='Paginator')
    if paginator_box:
        catalog_urls = paginator_box.find_all('a', class_='Paginator__text')
        for item in catalog_urls:
            result.append(f"https://www.samsonopt.ru{item.get('href')}")
    return result


def get_brand(soup):
    if soup.find('a', class_='ProductItem__brand'):
        return soup.find('a', class_='ProductItem__brand').text[18:]
    else:
        return ''


def get_id(soup):
    if soup.find('div', class_='ProductItem__infoContent').find('span', class_='ProductItem__code Badge Badge--code js-previewCode js-productCodeCopy'):
        return soup.find('div', class_='ProductItem__infoContent').find('span', class_='ProductItem__code Badge Badge--code js-previewCode js-productCodeCopy').text
    else:
        return ''
    

def get_party(soup):
    if soup.find('div', class_='ProductItem__infoContent').find('div', class_='ProductList__col ProductList__col--pt ProductList__col--package'):
        party = soup.find('div', class_='ProductItem__infoContent').find('div', class_='ProductList__col ProductList__col--pt ProductList__col--package').text.strip().replace('\xa0', '').replace(' ', '')
        return party.split('/')
    else:
        return ['', '', '']
    

def get_price(soup):
    if soup.get('data-ga-obj'):
        return eval(soup.get('data-ga-obj'))['price'].replace('.', ',')
    else:
        return ''


def get_name_url(soup):
    if soup.find('a', class_='ProductItem__link js-ga-item-link'):
        name = soup.find('a', class_='ProductItem__link js-ga-item-link')
        product_url = 'https://samsonopt.ru' + name.get('href')
        return name, product_url
    else:
        return ['', '', '']


def parse_catalog(url, session):
    html = get_html(url=url, session=session)
    soup = get_soup(html=html)
    product_content = soup.find_all('div', class_='ProductList__item js-ga-item-wrapper ProductList__item--rc js-itemContainer')
    for content in product_content:
        id = get_id(content)
        party_list = get_party(content)
        price = get_price(content)
        name, product_url = get_name_url(content)
        brand = get_brand(content)
            
        yield id, name.text, party_list[0],party_list[1], party_list[2], product_url, brand, price

        
def get_hierarchy(soup):
    breadcrumbs = soup.find('ul', class_='Breadcrumbs2__list')
    category = breadcrumbs.find_all('li', class_='Breadcrumbs2__item')
    subcategory = breadcrumbs.find_next('li', class_='Breadcrumbs2__item Breadcrumbs2__item--last')
    if len(category) == 5:
        return category[1].text, category[2].text, category[3].text, subcategory.text
    else:
        return category[1].text, category[2].text, '', subcategory.text
    

def timer(function):
    @functools.wraps(function)
    def wrapped_function(*args, **kwargs):
        started_time = time()
        result = function(*args, **kwargs)
        finished_time = time()
        elapsed = round(finished_time - started_time, 2)
        logger.info(f'Функция работала {elapsed} секунд(ы)')
        return result

    return wrapped_function


@timer
def main(session):
    logger.info(f'Программа запущена {time()}')
    file_index = datetime.now().strftime('%Y-%m-%d_%H-%M')
    result_file_path = f'.\\result\\result_{file_index}.csv'
    with open(result_file_path, mode="w", encoding='utf-8') as result_file, open('category2.txt') as f:
        names = ["Код",
                 "Наименование",
                 "Ком.уп",
                 "Мин.уп",
                 "Макс.уп",
                 "Брэнд",
                 "Цена",
                 "Ссылка на товар",
                 "Каталог",
                 "Категория",
                 "Подкатегория",
                 "Под-подкатегория"
                 ]
        file_writer = csv.DictWriter(result_file, delimiter = ";", lineterminator="\r", fieldnames=names)
        file_writer.writeheader()
        count = 5
        work_list = f.read().split('\n')
        for item in get_catalog(session=session):
            if item in work_list:
                catalog_group = get_catalog_groups(url=item, session=session)    
                for name, url in catalog_group.items():
                    if count == 0:
                        session.close
                        session = get_session()
                        count = 5
                    logger.info(f'До смены прокси {count} запрос(-a,-ов)')
                    logger.info(f'Анализируем страницу {name}')
                    count -= 1
                    html = get_html(url=url+'&SHOW_MORE=Y', session=session)
                    soup = get_soup(html=html)
                    logger.info('Проверяем если ли на этой странице нужная нам информация')
                    if check_subcategory(soup):
                        logger.info('Отлично! Необходимая информация найдена. Формируем список страниц для парсинга.')
                        pagination_url_list = get_pagination_urls(url=url, soup=soup)
                        category_l1,  category_l2, subcategory_l1,  subcategory_l2 = get_hierarchy(soup=soup)
                        pagination_url_count = len(pagination_url_list)
                        logger.info(f'Список страниц для парсинга получен. Страниц для парсинга: {pagination_url_count}')
                        for url in pagination_url_list:
                            pagination_url_count -= 1
                            logger.info(f'Парсим {url}')
                            for i in parse_catalog(url=url, session=session):
                                id, name, party1, party2, party3, product_url, brand, price = i
                                file_writer.writerow(
                                {"Код": id,
                                "Наименование": name,
                                "Ком.уп": party1,
                                "Мин.уп": party2,
                                "Макс.уп": party3,
                                "Брэнд": brand,
                                "Цена": price,
                                "Ссылка на товар": product_url,
                                "Каталог": category_l1,
                                "Категория": category_l2,
                                "Подкатегория": subcategory_l1,
                                "Под-подкатегория": subcategory_l2,
                                })
                            logger.success(f'Данные со страницы {url} успешно скопированы')
                            if pagination_url_count != 0:
                                time_to_steep = random.uniform(3, 5)
                                logger.info(f'Ждем {time_to_steep} секунды перед переходом на новую страницу...')
                                logger.info('='*30)
                                sleep(time_to_steep)
                    else:
                        logger.warning(f'На странице {url} нет интересующей нас информации.')
                    time_to_steep = random.uniform(5, 10)
                    logger.info(f'Ждем {time_to_steep} секунд перед переходом на другую категорию...')
                    logger.info('='*50)
                    sleep(time_to_steep)
    logger.success('Ура! все закончилось!')


if __name__ == "__main__":
    try:
        session = get_session()
        main(session=session)
    except Exception as e:
        logger.critical('Программа завершена некорректно')
        logger.critical(e)
    except KeyboardInterrupt:
        logger.warning('Программа завершена пользователем')
    finally:
        session.close
