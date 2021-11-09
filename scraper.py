"""
MeLi Brazil leads scraper per categories
Author: Alain Moré
Date: Nov 2021
"""

import csv
import json
import logging
import re
import time
import warnings
from datetime import datetime
from typing import NoReturn

import jinja2
import pandas as pd
from bs4 import BeautifulSoup
from bs4.element import ProcessingInstruction
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    filename="log_scraper.log",
    filemode="a",
    format="%(asctime)s :: %(levelname)s :: %(message)s",
)
log = logging.getLogger("__name__")

chrome_options = webdriver.ChromeOptions()
prefs = {"profile.default_content_setting_values.notifications": 2}
chrome_options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(chrome_options=chrome_options)
wait = WebDriverWait(driver, 4)

ROOT_URL = "https://lista.mercadolivre.com.br/"
MAX_PAGES = 2
existing_sellers = []
processed_sellers = []


def get_existing_sellers(connection):
    """
    Obtiene vendedores previamente scrapeados, para evitar guardar vendedores duplicados
    """
    log.info("Obteniendo vendedores existentes")
    params = dict(COUNTRY="BR")

    select = """
        SELECT VENDOR FROM {{COUNTRY}}_WRITABLE.MELI_SELLERS
    """
    template = jinja2.Environment(loader=jinja2.BaseLoader).from_string(select)
    formatted_template = template.render(**params)
    log.info(formatted_template)

    try:
        df = pd.read_sql_query(con=connection, sql=formatted_template)
        log.info(df)
        for element in df.values.tolist():
            existing_sellers.append(element[0])
    except Exception as ex:
        log.info("Error 1")
        log.error(ex)

    log.info(existing_sellers)
    log.info("")
    log.info("")
    log.info("")
    log.info("")
    log.info("")
    log.info("")


def insert_lead(connection, seller_info):
    """
    Inserta lead en snowflake
    """
    params = dict(
        LOCATION_FILTER=seller_info[0],
        VENDOR=seller_info[1],
        CATEGORY=seller_info[2],
        MELI_URL=seller_info[3],
        EXPERIENCE=seller_info[4],
        SALES=seller_info[5],
        SALES_PERIOD=seller_info[6],
        MELI_STATUS=seller_info[7],
        TOTAL_RATINGS=seller_info[8],
        POSITIVE_RATINGS=seller_info[9],
        NEUTRAL_RATINGS=seller_info[10],
        NEGATIVE_RATINGS=seller_info[11],
        MAIN_METRIC_1=seller_info[12],
        MAIN_METRIC_2=seller_info[13],
        LOCATION_MELI=seller_info[14],
        SCRAPE_DATETIME=datetime.now(),
    )

    insert = """
        insert into {{COUNTRY}}_WRITABLE.STORE_LEADS_MELI_BY_KEYWORD
        (LOCATION_FILTER,VENDOR,CATEGORY,MELI_URL,EXPERIENCE,
        SALES,SALES_PERIOD,MELI_STATUS,TOTAL_RATINGS,
        POSITIVE_RATINGS,NEUTRAL_RATINGS,NEGATIVE_RATINGS,
        MAIN_METRIC_1,MAIN_METRIC_2,LOCATION_MELI,SCRAPE_DATETIME)
        values( '{{LOCATION_FILTER}}','{{VENDOR}}','{{CATEGORY}}','{{MELI_URL}}','{{EXPERIENCE}}',
        '{{SALES}}','{{SALES_PERIOD}}','{{MELI_STATUS}}','{{TOTAL_RATINGS}}',
        '{{POSITIVE_RATINGS}}','{{NEUTRAL_RATINGS}}','{{NEGATIVE_RATINGS}}',
        '{{MAIN_METRIC_1}}','{{MAIN_METRIC_2}}','{{LOCATION_MELI}}','{{SCRAPE_DATETIME}}');
    """
    template = jinja2.Environment(loader=jinja2.BaseLoader).from_string(insert)
    formatted_template = template.render(**params)
    log.info(formatted_template)
    try:
        df_insert = pd.read_sql_query(con=connection, sql=formatted_template)
        log.info("Insert OK")
        log.info(df_insert)
    except Exception as ex:
        log.info("Insert ERROR")
        log.error(ex)


def get_DOM(url):
    """
    Regresa el DOM de la página actual como un objeto bs4
    """
    attempts = 0
    while attempts < 10:
        try:
            time.sleep(1.5)
            driver.get(url)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            break
        except:
            log.exception(
                "*********ERROR DE CONEXIÓN. ESPERANDO 10 SEGUNDOS PARA INTENTAR OTRA "
                "VEZ.******"
            )
            attempts = attempts + 1
            time.sleep(10)
    return soup


def get_leads(connection):
    """
    Navigates MeLi BR and scrapes the required information for each seller
    """
    with open("categories.csv", "r") as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            city = row["city"]
            category = row["category"]
            results = []
            results_soup = get_DOM((ROOT_URL + city + "/" + category).replace('"', ""))

            i = 0
            while i <= MAX_PAGES:
                # Algunos resultados aparecen como grid y otros como stack,
                # hay que considerar ambos casos
                results_list = results_soup.find_all(
                    "ol", {"class": "ui-search-layout ui-search-layout--stack"}
                )
                if results_list:
                    i = i + 1
                    for result_wrapper in results_list:
                        results = results + result_wrapper.find_all(
                            "a",
                            {"class": "ui-search-item__group__element ui-search-link"},
                        )
                    log.info(
                        "%i resultados encontrados STACK %s, página %i",
                        len(results),
                        category,
                        i,
                    )
                else:
                    results_list = results_soup.find_all(
                        "ol", {"class": "ui-search-layout ui-search-layout--grid"}
                    )
                    if results_list:
                        i = i + 1
                        for result_wrapper in results_list:
                            results = results + result_wrapper.find_all(
                                "a",
                                {"class": "ui-search-result__content ui-search-link"},
                            )
                        log.info(
                            "%i resultados encontrados GRID %s, página %i",
                            len(results),
                            category,
                            i,
                        )

                # Nos movemos a la siguiente página
                next_page_tag = results_soup.find("a", {"title": "Seguinte"})
                if next_page_tag:
                    log.info("Siguiente pagina...")
                    results_soup = get_DOM(next_page_tag["href"])
                else:
                    log.info("No mas paginas...")
                    break

            if results:
                results_size = len(results)
                log.info(">>>> Se encontraron %i resultados", results_size)
                i = 0
                for result in results:
                    i = i + 1
                    log.info(">>> Resultado %i de %i", i, results_size)
                    vendor_name = ""
                    vendor_link = ""
                    experience = ""
                    sales = 0
                    sales_period = 0
                    status = ""
                    total_scores = 0
                    positive_scores = 0
                    negative_scores = 0
                    neutral_scores = 0
                    main_metric_1 = ""
                    main_metric_2 = ""
                    location = ""
                    log.info("")
                    log.info(">>>>>>>>>>>")
                    log.info(result.text.strip())
                    log.info(result["href"])
                    product_soup = get_DOM(result["href"])
                    vendor_link_tag = product_soup.find(
                        "a", {"class": "ui-pdp-media__action ui-box-component__action"}
                    )
                    if vendor_link_tag:
                        vendor_link = vendor_link_tag["href"]
                        log.info(vendor_link)
                        vendor_soup = get_DOM(vendor_link)
                        vendor_name_tag = vendor_soup.find(
                            "h3", {"id": "store-info__name"}
                        )
                        if vendor_name_tag:
                            vendor_name = (vendor_name_tag.text.strip()).replace(
                                " Loja oficial", ""
                            )
                            vendor_name = vendor_name.replace("'", "")
                            log.info(vendor_name)
                        else:
                            brand_tag = vendor_soup.find("h3", {"id": "brand"})
                            if brand_tag:
                                vendor_name = (brand_tag.text.strip()).replace(
                                    " Loja oficial", ""
                                )
                            else:
                                log.info("Sin nombre de vendedor")

                        experience_tag = vendor_soup.find("p", {"class": "experience"})
                        if experience_tag:
                            experience_tmp = experience_tag.text.strip()
                            experience = (experience_tmp.split(" vendendo")[0]).strip()
                            log.info(experience)
                        else:
                            log.info("Sin info de experiencia")

                        sales_tag = vendor_soup.find(
                            "p", {"class": "seller-info__subtitle-sales"}
                        )
                        if sales_tag:
                            sales_tmp = sales_tag.text.strip()
                            sales = re.search(r"\d+", sales_tmp).group()
                            log.info(sales)

                            sales_period = " ".join(sales_tmp.split()[-4:])
                            log.info(sales_period)
                        else:
                            log.info("Sin info de ventas")

                        status_tag = vendor_soup.find("p", {"class": "message__title"})
                        if status_tag:
                            status = status_tag.text.strip()
                            log.info(status)
                        else:
                            log.info("Sin info de estatus")

                        location_tag = vendor_soup.find(
                            "p", {"class": "location-subtitle"}
                        )
                        if location_tag:
                            location = location_tag.text.strip()
                            log.info(location)
                        else:
                            log.info("Sin info de ubicación")

                        score_tags = vendor_soup.findChildren(
                            "span", attrs={"id": "feedback_good"}
                        )
                        if score_tags:
                            positive_scores = score_tags[0].text.strip()
                            start = positive_scores.find("(") + len("(")
                            end = positive_scores.find(")")
                            positive_scores = int(positive_scores[start:end])

                            neutral_scores = score_tags[1].text.strip()
                            start = neutral_scores.find("(") + len("(")
                            end = neutral_scores.find(")")
                            neutral_scores = int(neutral_scores[start:end])

                            negative_scores = score_tags[2].text.strip()
                            start = negative_scores.find("(") + len("(")
                            end = negative_scores.find(")")
                            negative_scores = int(negative_scores[start:end])

                            total_scores = (
                                positive_scores + neutral_scores + negative_scores
                            )
                            log.info(
                                "Pos: %i, Neu: %i, Neg: %i, Total: %i",
                                positive_scores,
                                neutral_scores,
                                negative_scores,
                                total_scores,
                            )


def main():
    """
    Función main, ejecuta el proceso paso por paso
    """
    snowflake_configuration = {
        "url": "hg51401.snowflakecomputing.com",
        "account": "hg51401",
        "user": "ALAIN.MORE@RAPPI.COM",
        "authenticator": "externalbrowser",
        "port": 443,
        "warehouse": "GROWTH_ANALYSTS",
        "role": "GROWTH_ROLE",
        "database": "fivetran",
    }
    engine = create_engine(URL(**snowflake_configuration))
    connection = engine.connect()
    try:
        get_existing_sellers(connection)
        get_leads(connection)
    finally:
        connection.close()
        engine.dispose()
        log.info("Engine y connection cerrados")


if __name__ == "__main__":
    main()
