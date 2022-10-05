import csv
from collections import defaultdict
from typing import Generator, Iterable, List

from pydantic import BaseModel
from tqdm import tqdm
import pycountry
from ftfy import fix_text

from geonames_api.models import GeonameItem, AlternativeName
from geonames_api.config import settings
from geonames_api.es_index_settings import (
    geoname_index_settings,
    geoname_index_mappings,
)
from elasticsearch import Elasticsearch, helpers


INCLUDE_FEATURE_CLASSES = {"A", "P", "L"}
INCLUDE_FEATURE_CODES = {
    # A:
    "ADM1",  # "a primary administrative division of a country, such as a state in the United States",
    "ADM2",  # a subdivision of a first-order administrative division
    "ADM3",  # a subdivision of a second-order administrative division
    "ADM4",  # a subdivision of a third-order administrative division
    "ADM5",  # a subdivision of a fourth-order administrative division
    "ADMD",  # an administrative division of a country, undifferentiated as to administrative level
    "PCLI",  # countries
    # P:
    "PPLC",  # capital
    "PPLA",  # seat of a first-order administrative division	seat of a first-order administrative division (PPLC takes precedence over PPLA)
    "PPLA2",  # seat of a second-order administrative division
    "PPLA3",  # seat of a third-order administrative division
    "PPLA4",  # seat of a fourth-order administrative division
    "PPLA5",  # seat of a fifth-order administrative division
    "PPL",  # a city, town, village, or other agglomeration of buildings where people live and work
    "PPLS",  # cities, towns, villages, or other agglomerations of buildings where people live and work
    "PPLX",  # section of populated place
    # L
    "RGN",  # region	an area distinguished by one or more observable physical or cultural characteristics
    "RGNE",  # a region of a country established for economic development or for statistical purposes
    "CTRB",  # a place where a number of businesses are located
    "PRT",
}


def get_country(country_code):
    """ """
    try:
        c = pycountry.countries.lookup(country_code)
    except LookupError:
        c = None
    return c


def get_index_geoname_items_it(
    all_countries_path: str,
    admin_codes_1: dict,
    admin_codes_2: dict,
    # place_id_to_alternative_names: dict,
    place_id_to_postal_codes: dict,
    include_countries: List[str] = None,
) -> Generator[GeonameItem, None, None]:
    """ """

    with open(all_countries_path) as f:
        #
        # total = sum(1 for x in f.read())
        # print(total)
        # f.seek(0)
        total = None
        reader = csv.reader(f, delimiter="\t")
        #
        for row in tqdm(reader, total=total):
            place_id = row[0]
            country_code = row[8]
            if include_countries and country_code not in include_countries:
                continue

            country = None
            if country_code:
                c = get_country(country_code)
                if c:
                    country = c.name

            feature_code = row[7] or None
            if feature_code not in INCLUDE_FEATURE_CODES:
                continue

            admin_code_1 = row[10] or None
            admin_code_2 = row[11] or None
            admin_name_1, admin_name_2 = None, None
            if admin_code_1:
                key_1 = f"{country_code}.{admin_code_1}"
                if key_1 in admin_codes_1:
                    admin_name_1 = admin_codes_1[key_1]["ascii_name"]

                if admin_code_2:
                    key_2 = f"{country_code}.{admin_code_1}.{admin_code_2}"
                    if key_2 in admin_codes_2:
                        admin_name_2 = admin_codes_2[key_2]["ascii_name"]

            # names
            place_name = fix_text(row[1])
            place_name_ascii = fix_text(row[2])
            alternatives_names_string = []
            if row[3].strip():
                alternatives_names_string = [fix_text(x) for x in row[3].split(",")]

            if place_name not in alternatives_names_string:
                alternatives_names_string.append(place_name)
            if place_name_ascii not in alternatives_names_string:
                alternatives_names_string.append(place_name_ascii)

            # add other names like combinaison of city, region, state, ...
            if admin_name_1 and admin_name_1 != place_name_ascii:
                alternatives_names_string.append(f"{place_name_ascii}, {admin_name_1}")
            if admin_name_1 and admin_name_2 and admin_name_2 != place_name_ascii:
                alternatives_names_string.append(
                    f"{place_name_ascii}, {admin_name_2}, {admin_name_1}"
                )

            alternatives_names = [
                AlternativeName(name=name) for name in alternatives_names_string
            ]

            # get postal codes and add postal codes present in alternative names
            postal_codes = place_id_to_postal_codes.get(place_id, [])
            for x in alternatives_names_string:
                if x.isdigit():
                    postal_codes.append(x)

            item = GeonameItem(
                geonameid=place_id,
                name=place_name,
                asciiname=place_name_ascii,
                # alternative_names_string=alternatives_names_string,
                alternative_names=alternatives_names,
                latitude=row[4] or None,
                longitude=row[5] or None,
                feature_class=row[6] or None,
                feature_code=feature_code,
                country_code=country_code,
                country=country,
                # cc2=row[9].split(",") if row[9].strip() else None,
                admin1_code=row[10] or None,
                admin2_code=row[11] or None,
                admin3_code=row[12] or None,
                admin4_code=row[13] or None,
                population=row[14] or None,
                elevation=row[15] or None,
                dem=row[16] or None,
                timezone=row[17],
                #
                postal_codes=postal_codes,
                admin1_name=admin_name_1,
                admin2_name=admin_name_2,
            )
            yield item


def get_es_actions_it(
    geoname_items_it: Iterable[GeonameItem],
    index_name: str,
    use_geoname_id: bool = False,
):
    """ """
    for item in geoname_items_it:
        action = {
            "_index": index_name,
            "_source": item.dict(),
        }
        if use_geoname_id:
            action["_id"] = item.geonameid
        #
        yield action


def index_geonames_data(
    es: Elasticsearch,
    geoname_items_it: Iterable[GeonameItem],
    index_name: str,
    n_threads: int = 1,
    use_geoname_id: bool = False,
):
    """ """
    actions = get_es_actions_it(geoname_items_it, index_name, use_geoname_id)
    if n_threads > 1:
        index_it = helpers.parallel_bulk(
            es, actions, thread_count=n_threads, raise_on_error=False
        )
    else:
        index_it = helpers.streaming_bulk(es, actions=actions, raise_on_error=False)
    #
    for ok, detail in index_it:
        if not ok:
            print(detail)


def load_admin_codes(path: str) -> dict:
    """ """
    admin_codes = {}
    with open(path) as f:
        reader = csv.reader(f, delimiter="\t")
        for row in tqdm(reader):
            code = row[0]
            admin_codes[code] = {
                "code": row[0],
                "name": fix_text(row[1]),
                "ascii_name": fix_text(row[2]),
                "geoname_id": row[3],
            }
    #
    return admin_codes


def load_postal_codes(
    alternative_names_path: str,
):
    """ """
    postal_codes = defaultdict(list)
    with open(alternative_names_path) as f:
        reader = csv.reader(f, delimiter="\t")
        for row in tqdm(reader):
            kind = row[2]
            if kind == "post":
                place_id = row[1]
                postal_codes[place_id].append(row[3])
    return postal_codes


# def load_alternative_names(alternative_names_path: str):
#     """ """
#     place_id_to_alternative_names = defaultdict(
#         lambda: {"postal_codes": [], "names": defaultdict(list)}
#     )
#     with open(alternative_names_path) as f:
#         reader = csv.reader(f, delimiter="\t")
#         for row in tqdm(reader):
#             place_id = row[1]
#             kind = row[2]
#             if kind == "post":
#                 place_id_to_alternative_names[place_id]["postal_codes"].append(name)
#             elif len(kind) == 2:
#                 name = fix_text(row[3])
#                 place_id_to_alternative_names[place_id]["names"][name].append(kind)
#             elif not kind:
#                 name = fix_text(row[3])
#                 place_id_to_alternative_names[place_id]["names"][name]
#     #
#     return place_id_to_alternative_names


def main():
    """ """

    es = Elasticsearch(**settings.es.es_client_params)

    #
    index_name = "geonames-v1.5"

    # create_index
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    es.indices.create(
        index=index_name,
        mappings=geoname_index_mappings,
        settings=geoname_index_settings,
    )

    #
    all_countries_path = "./data/allCountries.txt"
    admin_codes_1_path = "./data/admin1CodesASCII.txt"
    admin_codes_2_path = "./data/admin2Codes.txt"
    alternative_names_path = "./data/alternateNames/alternateNames.txt"

    admin_codes_1 = load_admin_codes(admin_codes_1_path)
    admin_codes_2 = load_admin_codes(admin_codes_2_path)
    place_id_to_postal_codes = load_postal_codes(alternative_names_path)
    # place_id_to_alternative_names = load_alternative_names(alternative_names_path)
    geoname_items_it = get_index_geoname_items_it(
        all_countries_path,
        admin_codes_1,
        admin_codes_2,
        place_id_to_postal_codes=place_id_to_postal_codes,
        # place_id_to_alternative_names
        # include_countries=["FR"],
    )
    #
    index_geonames_data(es, geoname_items_it, index_name=index_name, n_threads=6)


if __name__ == "__main__":
    main()
