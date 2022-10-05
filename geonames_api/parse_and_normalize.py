import logging
import re
from threading import Lock
from typing import List, Optional, Union

import cytoolz
import textdistance
from elasticsearch import Elasticsearch, AsyncElasticsearch
from ftfy import fix_text
from postal.parser import parse_address

from geonames_api.config import settings
from geonames_api.models import (
    ParsedLocation,
    GeonameItemES,
    JobLocation,
    ParsedAndNormalizedResult,
    NormalizedLocationResult,
    ParseAndNormalizeRequestData,
)
from geonames_api.queries import (
    build_query_from_parsed_location,
    build_query_from_job_location,
)
from geonames_api.utils import is_bad_loc

logger = logging.getLogger(__name__)

lock = Lock()

not_none_fields_mappings = [
    {"house_number": "postcode", "road": "city"},
    {"house_number": "postcode", "house": "city"},
    {"house": "city"},
]

french_city_code_pattern = re.compile("([\w\-'\s]+)\s[\(-]?(\d{2})[\)]?", re.UNICODE)


def parse_raw_location(raw_location: str, country_code: str = None) -> ParsedLocation:
    """ """
    with lock:
        p = {k: v.strip(" ,") for v, k in parse_address(raw_location)}
    parsed_location = ParsedLocation.parse_obj(p)

    # special case for french loc like: "City (DEP_NB)"
    all_parsed_fields = list(parsed_location.__fields__.keys())

    for not_none_fields_mapping in not_none_fields_mappings:

        not_none_fields = list(not_none_fields_mapping.keys())
        none_fields = [x for x in all_parsed_fields if x not in not_none_fields]
        if all(
            getattr(parsed_location, field) is not None for field in not_none_fields
        ) and all(getattr(parsed_location, x) is None for x in none_fields):

            for from_key, to_key in not_none_fields_mapping.items():
                value = getattr(parsed_location, from_key)
                #
                if to_key == "city" and "region" in value:
                    to_key = "state"

                if to_key == "postcode" and len(value) == 2:
                    value = value + "000"

                setattr(parsed_location, to_key, value)
                setattr(parsed_location, from_key, None)
    #
    if country_code == "FR":
        if parsed_location.city is None:
            m = french_city_code_pattern.match(raw_location)
        else:
            m = french_city_code_pattern.match(parsed_location.city)
        if m:
            parsed_location.city = m.group(1).strip(" -")
            dep_code = m.group(2)
            if parsed_location.postcode is None:
                parsed_location.postcode = f"{dep_code}000"
    #
    parsed_location.raw = raw_location

    return parsed_location


def group_by_margin(results: List[GeonameItemES], margin: float = 1):
    """
    assume results is sorted in decreasing order of score

    :param margin:
    :param results:
    :return:
    """
    groups = []
    group = []
    last_score = 0
    for res in results:
        if not last_score:
            group.append(res)
        else:
            if (last_score - res.score) <= margin:
                group.append(res)
            else:
                groups.append(group)
                group = [res]
        #
        last_score = res.score
    #
    if group:
        groups.append(group)
    #
    return groups


FEATURE_CODE_RANK = {
    "A": [
        "ADM1",  # "a primary administrative division of a country, such as a state in the United States",
        "ADM2",  # a subdivision of a first-order administrative division
        "ADM3",  # a subdivision of a second-order administrative division
        "ADM4",  # a subdivision of a third-order administrative division
        "ADM5",  # a subdivision of a fourth-order administrative division
        "ADMD",  # an administrative division of a country, undifferentiated as to administrative level
        "PCLI",  # countries
    ],
    "P": [
        "PPLC",  # capital
        "PPLA",  # seat of a first-order administrative division	seat of a first-order administrative division (PPLC takes precedence over PPLA)
        "PPL",  # a city, town, village, or other agglomeration of buildings where people live and work
        "PPLA2",  # seat of a second-order administrative division
        "PPLA3",  # seat of a third-order administrative division
        "PPLA4",  # seat of a fourth-order administrative division
        "PPLA5",  # seat of a fifth-order administrative division
        "PPLS",
    ],
    "L": [
        "RGN",  # region	an area distinguished by one or more observable physical or cultural characteristics
        "RGNE",  # a region of a country established for economic development or for statistical purposes
    ],
}


def check_if_best_result_close_enough(
    result: GeonameItemES, location: Union[ParsedLocation, JobLocation]
):
    """ """
    result_name_lower = result.asciiname.lower()
    s = None
    if location.city:
        s = textdistance.jaro_winkler.normalized_similarity(
            result_name_lower, location.city.lower()
        )

    if s is not None and s < 0.5:
        return False

    #
    return True


def select_best_matching_place(
    results: List[GeonameItemES],
    query_location: Union[ParsedLocation, JobLocation] = None,
) -> Optional[GeonameItemES]:
    """ """
    if not results:
        return

    gb = group_by_margin(results, margin=2)
    #
    fg = gb[0]
    #
    gb_class = cytoolz.groupby(lambda x: x.feature_class, fg)
    if "P" in gb_class:
        best_place = sorted(
            gb_class["P"], key=lambda x: FEATURE_CODE_RANK["P"].index(x.feature_code)
        )[0]
    elif "A" in gb_class:
        best_place = sorted(
            gb_class["A"], key=lambda x: FEATURE_CODE_RANK["A"].index(x.feature_code)
        )[0]
    elif "L" in gb_class:
        best_place = sorted(
            gb_class["L"], key=lambda x: FEATURE_CODE_RANK["L"].index(x.feature_code)
        )[0]
    else:
        raise ValueError("Items with unknown feature class in results :S")

    #
    if query_location:
        if not check_if_best_result_close_enough(best_place, query_location):
            logger.info(
                f"Discarding place: {best_place} because string distance is too high compared to {query_location}"
            )
            best_place = None
    #
    return best_place


def parse_and_normalize_raw_location(
    es: Elasticsearch, raw_location: str, country_code: str = None
) -> ParsedAndNormalizedResult:
    """ """
    match = None
    raw_location = fix_text(raw_location)
    parsed_location = parse_raw_location(raw_location, country_code=country_code)
    #
    if is_bad_loc(parsed_location.city or parsed_location.raw):
        logger.info(f"Got a bad location: {parsed_location.raw}")
        results = []
        query = None
    else:
        query = build_query_from_parsed_location(
            parsed_location, country_code=country_code
        )
        es_resp = es.search(query=query, index=settings.geonames_index)
        results = [
            GeonameItemES.parse_obj({"score": x["_score"], **x["_source"]})
            for x in es_resp["hits"]["hits"]
        ]

        if results:
            match = select_best_matching_place(results)

    result = ParsedAndNormalizedResult(
        match=match,
        candidates=results,
        parsed_location=parsed_location,
        query=query,
    )

    return result


async def parse_and_normalize_raw_location_async(
    es: AsyncElasticsearch, raw_location: str, country_code: str = None
) -> ParsedAndNormalizedResult:
    """ """
    match = None
    raw_location = fix_text(raw_location)
    parsed_location = parse_raw_location(raw_location, country_code=country_code)
    if is_bad_loc(parsed_location.city or parsed_location.raw):
        logger.info(f"Got a bad location: {parsed_location.raw}")
        results = []
        query = None
    else:
        query = build_query_from_parsed_location(
            parsed_location, country_code=country_code
        )
        es_resp = await es.search(query=query, index=settings.geonames_index)
        results = [
            GeonameItemES.parse_obj({"score": x["_score"], **x["_source"]})
            for x in es_resp["hits"]["hits"]
        ]
        if results:
            match = select_best_matching_place(results)

    result = ParsedAndNormalizedResult(
        match=match, candidates=results, parsed_location=parsed_location, query=query
    )

    return result


async def parse_and_normalize_raw_location_batch_async(
    es: AsyncElasticsearch, batch: List[ParseAndNormalizeRequestData]
) -> List[ParsedAndNormalizedResult]:
    """ """
    queries = []
    batch_parsed_locations = []
    for item in batch:
        raw_location = fix_text(item.raw_location)
        parsed_location = parse_raw_location(
            raw_location, country_code=item.country_code
        )
        batch_parsed_locations.append(parsed_location)
        #
        query = build_query_from_parsed_location(
            parsed_location, country_code=item.country_code
        )
        queries.append({"index": settings.geonames_index})
        queries.append({"query": query})

    es_responses = (await es.msearch(body=queries))["responses"]
    batch_results = []
    for i, es_resp in enumerate(es_responses):
        if "error" in es_resp:
            raise NotImplementedError()
        else:
            geonames_items = [
                GeonameItemES.parse_obj({"score": x["_score"], **x["_source"]})
                for x in es_resp["hits"]["hits"]
            ]
            match = select_best_matching_place(geonames_items)
            batch_results.append(
                ParsedAndNormalizedResult(
                    match=match,
                    candidates=geonames_items,
                    parsed_location=batch_parsed_locations[i],
                )
            )

    return batch_results


def need_to_reparse_city(location: JobLocation):
    """ """
    none_fields = ["state", "region", "postal_code", "street"]
    return location.city is not None and all(
        getattr(location, x) is None for x in none_fields
    )


def reparse_job_location(location: JobLocation):
    """ """
    city = location.city
    parsed_city = parse_raw_location(city)
    update = {}
    if parsed_city.city != city.lower():
        if parsed_city.state:
            update["state"] = parsed_city.state
        if parsed_city.state_district:
            update["region"] = parsed_city.state_district
        if parsed_city.postcode:
            update["postal_code"] = parsed_city.postcode
        if parsed_city.city:
            update["city"] = parsed_city.city
    #
    location = location.copy(update=update)
    #
    return location


def is_raw_location(location: JobLocation):
    """ """
    fields = ["city", "state", "region", "postal_code"]
    return all(getattr(location, x) is None for x in fields)


async def normalise_location_batch_async(
    es: AsyncElasticsearch, locations: List[JobLocation]
) -> List[NormalizedLocationResult]:
    """ """
    indices, queries = [], []
    # batch_parsed_locations = []
    for i, location in enumerate(locations):
        if is_raw_location(location):
            raw_location = fix_text(location.raw)
            parsed_location = parse_raw_location(
                raw_location, country_code=location.country_code
            )
            if is_bad_loc(parsed_location.city or parsed_location.raw):
                logger.info(f"Got a bad location: {parsed_location.raw}")
                continue
            else:
                query = build_query_from_parsed_location(
                    parsed_location, country_code=location.country_code
                )
        else:
            query = build_query_from_job_location(job_location=location)
        #
        queries.append({"index": settings.geonames_index})
        queries.append({"query": query})
        indices.append(i)

    es_responses = (await es.msearch(body=queries))["responses"]
    index_to_es_resp = {i: es_resp for i, es_resp in zip(indices, es_responses)}

    batch_results = []
    for i in range(len(locations)):
        es_resp = index_to_es_resp.get(i)
        if "error" in es_resp:
            raise NotImplementedError()

        if not es_resp:
            match = None
            candidates = []
        else:
            candidates = [
                GeonameItemES.parse_obj({"score": x["_score"], **x["_source"]})
                for x in es_resp["hits"]["hits"]
            ]
            match = select_best_matching_place(candidates)

        batch_results.append(
            NormalizedLocationResult(
                match=match,
                candidates=candidates,
                # parsed_location=batch_parsed_locations[i],
            )
        )
    return batch_results


def test():
    es = Elasticsearch(**settings.es_client_params)
    raw_location = "Tréflévenez (29)"
    raw_location = "Rezé"
    # raw_location = "Paris, Île-De-France"
    country_code = "ES"

    match = parse_and_normalize_raw_location(
        es=es, raw_location=raw_location, country_code=country_code
    )
    print(match.json(indent=4))


"""
The goal of this API is to parse and normalize locations from job posts.
Different cases are possible

    * We have just a "raw_location" from the job post (and a country_code)
        - Chamonix-Mont-Blanc (74)
        - Mérignac (33)



    * We have a "structured" location from the job post with at least a single prop filled from this object:
        {
            "country": "",
            "state": "",
            "region": "",
            "city": "",
            "country_code": "",
        }
        - {
            "country_code" : "FR",
            "country" : "France",
            "state" : null,
            "region" : "Pays de la Loire",
            "city" : "Saint-Nazaire",
            "raw" : "Saint-Nazaire, Pays de la Loire, France",
            "street" : null,
            "postal_code" : "44600"
        }
        - {
            "country_code" : "FR",
            "country" : "France",
            "state" : null,
            "region" : "PLO",
            "city" : "Saint-Nazaire",
            "raw" : "Saint-Nazaire, PLO, France",
            "street" : null,
            "postal_code" : null
        },
        {
            "country_code" : "FR",
            "country" : "France",
            "state" : null,
            "region" : "Rhône-Alpes",
            "city" : "Champagnier",
            "raw" : "Champagnier, Rhône-Alpes, France",
            "street" : null,
            "postal_code" : "38800"
        }


"""

"""
{
     "query": {
        "bool": {
            "must": [
                {
                    "nested": {
                        "path": "alternative_names",
                        "query": {"match": {"alternative_names.name": "Tours 37000"}},
                        "score_mode": "max"
                    }
                }
            ],
            "should": [
                {"match": {"country_code2": {"query": "FR", "boost": 3}}},
                {"match": {"asciiname": "Tours 37000"}},
                {"match": {"name": "Tours 37000"}},
                {"match": {"postal_codes": "Tours 37000"}}
            ]
        }
    }
}
"""
"""

total = 3115

per_room = total * 0.1
total_per_room = per_room * 5

total_shared = total - total_per_room
shared_per_person = total_shared / 6

total_single = per_room + shared_per_person
total_couple = per_room + 2 * shared_per_person

assert total_single * 4 + total_couple

"""
