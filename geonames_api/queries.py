from typing import List

from geonames_api.models import ParsedLocation, JobLocation
from geonames_api.utils import deaccent, get_pycountry


def build_query_from_parsed_location(
    parsed_location: ParsedLocation, country_code: str = None
) -> dict:
    """ """
    if parsed_location.city:
        name = parsed_location.city
    elif parsed_location.state:
        name = parsed_location.state
    elif parsed_location.state_district:
        name = parsed_location.state_district
    elif parsed_location.country:
        name = parsed_location.country
    else:
        name = parsed_location.raw

    should = [
        # {"match": {"asciiname": deaccent(name)}},
        # {"match": {"name": name}},
    ]

    admin1_name, admin2_name = None, None
    if parsed_location.state and not parsed_location.state_district:
        admin1_name = parsed_location.state
        admin2_name = parsed_location.state
    elif parsed_location.state and parsed_location.state_district:
        admin1_name = parsed_location.state
        admin2_name = parsed_location.state_district
    elif parsed_location.state_district:
        admin1_name = parsed_location.state_district
        admin2_name = parsed_location.state_district

    if admin1_name:
        should.append({"match": {"admin1_name": deaccent(admin1_name)}})
    if admin2_name:
        should.append({"match": {"admin2_name": deaccent(admin2_name)}})

    # to help US with state codes (NY, TX, ...)
    if admin1_name and len(admin1_name) == 2:
        should.append({"match": {"admin1_code": admin1_name.upper()}})

    # postal_code = None
    if parsed_location.postcode:
        postal_code = parsed_location.postcode
        should.append({"match": {"postal_codes": postal_code}})

        # case France, admin2_code = department number (case 91000 => 91)
        if len(postal_code) == 5:
            admin_code = postal_code[:2]
            should.append({"match": {"admin2_code": admin_code}})
    #
    cc = None
    if parsed_location.country:
        c = get_pycountry(parsed_location.country)
        if c:
            cc = c.alpha_2
    if cc is None and country_code:
        cc = country_code

    if cc:
        should.append({"match": {"country_code": {"query": cc, "boost": 5}}})

    #
    query = {
        "bool": {
            "must": [build_must_dis_max_name_query(name)],
            # "should": should,
            "should": build_should_dis_max_query(should, tie_breaker=0.5),
        }
    }
    return query


def build_must_dis_max_name_query(name, tie_breaker: float = 0.3):
    """ """
    return {
        "dis_max": {
            "queries": [
                {
                    "nested": {
                        "path": "alternative_names",
                        "query": {"match": {"alternative_names.name": name}},
                        "score_mode": "max",
                    }
                },
                {"match": {"asciiname": {"query": deaccent(name), "boost": 2}}},
                {"match": {"name": name}},
            ],
            "tie_breaker": tie_breaker,
        }
    }


def build_should_dis_max_query(should: List[dict], tie_breaker: float = 0.3):
    """ """
    dis_max_should = []
    if not should:
        return dis_max_should

    dis_max_should.append(
        {
            "dis_max": {
                "queries": should,
                "tie_breaker": tie_breaker,
            }
        }
    )
    return dis_max_should


def build_query_from_job_location(job_location: JobLocation) -> dict:
    """ """
    if job_location.city:
        name = job_location.city
    else:
        name = job_location.raw

    should = [
        # {"match": {"asciiname": deaccent(name)}},
        # {"match": {"name": name}},
    ]

    admin1_name, admin2_name = None, None
    if job_location.state and not job_location.region:
        admin1_name = job_location.state
        admin2_name = job_location.state
    elif job_location.state and job_location.region:
        admin1_name = job_location.state
        admin2_name = job_location.region
    elif job_location.region:
        admin1_name = job_location.region
        admin2_name = job_location.region

    if admin1_name:
        should.append({"match": {"admin1_name": deaccent(admin1_name)}})
    if admin2_name:
        should.append({"match": {"admin2_name": deaccent(admin2_name)}})

    # to help US with state codes (NY, TX, ...)
    if admin1_name and len(admin1_name) == 2:
        should.append({"match": {"admin1_code": admin1_name}})

    # postal_code = None
    if job_location.postal_code:
        should.append({"match": {"postal_codes": job_location.postal_code}})

    #
    if job_location.country_code:
        should.append(
            {
                "match": {
                    "country_code": {"query": job_location.country_code, "boost": 5}
                }
            }
        )

    #
    query = {
        "bool": {
            "must": [build_must_dis_max_name_query(name)],
            "should": build_should_dis_max_query(should, tie_breaker=0.5),
        }
    }
    return query
