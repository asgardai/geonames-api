import logging
from threading import Lock
from typing import List

from elasticsearch import Elasticsearch, AsyncElasticsearch
from fastapi import FastAPI, Depends
from postal.parser import parse_address
from pydantic import BaseModel
from starlette.requests import Request

from geonames_api.config import settings
from geonames_api.models import (
    ParsedAndNormalizedResult,
    JobLocation,
    NormalizedLocationResult,
    ParseAndNormalizeRequestBatchData,
    ParseAndNormalizeRequestData,
    NormalizeRequestData,
    NormalizeRequestBatchData,
    ParsedLocation,
)
from geonames_api.parse_and_normalize import (
    parse_and_normalize_raw_location_async,
    parse_and_normalize_raw_location_batch_async,
    normalise_location_batch_async,
    parse_raw_location,
)

logger = logging.getLogger(__name__)


app = FastAPI(title="geonames-api", openapi_url="/api/v1/openapi.json")


@app.on_event("startup")
async def startup_event():
    logger.info("startup: loading db connection and models...")
    # add mongo, es, ...
    app.es = Elasticsearch(**settings.es.es_client_params)
    app.es_async = AsyncElasticsearch(**settings.es.es_client_params)
    #
    logger.info("Loading postal model...")
    parse_address("21 rue Cujas, Paris")
    #
    logger.info("startup done.")


@app.get("/")
async def read_root():
    return {"asgard": "geonames-api"}


def get_es(request: Request) -> Elasticsearch:
    return request.app.es


def get_es_async(request: Request) -> AsyncElasticsearch:
    return request.app.es_async


@app.post("/parse_and_normalize_raw_location", response_model=ParsedAndNormalizedResult)
async def parse_and_normalize_raw_location_route(
    data: ParseAndNormalizeRequestData,
    es: AsyncElasticsearch = Depends(get_es_async),
):
    """ """
    result = await parse_and_normalize_raw_location_async(
        es=es, raw_location=data.raw_location, country_code=data.country_code
    )
    return result


@app.post(
    "/parse-and-normalize-raw-location-batch",
    response_model=List[ParsedAndNormalizedResult],
)
async def parse_and_normalize_raw_location_batch_route(
    data: ParseAndNormalizeRequestBatchData,
    es: AsyncElasticsearch = Depends(get_es_async),
):
    """ """
    results = await parse_and_normalize_raw_location_batch_async(es=es, batch=data.data)
    return results


@app.post("/normalize-job-location", response_model=NormalizedLocationResult)
async def normalize_job_location_route(
    data: NormalizeRequestData,
    es: AsyncElasticsearch = Depends(get_es_async),
):
    """ """
    results = await normalise_location_batch_async(es=es, locations=[data.location])
    return results[0]


@app.post(
    "/normalize-job-location-batch", response_model=List[NormalizedLocationResult]
)
async def normalize_job_location_batch_route(
    data: NormalizeRequestBatchData,
    es: AsyncElasticsearch = Depends(get_es_async),
):
    """ """

    results = await normalise_location_batch_async(es=es, locations=data.locations)
    return results


class ParseLocationRequestData(BaseModel):
    """ """

    location: str
    # country: str = None
    # language: str = None


class ParseLocationBatchRequestData(BaseModel):
    """ """

    batch: List[ParseLocationRequestData]


class ParseLocationBatchResponse(BaseModel):
    """ """

    success: bool
    data: List[ParsedLocation]


class ParseLocationResponse(BaseModel):
    """ """

    success: bool
    raw_location: str
    parsed_location: ParsedLocation = None


@app.get("/parse-location", response_model=ParseLocationResponse)
async def parse_location_route(location: str):
    """ """
    parsed_location = parse_raw_location(raw_location=location)
    return ParseLocationResponse(
        success=True, parsed_location=parsed_location, raw_location=location
    )


@app.post("/parse-location-batch", response_model=ParseLocationBatchResponse)
async def parse_location_batch(data: ParseLocationBatchRequestData):
    """ """
    parsed_locations = []
    for item in data.batch:
        parsed_locations.append(parse_raw_location(raw_location=item.location))
    #
    return ParseLocationBatchResponse(success=True, data=parsed_locations)
