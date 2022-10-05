from typing import List, Optional

from pydantic import BaseModel, Field, constr


class AlternativeName(BaseModel):
    """ """

    name: str
    langs: List[str] = None


class GeonameItem(BaseModel):
    """ """

    geonameid: str
    name: str
    asciiname: str
    # alternative_names_string: List[str] = []
    alternative_names: List[AlternativeName] = []
    latitude: float = None
    longitude: float = None
    feature_class: str = None
    feature_code: str = None
    country_code: str = None
    country: str = None
    # cc2: List[str] = None
    admin1_code: str = None
    admin2_code: str = None
    admin3_code: str = None
    admin4_code: str = None
    population: int = None
    elevation: int = None
    dem: int = None
    timezone: str = None
    #
    postal_codes: List[str] = None
    #
    admin1_name: str = None
    admin2_name: str = None


class GeonameItemES(GeonameItem):
    """ """

    score: float


class ParsedLocation(BaseModel):
    """
    Object to store result from pypostal parsing
    """

    house: str = None
    house_number: str = None
    road: str = None

    city: str = None
    country: str = None
    state: str = None
    postcode: str = None

    state_district: str = None
    suburb: str = None

    raw: str = None


class JobLocation(BaseModel):
    """
    Object representing a location as store on the job post
    """

    country_code: constr(min_length=2, max_length=2) = Field(
        None,
        description="Country code in ISO X format (2 letter)",
    )
    country: str = None
    state: str = None
    region: str = None
    city: str = None
    raw: str = None
    street: str = None
    postal_code: str = None


class ParseAndNormalizeRequestData(BaseModel):
    """ """

    raw_location: str
    country_code: str = None


class ParseAndNormalizeRequestBatchData(BaseModel):
    """ """

    data: List[ParseAndNormalizeRequestData]


class NormalizeRequestData(BaseModel):
    """ """

    location: JobLocation


class NormalizeRequestBatchData(BaseModel):
    """ """

    locations: List[JobLocation]


class NormalizedLocationResult(BaseModel):
    """ """

    match: Optional[GeonameItemES] = None
    candidates: List[GeonameItemES] = []
    query: dict = None


class ParsedAndNormalizedResult(NormalizedLocationResult):
    """ """

    parsed_location: ParsedLocation
