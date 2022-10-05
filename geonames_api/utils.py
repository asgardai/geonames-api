import pycountry
import unicodedata


def deaccent(text: str):
    """
    Remove accentuation from the given string.
    Input text is either a unicode string or utf8 encoded bytestring.

    Return input string with accents removed, as unicode.

    >>> deaccent("`éf chomutovských komunisto dostal poatou bílý práaek")
    u'Sef chomutovskych komunistu dostal postou bily prasek'

    """
    norm = unicodedata.normalize("NFD", text)
    result = "".join(ch for ch in norm if unicodedata.category(ch) != "Mn")
    return unicodedata.normalize("NFC", result)


def get_pycountry(country: str):
    """ """
    try:
        c = pycountry.countries.lookup(country)
    except LookupError:
        c = None
    return c


BAD_CITY_KEYWORDS = {
    "other",
    "headquarter",
    "headquarters",
    "remote",
    "teletravail",
    "anywhere",
    "teletrabajo",
    "telearbeit",
    "worldwide",
    "telelavoro",
    "teletrabajar",
    "null",
}


def is_bad_loc(loc: str) -> bool:
    tokens = loc.split()
    return any(x in tokens for x in BAD_CITY_KEYWORDS)
