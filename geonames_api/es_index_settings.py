geoname_index_settings = {
    "number_of_shards": 2,
    "number_of_replicas": 0,
    "similarity": {
        "scripted_tfidf": {
            "type": "scripted",
            "script": {
                "source": "double tf = Math.sqrt(doc.freq); double idf = Math.log((field.docCount+1.0)/(term.docFreq+1.0)) + 1.0; double norm = 1/Math.sqrt(doc.length); return query.boost * tf * idf * norm;"
            },
        },
        "bm25_b0": {"type": "BM25", "b": 0},
    },
}
geoname_index_mappings = {
    "properties": {
        "geonameid": {"type": "keyword"},
        "name": {"type": "text"},
        "asciiname": {"type": "text"},
        "alternative_names_string": {"type": "text", "similarity": "bm25_b0"},
        "alternative_names": {
            "type": "nested",
            "properties": {
                "name": {"type": "text", "similarity": "bm25_b0"},
                "langs": {"type": "keyword"},
            },
        },
        "latitude": {"type": "float"},
        "longitude": {"type": "float"},
        "feature_class": {"type": "keyword"},
        "feature_code": {"type": "keyword"},
        "country_code": {"type": "keyword"},
        "country": {"type": "keyword"},
        "admin1_code": {"type": "keyword"},
        "admin2_code": {"type": "keyword"},
        "admin3_code": {"type": "keyword"},
        "admin4_code": {"type": "keyword"},
        "admin1_name": {
            "type": "text",
            "fields": {"raw": {"type": "keyword"}},
        },
        "admin2_name": {
            "type": "text",
            "fields": {"raw": {"type": "keyword"}},
        },
        "postal_codes": {
            "type": "text",
            "similarity": "bm25_b0",
            "fields": {"raw": {"type": "keyword"}},
        },
        "population": {"type": "integer"},
        "elevation": {"type": "integer"},
        "dem": {"type": "integer"},
        "timzeone": {"type": "keyword"},
    }
}
