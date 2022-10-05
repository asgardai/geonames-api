from pydantic import BaseSettings


class EsSettings(BaseSettings):
    """ """

    host: str = "localhost"
    port: str = 9200
    username: str = None
    password: str = None

    class Config:
        env_file = ".env"
        env_prefix = "ES_"

    @property
    def es_client_params(self) -> dict:
        params = {
            "hosts": [f"{self.host}:{self.port}"],
            "timeout": 30,
            "max_retries": 3,
            "retry_on_timeout": True,
        }
        if self.username and self.password:
            params["http_auth"] = f"{self.username}:{self.password}"
        return params


class Settings(BaseSettings):
    """ """

    es: EsSettings = EsSettings()
    geonames_index = "geonames-v1.5"


settings = Settings()
