from typing import Literal

from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Provider(BaseModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    protocol: Literal["openai", "anthropic"]
    base_url: str
    model: str
    api_key: SecretStr = SecretStr("local-demo")
    input_price: int = Field(default=1, ge=0, le=10000)
    output_price: int = Field(default=3, ge=0, le=10000)
    price_version: str = Field(default="demo-v1", min_length=1, max_length=80)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql+asyncpg://relaylm:relaylm@database:5432/relaylm"
    jwt_secret: str = Field(default="local-demo-relaylm-replace-before-deployment", min_length=32)
    token_minutes: int = 60
    request_timeout: float = Field(default=20, ge=1, le=60)
    read_timeout: float = Field(default=3, ge=0.1, le=15)
    request_lease: int = Field(default=90, ge=70, le=300)
    circuit_threshold: int = 3
    circuit_cooldown: int = 10
    max_concurrent: int = 3
    requests_per_minute: int = 30
    providers: list[Provider] = [
        Provider(
            name="primary",
            protocol="openai",
            base_url="http://provider-a:8000",
            model="demo-openai",
        ),
        Provider(
            name="backup",
            protocol="anthropic",
            base_url="http://provider-b:8000",
            model="demo-anthropic",
            input_price=2,
            output_price=4,
        ),
    ]

    @field_validator("providers")
    @classmethod
    def two_distinct_providers(cls, providers):
        if len(providers) != 2 or len({p.name for p in providers}) != 2:
            raise ValueError("Configure two providers with different names")
        return providers


settings = Settings()
