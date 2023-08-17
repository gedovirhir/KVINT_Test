from pydantic_settings import BaseSettings, SettingsConfigDict

class BaseConfig(BaseSettings):
    model_config: SettingsConfigDict = SettingsConfigDict(env_file='.env')

class Settings(BaseConfig):
    RMQ_HOST: str = '127.0.0.1'
    RMQ_USER: str = 'guest'
    RMQ_PASSWORD: str = 'guest'

    PROCESS_COUNT: int = 10
    
config = Settings()
