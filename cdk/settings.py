from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    run_tests: bool = True
    dry_run: bool = False
    limit_granules: int = None
    stack_name: str
    stage: str
