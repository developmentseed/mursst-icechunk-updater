from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    run_tests: bool = True
    dry_run: bool = False
    limit_granules: Optional[int] = None
    stack_name: str
    stage: str
    store_name: str
    icechunk_direct_prefix: str
    local_test: bool = False
