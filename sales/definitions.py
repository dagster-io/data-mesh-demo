from dagster import Definitions, load_assets_from_modules

from . import assets

all_assets = load_assets_from_modules([assets], group_name="outbound")


defs = Definitions(
    assets=all_assets,
)