from dagster import Definitions, load_assets_from_modules, AutoMaterializePolicy

from data_platform.utils.storage import publish_data_products

from . import assets

all_assets = [
    asset.with_attributes(
        auto_materialize_policy=AutoMaterializePolicy.eager()
    )
    for asset in
    load_assets_from_modules([assets])
]

publish_data_products(assets.data_products)


defs = Definitions(
    assets=all_assets,
    asset_checks=[
        *assets.nullness_checks,
        *assets.distribution_checks,
        *assets.cardinality_checks,
        *assets.type_checks,
        *assets.freshness_checks
    ]
)
