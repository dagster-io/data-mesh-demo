from typing import Mapping, List, Set
from pathlib import Path
from dagster import AssetsDefinition, AssetExecutionContext, AssetKey, EventRecordsFilter, DagsterEventType

import json

DATA_PRODUCTS_FILE = Path(__file__).parent / "data_products.json"

def _read_data_products() -> Set[str]:
    """Read the list of data products from data_products.json.

    Returns:
        List[str]: A list of data products' asset keys
    """

    try:
        with open(DATA_PRODUCTS_FILE, "r") as f:
            data_products = json.load(f)
    except Exception:
        data_products = []

    return set(data_products)

def publish_data_products(data_products: List[AssetsDefinition]) -> None:
    """This function publishes data products from the central data platform team.
    Published data products are available for consumption by downstream domain teams.

    Args:
        data_products (List[str]): A list of asset names to publish.
    """

    asset_names = [asset.key.to_user_string() for asset in data_products]


    current_data_products = _read_data_products()
    current_data_products.update(asset_names)

    with open(DATA_PRODUCTS_FILE, "w") as f:
        json.dump(list(current_data_products), f)

def delete_data_products(data_products: List[str]) -> None:
    """This function deletes data products from the central data platform team.
    Deleted data products are no longer available for consumption by downstream domain teams.

    Args:
        data_products (List[str]): A list of asset names to delete.
    """
    
    current_data_products = _read_data_products()
    current_data_products.difference_update(data_products)

    with open(DATA_PRODUCTS_FILE, "w") as f:
        json.dump(list(current_data_products), f)
    

def get_data_products() -> Mapping[str, AssetKey]:
    """Get the list of data products from the central data platform team"""

    data_products = _read_data_products()

    # turn the set of data products into source assets
    assets = {
        f"{asset}": AssetKey(
            path=asset
        ) for asset in data_products
    }

    return assets

def get_location_of_data_product(data_product_name: str, context: AssetExecutionContext) -> Path:
    """Get the location of a data product.

    Args:
        data_product_name (str): The name of the data product

    Returns:
        Path: The location of the data product
    """

    LOCATION_KEY = "location"

    records = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION, asset_key=AssetKey(data_product_name)
        ),
        limit=1,
        ascending=False
    )

    if len(records) == 0:
        raise ValueError(f'Data product "{data_product_name}" not found')
    
    if LOCATION_KEY not in records[0].asset_materialization.metadata:
        raise ValueError(f'Data product "{data_product_name}" does not have a storage location associated with it.')

    return Path(records[0].asset_materialization.metadata[LOCATION_KEY].value)