from dagster import asset, AssetExecutionContext, AutoMaterializePolicy
from data_platform.utils.storage import get_location_of_data_product, get_data_products

data_products = get_data_products()

@asset(
    deps=[data_products["trending_artists"]],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def target_artists(context: AssetExecutionContext):
    """potential artists to target for promotion.
    """

    storage = get_location_of_data_product("trending_artists", context)

    # open the file URI and read the data
    with open(storage, "r") as f:
        data = f.read()
        context.log.info(f"Read data from {storage}: {data}")


@asset(
    deps=[data_products["best_friends"], data_products["subscriptions"]],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def family_plan_candidates(context: AssetExecutionContext):
    """potential customers to target for family plan promotion.
    """

    storage = get_location_of_data_product("best_friends", context)

    # open the file URI and read the data
    with open(storage, "r") as f:
        data = f.read()
        context.log.info(f"Read data from {storage}: {data}")