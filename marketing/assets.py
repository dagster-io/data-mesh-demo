from dagster import asset, AssetExecutionContext, AutoMaterializePolicy
from data_platform.utils.storage import get_location_of_data_product, get_data_products

data_products = get_data_products()

@asset(
    deps=[data_products["recommended_songs"], data_products["subscriptions"]],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    group_name="advertisements",
    owners=["team:marketing"],
    tags={
        "team": "marketing",
        "stability": "stable",
        "contains_pii": "true"
    }
)
def user_segments(context: AssetExecutionContext):
    """potential artists to target for promotion.
    """

    recommended_songs = get_location_of_data_product("recommended_songs", context)
    _subscriptions = get_location_of_data_product("subscriptions", context)

    # open the file URI and read the data
    with open(recommended_songs, "r") as f:
        data = f.read()
        context.log.info(f"Read data from {recommended_songs}: {data}")


@asset(
    deps=[data_products["best_friends"], data_products["trending_artists"]],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    group_name="experimentation"
)
def a_b_tests(context: AssetExecutionContext):
    """potential customers to target for family plan promotion.
    """

    storage = get_location_of_data_product("best_friends", context)

    # open the file URI and read the data
    with open(storage, "r") as f:
        data = f.read()
        context.log.info(f"Read data from {storage}: {data}")