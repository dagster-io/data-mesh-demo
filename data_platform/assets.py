from datetime import timedelta

from dagster import asset, MaterializeResult, asset_check, AssetCheckResult, AssetsDefinition, build_last_update_freshness_checks
import pandas as pd
import data_platform.constants as constants
import random
from pathlib import Path


@asset(
    group_name="bronze"
)
def ingested_users() -> None:
    """A list of customers ingested into the central data platform team"""
    
    df = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"]
    })

    df.to_csv(constants.INGESTED_USERS_PATH, index=False)

@asset(
    group_name="bronze"
)
def ingested_payments() -> None:
    """A list of payments ingested into the central data platform team"""
    
    df = pd.DataFrame({
        "payment_id": [1, 2, 3],
        "customer_id": [1, 2, 3],
        "payment_date": ["2022-01-01", "2022-02-01", "2022-03-01"],
        "amount": [10.0, 20.0, 30.0]
    })

    df.to_csv(constants.INGESTED_PAYMENTS_PATH, index=False)

@asset(
    group_name="bronze"
)
def ingested_playlists() -> None:
    """A list of playlists ingested into the central data platform team"""
    
    df = pd.DataFrame({
        "playlist_id": [1, 2, 3],
        "name": ["Playlist 1", "Playlist 2", "Playlist 3"],
        "owner_id": [1, 2, 3]
    })

    df.to_csv(constants.INGESTED_PLAYLISTS_PATH, index=False)

@asset(
    group_name="bronze"
)
def ingested_songs() -> None:
    """A list of songs ingested into the central data platform team"""
    
    df = pd.DataFrame({
        "song_id": [1, 2, 3],
        "name": ["Song 1", "Song 2", "Song 3"],
        "artist_id": [1, 2, 3],
        "album_id": [1, 2, 3]
    })

    df.to_csv(constants.INGESTED_SONGS_PATH, index=False)

@asset(
    group_name="bronze"
)
def ingested_artists() -> None:
    """A list of artists ingested into the central data platform team"""
    
    df = pd.DataFrame({
        "artist_id": [1, 2, 3],
        "name": ["Artist 1", "Artist 2", "Artist 3"],
        "genre": ["Pop", "Rock", "Hip Hop"]
    })

    df.to_csv(constants.INGESTED_ARTISTS_PATH, index=False)

@asset(
    group_name="bronze"
)
def ingested_albums() -> None:
    """A list of albums ingested into the central data platform team"""
    
    df = pd.DataFrame({
        "album_id": [1, 2, 3],
        "name": ["Album 1", "Album 2", "Album 3"],
        "artist_id": [1, 2, 3],
        "release_date": ["2022-01-01", "2022-02-01", "2022-03-01"]
    })

    df.to_csv(constants.INGESTED_ALBUMS_PATH, index=False)

@asset(
    group_name="bronze"
)
def ingested_plays() -> None:
    """A list of plays ingested into the central data platform team"""
    
    df = pd.DataFrame({
        "play_id": [1, 2, 3],
        "customer_id": [1, 2, 3],
        "song_id": [1, 2, 3],
        "play_date": ["2022-01-01", "2022-02-01", "2022-03-01"]
    })

    df.to_csv(constants.INGESTED_PLAYS_PATH, index=False)


@asset(
    group_name="bronze"
)
def ingested_followers() -> None:
    """A list of followers ingested into the central data platform team"""
    
    df = pd.DataFrame({
        "follower_id": [1, 2, 3],
        "user_id": [1, 2, 3],
        "follow_date": ["2022-01-01", "2022-02-01", "2022-03-01"]
    })

    df.to_csv(constants.INGESTED_FOLLOWERS_PATH, index=False)

@asset(
    group_name="silver",
    deps=[ingested_users]
)
def users() -> None:
    """Enriches the ingested users data"""
    
    df = pd.read_csv(constants.INGESTED_USERS_PATH)
    # Perform enrichment and calculations on the dataframe
    # ...
    df.to_csv(constants.USERS_PATH, index=False)

@asset(
    group_name="silver",
    deps=[ingested_payments]
)
def payments() -> None:
    """Enriches the ingested payments data"""
    
    df = pd.read_csv(constants.INGESTED_PAYMENTS_PATH)
    # Perform enrichment and calculations on the dataframe
    # ...
    df.to_csv(constants.PAYMENTS_PATH, index=False)

@asset(
    group_name="silver",
    deps=[ingested_playlists]
)
def playlists() -> None:
    """Enriches the ingested playlists data"""
    
    df = pd.read_csv(constants.INGESTED_PLAYLISTS_PATH)
    # Perform enrichment and calculations on the dataframe
    # ...
    df.to_csv(constants.PLAYLISTS_PATH, index=False)

@asset(
    group_name="silver",
    deps=[ingested_songs]
)
def songs() -> None:
    """Enriches the ingested songs data"""
    
    df = pd.read_csv(constants.INGESTED_SONGS_PATH)
    # Perform enrichment and calculations on the dataframe
    # ...
    df.to_csv(constants.SONGS_PATH, index=False)

@asset(
    group_name="silver",
    deps=[ingested_artists]
)
def artists() -> None:
    """Enriches the ingested artists data"""
    
    df = pd.read_csv(constants.INGESTED_ARTISTS_PATH)
    # Perform enrichment and calculations on the dataframe
    # ...
    df.to_csv(constants.ARTISTS_PATH, index=False)

@asset(
    group_name="silver",
    deps=[ingested_albums]
)
def albums() -> None:
    """Enriches the ingested albums data"""
    
    df = pd.read_csv(constants.INGESTED_ALBUMS_PATH)
    # Perform enrichment and calculations on the dataframe
    # ...
    df.to_csv(constants.ALBUMS_PATH, index=False)

@asset(
    group_name="silver",
    deps=[ingested_plays],
    owners=["team:data_platform"],
    tags={
        "team": "data_platform",
        "stability": "stable",
        "contains_pii": "false"
    }
)
def plays() -> None:
    """Enriches the ingested plays data"""
    
    df = pd.read_csv(constants.INGESTED_PLAYS_PATH)
    # Perform enrichment and calculations on the dataframe
    # ...
    df.to_csv(constants.PLAYS_PATH, index=False)

@asset(
    group_name="silver",
    deps=[ingested_followers],
    owners=["team:data_platform"],
    tags={
        "team": "data_platform",
        "stability": "stable",
        "contains_pii": "false"
    }
)
def followers() -> None:
    """Enriches the ingested followers data"""
    
    df = pd.read_csv(constants.INGESTED_FOLLOWERS_PATH)
    # Perform enrichment and calculations on the dataframe
    # ...
    df.to_csv(constants.FOLLOWERS_PATH, index=False)

@asset(
    group_name="gold",
    compute_kind="Databricks",
    deps=[users, payments],
    owners=["team:data_platform"],
    tags={
        "team": "data_platform",
        "stability": "stable",
        "contains_pii": "false"
    }
)
def subscriptions() -> MaterializeResult:
    """User subscriptions, derived from users and payments data"""
    
    df = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "start_date": ["2022-01-01", "2022-02-01", "2022-03-01"],
        "end_date": ["2022-12-31", "2022-12-31", "2022-12-31"],
        "monthly_price": [10.0, 20.0, 30.0],
        "total_paid": [120.0, 240.0, 360.0]
    })

    df.to_csv(constants.SUBSCRIPTIONS_PATH, index=False)

    return MaterializeResult(
        metadata={
            "location": Path(constants.SUBSCRIPTIONS_PATH).absolute(),
            "row_count": random.randint(120, 140),
            "standard_deviation": random.randint(1, 10),
            "average": random.randint(125, 135)
        }
    )

@asset(
    group_name="gold",
    compute_kind="Databricks",
    deps=[plays, songs, playlists],
    owners=["team:data_platform"],
    tags={
        "team": "data_platform",
        "stability": "stable",
        "contains_pii": "false"
    }
)
def top_songs() -> MaterializeResult:
    """The top songs based on a herustic of the number of plays"""
    
    df = pd.DataFrame({
        "song_id": [1, 2, 3],
        "name": ["Song 1", "Song 2", "Song 3"],
        "plays": [100, 200, 300]
    })

    df.to_csv(constants.TOP_SONGS_PATH, index=False)

    return MaterializeResult(
        metadata={
            "location": Path(constants.TOP_SONGS_PATH).absolute(),
            "row_count": random.randint(120, 140),
            "standard_deviation": random.randint(1, 10),
            "average": random.randint(125, 135)
        }
    )

@asset(
    group_name="gold",
    compute_kind="Databricks",
    deps=[plays, songs, artists],
    owners=["team:data_platform"],
    tags={
        "team": "data_platform",
        "stability": "stable",
        "contains_pii": "false"
    }
)
def trending_artists() -> MaterializeResult:
    """The artists that are trending on the music streaming platform
        Trending is calculated based on the number of plays and
        followers of the artist.
    """
    
    df = pd.DataFrame({
        "artist_id": [1, 2, 3],
        "name": ["Artist 1", "Artist 2", "Artist 3"],
        "plays": [1000, 2000, 3000]
    })

    df.to_csv(constants.TRENDING_ARTISTS_PATH, index=False)

    return MaterializeResult(
        metadata={
            "location": Path(constants.TRENDING_ARTISTS_PATH).absolute(),
            "row_count": random.randint(120, 140),
            "standard_deviation": random.randint(1, 10),
            "average": random.randint(125, 135)
        }
    )

@asset(
    group_name="gold",
    compute_kind="Databricks",
    deps=[songs, plays, users, followers],
    owners=["team:data_platform"],
    tags={
        "team": "data_platform",
        "stability": "stable",
        "contains_pii": "false"
    }
)
def recommended_songs() -> MaterializeResult:
    """The songs recommended to each user on the music streaming platform"""
    
    df = pd.DataFrame({
        "song_id": [1, 2, 3],
        "name": ["Song 1", "Song 2", "Song 3"],
        "plays": [100, 200, 300]
    })

    df.to_csv(constants.RECCOMENDED_SONGS_PATH, index=False)

    return MaterializeResult(
        metadata={
            "location": Path(constants.RECCOMENDED_SONGS_PATH).absolute(),
            "row_count": random.randint(120, 140),
            "standard_deviation": random.randint(1, 10),
            "average": random.randint(125, 135)
        }
    )

@asset(
    group_name="gold",
    compute_kind="Databricks",
    deps=[users, followers],
    owners=["team:data_platform"],
    tags={
        "team": "data_platform",
        "stability": "stable",
        "contains_pii": "false"
    }
)
def best_friends() -> MaterializeResult:
    """
    
    The best friends of each user in the music streaming platform.
    
    """
    
    df = pd.DataFrame({
        "user_id": [1, 2, 3],
        "best_friend_id": [2, 3, 1]
    })

    df.to_csv(constants.BEST_FRIENDS_PATH, index=False)

    return MaterializeResult(
        metadata={
            "location": Path(constants.BEST_FRIENDS_PATH).absolute(),
            "row_count": random.randint(120, 140),
            "standard_deviation": random.randint(1, 10),
            "average": random.randint(125, 135)
        }
    )

def generate_asset_check(asset: AssetsDefinition, assertion: str):
    asset_name = asset.key.to_user_string().replace("/", "_")

    @asset_check(
        asset=asset,
        name=f"is_{assertion}_for_{asset_name}",
    )
    def _asset_check() -> AssetCheckResult:
        return AssetCheckResult(
            passed=True
        )
    
    return _asset_check

data_products = [
    best_friends,
    trending_artists,
    top_songs,
    recommended_songs,
    subscriptions
]

nullness_checks = [
    generate_asset_check(asset, "not_null") for asset in data_products
]
distribution_checks = [
    generate_asset_check(asset, "fairly_distributed") for asset in data_products
]
cardinality_checks = [
    generate_asset_check(asset, "valid_cardinality") for asset in data_products
]
type_checks = [
    generate_asset_check(asset, "valid_type") for asset in data_products
]

freshness_checks = build_last_update_freshness_checks(
    assets=data_products, lower_bound_delta=timedelta(hours=2)
)