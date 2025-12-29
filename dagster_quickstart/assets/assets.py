"""HackerNews data ingestion assets."""

import json
from typing import Any, Dict, List

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)

from dagster_quickstart.utils.constants import (
    RETRY_POLICY_DELAY_DEFAULT,
    RETRY_POLICY_MAX_RETRIES_DEFAULT,
)
from dagster_quickstart.utils.exceptions import DatabaseError


class HNStoriesConfig(Config):
    """Configuration for HackerNews stories ingestion."""

    top_stories_limit: int = 10
    hn_top_story_ids_path: str = "hackernews_top_story_ids.json"
    hn_top_stories_path: str = "hackernews_top_stories.csv"


def _fetch_hackernews_api(url: str, context: AssetExecutionContext) -> Any:
    """Fetch data from HackerNews API with error handling.

    Args:
        url: URL to fetch from
        context: Dagster execution context for logging

    Returns:
        JSON response data

    Raises:
        DatabaseError: If API request fails
    """
    try:
        context.log.info(f"Fetching data from HackerNews API: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        context.log.error(f"HackerNews API request failed: {e}")
        raise DatabaseError(f"Failed to fetch data from HackerNews API: {e}") from e


@asset(
    kinds=["hackernewsapi"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_DEFAULT, delay=RETRY_POLICY_DELAY_DEFAULT
    ),
)
def hackernews_top_story_ids(context: AssetExecutionContext, config: HNStoriesConfig) -> None:
    """Get top stories from the HackerNews top stories endpoint."""
    context.log.info(f"Fetching top {config.top_stories_limit} story IDs from HackerNews")

    top_story_ids = _fetch_hackernews_api(
        "https://hacker-news.firebaseio.com/v0/topstories.json", context
    )

    try:
        with open(config.hn_top_story_ids_path, "w") as f:
            json.dump(top_story_ids[: config.top_stories_limit], f)
        context.log.info(
            f"Successfully saved {len(top_story_ids[: config.top_stories_limit])} story IDs"
        )
    except (IOError, OSError) as e:
        context.log.error(f"Failed to write story IDs to file: {e}")
        raise DatabaseError(f"Failed to write story IDs to {config.hn_top_story_ids_path}") from e


@asset(
    deps=[hackernews_top_story_ids],
    kinds=["hackernewsapi"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_DEFAULT, delay=RETRY_POLICY_DELAY_DEFAULT
    ),
)
def hackernews_top_stories(
    context: AssetExecutionContext, config: HNStoriesConfig
) -> MaterializeResult:
    """Get items based on story ids from the HackerNews items endpoint."""
    context.log.info("Loading story IDs from file")

    try:
        with open(config.hn_top_story_ids_path, "r") as f:
            hackernews_top_story_ids = json.load(f)
    except (IOError, OSError, json.JSONDecodeError) as e:
        context.log.error(f"Failed to read story IDs from file: {e}")
        raise DatabaseError(f"Failed to read story IDs from {config.hn_top_story_ids_path}") from e

    context.log.info(f"Fetching {len(hackernews_top_story_ids)} story details from HackerNews")

    results: List[Dict[str, Any]] = []
    for idx, item_id in enumerate(hackernews_top_story_ids, 1):
        context.log.debug(f"Fetching story {idx}/{len(hackernews_top_story_ids)}: {item_id}")
        item = _fetch_hackernews_api(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json", context
        )
        if item:
            results.append(item)

    if not results:
        context.log.warning("No story data retrieved")
        raise DatabaseError("No story data retrieved from HackerNews API")

    df = pd.DataFrame(results)
    context.log.info(f"Retrieved {len(df)} stories, saving to CSV")

    try:
        df.to_csv(config.hn_top_stories_path, index=False)
        context.log.info(f"Successfully saved stories to {config.hn_top_stories_path}")
    except (IOError, OSError) as e:
        context.log.error(f"Failed to write stories to CSV: {e}")
        raise DatabaseError(f"Failed to write stories to {config.hn_top_stories_path}") from e

    return MaterializeResult(
        metadata={
            "num_records": MetadataValue.int(len(df)),
            "preview": MetadataValue.md(str(df[["title", "by", "url"]].to_markdown())),
        }
    )
