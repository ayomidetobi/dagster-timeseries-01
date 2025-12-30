"""HackerNews API logic."""

import json
from typing import Any, Dict, List

import pandas as pd
import requests
from dagster import AssetExecutionContext

from dagster_quickstart.utils.exceptions import DatabaseError

from .config import HNStoriesConfig


def fetch_hackernews_api(url: str, context: AssetExecutionContext) -> Any:
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


def fetch_top_story_ids(context: AssetExecutionContext, config: HNStoriesConfig) -> List[int]:
    """Fetch top story IDs from HackerNews API.

    Args:
        context: Dagster execution context
        config: HackerNews configuration

    Returns:
        List of top story IDs

    Raises:
        DatabaseError: If API request fails
    """
    context.log.info(f"Fetching top {config.top_stories_limit} story IDs from HackerNews")

    top_story_ids = fetch_hackernews_api(
        "https://hacker-news.firebaseio.com/v0/topstories.json", context
    )

    return top_story_ids[: config.top_stories_limit]


def save_story_ids(story_ids: List[int], file_path: str, context: AssetExecutionContext) -> None:
    """Save story IDs to JSON file.

    Args:
        story_ids: List of story IDs to save
        file_path: Path to save the JSON file
        context: Dagster execution context for logging

    Raises:
        DatabaseError: If file write fails
    """
    try:
        with open(file_path, "w") as f:
            json.dump(story_ids, f)
        context.log.info(f"Successfully saved {len(story_ids)} story IDs")
    except (IOError, OSError) as e:
        context.log.error(f"Failed to write story IDs to file: {e}")
        raise DatabaseError(f"Failed to write story IDs to {file_path}") from e


def load_story_ids(file_path: str, context: AssetExecutionContext) -> List[int]:
    """Load story IDs from JSON file.

    Args:
        file_path: Path to the JSON file
        context: Dagster execution context for logging

    Returns:
        List of story IDs

    Raises:
        DatabaseError: If file read fails
    """
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except (IOError, OSError, json.JSONDecodeError) as e:
        context.log.error(f"Failed to read story IDs from file: {e}")
        raise DatabaseError(f"Failed to read story IDs from {file_path}") from e


def fetch_story_details(
    story_ids: List[int], context: AssetExecutionContext
) -> List[Dict[str, Any]]:
    """Fetch story details from HackerNews API.

    Args:
        story_ids: List of story IDs to fetch
        context: Dagster execution context

    Returns:
        List of story detail dictionaries

    Raises:
        DatabaseError: If API requests fail or no data retrieved
    """
    context.log.info(f"Fetching {len(story_ids)} story details from HackerNews")

    results: List[Dict[str, Any]] = []
    for idx, item_id in enumerate(story_ids, 1):
        context.log.debug(f"Fetching story {idx}/{len(story_ids)}: {item_id}")
        item = fetch_hackernews_api(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json", context
        )
        if item:
            results.append(item)

    if not results:
        context.log.warning("No story data retrieved")
        raise DatabaseError("No story data retrieved from HackerNews API")

    return results


def save_stories_to_csv(
    stories: List[Dict[str, Any]], file_path: str, context: AssetExecutionContext
) -> pd.DataFrame:
    """Save stories to CSV file.

    Args:
        stories: List of story dictionaries
        file_path: Path to save the CSV file
        context: Dagster execution context for logging

    Returns:
        DataFrame with stories

    Raises:
        DatabaseError: If file write fails
    """
    df = pd.DataFrame(stories)
    context.log.info(f"Retrieved {len(df)} stories, saving to CSV")

    try:
        df.to_csv(file_path, index=False)
        context.log.info(f"Successfully saved stories to {file_path}")
    except (IOError, OSError) as e:
        context.log.error(f"Failed to write stories to CSV: {e}")
        raise DatabaseError(f"Failed to write stories to {file_path}") from e

    return df
