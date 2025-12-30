"""HackerNews data ingestion assets."""

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)

from dagster_quickstart.utils.constants import (
    RETRY_POLICY_DELAY_DEFAULT,
    RETRY_POLICY_MAX_RETRIES_DEFAULT,
)

from .config import HNStoriesConfig
from .logic import (
    fetch_story_details,
    fetch_top_story_ids,
    load_story_ids,
    save_stories_to_csv,
    save_story_ids,
)


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
    story_ids = fetch_top_story_ids(context, config)
    save_story_ids(story_ids, config.hn_top_story_ids_path, context)


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
    story_ids = load_story_ids(config.hn_top_story_ids_path, context)
    stories = fetch_story_details(story_ids, context)
    df = save_stories_to_csv(stories, config.hn_top_stories_path, context)

    return MaterializeResult(
        metadata={
            "num_records": MetadataValue.int(len(df)),
            "preview": MetadataValue.md(str(df[["title", "by", "url"]].to_markdown())),
        }
    )
