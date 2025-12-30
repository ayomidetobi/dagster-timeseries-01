"""Configuration for HackerNews stories ingestion."""

from dagster import Config


class HNStoriesConfig(Config):
    """Configuration for HackerNews stories ingestion."""

    top_stories_limit: int = 10
    hn_top_story_ids_path: str = "hackernews_top_story_ids.json"
    hn_top_stories_path: str = "hackernews_top_stories.csv"
