from googleapiclient.discovery import build
import pandas as pd
from typing import Optional


def get_channel_videos(channel_id_or_name: str, api_key: str) -> pd.DataFrame:
    """
    Retrieve all videos from a YouTube channel and return them as a DataFrame.

    Args:
        channel_id_or_name: Channel ID (starting with UC) or channel name
        api_key: YouTube Data API key

    Returns:
        DataFrame containing video details with columns:
        title, published_at, description, video_url, thumbnail_url
    """
    youtube = build("youtube", "v3", developerKey=api_key)

    # If channel name provided, get channel ID
    if not channel_id_or_name.startswith("UC"):
        response = (
            youtube.search()
            .list(q=channel_id_or_name, type="channel", part="id", maxResults=1)
            .execute()
        )

        if not response["items"]:
            raise ValueError(f"Channel not found: {channel_id_or_name}")
        channel_id = response["items"][0]["id"]["channelId"]
    else:
        channel_id = channel_id_or_name

    # Get channel's uploads playlist ID
    response = youtube.channels().list(part="contentDetails", id=channel_id).execute()

    if not response["items"]:
        raise ValueError(f"Channel ID not found: {channel_id}")

    playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # Fetch all videos from uploads playlist
    videos = []
    next_page_token = None

    while True:
        response = (
            youtube.playlistItems()
            .list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token,
            )
            .execute()
        )

        for item in response["items"]:
            snippet = item["snippet"]
            video_id = snippet["resourceId"]["videoId"]
            videos.append(
                {
                    "title": snippet["title"],
                    "published_at": snippet["publishedAt"],
                    "description": snippet["description"],
                    "video_url": f"https://www.youtube.com/watch?v={video_id}",
                    "thumbnail_url": snippet["thumbnails"]["high"]["url"],
                }
            )

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return pd.DataFrame(videos)
