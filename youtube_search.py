import requests
import json
from typing import List, Dict

YOUTUBE_ENDPOINT = "https://www.youtube.com"


def get_youtube_init_data(url: str):
    try:
        page = requests.get(url)
        data = page.text

        # Extract ytInitialData
        init_data_split = data.split("var ytInitialData =")
        if len(init_data_split) > 1:
            init_data_json = init_data_split[1].split("</script>")[0].strip()[:-1]
            init_data = json.loads(init_data_json)

            # Extract API token
            api_token_split = data.split("innertubeApiKey")
            api_token = (
                api_token_split[1].split(",")[0].split('"')[2] if len(api_token_split) > 1 else None
            )

            # Extract INNERTUBE context
            context_split = data.split("INNERTUBE_CONTEXT")
            context = (
                json.loads(context_split[1].strip()[2:-2]) if len(context_split) > 1 else None
            )

            return {"initdata": init_data, "apiToken": api_token, "context": context}

        else:
            print("Error: Cannot get initial data")
            return None

    except Exception as e:
        print(f"Error fetching YouTube data: {e}")
        return None


def get_video_details(video_id: str):
    url = f"{YOUTUBE_ENDPOINT}/watch?v={video_id}"
    data = get_youtube_init_data(url)

    if data:
        try:
            player_data_split = data['initdata']['contents']['twoColumnWatchNextResults']['results']['results']['contents']
            video_primary_info = player_data_split[0]['videoPrimaryInfoRenderer']
            video_secondary_info = player_data_split[1]['videoSecondaryInfoRenderer']

            video_details = {
                "id": video_id,
                "title": video_primary_info['title']['runs'][0]['text'],
                "thumbnail": data['initdata']['contents']['twoColumnWatchNextResults']['thumbnail'],
                "isLive": video_primary_info['viewCount']['videoViewCountRenderer'].get('isLive', False),
                "channel": video_secondary_info['owner']['videoOwnerRenderer']['title']['runs'][0]['text'],
                "channelId": data['initdata']['contents']['twoColumnWatchNextResults']['channelId'],
                "description": data['initdata']['contents']['twoColumnWatchNextResults']['description'],
            }

            return video_details

        except KeyError as e:
            print(f"Error parsing video details: {e}")
            return None
    else:
        return None


def search_youtube(query: str, max_results: int = 10):
    url = f"{YOUTUBE_ENDPOINT}/results?search_query={query}"
    data = get_youtube_init_data(url)

    if not data:
        return None

    section_list_renderer = (
        data['initdata']['contents']
        .get('twoColumnSearchResultsRenderer', {})
        .get('primaryContents', {})
        .get('sectionListRenderer', {})
    )

    items = []
    continuation_token = None

    for content in section_list_renderer.get('contents', []):
        if 'continuationItemRenderer' in content:
            continuation_token = content['continuationItemRenderer']['continuationEndpoint']['continuationCommand']['token']
        elif 'itemSectionRenderer' in content:
            for item in content['itemSectionRenderer'].get('contents', []):
                if 'videoRenderer' in item:
                    video_renderer = item['videoRenderer']
                    items.append({
                        "id": video_renderer['videoId'],
                        "type": "video",
                        "thumbnail": video_renderer.get('thumbnail', {}),
                        "title": video_renderer['title']['runs'][0]['text'],
                        "channelTitle": video_renderer.get('ownerText', {}).get('runs', [{}])[0].get('text', ''),
                        "length": video_renderer.get('lengthText', {}).get('simpleText', ''),
                        "isLive": 'LIVE' in video_renderer.get('badges', [{}])[0].get('metadataBadgeRenderer', {}).get('style', ''),
                    })
                    if len(items) >= max_results:
                        return items

    # Continuation (for pagination, if needed)
    next_page_context = {
        "continuation": continuation_token,
        "apiToken": data['apiToken'],
        "context": data['context'],
    }
    return {"items": items, "nextPage": next_page_context}