import youtube_search
import json

results = youtube_search.search_youtube("Python programming", max_results=5)
print(json.dumps(results, indent=2))