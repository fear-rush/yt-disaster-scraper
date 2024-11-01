from pytubefix import Search

results = Search('Github Issue Best Practices')

for video in results.videos:
  print(f'Title: {video.title}')
  print(f'URL: {video.watch_url}')
  print(f'Duration: {video.length} sec')
  print('---')
  
