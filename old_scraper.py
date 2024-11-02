
import os
import time
import logging
import re
import whisper
import warnings
from pytubefix import YouTube
from pytubefix.cli import on_progress
from typing import List, Dict, Optional
from datetime import datetime

from dotenv import load_dotenv
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled

from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Configure warning for torch dtype FP16. because by default is using FP32
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

Base = declarative_base()

class YouTubeTranscriptCorpus(Base):
    __tablename__ = 'youtube_transcript_corpus'
     
    id = Column(String(50), unique=True, nullable=False, primary_key=True)  # YouTube video ID
    title = Column(String(500), nullable=False)
    channel_title = Column(String(500))
    published_at = Column(DateTime)
    transcript_text = Column(Text)
    language = Column(String(10))
    has_caption = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class YouTubeProcessedData(Base):
    __tablename__ = 'youtube_processed_data'
    
    id = Column(String(50), unique=True, nullable=False, primary_key=True)  # YouTube video ID
    processed_at = Column(DateTime, default=datetime.utcnow)

class YouTubeTranscriptScraper:
    def __init__(self):
        """
        Initialize YouTube Transcript Scraper
        
        :param api_key: YouTube Data API v3 key
        :param database_url: PostgreSQL database connection string
        """
        # YouTube API setup
        self.api_key = os.getenv('YOUTUBE_API_KEY')
        if not self.api_key:
            raise ValueError("No YouTube API key provided")
        
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
        
        # Database setup
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("No database URL provided")
        
        self.engine = create_engine(self.database_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
    
    def search_videos(self, 
                query: str, 
                max_results: int = 50, 
                language: str = 'id') -> List[Dict]:
      """
      Search for videos using YouTube Data API with optimized request handling
      to minimize API quota usage.

      :param query: Search query
      :param max_results: Maximum number of results to retrieve
      :param language: Language preference
      :return: List of video metadata
      """
      videos = []
      try:
          # Calculate the number of API calls needed
          # YouTube API allows max 50 results per request
          items_per_request = 50
          required_requests = (max_results + items_per_request - 1) // items_per_request
          
          # Make a single request if max_results <= 50
          if max_results <= items_per_request:
              request = self.youtube.search().list(
                  q=query,
                  type='video',
                  part='id,snippet',
                  maxResults=max_results,
                  relevanceLanguage=language
              )
              response = request.execute()
              
              for item in response.get('items', []):
                  video_info = {
                      'video_id': item['id']['videoId'],
                      'title': item['snippet']['title'],
                      'channel_title': item['snippet']['channelTitle'],
                      'published_at': datetime.fromisoformat(
                          item['snippet']['publishedAt'].replace('Z', '+00:00')
                      )
                  }
                  videos.append(video_info)
                  
          # Handle pagination if more results are needed
          else:
              page_token = None
              for _ in range(required_requests):
                  remaining_results = max_results - len(videos)
                  if remaining_results <= 0:
                      break
                      
                  request = self.youtube.search().list(
                      q=query,
                      type='video',
                      part='id,snippet',
                      maxResults=min(items_per_request, remaining_results),
                      relevanceLanguage=language,
                      pageToken=page_token
                  )
                  response = request.execute()
                  
                  for item in response.get('items', []):
                      video_info = {
                          'video_id': item['id']['videoId'],
                          'title': item['snippet']['title'],
                          'channel_title': item['snippet']['channelTitle'],
                          'published_at': datetime.fromisoformat(
                              item['snippet']['publishedAt'].replace('Z', '+00:00')
                          )
                      }
                      videos.append(video_info)
                  
                  # Get next page token
                  page_token = response.get('nextPageToken')
                  if not page_token:
                      break
                      
                  # Add delay between requests to prevent rate limiting
                  time.sleep(1)
                  
                  # Log the progress
                  logger.info(f"Fetched {len(videos)}/{max_results} videos...")
      
      except Exception as e:
          logger.error(f"Error searching videos: {e}")
      
      # Ensure we don't exceed max_results
      return videos[:max_results]
        
    
    def clean_transcript(self, text: str) -> str:
        """
        Clean and preprocess transcript text
        
        :param text: Raw transcript text
        :return: Cleaned transcript text
        """
        # Remove timestamps, extra whitespaces
        text = re.sub(r'\d+:\d+:\d+\s*\n', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Filter out very short or non-Indonesian looking transcripts
        if len(text.split()) < 10:
            return ""
        
        # Basic Indonesian language detection 
        # (rough check for Indonesian-like words or patterns)
        indonesian_indicators = [
            'yang', 'dari', 'dengan', 'untuk', 'dalam', 'pada', 'ini', 'itu',
            'jadi', 'ada', 'tidak', 'sudah', 'akan', 'seperti'
        ]
        if not any(word in text.lower() for word in indonesian_indicators):
            return ""
        
        return text
    
    def get_transcript(self, 
                       video_id: str, 
                       preferred_languages: List[str] = ['id', 'en']) -> Optional[Dict]:
        """
        Retrieve transcript for a given video with multiple fallback methods.

        :param video_id: YouTube video ID
        :param preferred_languages: List of preferred language codes
        :return: Transcript dictionary or None
        """
        try:
            # Attempt to fetch transcript in preferred languages
            transcript_list = YouTubeTranscriptApi.get_transcript(
                video_id, 
                languages=preferred_languages
            )
            
            # Combine transcript text
            full_transcript = ' '.join([entry['text'] for entry in transcript_list])
            cleaned_transcript = self.clean_transcript(full_transcript)
            
            if cleaned_transcript:
                return {
                    'transcript_text': cleaned_transcript,
                    'language': transcript_list[0].get('language', 'unknown'),
                    'has_caption': True
                }

        except (NoTranscriptFound, TranscriptsDisabled) as e:
            try:
                # Attempt to fetch auto-generated transcript if available
                available_transcripts = YouTubeTranscriptApi.list_transcripts(video_id)
                
                for transcript in available_transcripts:
                    if transcript.is_generated:
                        full_transcript = ' '.join([entry['text'] for entry in transcript.fetch()])
                        cleaned_transcript = self.clean_transcript(full_transcript)
                        
                        if cleaned_transcript:
                            return {
                                'transcript_text': cleaned_transcript,
                                'language': transcript.language_code,
                                'has_caption': True
                            }
            except Exception:
                pass

            # Fallback to local Whisper transcription
            transcript_text = self.get_transcript_with_whisper(video_id)
            if transcript_text:
                return {
                    'transcript_text': transcript_text,
                    'language': 'id',
                    'has_caption': False
                }
            else:
                logger.warning(f"No transcript found for video {video_id}")
                return None
        
        except Exception as e:
            logger.error(f"Unexpected error getting transcript for {video_id}: {e}")
            return None
        
    def get_transcript_with_whisper(self, video_id: str) -> Optional[str]:
        """
        Retrieve transcript using Whisper if other methods fail.

        :param video_id: YouTube video ID
        :return: Transcribed text or None
        """
        try:
            youtube_url = f"https://www.youtube.com/watch?v={video_id}"
            yt = YouTube(youtube_url, on_progress_callback=on_progress)
            
            download_dir = os.path.join(os.getcwd(), 'downloaded_audio')
            os.makedirs(download_dir, exist_ok=True)
            
            temp_audio_path = os.path.join(download_dir, f"{video_id}.mp3")    

            # Retry up to 3 times if there's connection timeout issue
            for attempt in range(3): 
                try:
                    logger.info(f"Download {yt.title}")
                    print(f"Temporary audio file path: {temp_audio_path}")
                    
                    ys = yt.streams.filter(only_audio=True).first()
                    ys.download(output_path=os.path.dirname(temp_audio_path), filename=f"{video_id}.mp3")

                    if os.path.exists(temp_audio_path) and os.path.getsize(temp_audio_path) > 0:
                        logger.info("Audio file downloaded successfully.")
                    else:
                        logger.error("Downloaded audio file is empty or not found.")
                    
                    # Transcribe audio using Whisper
                    model = whisper.load_model("turbo")
                    result = model.transcribe(temp_audio_path, language="id")
                    transcript_text = result['text']
                    
                    # Clean up temp file
                    os.remove(temp_audio_path)
                    
                    return self.clean_transcript(transcript_text)
                
                except Exception as e:
                    logger.error(f"Retry {attempt + 1} failed for video {video_id}: {e}")
                    time.sleep(1)  

        except Exception as e:
            logger.error(f"Error during Whisper transcription for video {video_id}: {e}")
            return None
    
    def build_corpus(self, 
                    query: str, 
                    max_results: int = 50, 
                    language: str = 'id') -> int:
        """
        Build corpus from YouTube videos
        
        :param query: Search query
        :param max_results: Maximum number of results
        :param language: Language preference
        :return: Number of videos processed
        """
        # Create database session
        session = self.Session()
        processed_count = 0

        try:
            videos = self.search_videos(query, max_results, language)
            
            for video in videos:
                video_id = video['video_id']
                video_title = video['title']

                # Start a new transaction for each video
                try:
                    # Check if video exists in either table using SELECT FOR UPDATE
                    # This locks the rows and prevents race conditions
                    existing_corpus = session.query(YouTubeTranscriptCorpus).filter_by(id=video_id)\
                        .with_for_update(skip_locked=True).first()
                    existing_processed = session.query(YouTubeProcessedData).filter_by(id=video_id)\
                        .with_for_update(skip_locked=True).first()

                    if existing_corpus or existing_processed:
                        logger.info(f"Skipping already processed video: {video_title} (ID: {video_id})")
                        session.commit()
                        continue
                    
                    # Mark as processed first
                    processed_entry = YouTubeProcessedData(id=video_id)
                    session.add(processed_entry)
                    session.flush()  # Ensure the processed entry is written before continuing
                    
                    # Get and process transcript
                    transcript_data = self.get_transcript(video_id)
                    
                    if transcript_data and transcript_data['transcript_text']:
                        corpus_entry = YouTubeTranscriptCorpus(
                            id=video_id,
                            title=video_title,
                            channel_title=video['channel_title'],
                            published_at=video['published_at'],
                            transcript_text=transcript_data['transcript_text'],
                            language=transcript_data.get('language', 'unknown'),
                            has_caption=transcript_data.get('has_caption', False)
                        )
                        
                        session.add(corpus_entry)
                        session.commit()
                        processed_count += 1
                        logger.info(f"Processed video: {video_title}")
                    else:
                        # If no transcript, still commit the processed entry
                        session.commit()
                        
                except IntegrityError as e:
                    session.rollback()
                    logger.warning(f"Race condition occurred for video {video_id}, skipping: {str(e)}")
                    continue
                    
                # Prevent rate limiting
                time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error building corpus: {e}")
            session.rollback()
            raise
            
        finally:
            session.close()
        
        logger.info(f"Total processed videos for query '{query}': {processed_count}")
        return processed_count

def main():
    # Initialize scraper
    scraper = YouTubeTranscriptScraper()
    
    queries = [
        # "Bencana di Pantai Selatan Yogyakarta",
        # "Gelombang Tinggi Pantai Parangtritis",
        # "Kearifan Lokal Bencana Alam Pantai Selatan Yogyakarta",
        # "Tanda tanda bencana di Pantai Selatan",
        # "Kejadian Sebelum bencana di Pantai Parangtritis",
        # "Tanda alam sebelum adanya bencana di Pantai Selatan menurut masyarakat",
        # "Saksi bencana di Pantai Selatan Yogyakarta",
        # "Kepercayaan masyarakat terkait bencana di Pantai Parangtritis",
        # "Peringatan dini bencana alam di Pantai Parangtritis"
       
        # "Kearifan lokal Parangtritis Yogyakarta",
        # "Cerita rakyat Parangtritis dan bencana alam",
        # "Mitigasi bencana berbasis kearifan lokal di Parangtritis",
        # "Tradisi masyarakat Parangtritis dalam menghadapi bencana",
        # "Pantai Parangtritis dan kearifan lokal dalam menghindari bencana",
        # "Parangtritis Yogyakarta: budaya dan mitigasi bencana",
        # "Pengaruh legenda Nyi Roro Kidul terhadap kebencanaan di Parangtritis",
        # "Tradisi masyarakat Yogyakarta dalam menghadapi bencana alam",
        # "Kearifan lokal Yogyakarta dalam mengantisipasi bencana pantai",
        # "Pentingnya kearifan lokal untuk mitigasi bencana di Pantai Parangtritis",
        # "Tanda sebelum bencana menurut masyarakat parangtritis",
        # "Kejadian aneh sebelum bencana di parangtritis",
        
        # "Legenda Parangtritis dan peringatan bencana",
        # "Wisata budaya dan kearifan lokal Parangtritis",
        # "Peran kearifan lokal dalam mitigasi tsunami di Parangtritis",
        # "Kepercayaan masyarakat Parangtritis tentang bencana alam",
        # "Kisah rakyat Yogyakarta terkait keselamatan di laut",
        # "Kearifan lokal untuk menghadapi cuaca ekstrem di Parangtritis",
        # "Upacara tradisional Parangtritis untuk menghindari bencana",
        # "Hubungan budaya dan alam di Pantai Parangtritis",
        # "Mitigasi bencana di Yogyakarta melalui tradisi lokal",
        # "Kearifan lokal Jawa dalam menjaga keselamatan pantai"
        
        "Budaya Parangtritis dan kesiapsiagaan bencana",
        "Peran kearifan lokal dalam menjaga alam Parangtritis",
        "Mitos dan legenda terkait bencana di Yogyakarta",
        "Pengaruh budaya lokal terhadap penanganan bencana",
        "Tradisi lokal Parangtritis untuk keselamatan wisatawan",
        "Upaya masyarakat Yogyakarta dalam mitigasi bencana",
        "Kisah spiritual Parangtritis dan bencana alam",
        "Peran tradisi dalam pencegahan bencana di Parangtritis",
        "Kepercayaan lokal Parangtritis terkait keselamatan pantai",
        "Cerita rakyat Yogyakarta tentang bahaya alam",
        "Pantai Parangtritis dan kearifan lokal Jawa",
        "Kebiasaan lokal Parangtritis dalam menghadapi tsunami",
        "Nilai-nilai budaya Parangtritis dalam mitigasi bencana",
        "Budaya masyarakat Yogyakarta dan perlindungan alam",
        "Kearifan lokal untuk menjaga keamanan pantai Parangtritis",
        "Cerita mistis Parangtritis terkait keselamatan laut",
        "Tradisi labuhan dan keselamatan di Pantai Parangtritis",
        "Mitigasi bencana berbasis budaya lokal di Yogyakarta",
        "Legenda Ratu Pantai Selatan dan bencana Parangtritis",
        "Kearifan lokal sebagai penuntun keselamatan di Parangtritis"
    ]
    
    total_processed = 0
    for query in queries:
        logger.info(f"Processing query: {query}")
        processed = scraper.build_corpus(query, max_results=500)
        total_processed += processed
    
    logger.info(f"Overall total processed videos: {total_processed}")

if __name__ == "__main__":
    main()