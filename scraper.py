import os
import time
import logging
import re
import whisper
import torch
import warnings
import aiotube
from pytubefix import YouTube
from pytubefix.cli import on_progress
from typing import List, Dict, Optional, Set
from datetime import datetime

from dotenv import load_dotenv
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled

from sqlalchemy import create_engine, Column, String, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

import youtube_search

# Configure warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# Configure logging
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

class YouTubeTranscriptScraper:
    def __init__(self):
        """Initialize YouTube Transcript Scraper with cached data and CUDA support"""
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

        # Cache existing video IDs and transcripts
        self.cached_video_ids: Set[str] = set()
        self.cached_transcripts: Set[str] = set()
        self._load_cache()

        # Initialize Whisper model with CUDA if available
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Using device: {self.device}")
        self.whisper_model = None  # Lazy loading

    def _load_cache(self):
        """Load existing video IDs and transcripts into memory"""
        session = self.Session()
        try:
            entries = session.query(YouTubeTranscriptCorpus).all()
            self.cached_video_ids = {entry.id for entry in entries}  # Change to use `id`
            self.cached_transcripts = {entry.transcript_text for entry in entries if entry.transcript_text}
            logger.info(f"Cached {len(self.cached_video_ids)} video IDs and {len(self.cached_transcripts)} transcripts")
        finally:
            session.close()

    def _load_whisper_model(self):
        """Lazy load Whisper model"""
        if self.whisper_model is None:
            self.whisper_model = whisper.load_model("turbo", device=self.device)
            logger.info("Whisper model loaded successfully")

    def clean_transcript(self, text: str) -> str:
        """Clean and preprocess transcript text"""
        # Remove timestamps, extra whitespaces
        text = re.sub(r'\d+:\d+:\d+\s*\n', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Filter out very short or non-Indonesian looking transcripts
        if len(text.split()) < 10:
            return ""
        
        # Basic Indonesian language detection 
        indonesian_indicators = [
            'yang', 'dari', 'dengan', 'untuk', 'dalam', 'pada', 'ini', 'itu',
            'jadi', 'ada', 'tidak', 'sudah', 'akan', 'seperti'
        ]
        if not any(word in text.lower() for word in indonesian_indicators):
            return ""
        
        return text

    def search_videos(self, query: str, max_results: int = 50, language: str = 'id') -> List[Dict]:
        """Efficiently search for videos using the custom youtube_search module and filter out existing ones."""
        videos = []
        seen_video_ids = set()

        try:
            # Call your custom search function to get results
            search_results = youtube_search.search_youtube(query, max_results=max_results)
            
            # Debug: Print the structure of search_results
            logger.debug(f"Search results: {search_results} (type: {type(search_results)})")

            # Check if search_results is a list
            if isinstance(search_results, dict):
                # Log the keys of the dictionary to understand its structure
                logger.debug(f"Keys in search results dictionary: {search_results.keys()}")

                # Assuming your videos are in a key named 'items' (you may need to adjust based on the actual key)
                items = search_results.get('items', [])
                if not isinstance(items, list):
                    logger.error("Expected 'items' in search results to be a list, but got: {}".format(type(items)))
                    return videos
            elif not isinstance(search_results, list):
                logger.error("Expected search results to be a list, but got: {}".format(type(search_results)))
                return videos
            else:
                items = search_results

            for video_data in items:
                # Debug: Print each video_data
                logger.debug(f"Processing video data: {video_data}")

                # Ensure video_data is a dictionary
                if not isinstance(video_data, dict):
                    logger.error("Expected video_data to be a dictionary, but got: {}".format(type(video_data)))
                    continue

                video_id = video_data.get('id')

                # Skip duplicates by checking cached and seen video IDs
                if video_id not in self.cached_video_ids and video_id not in seen_video_ids:
                    video_info = {
                        'id': video_id,  # Use 'id' instead of 'video_id'
                        'title': video_data.get('title', 'Unknown Title'),
                        'channel_title': video_data.get('channelTitle', 'Unknown Channel'),
                        'thumbnail_url': video_data['thumbnail']['thumbnails'][0]['url'] if 'thumbnail' in video_data else None,
                        'length': video_data.get('length', 'Unknown Length'),
                        'is_live': video_data.get('isLive', False),
                        'published_at': None,  # Adjust this if your data includes publishedAt
                    }
                    videos.append(video_info)
                    seen_video_ids.add(video_id)  # Track video to prevent duplicates

                    # Stop when reaching max_results
                    if len(videos) >= max_results:
                        break

        except Exception as e:
            logger.error(f"Error searching videos with youtube_search: {e}")

        logger.info(f"Total unique videos found after filtering: {len(videos)}")
        return videos
    
    def get_transcript(self, video_id: str, preferred_languages: List[str] = ['id', 'en']) -> Optional[Dict]:
        """
        Retrieve transcript with thorough duplicate checking before any retrieval attempts.
        If the transcript is disabled, generate it using Whisper.
        """
        # First check if video already exists in cache
        if video_id in self.cached_video_ids:
            logger.info(f"Skipping transcript retrieval - Video {video_id} already exists in database")
            return None

        # Double-check database directly to ensure most up-to-date status
        session = self.Session()
        try:
            existing_entry = session.query(YouTubeTranscriptCorpus).filter_by(id=video_id).first()
            if existing_entry:
                logger.info(f"Skipping transcript retrieval - Video {video_id} found in database")
                self.cached_video_ids.add(video_id)
                return None
        finally:
            session.close()

        try:
            # Step 1: Try to get transcript from YouTube API
            transcript_list = YouTubeTranscriptApi.get_transcript(
                video_id, 
                languages=preferred_languages
            )
            
            # Combine and clean transcript text
            full_transcript = ' '.join([entry['text'] for entry in transcript_list])
            cleaned_transcript = self.clean_transcript(full_transcript)
            
            if cleaned_transcript:
                # Check for duplicate transcript content
                if cleaned_transcript in self.cached_transcripts:
                    logger.info(f"Skipping - Duplicate transcript content found for video {video_id}")
                    return None
                
                return {
                    'transcript_text': cleaned_transcript,
                    'language': transcript_list[0].get('language', 'unknown'),
                    'has_caption': True
                }

        except (NoTranscriptFound, TranscriptsDisabled) as e:
            logger.info(f"No YouTube transcript found for {video_id}, trying Whisper transcription...")
            # Fall back to Whisper transcription
            transcript_data = self.get_transcript_with_whisper(video_id)
            
            if transcript_data:
                return {
                    'transcript_text': transcript_data,
                    'language': 'id',  # Assuming default language for Whisper; modify as needed
                    'has_caption': False
                }
            else:
                logger.warning(f"Whisper transcription failed for video {video_id}")

        except Exception as e:
            logger.error(f"Error retrieving transcripts for video {video_id}: {e}")

        return None

    def get_transcript_with_whisper(self, video_id: str) -> Optional[str]:
        """Retrieve transcript using Whisper with CUDA support"""
        try:
            youtube_url = f"https://www.youtube.com/watch?v={video_id}"
            yt = YouTube(youtube_url, on_progress_callback=on_progress)
            
            download_dir = os.path.join(os.getcwd(), 'downloaded_audio')
            os.makedirs(download_dir, exist_ok=True)
            
            temp_audio_path = os.path.join(download_dir, f"{video_id}.mp3")

            try:
                logger.info(f"Downloading {yt.title}")
                ys = yt.streams.filter(only_audio=True).first()
                ys.download(output_path=os.path.dirname(temp_audio_path), 
                          filename=f"{video_id}.mp3")

                if not os.path.exists(temp_audio_path) or os.path.getsize(temp_audio_path) == 0:
                    logger.error("Downloaded audio file is empty or not found")
                    return None

                # Lazy load Whisper model
                self._load_whisper_model()
                
                # Transcribe with CUDA
                result = self.whisper_model.transcribe(
                    temp_audio_path,
                    language="id",
                    fp16=torch.cuda.is_available()  # Use FP16 if CUDA available
                )
                transcript_text = result['text']
                
                return self.clean_transcript(transcript_text)
                
            finally:
                # Clean up temp file
                if os.path.exists(temp_audio_path):
                    os.remove(temp_audio_path)

        except Exception as e:
            logger.error(f"Error during Whisper transcription for video {video_id}: {e}")
            return None

    def build_corpus(self, 
                    query: str, 
                    max_results: int = 50, 
                    language: str = 'id') -> int:
        """Build corpus with optimized duplicate checking"""
        session = self.Session()
        processed_count = 0

        try:
            videos = self.search_videos(query, max_results, language)
            
            for video in videos:
                video_id = video['id']
                
                # Get transcript (includes duplicate checking)
                transcript_data = self.get_transcript(video_id)
                
                if transcript_data and transcript_data['transcript_text']:
                    transcript_text = transcript_data['transcript_text']
                    
                    corpus_entry = YouTubeTranscriptCorpus(
                        id=video_id,
                        title=video['title'],
                        channel_title=video['channel_title'],
                        published_at=video['published_at'],
                        transcript_text=transcript_text,
                        language=transcript_data.get('language', 'unknown'),
                        has_caption=transcript_data.get('has_caption', False)
                    )
                    
                    try:
                        session.add(corpus_entry)
                        session.commit()
                        # Update cache after successful commit
                        self.cached_video_ids.add(video_id)
                        self.cached_transcripts.add(transcript_text)
                        processed_count += 1
                        logger.info(f"Successfully processed video: {video['title']}")
                    except IntegrityError as e:
                        session.rollback()
                        logger.error(f"Database error for video {video_id}: {str(e)}")
                
                time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error building corpus: {e}")
            session.rollback()
            
        finally:
            session.close()
        
        logger.info(f"Total processed videos for query '{query}': {processed_count}")
        return processed_count

def main():
    scraper = YouTubeTranscriptScraper()
    
    queries = [
        "Bencana di Pantai Selatan Yogyakarta",
        "Gelombang Tinggi Pantai Parangtritis",
        "Kearifan Lokal Bencana Alam Pantai Selatan Yogyakarta",
        "Tanda tanda bencana di Pantai Selatan",
        "Kejadian Sebelum bencana di Pantai Parangtritis",
        "Tanda alam sebelum adanya bencana di Pantai Selatan menurut masyarakat",
        "Saksi bencana di Pantai Selatan Yogyakarta",
        "Kepercayaan masyarakat terkait bencana di Pantai Parangtritis",
        "Peringatan dini bencana alam di Pantai Parangtritis"
    ]
    
    total_processed = 0
    for query in queries:
        logger.info(f"Processing query: {query}")
        processed = scraper.build_corpus(query, max_results=500)
        total_processed += processed
    
    logger.info(f"Overall total processed videos: {total_processed}")

if __name__ == "__main__":
    main()