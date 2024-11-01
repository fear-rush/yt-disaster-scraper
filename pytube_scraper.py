import os
import time
import logging
import re
import whisper
import warnings
from pytube import Search
from pytubefix import YouTube
from pytubefix.cli import on_progress
from typing import List, Dict, Optional
from datetime import datetime

from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled

from sqlalchemy import create_engine, Column, String, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Configure warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# Logging configuration
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

Base = declarative_base()

class YouTubeTranscriptCorpus(Base):
    __tablename__ = 'yt_transcript_corpus'
    
    id = Column(String(50), primary_key=True)
    title = Column(String(500), nullable=False)
    channel_title = Column(String(500))
    published_at = Column(DateTime)
    transcript_text = Column(Text)
    language = Column(String(10))
    has_caption = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class YouTubeTranscriptScraper:
    def __init__(self):
        """
        Initialize YouTube Transcript Scraper
        
        :param database_url: PostgreSQL database connection string
        """
        # Database setup
        self.database_url = os.getenv('POSTGRESQL_URL')
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
        Search for videos using Pytube Search
        
        :param query: Search query
        :param max_results: Maximum number of results to retrieve
        :param language: Language preference (note: pytube doesn't support language filtering)
        :return: List of video metadata
        """
        videos = []
        try:
            # Perform initial search
            search = Search(query)
            
            # Collect initial results
            while len(videos) < max_results and len(search.results) > 0:
                for result in search.results:
                    if len(videos) >= max_results:
                        break
                    
                    video_info = {
                        'id': result.video_id,
                        'title': result.title,
                        'channel_title': result.author,
                        'published_at': datetime.fromtimestamp(result.publish_date.timestamp()) if result.publish_date else None
                    }
                    videos.append(video_info)
                
                # If we haven't reached max results, try to get next page of results
                if len(videos) < max_results:
                    try:
                        search.get_next_results()
                    except Exception as e:
                        logger.warning(f"No more results available: {e}")
                        break
        
        except Exception as e:
            logger.error(f"Error searching videos with Pytube: {e}")
        
        print(len(videos))
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
        indonesian_indicators = [
            'yang', 'dari', 'dengan', 'untuk', 'dalam', 'pada', 'ini', 'itu',
            'jadi', 'ada', 'tidak', 'sudah', 'akan', 'seperti'
        ]
        if not any(word in text.lower() for word in indonesian_indicators):
            return ""
        
        return text
    
    def get_transcript(self, video_id: str, 
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

        except (NoTranscriptFound, TranscriptsDisabled):
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
                    
                    ys = yt.streams.filter(only_audio=True).first()
                    ys.download(output_path=os.path.dirname(temp_audio_path), filename=f"{video_id}.mp3")
                    
                    if os.path.exists(temp_audio_path) and os.path.getsize(temp_audio_path) > 0:
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
        
        if os.path.exists(temp_audio_path):
            os.remove(temp_audio_path)
            
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
        session = self.Session()
        try:
            # Search existing entries
            existing_entries = {entry.id: entry.transcript_text for entry in session.query(YouTubeTranscriptCorpus).all()}
            
            videos = self.search_videos(query, max_results, language)
            processed_count = 0
            
            for video in videos:
                video_id = video['id']
                video_title = video['title']

                if video_id in existing_entries:
                    logger.info(f"Skipping already processed video: {video_title} (ID: {video_id})")
                    continue
                
                transcript_data = self.get_transcript(video_id)

                if transcript_data and transcript_data['transcript_text']:
                    transcript_text = transcript_data['transcript_text']

                    if any(existing_transcript == transcript_text for existing_transcript in existing_entries.values()):
                        logger.info(f"Skipping duplicate transcript for video: {video_title}")
                        continue

                    corpus_entry = YouTubeTranscriptCorpus(
                        id=video_id,
                        title=video_title,
                        channel_title=video['channel_title'],
                        published_at=video['published_at'],
                        transcript_text=transcript_text,
                        language=transcript_data.get('language', 'unknown'),
                        has_caption=transcript_data.get('has_caption', False)
                    )
                    
                    try:
                        session.add(corpus_entry)
                        session.commit()
                        processed_count += 1
                        logger.info(f"Processed video: {video_title}")
                    except IntegrityError as e:
                        session.rollback()
                        logger.error(f"Error inserting data for video {video_id}: {str(e)}")

                # Prevent rate limiting
                time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error building corpus: {e}")
            session.rollback()
            
        finally:
            session.close()
        
        logger.info(f"Total processed videos for query '{query}': {processed_count}")
        return processed_count


def main():
    # Initialize scraper
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
        processed = scraper.build_corpus(query, max_results=100)
        total_processed += processed
    
    logger.info(f"Overall total processed videos: {total_processed}")

if __name__ == "__main__":
    main()