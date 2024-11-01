import os
import time
import logging
import re
from typing import List, Dict, Optional
from datetime import datetime, timezone

from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# SQLAlchemy Base and Model
Base = declarative_base()

class YouTubeTranscriptCorpus(Base):
    __tablename__ = 'youtube_transcript_corpus'
    
    id = Column(Integer, primary_key=True)
    video_id = Column(String(50), unique=True, nullable=False)
    title = Column(String(500), nullable=False)
    channel_title = Column(String(500))
    published_at = Column(DateTime)
    transcript_text = Column(Text)
    language = Column(String(10))
    has_caption = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now(timezone.utc))

class YouTubeTranscriptScraper:
    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize YouTube Transcript Scraper
        
        :param database_url: PostgreSQL database connection string
        """
        self.database_url = database_url or os.getenv('POSTGRESQL_URL')
        if not self.database_url:
            raise ValueError("No database URL provided")
        
        self.engine = create_engine(self.database_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
    
    def search_videos(self, query: str, max_results: int = 50) -> List[Dict]:
        videos = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)  # Non-headless to debug
            page = browser.new_page()
            
            search_url = f"https://www.youtube.com/results?search_query={query}"
            page.goto(search_url)
            time.sleep(3)
            
            video_elements = page.query_selector_all('a#video-title')
            for video_element in video_elements[:max_results]:
                video_id = video_element.get_attribute('href').split('=')[1]
                title = video_element.inner_text()
                published_at = datetime.now(timezone.utc)  # Placeholder for the actual publish date
                videos.append({
                    'video_id': video_id,
                    'title': title,
                    'published_at': published_at,
                    'channel_title': "Unknown" 
                })
                if len(videos) >= max_results:
                    break
            
            browser.close()
        
        return videos
    
    def get_transcript(self, video_id: str) -> Optional[str]:
        transcript_text = None
        with sync_playwright() as p:
            # Use the default installed browser
            browser = p.chromium.launch(headless=False, channel="chrome")  # Launch as Chrome
            page = browser.new_page()
            
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            page.goto(video_url)
            time.sleep(3)
            
            try:
                # Check for region restriction
                if "This video can't be played in this browser" in page.content():
                    logger.warning(f"Video {video_id} restricted in this browser.")
                    return None
                
                # Open transcript
                page.click('button[aria-label="More actions"]', timeout=60000)
                time.sleep(1)
                page.click('tp-yt-paper-item:has-text("Show transcript")', timeout=60000)
                time.sleep(3)
                
                # Gather transcript text
                transcript_elements = page.query_selector_all('yt-formatted-string.cue')
                transcript_text = ' '.join([element.inner_text() for element in transcript_elements])
            
            except PlaywrightTimeoutError:
                logger.error(f"Timeout while retrieving transcript for video {video_id}")
            except Exception as e:
                logger.error(f"Error retrieving transcript for video {video_id}: {e}")
            
            finally:
                browser.close()
        
        if transcript_text:
            return self.clean_transcript(transcript_text)
        return None
    
    def clean_transcript(self, text: str) -> str:
        text = re.sub(r'\d+:\d+', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        indonesian_indicators = ['yang', 'dari', 'dengan', 'untuk', 'dalam', 'pada', 'ini']
        if any(word in text.lower() for word in indonesian_indicators):
            return text
        return ""
    
    def build_corpus(self, query: str, max_results: int = 50) -> int:
        videos = self.search_videos(query, max_results)
        processed_count = 0
        session = self.Session()
        
        try:
            for video in videos:
                transcript_text = self.get_transcript(video['video_id'])
                
                if transcript_text:
                    corpus_entry = YouTubeTranscriptCorpus(
                        video_id=video['video_id'],
                        title=video['title'],
                        channel_title=video['channel_title'],
                        published_at=video['published_at'],
                        transcript_text=transcript_text,
                        language='id',
                        has_caption=True
                    )
                    
                    try:
                        session.add(corpus_entry)
                        session.commit()
                        processed_count += 1
                        logger.info(f"Processed video: {video['title']}")
                    except IntegrityError:
                        session.rollback()
                        logger.warning(f"Duplicate video: {video['video_id']}")
                
                time.sleep(0.5)
        
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
        "Kearifan Lokal Bencana Alam",
        "Peringatan Dini Bencana Alam",
    ]
    
    total_processed = 0
    for query in queries:
        logger.info(f"Processing query: {query}")
        processed = scraper.build_corpus(query, max_results=3)
        total_processed += processed
    
    logger.info(f"Overall total processed videos: {total_processed}")

if __name__ == "__main__":
    main()


