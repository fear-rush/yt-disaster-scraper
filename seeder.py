from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os


load_dotenv()

NEW_DATABASE_URL = os.getenv('DATABASE_URL')

engine = create_engine(NEW_DATABASE_URL)
Session = sessionmaker(bind=engine)

def seed_database_from_file(sql_file_path: str):
    session = Session()
    try:
        with open(sql_file_path, "r", encoding="utf-8") as file:
            sql_commands = file.read()
        
        with engine.connect() as connection:
            connection.execute(sql_commands)
        
        print("Database seeded successfully from SQL file!")
    except Exception as e:
        print(f"Error seeding database: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    seed_database_from_file("youtube_transcript_corpus.sql")
