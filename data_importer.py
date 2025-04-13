import pymysql
import csv
import os
from typing import Dict, List, Optional
from pathlib import Path

class DatabaseManager:
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.connection = None
        
    def connect(self) -> None:
        """Establish a connection to the MySQL database."""
        try:
            self.connection = pymysql.connect(**self.db_config)
            print("Connected to MySQL database successfully")
        except pymysql.MySQLError as e:
            print(f"Error connecting to MySQL: {e}")
            raise

    def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            print("MySQL connection closed")

    def __enter__(self) -> 'DatabaseManager':
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

class CSVReader:
    @staticmethod
    def read_file(filename: str, columns: Optional[List[str]] = None) -> List[Dict]:
        """Read a CSV file and return a list of dictionaries."""
        data = []
        try:
            with open(filename, 'r', newline='', encoding='utf-8') as file:
                reader = csv.reader(file)
                if columns is None:
                    columns = next(reader)
                    file.seek(0)
                    reader = csv.reader(file)
                
                for row in reader:
                    if len(row) == len(columns):
                        data.append(dict(zip(columns, row)))
            return data
        except Exception as e:
            print(f"Error reading CSV file {filename}: {e}")
            raise

class DataImporter:
    def __init__(self, db_manager: DatabaseManager, data_dir: str):
        self.db_manager = db_manager
        self.data_dir = Path(data_dir)
        self.setup_tables()

    def setup_tables(self) -> None:
        """Create necessary database tables if they don't exist."""
        queries = {
            'episodes': """
                CREATE TABLE IF NOT EXISTS episodes (
                    episode_id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255),
                    season_number INT,
                    episode_number INT,
                    painting_img_src VARCHAR(255),
                    painting_yt_src VARCHAR(255),
                    air_date DATE
                )
            """,
            'colors': """
                CREATE TABLE IF NOT EXISTS colors (
                    color_id INT AUTO_INCREMENT PRIMARY KEY,
                    color_name VARCHAR(255),
                    color_hex VARCHAR(255)
                )
            """,
            'episode_colors': """
                CREATE TABLE IF NOT EXISTS episode_colors (
                    episode_id INT,
                    color_id INT,
                    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id),
                    FOREIGN KEY (color_id) REFERENCES colors(color_id)
                )
            """,
            'subject_matters': """
                CREATE TABLE IF NOT EXISTS subject_matters (
                    subject_matter_id INT AUTO_INCREMENT PRIMARY KEY,
                    subject_matter_name VARCHAR(255)
                )
            """,
            'episode_subject_matters': """
                CREATE TABLE IF NOT EXISTS episode_subject_matters (
                    episode_id INT,
                    subject_matter_id INT,
                    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id),
                    FOREIGN KEY (subject_matter_id) REFERENCES subject_matters(subject_matter_id)
                )
            """
        }

        try:
            with self.db_manager.connection.cursor() as cursor:
                for table_name, query in queries.items():
                    cursor.execute(query)
                    print(f"Created table: {table_name}")
            self.db_manager.connection.commit()
        except Exception as e:
            print(f"Error creating tables: {e}")
            raise

    def import_data(self) -> None:
        """Import data from CSV files into the database."""
        try:
            # Import colors data
            colors_data = CSVReader.read_file(
                str(self.data_dir / 'The Joy Of Painting - Colors Used.csv'),
                ['painting_title', 'season', 'episode', 'img_src', 'youtube_src', 'colors', 'color_hex']
            )

            # Import subject matters
            subject_matters_data = CSVReader.read_file(
                str(self.data_dir / 'The Joy Of Painting - Subject Matter.csv'),
                ['EPISODE']
            )

            # Import episode dates
            dates_data = CSVReader.read_file(
                str(self.data_dir / 'The Joy Of Painting - Episode Dates.csv'),
                ['Title', 'Date']
            )

            self._import_episodes(colors_data, dates_data)
            self._import_colors(colors_data)
            self._import_subject_matters(subject_matters_data)
            self._import_episode_colors(colors_data)
            self._import_episode_subject_matters(subject_matters_data)

        except Exception as e:
            print(f"Error importing data: {e}")
            raise

    def _import_episodes(self, colors_data: List[Dict], dates_data: List[Dict]) -> None:
        """Import episode data into the episodes table."""
        sql = """
            INSERT INTO episodes 
            (title, season_number, episode_number, painting_img_src, painting_yt_src, air_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        try:
            with self.db_manager.connection.cursor() as cursor:
                for row in colors_data:
                    title = row['painting_title']
                    season = int(row['season'])
                    episode = int(row['episode'])
                    img_src = row['img_src']
                    youtube_src = row['youtube_src']
                    air_date = next((date['Date'] for date in dates_data 
                                   if date['Title'].strip() == title), None)
                    
                    cursor.execute(sql, (title, season, episode, img_src, youtube_src, air_date))
                    print(f"Inserted episode: {title}")
            self.db_manager.connection.commit()
        except Exception as e:
            print(f"Error importing episodes: {e}")
            raise

    def _import_colors(self, colors_data: List[Dict]) -> None:
        """Import color data into the colors table."""
        sql = "INSERT INTO colors (color_name, color_hex) VALUES (%s, %s)"
        
        try:
            with self.db_manager.connection.cursor() as cursor:
                color_names_hex = [(row['colors'], row['color_hex']) for row in colors_data]
                for color, hex in color_names_hex:
                    cursor.execute(sql, (color, hex))
                    print(f"Inserted color: {color}")
            self.db_manager.connection.commit()
        except Exception as e:
            print(f"Error importing colors: {e}")
            raise

    def _import_subject_matters(self, subject_matters_data: List[Dict]) -> None:
        """Import subject matters into the subject_matters table."""
        sql = "INSERT INTO subject_matters (subject_matter_name) VALUES (%s)"
        
        try:
            with self.db_manager.connection.cursor() as cursor:
                subject_matters = set()
                for row in subject_matters_data:
                    subjects = row['EPISODE'].split(';')
                    subject_matters.update(subjects)
                
                for subject in subject_matters:
                    cursor.execute(sql, (subject,))
                    print(f"Inserted subject matter: {subject}")
            self.db_manager.connection.commit()
        except Exception as e:
            print(f"Error importing subject matters: {e}")
            raise

def main():
    # Configuration
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'root',
        'database': 'joy_of_painting'
    }
    
    try:
        # Create database manager
        with DatabaseManager(db_config) as db_manager:
            # Create data importer
            importer = DataImporter(db_manager, './data')
            # Import data
            importer.import_data()
            print("Data import completed successfully")
    except Exception as e:
        print(f"Error during import process: {e}")

if __name__ == "__main__":
    main()