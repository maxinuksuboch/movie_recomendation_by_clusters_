import psycopg2
from psycopg2.extras import RealDictCursor
import dotenv
import os

dotenv.load_dotenv()



def create_connection():
    connection = None
    try:
        connection = psycopg2.connect(
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        print("Connection to PostgreSQL DB successful")
    except psycopg2.OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

connection = create_connection()



def get_movies_by_cluster(cluster_id, limit=10):
    """Получает фильмы по кластеру из БД"""
    conn = connection


    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        f"SELECT title, genres FROM moviecluster WHERE cluster = {cluster_id} ORDER BY mean_rating DESC LIMIT {limit}",
        (cluster_id, limit)
    )
    movies = cursor.fetchall()
    conn.close()
    return [dict(movie) for movie in movies]