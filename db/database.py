from psycopg2.extras import RealDictCursor
import psycopg2
import dotenv
import os
import random

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
conn = connection
cursor = conn.cursor(cursor_factory=RealDictCursor)

def get_movies_by_cluster_for_new_user(userId, cluster, limit=70):
    query = """
           WITH top_movies AS (
    SELECT movieid, title
    FROM movie_with_clusters
    WHERE cluster = %s
    ORDER BY count_movie_rating, mean_movie_rating DESC
    LIMIT %s
)
SELECT DISTINCT mwc.title
FROM movie_with_clusters mwc
JOIN top_movies tm ON mwc.movieid = tm.movieid
WHERE mwc.movieid NOT IN (
    SELECT ur."MovieId"
    FROM "UserRatings" ur
    WHERE ur."UserId" = %s
)
LIMIT %s;


           """
    cursor.execute(query, (int(cluster), limit, userId,  limit))
    movies = cursor.fetchall()
    rec_movies = [dict(movie) for movie in movies]
    random.shuffle(rec_movies)
    return rec_movies

def update_user_cluster(cluster, userId):
    query = """UPDATE "Users" SET user_cluster = %s WHERE "UserId" = %s"""
    cursor.execute(query, (cluster, userId))
    conn.commit()


def get_movie_dataframe_for_user(userId):
    query = """
        SELECT ur."UserId" as "userId", ur."MovieId" as "movieId", ur."Timestamp", mwc.genres, mwc.cluster, ur."Rating" as "rating", mwc.mean_movie_rating
        FROM "UserRatings" ur JOIN movie_with_clusters mwc ON ur."MovieId"=mwc.movieid
        WHERE ur."UserId" = %s"""
    cursor.execute(
        query,
        (userId,)
    )
    movie_data = cursor.fetchall()
    return [dict(movie) for movie in movie_data]

def get_movies_df(userId):
    cursor.execute(
        '''
        SELECT mwc.movieid, mwc.title, mwc.genres
        FROM "UserRatings" ur
        JOIN movie_with_clusters mwc ON ur."MovieId" = mwc.movieid
        WHERE ur."UserId" = %s
        ''',
        (userId,)
    )
    movie_df = cursor.fetchall()
    return [dict(movie) for movie in movie_df]

def get_cluster_by_userId(userId):
    cursor.execute(
        """SELECT user_cluster
        FROM "Users" u
        WHERE u."UserId" = %s""",
        (userId,)
    )
    result = cursor.fetchone()
    if result and result['user_cluster'] is not None:
        return int(result['user_cluster'])  # безопасно привести к int
    else:
        return None


