import numpy as np
import joblib
from db.database import get_movies_by_cluster_for_new_user, update_user_cluster
import tensorflow as tf
import os
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

kmeans_model = joblib.load('models/kmeans_model.pkl')
embedding_model = tf.keras.models.load_model('models/embedd_model.keras')

all_genres = ['(no genres listed)', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy',
              'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'IMAX',
              'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
def get_user_embedding(user_genres):
    genre_indices = [all_genres.index(genre) for genre in user_genres if genre in all_genres]
    genre_indices += [0] * (10 - len(genre_indices))  # Заполняем нулями до 20
    genre_vector = np.array(genre_indices, dtype=np.float32).reshape(1, -1)
    embedding = embedding_model.predict(genre_vector)
    return embedding

def get_recommendations(user_id, user_genres):
    """Получает рекомендации, используя KMeans и БД"""
    embedding = get_user_embedding(user_genres).astype(np.float64)
    cluster = kmeans_model.predict(embedding.reshape(1, -1))[0]

    update_user_cluster(int(cluster), user_id)

    recommended_movies = get_movies_by_cluster_for_new_user(user_id, cluster,limit=10)
    if not recommended_movies:
        return [{"title": "No recommendations available", "genres": "N/A"}]

    return recommended_movies