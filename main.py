import os
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

import tensorflow as tf
import joblib
import numpy as np
from flask import Flask, request, jsonify
from db.database import get_movies_by_cluster  # Импортируем БД-функции

# Загружаем модели
kmeans_model = joblib.load('models/kmeans_model.pkl')
embedding_model = tf.keras.models.load_model('models/embedd_model.keras')

# Жанры (как в модели)
all_genres = ['(no genres listed)', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy',
              'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'IMAX',
              'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']

def get_user_embedding(user_genres):
    """Генерирует эмбеддинг пользователя на основе любимых жанров"""
    genre_indices = [all_genres.index(genre) for genre in user_genres if genre in all_genres]
    genre_indices += [0] * (20 - len(genre_indices))  # Заполняем нулями до 20
    genre_vector = np.array(genre_indices, dtype=np.float32).reshape(1, -1)
    embedding = embedding_model.predict(genre_vector)
    return embedding

def get_recommendations(user_id, user_genres):
    """Получает рекомендации, используя KMeans и БД"""
    embedding = get_user_embedding(user_genres).astype(np.float64)
    cluster_id = kmeans_model.predict(embedding.reshape(1, -1))[0]

    recommended_movies = get_movies_by_cluster(cluster_id, limit=10)
    if not recommended_movies:
        return [{"title": "No recommendations available", "genres": "N/A"}]

    return recommended_movies

# Flask API
app = Flask(__name__)

@app.route('/recommend', methods=['POST'])
def recommend():
    data = request.json
    user_id = data['userId']
    user_genres = data['genres']

    recommendations = get_recommendations(user_id, user_genres)

    return jsonify(recommendations)

if __name__ == '__main__':
    app.run(debug=True)
