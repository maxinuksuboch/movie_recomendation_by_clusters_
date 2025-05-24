import os
from flask import Flask, request, jsonify
from func_for_users.for_new_user import get_recommendations
from func_for_users.for_old_user import prepeare_df_for_xgb_model
from db.database import get_movies_by_cluster_for_new_user
from flask_cors import CORS
import json

os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

# Flask API
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}},
     allow_headers=["Content-Type", "Authorization"],
     methods=["GET", "POST", "OPTIONS"])
@app.route('/start_page', methods=['POST'])
def recommend_for_new_users():
    data = request.json
    user_id = data['userId']
    user_genres = data['genres']

    get_recommendations(user_id, user_genres)

    return jsonify({"status": "success", "message": "Жанры успешно получены"}), 200


@app.route('/recommend', methods=['POST'])
def recommend_for_old_users():
    data = request.json
    userId = data['userId']

    cluster = prepeare_df_for_xgb_model(userId)
    print(cluster)
    rec_movies_for_old_user = get_movies_by_cluster_for_new_user(userId, cluster)
    print(rec_movies_for_old_user)
    serialized_data = json.dumps(rec_movies_for_old_user, ensure_ascii=False)
    return serialized_data, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)