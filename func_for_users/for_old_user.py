import pandas as pd
import xgboost as xgb

from xgboost import Booster
from db.database import get_movie_dataframe_for_user, get_movies_df, update_user_cluster, get_movies_by_cluster_for_new_user, get_cluster_by_userId
import ast
pd.options.display.width = 0


all_genres = ['(no genres listed)', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy',
              'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'IMAX',
              'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']

xgb_model = Booster()
xgb_model.load_model('models/xgb_model_predict.pkl')

def prepeare_df_for_xgb_model(userId):
    #print(get_cluster_by_userId(userId))
    df = pd.DataFrame(get_movie_dataframe_for_user(userId))

    if df.empty:
        return get_cluster_by_userId(userId)


    #Подготовка даты для датасета-------------------------------------------------------------------
    else:
        datetimes = pd.to_datetime(df['Timestamp'], unit='s')
        df['month'] = datetimes.dt.month
        df['day'] = datetimes.dt.day
        df['day_of_week'] = datetimes.dt.day_of_week
        df['hour'] = datetimes.dt.hour

        def get_part_of_day(hour):
            if 5 <= hour < 12:
                return 1
            elif 12 <= hour < 17:
                return 2
            elif 17 <= hour < 21:
                return 3
            else:
                return 4

        df['part_of_day'] = df['hour'].apply(get_part_of_day)
        df.drop('Timestamp', axis=1, inplace=True)

        #Метркии для обучения-----------------------------------------------------------------------------
        df_mean_cluster = df.groupby(by='cluster')['rating'].mean()
        movie_rating_count = df.groupby('movieId')['rating'].count()
        mean_user_ratings = df.groupby('userId')['rating'].mean()

        full_df = df.merge(df_mean_cluster, on='cluster', suffixes=['_user', '_mean_cluster']) \
            .merge(movie_rating_count, on='movieId', suffixes=['', '_movie_count']) \
            .merge(mean_user_ratings, on='userId', suffixes=['', '_mean_user'])

        full_df['diff_mean_ratings'] = full_df['rating_user'] - full_df['rating_mean_user']
        full_df.rename(columns={'rating': "rating_movie_count"}, inplace=True)

        #Кодирование жанров-------------------------------------------------------------------------------
        genres_list = full_df['genres'].apply(lambda x: x[0].split('|'))
        genres_list = genres_list.apply(lambda x: x[0])
        d_list = pd.Series(genres_list)

        genres_list = d_list.apply(ast.literal_eval)

        #--------------------------
        df = pd.DataFrame(genres_list, columns=['genres'])

        def encode_genres(row, all_genres):
            return {genre: int(genre in row) for genre in all_genres}

        genre_df = df['genres'].apply(lambda x: encode_genres(x, all_genres))
        encoded_genres = pd.DataFrame(list(genre_df))


        #---------------------------

        movies_df = pd.DataFrame(get_movies_df(userId))

        movie_encoded = pd.concat([movies_df.drop('genres', axis=1), encoded_genres], axis=1)
        test_df = full_df.merge(movie_encoded.drop('title', axis=1), left_on='movieId', right_on='movieid', how='inner')


        mean_genres_by_users = pd.concat(
            [
                test_df['userId'],
                test_df[all_genres].multiply(test_df['rating_user'], axis=0)
            ], axis=1
        ).groupby('userId').mean()


        full_df = test_df.merge(mean_genres_by_users, how='left', on='userId', suffixes=('', '_mean_gen'))



        full_df = full_df[['userId', 'movieId', 'month', 'day', 'day_of_week', 'hour',
           'part_of_day', 'cluster', 'rating_mean_cluster', 'mean_movie_rating',
           'rating_movie_count', 'diff_mean_ratings', '(no genres listed)',
           'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime',
           'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'IMAX',
           'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western',
           '(no genres listed)_mean_gen', 'Action_mean_gen', 'Adventure_mean_gen',
           'Animation_mean_gen', 'Children_mean_gen', 'Comedy_mean_gen',
           'Crime_mean_gen', 'Documentary_mean_gen', 'Drama_mean_gen',
           'Fantasy_mean_gen', 'Film-Noir_mean_gen', 'Horror_mean_gen',
           'IMAX_mean_gen', 'Musical_mean_gen', 'Mystery_mean_gen',
           'Romance_mean_gen', 'Sci-Fi_mean_gen', 'Thriller_mean_gen',
           'War_mean_gen', 'Western_mean_gen', 'genres']]

        full_df.rename(columns={'mean_movie_rating' : 'rating_mean_movie'}, inplace=True)
        full_df['userId'] = 0

        dtest = xgb.DMatrix(full_df.drop(columns='genres', axis=1))

        predict = xgb_model.predict(dtest)

        pred = pd.Series(predict)
        res_df = full_df.merge(pred.to_frame(), left_index=True, right_index=True)
        res_df.rename(columns={0: "predict_rating_cluster"}, inplace=True)

        predict_rating_cluster = res_df.groupby('cluster')['predict_rating_cluster'].mean()


        res_df.merge(predict_rating_cluster, on='cluster')


        res_df = res_df[['userId', 'cluster', 'predict_rating_cluster']]
        df = res_df.groupby('cluster')['predict_rating_cluster'].mean()
        update_user_cluster(int(df.idxmax()), userId)


        return int(df.idxmax())
