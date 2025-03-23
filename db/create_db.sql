CREATE TABLE Users (
    id SERIAL PRIMARY KEY,
    login VARCHAR(255) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password TEXT NOT NULL
);

CREATE TABLE Movie (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    mean_rating FLOAT DEFAULT 0, 
    genres TEXT[], 
    cluster INT
);

CREATE TABLE UserRatings (
    user_id INT REFERENCES Users(id) ON DELETE CASCADE,
    movie_id INT REFERENCES Movie(id) ON DELETE CASCADE,
    rating FLOAT CHECK (rating >= 0 AND rating <= 5),
    PRIMARY KEY (user_id, movie_id)
);

CREATE TABLE movieCluster (
    id INT,
    title VARCHAR(255) NOT NULL,
    mean_rating FLOAT DEFAULT 0, 
    genres TEXT[], 
    cluster INT
);

