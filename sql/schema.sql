CREATE DATABASE joy_of_painting;

USE joy_of_painting;

CREATE TABLE episodes (
    episode_id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255),
    season_number INT,
    episode_number INT,
    painting_img_src VARCHAR(255),
    painting_yt_src VARCHAR(255),
    air_date DATE
);

CREATE TABLE colors (
    color_id INT AUTO_INCREMENT PRIMARY KEY,
    color_name VARCHAR(255),
    color_hex VARCHAR(255)
);

CREATE TABLE episode_colors (
    episode_id INT,
    color_id INT,
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id),
    FOREIGN KEY (color_id) REFERENCES colors(color_id)
);

CREATE TABLE subject_matters (
    subject_matter_id INT AUTO_INCREMENT PRIMARY KEY,
    subject_matter_name VARCHAR(255)
);

CREATE TABLE episode_subject_matters (
    episode_id INT,
    subject_matter_id INT,
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id),
    FOREIGN KEY (subject_matter_id) REFERENCES subject_matters(subject_matter_id)
);