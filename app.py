from flask import Flask, jsonify, request
import pymysql
from typing import Dict, List

app = Flask(__name__)

# Database configuration
db_config = {
    'host': 'localhost',
    'user': 'data_importer',
    'password': 'secure_password',
    'database': 'joy_of_painting'
}

def get_db_connection():
    """Create and return a database connection."""
    return pymysql.connect(**db_config)

@app.route('/api/episodes', methods=['GET'])
def get_episodes():
    """Get all episodes from the database."""
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM episodes")
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                episodes = [dict(zip(columns, row)) for row in results]
                return jsonify(episodes)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/episodes/<int:episode_id>', methods=['GET'])
def get_episode(episode_id: int):
    """Get a specific episode by ID."""
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM episodes WHERE episode_id = %s", (episode_id,))
                columns = [desc[0] for desc in cursor.description]
                result = cursor.fetchone()
                if result:
                    return jsonify(dict(zip(columns, result)))
                return jsonify({"error": "Episode not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)