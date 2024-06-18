import json
from flask import Flask, jsonify, request
import os
import boto3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

def download_from_s3(bucket, object_name, file_name):
    s3_client = boto3.client('s3',
                             aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                             aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        s3_client.download_file(bucket, object_name, file_name)
        logger.info(f"Successfully downloaded {object_name} from {bucket}")
        return True
    except Exception as e:
        logger.error(f"Error downloading file from S3: {e}")
        return False

def get_games_data():
    file_name = 'jeopardy_games.json'
    if download_from_s3(os.environ['S3_BUCKET_NAME'], file_name, file_name):
        try:
            with open(file_name, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading file {file_name}: {e}")
            return None
    else:
        return None


@app.route('/api/games', methods=['GET'])
def get_all_games():
    page = request.args.get('page', default=1, type=int)
    size = request.args.get('size', default=10, type=int)

    games_data = get_games_data()
    if games_data is None:
        return jsonify({'error': 'Error retrieving data'}), 500

    start = (page - 1) * size
    end = start + size

    total_games = len(games_data)
    paginated_games = games_data[start:end]

    response = {
        'page': page,
        'size': size,
        'total_games': total_games,
        'total_pages': (total_games + size - 1) // size,
        'games': paginated_games
    }

    return jsonify(response)

@app.route('/api/games/ids/<int:game_id>', methods=['GET'])
def get_game_by_id(game_id):
    games_data = get_games_data()
    if games_data is None:
        return jsonify({'error': 'Error retrieving data'}), 500

    for game in games_data:
        if 'error' not in game and game['game_id'] == game_id:
            return jsonify(game)
    return jsonify({'error': f'Game {game_id} not found'}), 404

@app.route('/api/games/date/<string:game_date>', methods=['GET'])
def get_games_by_date(game_date):
    games_data = get_games_data()
    if games_data is None:
        return jsonify({'error': 'Error retrieving data'}), 500

    games_by_date = [game for game in games_data if game.get('game_date', '').startswith(game_date)]

    if not games_by_date:
        return jsonify({'error': f'No games found for date {game_date}'}), 404

    return jsonify(games_by_date)

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)