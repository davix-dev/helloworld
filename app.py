import os
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from flask import Flask, request, jsonify
from contextlib import contextmanager
import logging
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
# Supabase connection string format:
# postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT-REF].supabase.co:5432/postgres
DATABASE_URL = os.environ.get("DATABASE_URL")
API_SECRET = os.environ.get("API_SECRET")

# Database context manager
@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# Initialize database
def init_db():
    """Create table if it doesn't exist"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS feedback (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL,
                userId BIGINT UNIQUE NOT NULL
            )
        ''')
        # Create index for faster lookups
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_userId ON feedback(userId)
        ''')
    logger.info("Database initialized successfully")

# Initialize database on startup
try:
    init_db()
except Exception as e:
    logger.error(f"Database initialization failed: {e}")

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200

@app.route('/api/submit', methods=['POST'])
def submit():
    """
    Handle submission from Roblox game server
    Idempotent - safe to call multiple times with same userId
    """
    
    # Verify API secret
    auth_header = request.headers.get('X-API-Secret')
    if API_SECRET and auth_header != API_SECRET:
        logger.warning(f"Invalid API secret from IP: {request.remote_addr}")
        return jsonify({"success": False, "reason": "unauthorized"}), 401
    
    # Validate content type
    if not request.is_json:
        return jsonify({"success": False, "reason": "invalid_content_type"}), 400
    
    # Parse JSON payload
    try:
        data = request.get_json()
    except Exception as e:
        logger.error(f"JSON parse error: {e}")
        return jsonify({"success": False, "reason": "invalid_json"}), 400
    
    # Validate required fields
    user_id = data.get('userId')
    username = data.get('username')
    
    if not user_id or not username:
        return jsonify({
            "success": False, 
            "reason": "missing_required_fields"
        }), 400
    
    # Attempt to insert into database
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO feedback (username, userId)
                VALUES (%s, %s)
            ''', (username, user_id))
            
            logger.info(f"New submission: userId={user_id}, username={username}")
            return jsonify({"success": True}), 200
            
    except psycopg2.IntegrityError as e:
        # Duplicate userId - this is expected and safe
        logger.info(f"Duplicate submission ignored: userId={user_id}")
        return jsonify({
            "success": False, 
            "reason": "already_submitted"
        }), 200  # Return 200 to prevent Roblox retries
    
    except Exception as e:
        # Unexpected error
        logger.error(f"Database error: {e}")
        return jsonify({
            "success": False, 
            "reason": "internal_error"
        }), 500

@app.route('/api/stats', methods=['GET'])
def stats():
    """Get submission statistics"""
    auth_header = request.headers.get('X-API-Secret')
    if API_SECRET and auth_header != API_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    
    try:
        with get_db() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute('SELECT COUNT(*) as total FROM feedback')
            result = cursor.fetchone()
            
            return jsonify({
                "total_submissions": result['total']
            }), 200
    except Exception as e:
        logger.error(f"Stats error: {e}")
        return jsonify({"error": "internal_error"}), 500

@app.route('/api/list', methods=['GET'])
def list_submissions():
    """List all submissions (admin only)"""
    auth_header = request.headers.get('X-API-Secret')
    if API_SECRET and auth_header != API_SECRET:
        return jsonify({"error": "unauthorized"}), 401
    
    try:
        with get_db() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute('SELECT id, username, userId FROM feedback ORDER BY id DESC LIMIT 100')
            results = cursor.fetchall()
            
            return jsonify({
                "submissions": results,
                "count": len(results)
            }), 200
    except Exception as e:
        logger.error(f"List error: {e}")
        return jsonify({"error": "internal_error"}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
