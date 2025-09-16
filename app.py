from flask import Flask, render_template, request, jsonify, redirect, url_for
from database import DatabaseManager
import uuid
import os
from datetime import datetime

app = Flask(__name__)
db_manager = DatabaseManager()

@app.route('/')
def index():
    """Main page showing all users"""
    try:
        users = db_manager.get_all_users()
        db_info = db_manager.get_database_info()
        return render_template('index.html', users=users, db_info=db_info)
    except Exception as e:
        return render_template('index.html', users=[], db_info={"type": "error", "status": str(e)})

@app.route('/create_user', methods=['GET', 'POST'])
def create_user():
    """Create a new user"""
    if request.method == 'POST':
        try:
            user_data = {
                'name': request.form['name'],
                'email': request.form['email'],
                'age': int(request.form['age']),
                'city': request.form['city'],
                'created_at': datetime.utcnow().isoformat()
            }
            
            created_user = db_manager.create_user(user_data)
            return redirect(url_for('index'))
        except Exception as e:
            return render_template('create_user.html', error=str(e))
    
    return render_template('create_user.html')

@app.route('/delete_user/<user_id>', methods=['POST'])
def delete_user(user_id):
    """Delete a user"""
    try:
        success = db_manager.delete_user(user_id)
        if success:
            return jsonify({'success': True, 'message': 'User deleted successfully'})
        else:
            return jsonify({'success': False, 'message': 'User not found'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/users')
def api_users():
    """API endpoint to get all users"""
    try:
        users = db_manager.get_all_users()
        return jsonify({'success': True, 'users': users})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/db_info')
def api_db_info():
    """API endpoint to get database info"""
    try:
        db_info = db_manager.get_database_info()
        return jsonify(db_info)
    except Exception as e:
        return jsonify({'type': 'error', 'status': str(e)})

@app.route('/api/switch_database', methods=['POST'])
def switch_database():
    """Switch between MongoDB and HCD databases"""
    try:
        data = request.get_json()
        new_db_type = data.get('database_type')
        
        if new_db_type not in ['mongodb', 'hcd', 'astra']:
            return jsonify({'success': False, 'message': 'Invalid database type'})
        
        # Update .env file
        env_path = '.env'
        env_vars = {}
        
        # Read existing .env file
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        env_vars[key] = value
        
        # Update DATABASE_TYPE
        env_vars['DATABASE_TYPE'] = new_db_type
        
        # Write back to .env file
        with open(env_path, 'w') as f:
            for key, value in env_vars.items():
                f.write(f'{key}={value}\n')
        
        # Update environment variable for current session
        os.environ['DATABASE_TYPE'] = new_db_type
        
        # Reinitialize database manager
        global db_manager
        db_manager = DatabaseManager()
        
        return jsonify({
            'success': True, 
            'message': f'Successfully switched to {new_db_type.upper()}'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/sync_to_hcd', methods=['POST'])
def sync_to_hcd():
    """Sync all MongoDB records to DataStax HCD"""
    try:
        result = db_manager.sync_mongodb_to_hcd()
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'message': f'Sync failed: {str(e)}'})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
