import os
from typing import Dict, List, Optional, Any
from pymongo import MongoClient
from astrapy import DataAPIClient
from astrapy.authentication import UsernamePasswordTokenProvider
from astrapy.constants import Environment
from dotenv import load_dotenv

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.db_type = os.getenv('DATABASE_TYPE', 'mongodb')
        self.collection = None
        self._setup_connection()
    
    def _setup_connection(self):
        """Setup database connection based on configuration"""
        if self.db_type == 'mongodb':
            self._setup_mongodb()
        elif self.db_type == 'hcd':
            self._setup_hcd()
        elif self.db_type == 'astra':
            self._setup_astra()
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    def _setup_astra(self):
        """Setup Astra DB connection"""
        astra_db_id = os.getenv('ASTRA_DB_ID')
        astra_region = os.getenv('ASTRA_DB_REGION')
        astra_token = os.getenv('ASTRA_DB_TOKEN')
        astra_keyspace = os.getenv('ASTRA_DB_KEYSPACE', 'default_keyspace')

        if not all([astra_db_id, astra_region, astra_token]):
            raise ValueError("Astra DB configuration incomplete. Check ASTRA_DB_ID, ASTRA_DB_REGION, and ASTRA_DB_TOKEN")

        from astrapy import AstraDB
        self.client = AstraDB(
            astra_db_id,
            astra_region,
            astra_token,
            namespace=astra_keyspace
        )
        self.collection = self.client.collection("users")
    
    def _setup_mongodb(self):
        """Setup MongoDB connection"""
        uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        database_name = os.getenv('MONGODB_DATABASE', 'user_profiles')
        
        self.client = MongoClient(uri)
        self.database = self.client[database_name]
        self.collection = self.database.users
    
    def _setup_hcd(self):
        """Setup HCD Data API connection"""
        api_endpoint = os.getenv('HCD_API_ENDPOINT')
        username = os.getenv('HCD_USERNAME')
        password = os.getenv('HCD_PASSWORD')
        keyspace = os.getenv('HCD_KEYSPACE', 'default_keyspace')
        
        if not all([api_endpoint, username, password]):
            raise ValueError("HCD configuration incomplete. Check HCD_API_ENDPOINT, HCD_USERNAME, and HCD_PASSWORD")
        
        token = UsernamePasswordTokenProvider(username, password)
        client = DataAPIClient(environment=Environment.HCD)
        self.database = client.get_database(api_endpoint, token=token)
        
        # Ensure keyspace exists
        try:
            self.database.get_database_admin().create_keyspace(keyspace, update_db_keyspace=True)
        except Exception:
            # Keyspace might already exist
            pass
        
        # Create or get the users collection
        try:
            self.collection = self.database.create_collection("users")
        except Exception:
            # Collection might already exist
            self.collection = self.database.get_collection("users")
    
    def create_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new user"""
        # Ensure we have a UUID string as _id for both databases
        if '_id' not in user_data or not user_data['_id']:
            import uuid
            user_data['_id'] = str(uuid.uuid4())
        
        # Same API for both MongoDB and HCD
        result = self.collection.insert_one(user_data)
        return user_data
    
    def get_all_users(self) -> List[Dict[str, Any]]:
        """Get all users"""
        # Same API for both MongoDB and HCD
        cursor = self.collection.find({})
        return list(cursor)
    
    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by ID"""
        # Same API for both MongoDB and HCD
        return self.collection.find_one({"_id": user_id})
    
    def delete_user(self, user_id: str) -> bool:
        """Delete user by ID"""
        # Same API for both MongoDB and HCD
        result = self.collection.delete_one({"_id": user_id})
        return result.deleted_count > 0
    
    def sync_mongodb_to_hcd(self) -> Dict[str, Any]:
        """Sync all MongoDB records to HCD without transformation"""
        if self.db_type != 'mongodb':
            return {"success": False, "message": "Sync only available from MongoDB"}
        
        try:
            # Use a cursor to process users in batches
            batch_size = 100
            cursor = self.collection.find({})
            total_synced = 0
            errors = []
            batch = []

            # Setup HCD connection temporarily
            hcd_manager = DatabaseManager()
            hcd_manager.db_type = 'hcd'
            hcd_manager._setup_hcd()

            for user in cursor:
                batch.append(user)
                if len(batch) == batch_size:
                    try:
                        hcd_manager.collection.insert_many(batch)
                        total_synced += len(batch)
                    except Exception as e:
                        for b_user in batch:
                            errors.append(f"User {b_user.get('_id', 'unknown')}: {str(e)}")
                    batch = []

            # Process any remaining users in the last batch
            if batch:
                try:
                    hcd_manager.collection.insert_many(batch)
                    total_synced += len(batch)
                except Exception as e:
                    for b_user in batch:
                        errors.append(f"User {b_user.get('_id', 'unknown')}: {str(e)}")

            message = f"Successfully synced {total_synced} users to DataStax HCD"
            if errors:
                message += f". {len(errors)} users skipped (likely duplicates)"

            return {
                "success": True,
                "message": message,
                "synced_count": total_synced,
                "errors": errors[:5]  # Return first 5 errors only
            }

        except Exception as e:
            return {"success": False, "message": f"Sync failed: {str(e)}"}
    
    def get_database_info(self) -> Dict[str, str]:
        """Get information about current database connection"""
        return {
            "type": self.db_type,
            "status": "connected"
        }
