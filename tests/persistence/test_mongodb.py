import unittest
from unittest.mock import patch, MagicMock
import os
import sys
import pymongo  # Import the actual library to check for exceptions
import importlib

# Adjust path to import from src
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Import the functions/variables to test
from src.persistence.mongodb import (
    connect_to_mongo,
    close_mongo_connection,
    get_db,  # Use the actual function name
    _get_collection,
    client,  # Import the module-level client variable to check it
    MONGO_URI,  # Import config for verification
    MONGO_DB_NAME
)

# Provide direct module reference for convenience in patching
import src.persistence.mongodb as mongodb_module

# Ensure MONGO_DB_URI is set for tests, even if .env isn't loaded perfectly in test env
# You might want a dedicated test .env or config setup later
TEST_MONGO_URI = os.getenv("MONGO_DB_URI", "mongodb://test_uri")
TEST_MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "test_db")

# Test suite for MongoDB persistence helpers
class TestMongoDbConnection(unittest.TestCase):

    def setUp(self):
        # Configure the mock_getenv
        # Make it return test values for URI and DB Name, and None for others
        def side_effect(key, default=None):
            if key == "MONGO_DB_URI":
                return TEST_MONGO_URI
            if key == "MONGO_DB_NAME":
                return TEST_MONGO_DB_NAME
            return default  # Or return os.environ.get(key, default) if needed

        # Start patcher for os.getenv on mongodb_module
        self.getenv_patcher = patch('src.persistence.mongodb.os.getenv', side_effect=side_effect)
        self.getenv_patcher.start()

        # Reset module-level state and patch constants on mongodb_module
        mongodb_module.client = None
        mongodb_module.db = None
        # Override constants to use test values
        mongodb_module.MONGO_URI = TEST_MONGO_URI
        mongodb_module.MONGO_DB_NAME = TEST_MONGO_DB_NAME

    def tearDown(self):
        # Stop getenv patcher
        self.getenv_patcher.stop()

    # Patch MongoClient for individual tests or groups
    @patch('src.persistence.mongodb.MongoClient')
    def test_connect_to_mongo_success(self, mock_mongo_client):
        """Test successful connection to MongoDB."""
        mock_instance = MagicMock()
        mock_mongo_client.return_value = mock_instance
        # Ensure client starts as None using nested patches
        with patch.object(mongodb_module, 'client', None):
            with patch.object(mongodb_module, 'db', None):
                connect_to_mongo()
                # Assert MongoClient was called
                mock_mongo_client.assert_called_once_with(TEST_MONGO_URI, serverSelectionTimeoutMS=5000)
                # Assert the module client was set (access might still lint, but it's after call)
                self.assertIsNotNone(mongodb_module.client)
                self.assertIsNotNone(mongodb_module.db)

    @patch('src.persistence.mongodb.MongoClient')
    def test_connect_to_mongo_failure(self, mock_mongo_client):
        """Test handling of connection failure."""
        mock_mongo_client.side_effect = pymongo.errors.ConnectionFailure("Test connection failed")
        # Use nested patches
        with patch.object(mongodb_module, 'client', None):
            with patch.object(mongodb_module, 'db', None):
                with self.assertRaises(pymongo.errors.ConnectionFailure):
                    connect_to_mongo()
                # Check module state was reset on failure
                self.assertIsNone(mongodb_module.client)
                self.assertIsNone(mongodb_module.db)
        mock_mongo_client.assert_called_once_with(TEST_MONGO_URI, serverSelectionTimeoutMS=5000)

    @patch('src.persistence.mongodb.MongoClient')
    def test_connect_to_mongo_already_connected(self, mock_mongo_client):
        """Test that connect_to_mongo doesn't reconnect if already connected."""
        mock_existing_client = MagicMock()
        mock_existing_db = MagicMock()
        # Use nested patches to set the initial state
        with patch.object(mongodb_module, 'client', mock_existing_client):
            with patch.object(mongodb_module, 'db', mock_existing_db):
                returned_db = connect_to_mongo()
                # Assert MongoClient was NOT called again
                mock_mongo_client.assert_not_called()
                # Assert the existing db instance was returned
                self.assertEqual(returned_db, mock_existing_db)
                # Assert module state remains unchanged
                self.assertEqual(mongodb_module.client, mock_existing_client)
                self.assertEqual(mongodb_module.db, mock_existing_db)

    # --- Tests for get_db --- #
    def test_get_db_not_connected(self):
        """Test get_db calls connect_to_mongo if client is not connected."""
        # Patch connect_to_mongo for this specific test
        # Nested patches for setting initial state and mocking connect
        with patch.object(mongodb_module, 'db', None):
            with patch.object(mongodb_module, 'client', None):  # Also ensure client is None
                with patch('src.persistence.mongodb.connect_to_mongo') as mock_connect:
                    mock_db_instance = MagicMock()
                    mock_connect.return_value = mock_db_instance
                    db = get_db()
                    mock_connect.assert_called_once()
                    self.assertEqual(db, mock_db_instance)

    def test_get_db_already_connected(self):
        """Test get_db returns the existing db object if already connected."""
        mock_db_instance = MagicMock()
        # Nested patches to simulate existing connection state and mock connect
        with patch.object(mongodb_module, 'db', mock_db_instance):
            with patch.object(mongodb_module, 'client', MagicMock()):  # Ensure client is set
                with patch('src.persistence.mongodb.connect_to_mongo') as mock_connect:
                    db = get_db()
                    mock_connect.assert_not_called()
                    self.assertEqual(db, mock_db_instance)

    # --- Tests for _get_collection --- #
    def test_get_collection_not_connected(self):
        """Test _get_collection calls get_db which handles connection."""
        # Ensure client/db are None initially
        with patch.object(mongodb_module, 'client', None):
            with patch.object(mongodb_module, 'db', None):
                # Mock the get_db function itself for this test
                with patch('src.persistence.mongodb.get_db') as mock_get_db:
                    # Simulate get_db raising an error
                    mock_get_db.side_effect = pymongo.errors.ConnectionFailure(
                        "Database connection not established."
                    )
                    with self.assertRaisesRegex(
                        pymongo.errors.ConnectionFailure, 
                        "Database connection not established."
                    ):
                        _get_collection("test_collection")
                    mock_get_db.assert_called_once()

    def test_get_collection_success(self):
        """Test _get_collection returns the correct collection object."""
        # Mock get_db to return a mock database
        mock_collection_instance = MagicMock()
        mock_db_instance = MagicMock()
        mock_db_instance.__getitem__.return_value = mock_collection_instance
        
        with patch('src.persistence.mongodb.get_db') as mock_get_db:
            mock_get_db.return_value = mock_db_instance
            
            collection_name = "my_test_collection"
            collection = _get_collection(collection_name)
            
            self.assertEqual(collection, mock_collection_instance)
            mock_get_db.assert_called_once()
            # Verify db["collection_name"] was called
            mock_db_instance.__getitem__.assert_called_once_with(collection_name)

    # --- Tests for close_mongo_connection --- #
    def test_close_connection_success(self):
        """Test closing an active connection."""
        mock_client_instance = MagicMock()
        # Nested patches for initial state
        with patch.object(mongodb_module, 'client', mock_client_instance):
            with patch.object(mongodb_module, 'db', MagicMock()):
                close_mongo_connection()
                # Assert the mock client's close method was called
                mock_client_instance.close.assert_called_once()
                # Assert the module-level variables are reset AFTER the call
                self.assertIsNone(mongodb_module.client)
                self.assertIsNone(mongodb_module.db)
        
    def test_close_connection_not_connected(self):
        """Test closing when not connected (should be a no-op)."""
        # Nested patches for initial state
        with patch.object(mongodb_module, 'client', None):
            with patch.object(mongodb_module, 'db', None):
                mock_close_func = MagicMock()
                # Patching a dummy object to check if close is called anywhere
                # This patch isn't strictly necessary but confirms no unexpected close occurs
                with patch.object(MagicMock(), 'close', mock_close_func):
                    close_mongo_connection()
                    mock_close_func.assert_not_called()
                # Assert module state remains None
                self.assertIsNone(mongodb_module.client)
                self.assertIsNone(mongodb_module.db)


if __name__ == '__main__':
    unittest.main()