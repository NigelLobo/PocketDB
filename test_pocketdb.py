
import unittest
import os
from pocketdb import PocketDB, PocketDBError, PocketDBKeyNotFoundError, PocketDBInvalidKeyError, PocketDBInvalidValueError


class TestPocketDB(unittest.TestCase):
    """Test cases for PocketDB functionality."""

    def setUp(self):
        """Set up test database."""
        self.db = PocketDB(name="test_db")

    def tearDown(self):
        """Clean up after tests."""
        self.db.quit()
        # Clean up any test files
        for filename in ["test_db.pdb", "test_save.pdb"]:
            if os.path.exists(filename):
                os.remove(filename)

    def test_basic_operations(self):
        """Test basic CRUD operations."""
        self.assertTrue(self.db.set("key1", "value1"))
        self.assertEqual(self.db.get("key1"), "value1")

        self.db.set("key1", "new_value")
        self.assertEqual(self.db.get("key1"), "new_value")

        self.assertTrue(self.db.delete("key1"))
        self.assertFalse(self.db.exists("key1"))

        with self.assertRaises(PocketDBKeyNotFoundError):
            self.db.get("nonexistent")

    def test_key_validation(self):

        with self.assertRaises(PocketDBInvalidKeyError):
            self.db.set(123, "value")

        with self.assertRaises(PocketDBInvalidKeyError):
            self.db.set("", "value")

        with self.assertRaises(PocketDBInvalidKeyError):
            self.db.set("   ", "value")

        self.assertTrue(self.db.set("valid_key", "value"))
        self.assertTrue(self.db.set("key_with_underscores", "value"))
        self.assertTrue(self.db.set("key-with-dashes", "value"))

    def test_value_validation(self):

        self.assertTrue(self.db.set("key1", "string"))
        self.assertTrue(self.db.set("key2", 123))
        self.assertTrue(self.db.set("key3", 3.14))
        self.assertTrue(self.db.set("key4", True))
        self.assertTrue(self.db.set("key5", None))
        self.assertTrue(self.db.set("key6", [1, 2, 3]))
        self.assertTrue(self.db.set("key7", {"a": 1, "b": 2}))

        # functions are invalid as values
        def dummy():
            pass

        with self.assertRaises(PocketDBInvalidValueError):
            self.db.set("key8", dummy)


def run_demo():
    """Run a demonstration of PocketDB features."""
    print("=== PocketDB Demo ===\n")

    db = PocketDB(name="demo")

    print("1. Basic Operations:")
    db.set("name", "Nigel")
    db.set("age", 22)
    db.set("city", "Toronto")

    print(f"   Name: {db.get('name')}")
    print(f"   Age: {db.get('age')}")
    print(f"   City: {db.get('city')}")
    print(f"   Database size: {db.size()}")

    print("\n2. TTL (Time To Live):")
    db.set("temp_data", "This will expire", ttl=2)
    print(f"   Temporary data: {db.get('temp_data')}")

    db.quit()
    print("\n=== Demo Complete ===")


if __name__ == "__main__":
    run_demo()

    print("\n=== Running Tests ===")
    unittest.main(verbosity=2)
