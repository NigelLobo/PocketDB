import random
import time
import os
from pocketdb import PocketDB

TEST_DB_NAME = 'performance_test_db'
NUMBER_OF_ITEMS = 10**5
KEY_PREFIX = "key_"

# A global dictionary to hold keys and values for consistency across tests
keys_to_test = [f"{KEY_PREFIX}{i}" for i in range(NUMBER_OF_ITEMS)]
values_to_test = [f"value_{i}" for i in range(NUMBER_OF_ITEMS)]


def setup_db(db_name):
    """Sets up a clean database for testing."""
    if os.path.exists(db_name):
        os.remove(db_name)
    return PocketDB(db_name)


def teardown_db(db):
    """Closes and deletes the database file."""
    db.quit()
    if os.path.exists(db.default_filename):
        os.remove(db.default_filename)


def run_test(name, func):
    """A helper function to run and time a test."""
    print(f"--- Running Test: {name} ---")
    start = time.perf_counter()
    func()
    end = time.perf_counter()
    print(f"Elapsed time: {end - start:.4f} seconds\n")

# A helper to populate the database for read/update/delete tests


def populate_db(db, items):
    """A helper to populate the database."""
    print(f"Populating DB with {items} items...")
    for i in range(items):
        db.set(keys_to_test[i], values_to_test[i])
    print("Population complete.\n")

# --- 1. Write Performance Tests ---


def test_sequential_writes():
    """Tests writing items in a sequential loop."""
    db = setup_db(TEST_DB_NAME)
    for i in range(NUMBER_OF_ITEMS):
        db.set(keys_to_test[i], values_to_test[i])
    teardown_db(db)


def test_random_writes():
    """Tests writing items with non-sequential keys."""
    db = setup_db(TEST_DB_NAME)
    random_indices = list(range(NUMBER_OF_ITEMS))
    random.shuffle(random_indices)
    for i in random_indices:
        db.set(keys_to_test[i], values_to_test[i])
    teardown_db(db)


def test_large_value_writes():
    """Tests writing large values to the database."""
    db = setup_db(TEST_DB_NAME)
    large_value = "X" * 1024 * 10  # 10KB value
    for i in range(int(NUMBER_OF_ITEMS / 10)):
        db.set(keys_to_test[i], large_value)
    teardown_db(db)

# --- 2. Read Performance Tests ---


def test_sequential_reads():
    """Tests reading items in the order they were written."""
    db = setup_db(TEST_DB_NAME)
    populate_db(db, NUMBER_OF_ITEMS)
    for i in range(NUMBER_OF_ITEMS):
        db.get(keys_to_test[i])
    teardown_db(db)


def test_random_reads():
    """Tests reading items with a random access pattern."""
    db = setup_db(TEST_DB_NAME)
    populate_db(db, NUMBER_OF_ITEMS)
    random_indices = list(range(NUMBER_OF_ITEMS))
    random.shuffle(random_indices)
    for i in random_indices:
        db.get(keys_to_test[i])
    teardown_db(db)

# --- 3. Update and Delete Performance Tests ---


def test_updates():
    """Tests updating existing keys."""
    db = setup_db(TEST_DB_NAME)
    populate_db(db, NUMBER_OF_ITEMS)
    for i in range(NUMBER_OF_ITEMS):
        db.set(keys_to_test[i], "new_value")
    teardown_db(db)


def test_deletes():
    """Tests deleting existing keys."""
    db = setup_db(TEST_DB_NAME)
    populate_db(db, NUMBER_OF_ITEMS)
    for i in range(NUMBER_OF_ITEMS):
        db.delete(keys_to_test[i])
    teardown_db(db)

# --- 4. Mixed Workload Test ---


def test_mixed_workload():
    """Tests a mix of reads (90%) and writes (10%)."""
    db = setup_db(TEST_DB_NAME)
    populate_db(db, int(NUMBER_OF_ITEMS / 2))  # Start with some data

    # We will perform a total of NUMBER_OF_ITEMS operations
    for _ in range(NUMBER_OF_ITEMS):
        # 90% chance of a read, 10% chance of a write
        if random.random() < 0.9:
            key_index = random.randint(0, int(NUMBER_OF_ITEMS/2) - 1)
            db.get(keys_to_test[key_index])
        else:
            key_index = random.randint(
                int(NUMBER_OF_ITEMS/2), NUMBER_OF_ITEMS - 1)
            db.set(keys_to_test[key_index], values_to_test[key_index])
    teardown_db(db)

# ====================================================================
# Main execution block
# ====================================================================


if __name__ == "__main__":
    print(f"Starting performance tests with {NUMBER_OF_ITEMS} items.")
    start = time.perf_counter()

    # Run the write tests first
    run_test("Sequential Writes", test_sequential_writes)
    run_test("Random Writes", test_random_writes)
    run_test("Large Value Writes (10KB)", test_large_value_writes)

    # Run the read tests on a pre-populated DB
    run_test("Sequential Reads", test_sequential_reads)
    run_test("Random Reads", test_random_reads)

    # Run update and delete tests
    run_test("Updates (overwriting existing keys)", test_updates)
    run_test("Deletes", test_deletes)

    # Run the mixed workload test
    run_test("Mixed Read/Write Workload (90/10)", test_mixed_workload)
    end = time.perf_counter()

    print("All performance tests completed.")
    print(f"Total Elapsed Time: {end - start:.4f} seconds\n")
