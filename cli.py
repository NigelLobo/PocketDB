
import cmd
import sys
import os
import json
import shlex
import argparse

from pocketdb import PocketDB, PocketDBInvalidKeyError, PocketDBInvalidValueError, PocketDBKeyNotFoundError


class PocketDBCLI(cmd.Cmd):
    """
    Interactive command-line interface for PocketDB.

    Provides the set of commands for managing an in-memory
    key-value store with features like TTL and persistence.
    """

    intro = """
╔══════════════════════════════════════════════════════════════╗
║                    PocketDB Interactive CLI                  ║
║                                                              ║
║  Type 'help' for available commands                          ║
║  Type 'help <command>' for detailed command help             ║
║  Type 'quit' or 'exit' to exit                               ║
╚══════════════════════════════════════════════════════════════╝
"""

    def __init__(self, db_name: str = "cli_db"):
        super().__init__()
        self.db = PocketDB(name=db_name)
        self.prompt = f"pocketdb ({self.db.name})> "
        self.current_file = None

    def do_save(self, arg):
        """
        Save database to disk.

        Usage: save [filename]

        Examples:
            save
            save backup.pdb
            save my_database.pdb
        """
        try:
            args = shlex.split(arg)
            filename = args[0] if args else None

            if self.db.save_to_disk(filename):
                saved_to = filename or self.db.default_filename
                print(f"✓ Database saved to '{saved_to}'")
                self.current_file = saved_to
            else:
                print("Error: Failed to save database")

        except Exception as e:
            print(f"Error: {e}")

    def do_load(self, arg):
        """
        Load database from disk.

        Usage: load [filename]

        Examples:
            load
            load backup.pdb
            load my_database.pdb
        """
        try:
            args = shlex.split(arg)
            filename = args[0] if args else None

            if self.db.load_from_disk(filename):
                loaded_from = filename or self.db.default_filename
                print(f"✓ Database loaded from '{loaded_from}'")
                self.current_file = loaded_from
            else:
                print("Error: Failed to load database")

        except Exception as e:
            print(f"Error: {e}")

    def do_set(self, arg):
        """
        Set a key-value pair in the database.

        Usage: set <key> <value> [ttl]

        Examples:
            set user:1 "Nigel Lobo"
            set session:123 "active" 3600
            set counter 42
            set config '{"debug": true, "port": 8080}'
        """
        try:
            args = shlex.split(arg)
            if len(args) < 2:
                print("Error: Usage: set <key> <value> [ttl]")
                return

            key = args[0]
            value_str = args[1]
            ttl = int(args[2]) if len(args) > 2 and args[2].isdigit() else None

            # Try to parse as JSON first, then as literal
            try:
                value = json.loads(value_str)
            except json.JSONDecodeError:
                # Try to parse as literal values
                if value_str.lower() == 'true':
                    value = True
                elif value_str.lower() == 'false':
                    value = False
                elif value_str.lower() == 'null':
                    value = None
                elif value_str.isdigit():
                    value = int(value_str)
                elif value_str.replace('.', '').replace('-', '').isdigit() and value_str.count('.') == 1:
                    value = float(value_str)
                else:
                    value = value_str

            self.db.set(key, value, ttl)
            print(f"✓ Set '{key}' => {value}")
            if ttl:
                print(f"  TTL: {ttl} seconds")

        except (PocketDBInvalidKeyError, PocketDBInvalidValueError) as e:
            print(f"Error: {e}")
        except ValueError as e:
            print(f"Error: Invalid TTL value - {e}")
        except Exception as e:
            print(f"Error: {e}")

    def do_get(self, arg):
        """
        Get a value by key.

        Usage: get <key>

        Examples:
            get user:1
            get session:123
        """
        try:
            args = shlex.split(arg)
            if len(args) != 1:
                print("Error: Usage: get <key>")
                return

            key = args[0]
            value = self.db.get(key)
            print(f"'{key}' => {value}")

        except PocketDBKeyNotFoundError:
            print(f"Error: Key '{key}' not found")
        except PocketDBInvalidKeyError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Error: {e}")

    def do_delete(self, arg):
        """
        Delete a key-value pair from the database.

        Usage: delete <key>

        Examples:
            delete user:1
            delete session:123
        """
        try:
            args = shlex.split(arg)
            if len(args) != 1:
                print("Error: Usage: delete <key>")
                return

            key = args[0]
            if self.db.delete(key):
                print(f"✓ Deleted '{key}'")
            else:
                print(f"Key '{key}' not found")

        except PocketDBInvalidKeyError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Error: {e}")

    def do_exists(self, arg):
        """
        Check if a key exists in the database.

        Usage: exists <key>

        Examples:
            exists user:1
            exists session:123
        """
        try:
            args = shlex.split(arg)
            if len(args) != 1:
                print("Error: Usage: exists <key>")
                return

            key = args[0]
            exists = self.db.exists(key)
            print(f"'{key}' exists: {exists}")

        except PocketDBInvalidKeyError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Error: {e}")

    def do_size(self, arg):
        """
        Get the number of key-value pairs in the database.

        Usage: size
        """
        try:
            size = self.db.size()
            print(f"Database size: {size} keys")
        except Exception as e:
            print(f"Error: {e}")

    def do_clear(self, arg):
        """
        Clear all data from the database.

        Usage: clear
        """
        try:
            if arg.strip():
                print("Error: Usage: clear")
                return

            confirm = input("Are you sure you want to clear all data? (y/N): ")
            if confirm.lower() in ['y', 'yes']:
                self.db.clear()
                print("✓ Database cleared")
            else:
                print("Operation cancelled")

        except Exception as e:
            print(f"Error: {e}")

    def do_stats(self, arg):
        """
        Get database statistics.

        Usage: stats
        """
        try:
            if arg.strip():
                print("Error: Usage: stats")
                return

            stats = self.db.stats()
            print("Database Statistics:")
            print(f"  Name: {self.db.name}")
            print(
                f"  Save interval: {self.db._AUTO_SAVE_INTERVAL_SECS} seconds")
            print(f"  Current file: {self.current_file or 'None'}")
            print(f"  Size: {stats['size']} keys")
            print(f"  TTL keys: {stats['ttl_keys']} keys")

        except Exception as e:
            print(f"Error: {e}")

    def do_help(self, arg):
        """List available commands or show help for a specific command."""
        if arg:
            super().do_help(arg)
        else:
            print("\nAvailable Commands:")
            print("  Basic Operations:")
            print("    set <key> <value> [ttl]     - Set a key-value pair")
            print("    get <key>                    - Get a value by key")
            print("    delete <key>                 - Delete a key-value pair")
            print("    exists <key>                 - Check if key exists")
            print("    reset                        - reset all data")
            print("    size                         - Get database size")
            print("")
            print("  Query Operations:")
            print(
                "    keys [pattern]               - List keys (with optional pattern)")
            print("    values                       - List all values")
            print("    items                        - List all key-value pairs")
            print("")
            print("  Persistence:")
            print("    save [filename]              - Save database to disk")
            print("    load [filename]              - Load database from disk")
            print("")
            print("  Statistics:")
            print("    stats                        - Get database statistics")
            print(
                "    help [command]               - Show this help or command help")
            print("")
            print("  System:")
            print("    quit, exit                   - Exit the CLI")
            print("")
            print("Examples:")
            print("  set user:1 'John Doe'")
            print("  set session:123 'active' 3600")
            print("  get user:1")
            print("  keys user:*")
            print("  save backup.pdb")

    def do_reset(self, arg):
        """
        Reset all data from the database.

        Usage: clear
        """
        try:
            if arg.strip():
                print("Error: Usage: clear")
                return

            confirm = input("Are you sure you want to clear all data? (y/N): ")
            if confirm.lower() in ['y', 'yes']:
                self.db.reset()
                print("✓ Database reset")
            else:
                print("Χ Operation cancelled")

        except Exception as e:
            print(f"Error: {e}")

    def do_keys(self, arg):
        """
        List all keys or keys matching a pattern.

        Usage: keys [pattern]

        Examples:
            keys
            keys user:*
            keys *session*
            keys temp*
        """
        try:
            args = shlex.split(arg)
            pattern = args[0] if args else "*"

            keys = self.db.keys(pattern)
            if keys:
                print(f"Keys matching '{pattern}':")
                for key in keys:
                    print(f"  {key}")
                print(f"Total: {len(keys)} keys")
            else:
                print(f"No keys found matching '{pattern}'")

        except Exception as e:
            print(f"Error: {e}")

    def do_values(self, arg):
        """
        List all values in the database.

        Usage: values
        """
        try:
            values = self.db.values()
            if values:
                print("Values in database:")
                for i, value in enumerate(values, 1):
                    print(f"  {i}. {value}")
                print(f"Total: {len(values)} values")
            else:
                print("Database is empty")

        except Exception as e:
            print(f"Error: {e}")

    def do_items(self, arg):
        """
        List all key-value pairs in the database.

        Usage: items
        """
        try:
            items = self.db.items()
            if items:
                print("Key-value pairs:")
                for key, value in items:
                    print(f"  {key} = {value}")
                print(f"Total: {len(items)} items")
            else:
                print("Database is empty")

        except Exception as e:
            print(f"Error: {e}")

    def default(self, line):
        print(f"Unknown command: {line}")
        print("Type 'help' for available commands")

    def do_quit(self, arg):
        self.db.quit()
        return True

    def do_exit(self, arg):
        return self.do_quit(arg)


def main():
    parser = argparse.ArgumentParser(description="PocketDB Interactive CLI")
    parser.add_argument("--name", "-n", default="cli_db",
                        help="Database name (default: cli_db)")
    parser.add_argument("--file", "-f",
                        help="Load database from file on startup")
    args = parser.parse_args()

    try:
        cli = PocketDBCLI(db_name=args.name)

        if args.file:
            try:
                cli.db.load_from_disk(args.file)
                cli.current_file = args.file
                print(f"✓ Loaded database from '{args.file}'")
            except Exception as e:
                print(
                    f"Could not load from '{args.file}': {e}. Continuing with blank database")

        cli.cmdloop()

    except KeyboardInterrupt:
        print("\nGoodbye!")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
