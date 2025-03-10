import json
from connection import get_db_connection

def insert_data_from_json(file_path):
    """Insert data from a JSON file into the PostgreSQL database."""
    connection = None  # Initialize the connection variable
    try:
        # Try reading the file with UTF-8 encoding
        try:
            with open(file_path, "r", encoding='utf-8') as file:
                data = json.load(file)
        except UnicodeDecodeError as decode_error:
            print(f"Unicode decode error: {decode_error}")
            return  # Exit early if there's a decode error

        # Get the database connection
        connection = get_db_connection()
        cursor = connection.cursor()

        # Define the SQL query for inserting data
        insert_query = """
        INSERT INTO bank (name, branch_name, global_rating, location)
        VALUES (%s, %s, %s, %s)
        """

        # Insert each record into the database
        records_inserted = 0
        for record in data:
            # Debugging output
            print(f"Processing record: {record}")

            # Ensure the keys exist in the record to avoid KeyError
            if "formattedAddress" in record and "rating" in record:
                # Process the branch name correctly
                address_parts = record["formattedAddress"].split(",")
                branch = address_parts[0] if address_parts else ""
                if len(address_parts) > 1:
                    branch += address_parts[1]

                name = record.get("displayName", {}).get("text", "")

                # Sanitize data before insertion: remove or replace invalid UTF-8 characters
                def sanitize_text(text):
                    if not text:
                        return ""
                    return ''.join([char if ord(char) < 128 else '' for char in text])

                branch = sanitize_text(name + " " + branch.strip())
                name = sanitize_text(name.strip())
                location = sanitize_text(record["formattedAddress"].strip())

                # Handle possible missing ratings
                rating = record["rating"] if "rating" in record else None

                # Encode data to UTF-8 (ignoring non-UTF-8 characters)
                name = name.encode('utf-8', 'ignore').decode('utf-8')
                branch = branch.encode('utf-8', 'ignore').decode('utf-8')
                location = location.encode('utf-8', 'ignore').decode('utf-8')

                cursor.execute(insert_query, (name, branch, rating, location))
                records_inserted += 1
            else:
                print(f"Missing data in record: {record}")

        # Commit the transaction
        connection.commit()
        print(f"Data successfully inserted into the database. {records_inserted} records inserted.")

    except json.JSONDecodeError as json_error:
        print(f"JSON decode error: {json_error}")
    except Exception as error:
        print(f"Error inserting data: {error}")
        # If connection exists, rollback any partial transaction
        if connection:
            connection.rollback()

    finally:
        # Close the database connection if it was opened
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection closed.")

# Call the function with the path to your JSON file
if __name__ == "__main__":
    insert_data_from_json("banques2.json")
