import json
from connection import get_db_connection

def insert_data_from_json(file_path):
    """Insert data from a JSON file into the PostgreSQL database."""
    connection = None
    try:
        with open(file_path, "r", encoding='utf-8') as file:
            data = json.load(file)

        connection = get_db_connection()
        cursor = connection.cursor()

        insert_bank_query = """
        INSERT INTO bank (name, branch_name, global_rating, location)
        VALUES (%s, %s, %s, %s)
        RETURNING bank_id
        """

        insert_review_query = """
        INSERT INTO review (review_text, rating, display_name, review_date, bank_id)
        VALUES (%s, %s, %s, %s, %s)
        """

        records_inserted = 0
        reviews_inserted = 0
        
        # Check if data is a list or a single dictionary
        records = data if isinstance(data, list) else [data]
        
        for record in records:
            print("\nProcessing bank record:")
            print(json.dumps(record, indent=2, ensure_ascii=False))

            if "formattedAddress" in record:
                # Process bank data
                address_parts = record["formattedAddress"].split(",")
                branch = address_parts[0] if address_parts else ""
                if len(address_parts) > 1:
                    branch += address_parts[1]

                name = record.get("displayName", {}).get("text", "")
                branch = f"{name} {branch.strip()}".strip()
                name = name.strip()
                location = record["formattedAddress"].strip()
                rating = record.get("rating")

                print(f"\nInserting bank: {name} | {branch} | {rating} | {location}")

                cursor.execute(insert_bank_query, (name, branch, rating, location))
                bank_id = cursor.fetchone()[0]
                records_inserted += 1
                print(f"Inserted bank with ID: {bank_id}")

                # Process reviews
                if "reviews" in record:
                    print(f"\nFound {len(record['reviews'])} reviews for this bank")
                    for i, review in enumerate(record["reviews"], 1):
                        review_text = review.get("text", {}).get("text", "")
                        review_rating = review.get("rating")
                        display_name = review.get("authorAttribution", {}).get("displayName", "Anonymous")
                        review_date = review.get("publishTime", "")[:10]  # YYYY-MM-DD
                        
                        print(f"\nReview {i}:")
                        print(f"Rating: {review_rating}")
                        print(f"Author: {display_name}")
                        print(f"Date: {review_date}")
                        print(f"Text: {review_text[:50]}...")  # Print first 50 chars

                        if review_text:  # Only insert if there's review text
                            cursor.execute(insert_review_query, 
                                         (review_text, review_rating, display_name, review_date, bank_id))
                            reviews_inserted += 1
                            print("Review inserted successfully")
                        else:
                            print("Skipping review - empty text")
                else:
                    print("No reviews found for this bank")

        connection.commit()
        print(f"\nSummary: {records_inserted} bank records and {reviews_inserted} reviews inserted.")

    except json.JSONDecodeError as json_error:
        print(f"JSON decode error: {json_error}")
    except Exception as error:
        print(f"Error inserting data: {error}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection closed.")

if __name__ == "__main__":
    insert_data_from_json("banques2.json")