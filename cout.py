import json

def count_word_bank(json_file_path):
    try:
        # Load the JSON file
        with open(json_file_path, 'r') as file:
            data = json.load(file)

        # Convert the JSON data to a string
        json_string = json.dumps(data)

        # Count the occurrences of the word "bank" (case-insensitive)
        word_count = json_string.lower().count('rating')

        return word_count

    except Exception as e:
        print(f"An error occurred: {e}")
        return 0

# Example usage
json_file_path = 'results.json'  # Replace with your JSON file path
word_count = count_word_bank(json_file_path)
print(f"The word 'bank' appears {word_count} times in the JSON file.")