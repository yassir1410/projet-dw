import json

# Load the JSON file
with open('results.json', 'r') as file:
    data = json.load(file)

total_key_value_pairs = 0

# If the JSON is an array of objects
if isinstance(data, list):
    print(f"Total number of objects: {len(data)}")
    for idx, obj in enumerate(data):
        if isinstance(obj, dict):
            key_count = len(obj)
            total_key_value_pairs += key_count
            print(f"Object {idx + 1} has {key_count} key-value pairs.")
    print(f"Total key-value pairs in all objects: {total_key_value_pairs}")

# If the JSON is a single object
elif isinstance(data, dict):
    key_count = len(data)
    print("The JSON contains a single object.")
    print(f"The single object has {key_count} key-value pairs.")
    total_key_value_pairs = key_count

print(f"Total key-value pairs in the JSON file: {total_key_value_pairs}")
