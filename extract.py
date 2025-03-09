import json

# Étape 1 : Lire un fichier JSON
with open('results.json', 'r') as file:
    data = json.load(file)


 


banques = []

# Check if data is empty
if not data:
    print("Data is empty.")
else:
    # Iterate over data
    for response in data:
        # Check if "places" key exists
        if "places" in response:
            place = response["places"]
            # Iterate over each place
            for banque in place:
                banques.append(banque)
        else:
            print("Key 'places' not found in response:", response)

# Write the result to a JSON file
with open("banques.json", "w", encoding="utf-8") as json_file:
    json.dump(banques, json_file, ensure_ascii=False, indent=4)

print("Data has been written to 'banques.json'.")
# print(data[0]["places"][0]["rating"])