import json

# Étape 1 : Lire un fichier JSON contenant les résultats des requêtes API
with open('res2.json', 'r') as file:
    data = json.load(file)

banques = []

# Vérifier si les données sont vides
if not data:
    print("Les données sont vides.")
else:
    # Parcourir les données
    for response in data:
        # Vérifier si la clé "results" existe dans la réponse
        if "results" in response:
            results = response["results"]
            # Vérifier si "results" n'est pas vide
            if not results:
                print("Aucune banque trouvée dans 'results' pour une réponse.")
            # Parcourir chaque résultat (banque)
            for banque in results:
                banques.append(banque)
        else:
            print("Clé 'results' non trouvée dans la réponse:", response)

# Afficher le nombre de banques enregistrées
print("Nombre total de banques : ", len(banques))

# Écrire le résultat dans un fichier JSON
if len(banques) > 0:
    with open("banques2.json", "w", encoding="utf-8") as json_file:
        json.dump(banques, json_file, ensure_ascii=False, indent=4)
    print("Les données ont été écrites dans 'banques2.json'.")
else:
    print("Aucune banque trouvée. Aucun fichier écrit.")
