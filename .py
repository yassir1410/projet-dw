import requests
import json
import time  # for adding delay between requests

url = 'https://places.googleapis.com/v1/places:searchText'
headers = {
    'Content-Type': 'application/json',
    'X-Goog-Api-Key': 'AIzaSyDUTyVkdL9hpCbM5w4KHQDjryeOLoj0d5k',  # Replace with your actual API key
    'X-Goog-FieldMask': 'places.name,places.displayName,places.formattedAddress,places.types,places.websiteUri,places.location,places.reviews,places.rating'
}

villes = [
    # Casablanca (Région Casablanca-Settat)
    "Banque Casablanca - Maarif", 
    "Banque Casablanca - California", 
    "Banque Casablanca - Sidi Belyout", 
    "Banque Casablanca - Anfa", 
    "Banque Casablanca - Mohammedia", 
    "Banque Casablanca - Hay Hassani", 
    "Banque Casablanca - Belvédère", 
    "Banque Casablanca - Derb Ghallef", 
    "Banque Casablanca - Aïn Diab", 
    "Banque Casablanca - Sidi Moumen", 
    "Banque Casablanca - Oulfa", 
    "Banque Casablanca - Bourgogne", 
    "Banque Casablanca - Hay Mohammadi", 
    "Banque Casablanca - Zerktouni", 
    
    # Rabat (Région Rabat-Salé-Kénitra)
    "Banque Rabat - Agdal", 
    "Banque Rabat - Hay Riad", 
    "Banque Rabat - Centre Ville", 
    "Banque Rabat - Souissi", 
    "Banque Rabat - Hassan", 
    "Banque Rabat - Belvédère", 
    "Banque Rabat - Témara", 
    "Banque Rabat - Salé", 
    "Banque Rabat - Hay El Fath", 
    "Banque Rabat - Oulja", 
    
    # Fès (Région Fès-Meknès)
    "Banque Fès - Ville Nouvelle", 
    "Banque Fès - Médina", 
    "Banque Fès - Batha", 
    "Banque Fès - Noujlid", 
    "Banque Fès - Talaa Kebira", 
    "Banque Fès - Al-Andalous", 
    "Banque Fès - Sidi Harazem", 
    "Banque Fès - Ain Nokbi", 
    "Banque Fès - Riad", 
    
    # Marrakech (Région Marrakech-Safi)
    "Banque Marrakech - Gueliz", 
    "Banque Marrakech - Menara", 
    "Banque Marrakech - Medina", 
    "Banque Marrakech - Palmeraie", 
    "Banque Marrakech - Hivernage", 
    "Banque Marrakech - Agdal", 
    "Banque Marrakech - Targa", 
    "Banque Marrakech - Route de Casablanca", 
    "Banque Marrakech - Ouarzazate", 
    "Banque Marrakech - Sidi Youssef Ben Ali", 
    
    # Tanger (Région Tanger-Tétouan-Al Hoceima)
    "Banque Tanger - Malabata", 
    "Banque Tanger - Centre Ville", 
    "Banque Tanger - Achakar", 
    "Banque Tanger - Mnar Park", 
    "Banque Tanger - Boulevard Mohamed VI", 
    "Banque Tanger - Plage", 
    "Banque Tanger - Kasbah", 
    "Banque Tanger - Marshan", 
    "Banque Tanger - Al Asilah", 
    
    # Agadir (Région Souss-Massa)
    "Banque Agadir - Hay Mohammadi", 
    "Banque Agadir - Founty", 
    "Banque Agadir - Talborjt", 
    "Banque Agadir - Inezgane", 
    "Banque Agadir - Al Massira", 
    "Banque Agadir - Cite Charaf", 
    "Banque Agadir - Bensergao", 
    "Banque Agadir - Cité Dakhla", 
    
    # Salé (Région Rabat-Salé-Kénitra)
    "Banque Salé - Hay Riad", 
    "Banque Salé - Tinja", 
    "Banque Salé - Medina", 
    "Banque Salé - Al Kamra", 
    "Banque Salé - Sidi Moussa", 
    
    # Oujda (Région de l'Oriental)
    "Banque Oujda - Centre Ville", 
    "Banque Oujda - Hay Al Manar", 
    "Banque Oujda - Hay Mohammadi", 
    "Banque Oujda - Hay El Massira", 
    
    # Tétouan (Région Tanger-Tétouan-Al Hoceima)
    "Banque Tétouan - Centre Ville", 
    "Banque Tétouan - Souika", 
    "Banque Tétouan - Rue Hassan II", 
    "Banque Tétouan - Cité El Alia", 
    
    # Essaouira (Région Marrakech-Safi)
    "Banque Essaouira - Medina", 
    "Banque Essaouira - Hay Dakhla", 
    "Banque Essaouira - Moulay Hassan", 
    
    # Kénitra (Région Rabat-Salé-Kénitra)
    "Banque Kénitra - Centre Ville", 
    "Banque Kénitra - Hay Al Massira", 
    "Banque Kénitra - Hay Anassir", 
    "Banque Kénitra - Ville Nouvelle", 
    
    # Nador (Région de l'Oriental)
    "Banque Nador - Hay Mohammadi", 
    "Banque Nador - Hay Al Wahda", 
    "Banque Nador - Centre Ville", 
    
    # Taza (Région de Fès-Meknès)
    "Banque Taza - Centre Ville", 
    "Banque Taza - Hay Al Amal", 
    
    # Midelt (Région de Drâa-Tafilalet)
    "Banque Midelt - Centre Ville", 
    "Banque Midelt - Hay Al Amal", 
    
    # Guelmim (Région Guelmim-Oued Noun)
    "Banque Guelmim - Centre Ville", 
    "Banque Guelmim - Hay Al Qods", 
    
    # Taroudant (Région Souss-Massa)
    "Banque Taroudant - Centre Ville", 
    "Banque Taroudant - Hay Anassir", 
    "Banque Taroudant - Hay Al Massira", 
    
    # Laâyoune (Région Laâyoune-Sakia El Hamra)
    "Banque Laâyoune - Centre Ville", 
    "Banque Laâyoune - Hay El Massira", 
    "Banque Laâyoune - Hay Moulay Abdelhadi", 
    
    # Dakhla (Région Dakhla-Oued Ed-Dahab)
    "Banque Dakhla - Centre Ville", 
    "Banque Dakhla - Hay Al Waha", 
    "Banque Dakhla - Hay Tafdna", 
    
    # Beni Mellal (Région Béni Mellal-Khénifra)
    "Banque Beni Mellal - Centre Ville", 
    "Banque Beni Mellal - Hay Mohammadi", 
    "Banque Beni Mellal - Al Qods", 
    
    # Khenifra (Région Béni Mellal-Khénifra)
    "Banque Khenifra - Centre Ville", 
    "Banque Khenifra - Hay Al Irfane", 
    
    # Azrou (Région Fès-Meknès)
    "Banque Azrou - Centre Ville", 
    "Banque Azrou - Hay Mohammed V", 
    
    # Ifrane (Région Fès-Meknès)
    "Banque Ifrane - Centre Ville", 
    "Banque Ifrane - Hay Al Wifaq", 
    
    # Imouzzer Kandar (Région Fès-Meknès)
    "Banque Imouzzer Kandar - Centre Ville", 
    "Banque Imouzzer Kandar - Hay El Massira",
    
    # Chefchaouen (Région Tanger-Tétouan-Al Hoceima)
    "Banque Chefchaouen - Medina", 
    "Banque Chefchaouen - Riad", 
    
    # Ouarzazate (Région Drâa-Tafilalet)
    "Banque Ouarzazate - Centre Ville", 
    "Banque Ouarzazate - Kasbah", 
    "Banque Ouarzazate - Hay Moulay Rachid", 
    
    # Skhirat (Région Rabat-Salé-Kénitra)
    "Banque Skhirat - Centre Ville",
    
    # Tiznit (Région Souss-Massa)
    "Banque Tiznit - Centre Ville", 
    "Banque Tiznit - Hay Anassir", 
    "Banque Tiznit - Hay Al Massira", 
    
    # Rissani (Région Drâa-Tafilalet)
    "Banque Rissani - Centre Ville",
    
    # Goulmima (Région Drâa-Tafilalet)
    "Banque Goulmima - Centre Ville",
    
    # Tinghir (Région Drâa-Tafilalet)
    "Banque Tinghir - Centre Ville",
    
    # Tinghir (Région Drâa-Tafilalet)
    "Banque Tinghir - Centre Ville",
    
    # Erfoud (Région Drâa-Tafilalet)
    "Banque Erfoud - Centre Ville"
]


responses = []

for ville in villes:
    data = {
        "textQuery": ville
    }
    
    # List to hold all results for the current ville
    ville_results = []

    while True:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        
        if response.status_code == 200:
            result_data = response.json()
            # Add the results from the current request to the list for this ville
            ville_results.extend(result_data.get('places', []))
            
            # Check if there is a next_page_token
            next_page_token = result_data.get('next_page_token')
            
            if next_page_token:
                # Prepare for the next page request
                data['next_page_token'] = next_page_token
                time.sleep(2)  # Add a small delay before making the next request
            else:
                # No more pages, exit the loop
                break
        else:
            print(f"Error for {ville}: {response.status_code} - {response.text}")
            ville_results.append({"error": f"Error for {ville}: {response.status_code} - {response.text}"})
            break

    # Append the results for the current ville to the responses list
    responses.append({
        "ville": ville,
        "results": ville_results
    })

# Write the JSON response to a file
file_path = "res2.json"
with open(file_path, 'w') as json_file:
    json.dump(responses, json_file, indent=4)

print(f"JSON response saved to {file_path}")
