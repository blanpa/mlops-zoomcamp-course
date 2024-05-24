import requests
import os
from datetime import datetime

def download_tlc_data(record_type, year, months, output_base_dir="tlc_data"):
    """
    Lädt TLC-Daten für den angegebenen Datensatztyp, Jahr und die Monate herunter und speichert sie in einem entsprechenden Verzeichnis.

    :param record_type: Der Typ des Datensatzes (z.B. 'yellow_tripdata', 'green_tripdata', 'fhv_tripdata', 'hvfhv_tripdata')
    :param year: Das Jahr, für das die Daten heruntergeladen werden sollen
    :param months: Eine Liste von Monaten, für die die Daten heruntergeladen werden sollen
    :param output_base_dir: Das Basisverzeichnis, in dem die Daten gespeichert werden sollen (Standard: 'tlc_data')
    """
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    output_dir = os.path.join(output_base_dir, record_type)
    
    for month in months:
        file_name = f"{record_type}_{year}-{month:02d}.parquet"
        url = base_url + file_name
        output_path = os.path.join(output_dir, file_name)

        print(f"Downloading from: {url}")

        # Überprüfen, ob das Verzeichnis existiert, andernfalls erstellen
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(output_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Datei heruntergeladen und gespeichert unter: {output_path}")
        else:
            print(f"Fehler beim Herunterladen der Datei: {response.status_code}")
