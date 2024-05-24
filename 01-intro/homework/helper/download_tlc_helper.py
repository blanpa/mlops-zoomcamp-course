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

def select_record_type():
    """
    Bietet eine Auswahl an Datensatztypen und gibt den ausgewählten Datensatztyp zurück.
    
    :return: Der ausgewählte Datensatztyp als String
    """
    record_types = {
        1: "yellow_tripdata",
        2: "green_tripdata",
        3: "fhv_tripdata",
        4: "hvfhv_tripdata"
    }

    print("Verfügbare Datensatztypen:")
    print("1. Yellow Taxi Trip Records")
    print("2. Green Taxi Trip Records")
    print("3. For-Hire Vehicle Trip Records")
    print("4. High Volume For-Hire Vehicle Trip Records")
    
    choice = int(input("Wählen Sie einen Datensatztyp aus (Nummer eingeben): "))
    return record_types.get(choice, "yellow_tripdata")

def select_year_months():
    """
    Bietet eine Auswahl an Jahren und Monaten und gibt das ausgewählte Jahr und die ausgewählten Monate zurück.
    
    :return: Das ausgewählte Jahr und eine Liste von ausgewählten Monaten
    """
    current_year = datetime.now().year
    years = list(range(2009, current_year + 1))

    print("Verfügbare Jahre:")
    for i, year in enumerate(years):
        print(f"{i + 1}. {year}")
    year_choice = int(input("Wählen Sie ein Jahr aus (Nummer eingeben): ")) - 1
    selected_year = years[year_choice]

    print("Verfügbare Monate (mehrere Monate durch Komma trennen, z.B. 1,2,3):")
    months = list(range(1, 13))
    for i, month in enumerate(months):
        print(f"{i + 1}. {month:02d}")
    month_choices = input("Wählen Sie Monate aus (Nummern durch Komma trennen): ").split(',')
    selected_months = [int(month.strip()) for month in month_choices]

    return selected_year, selected_months

if __name__ == "__main__":
    record_type = select_record_type()
    year, months = select_year_months()
    download_tlc_data(record_type, year, months)
