import requests
import pandas as pd
import os
from datetime import datetime

# Coordenadas de las ciudades
CITIES = {
    'Madrid': {'lat': 40.4168, 'lon': -3.7038},
    'Barcelona': {'lat': 41.3851, 'lon': 2.1734},
    'Paris': {'lat': 48.8566, 'lon': 2.3522},
    'London': {'lat': 51.5074, 'lon': -0.1278},
    'Berlin': {'lat': 52.5200, 'lon': 13.4050},
    'Rome': {'lat': 41.9028, 'lon': 12.4964},
    'Amsterdam': {'lat': 52.3676, 'lon': 4.9041},
    'Warsaw': {'lat': 52.2297, 'lon': 21.0122}
}

# Parámetros temporales
START_DATE = '2015-01-01'
END_DATE = '2024-12-31'

def download_climate_data(city_name, lat, lon):
    """
    Descarga datos climáticos históricos de Open-Meteo API
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': START_DATE,
        'end_date': END_DATE,
        'daily': [
            'temperature_2m_max',
            'temperature_2m_min',
            'temperature_2m_mean',
            'precipitation_sum',
            'rain_sum',
            'windspeed_10m_max',
            'relative_humidity_2m_mean'
        ],
        'timezone': 'Europe/Berlin'
    }
    
    print(f"Descargando datos climáticos para {city_name}...")
    
    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        # Convertir a DataFrame
        df = pd.DataFrame({
            'date': pd.to_datetime(data['daily']['time']),
            'city': city_name,
            'temp_max': data['daily']['temperature_2m_max'],
            'temp_min': data['daily']['temperature_2m_min'],
            'temp_mean': data['daily']['temperature_2m_mean'],
            'precipitation': data['daily']['precipitation_sum'],
            'rain': data['daily']['rain_sum'],
            'wind_speed_max': data['daily']['windspeed_10m_max'],
            'humidity_mean': data['daily']['relative_humidity_2m_mean']
        })
        
        print(f"  Datos descargados: {len(df)} registros")
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"  ERROR al descargar datos de {city_name}: {e}")
        return None

def main():
    """
    Función principal para descargar datos de todas las ciudades
    """
    all_data = []
    
    # Crear directorio
    os.makedirs('data/raw', exist_ok=True)

    # Descarga de datos
    for city_name, coords in CITIES.items():
        df = download_climate_data(city_name, coords['lat'], coords['lon'])
        if df is not None:
            all_data.append(df)
    
    # Combinar todos los datos
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Guardar en CSV
        output_path = 'data/raw/climate_data_2015_2024.csv'
        combined_df.to_csv(output_path, index=False)
        
        print(f"\nDatos guardados en: {output_path}")
        print(f"Total de registros: {len(combined_df)}")
        print(f"Rango de fechas: {combined_df['date'].min()} a {combined_df['date'].max()}")
        print(f"Ciudades incluidas: {combined_df['city'].unique().tolist()}")
    else:
        print("\nNo se pudo descargar ningún dato.")

if __name__ == "__main__":
    main()