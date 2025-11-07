import pandas as pd
import os
import time
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from openaq import OpenAQ
from pandas import json_normalize

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ciudades objetivo
CITIES = {
    'Madrid': {'lat': 40.4168, 'lon': -3.7038, 'country': 'ES'},
    'Barcelona': {'lat': 41.3851, 'lon': 2.1734, 'country': 'ES'},
    'Paris': {'lat': 48.8566, 'lon': 2.3522, 'country': 'FR'},
    'London': {'lat': 51.5074, 'lon': -0.1278, 'country': 'GB'}
}

# NUEVO: Configuración de años a descargar
YEARS_TO_DOWNLOAD = [2020, 2021, 2022, 2023, 2024]  # 5 años de datos

def download_openaq_historical_data():
    """
    Descarga datos históricos usando el SDK oficial de OpenAQ v3 - MÚLTIPLES AÑOS
    """
    print("="*60)
    print("DESCARGANDO DATOS HISTÓRICOS REALES DE CALIDAD DEL AIRE")
    print(f"Fuente: OpenAQ API v3 (SDK Oficial)")
    print(f"Años objetivo: {YEARS_TO_DOWNLOAD}")
    print("="*60)
    
    api_key = os.getenv('OPENAQ_API_KEY')
    if not api_key:
        logger.error("OPENAQ_API_KEY no encontrada")
        return None
    
    all_data = []
    
    with OpenAQ(api_key=api_key.strip()) as client:
        try:
            for city_name, city_info in CITIES.items():
                logger.info(f"Procesando {city_name}...")
                
                # Buscar locations cerca de la ciudad
                locations = find_city_locations(client, city_info, city_name)
                
                if not locations:
                    logger.warning(f"No se encontraron estaciones para {city_name}")
                    continue
                
                # Descargar mediciones para cada año
                for year in YEARS_TO_DOWNLOAD:
                    logger.info(f"  Descargando año {year}...")
                    year_measurements = download_city_measurements_by_year(client, locations, city_name, year)
                    
                    if year_measurements:
                        all_data.extend(year_measurements)
                        logger.info(f"  {city_name} {year}: {len(year_measurements)} mediciones obtenidas")
                    
                    time.sleep(2) 
                
                time.sleep(5) 
                
        except Exception as e:
            logger.error(f"Error durante la descarga: {e}")
    
    if all_data:
        df = pd.DataFrame(all_data)
        save_historical_data(df)
        return df
    else:
        logger.error("No se obtuvieron datos históricos")
        return None

def find_city_locations(client, city_info, city_name):
    """
    Busca estaciones de monitoreo cerca de una ciudad usando SDK oficial
    """
    logger.info(f"  Buscando estaciones cerca de {city_name}")
    
    try:
        # coordenadas como tupla según documentación del SDK
        response = client.locations.list(
            coordinates=(city_info['lat'], city_info['lon']),
            radius=25000,  # 25km
            limit=50
        )
        
        if response.results:
            locations = response.results
            logger.info(f"    {len(locations)} estaciones encontradas")
            
            # Mostrar las primeras estaciones
            for i, loc in enumerate(locations[:3]):
                distance = getattr(loc, 'distance', 'N/A')
                logger.info(f"    {i+1}. {loc.name} ({distance}m)")
            
            return locations[:2] 
        else:
            logger.warning(f"    No hay estaciones cerca de {city_name}")
            return find_locations_by_country(client, city_info['country'], city_name)
            
    except Exception as e:
        logger.error(f"    Error buscando estaciones: {e}")
        return []

def find_locations_by_country(client, country_code, city_name):
    """
    Búsqueda alternativa por país usando SDK oficial
    """
    try:
        response = client.locations.list(
            country=country_code,
            limit=10
        )
        
        if response.results:
            locations = response.results
            logger.info(f"    Encontradas {len(locations)} estaciones en país {country_code}")
            
            # Filtrar por ciudad si es posible
            city_locations = []
            for loc in locations:
                loc_name = loc.name.lower()
                if city_name.lower() in loc_name or city_name.lower()[:4] in loc_name:
                    city_locations.append(loc)
            
            if city_locations:
                logger.info(f"    {len(city_locations)} estaciones filtradas para {city_name}")
                return city_locations[:1] 
            else:
                logger.info(f"    Usando estaciones generales del país")
                return locations[:1] 
        
        return []
        
    except Exception as e:
        logger.error(f"    Error búsqueda por país: {e}")
        return []

def download_city_measurements_by_year(client, locations, city_name, year):
    """
    Descarga mediciones históricas de un año específico
    """
    measurements = []
    target_parameters = ['pm25', 'pm10', 'no2', 'o3']
    
    for location in locations[:1]: 
        location_id = location.id
        location_name = location.name
        
        logger.info(f"    Descargando de: {location_name} (Año {year})")
        
        # Obtener sensors de esta location
        sensors = get_location_sensors(location, target_parameters)
        
        if not sensors:
            logger.warning(f"      No hay sensores válidos en esta estación")
            continue
        
        # Para cada sensor, descargar mediciones del año
        for sensor in sensors[:2]:  # Máximo 2 sensores
            sensor_data = download_sensor_measurements_year(client, sensor, city_name, year)
            if sensor_data:
                measurements.extend(sensor_data)
                logger.info(f"        {year} - Sensor {sensor['parameter']}: {len(sensor_data)} mediciones")
            
            time.sleep(1.5)  # Rate limiting entre sensores
        
        time.sleep(3)  # Rate limiting entre locations
    
    return measurements

def get_location_sensors(location, target_parameters):
    """
    Extrae sensores de una location que coincidan con parámetros objetivo
    """
    sensors = []
    
    try:
        if hasattr(location, 'sensors') and location.sensors:
            for sensor in location.sensors:
                if hasattr(sensor, 'parameter') and sensor.parameter:
                    param_name = sensor.parameter.name.lower()
                    if param_name in target_parameters:
                        sensors.append({
                            'id': sensor.id,
                            'parameter': param_name,
                            'units': sensor.parameter.units,
                            'location_name': location.name,
                            'location_id': location.id
                        })
        
        logger.info(f"        Sensores encontrados: {len(sensors)} de parámetros objetivo")
        for sensor in sensors:
            logger.info(f"          - {sensor['parameter']} (ID: {sensor['id']})")
        
        return sensors
        
    except Exception as e:
        logger.error(f"        Error extrayendo sensores: {e}")
        return []

def download_sensor_measurements_year(client, sensor, city_name, year):
    """
    Descarga mediciones de un sensor específico para un año completo
    """
    measurements = []
    
    try:
        # Descargar datos por trimestres
        quarters = [
            (f"{year}-01-01T00:00:00Z", f"{year}-03-31T23:59:59Z"),
            (f"{year}-04-01T00:00:00Z", f"{year}-06-30T23:59:59Z"),
            (f"{year}-07-01T00:00:00Z", f"{year}-09-30T23:59:59Z"),
            (f"{year}-10-01T00:00:00Z", f"{year}-12-31T23:59:59Z")
        ]
        
        for i, (date_from, date_to) in enumerate(quarters, 1):
            try:
                response = client.measurements.list(
                    sensors_id=sensor['id'],
                    datetime_from=date_from,
                    datetime_to=date_to,
                    limit=1000
                )
                
                if response.results:
                    data = response.dict()
                    
                    if data.get('results'):
                        quarter_count = len(data['results'])
                        logger.info(f"          Q{i}: {quarter_count} mediciones")
                        
                        for result in data['results']:
                            processed = process_sensor_measurement(result, sensor, city_name)
                            if processed:
                                measurements.append(processed)
                
                time.sleep(1)  # Pausa entre trimestres
                
            except Exception as e:
                logger.error(f"          Error Q{i} {year}: {e}")
                continue
                
    except Exception as e:
        logger.error(f"          Error descargando sensor {sensor['id']} año {year}: {e}")
    
    return measurements

def process_sensor_measurement(measurement_dict, sensor, city_name):
    """
    Procesa una medición de sensor específico
    """
    try:
        # Extraer fecha de la estructura correcta
        datetime_utc = None
        
        # La fecha está en period.datetime_from.utc
        if 'period' in measurement_dict:
            period = measurement_dict['period']
            if 'datetime_from' in period and 'utc' in period['datetime_from']:
                datetime_utc = period['datetime_from']['utc']
        
        # Backup: buscar en otros campos
        if not datetime_utc:
            for field in ['datetime', 'date', 'timestamp']:
                if field in measurement_dict:
                    datetime_utc = measurement_dict[field]
                    break
        
        value = measurement_dict.get('value')
        
        # Validar que value sea numérico
        if value is None:
            return None
        
        try:
            value = float(value)
        except (ValueError, TypeError):
            return None
        
        # Validar fecha
        if not datetime_utc:
            return None
        
        # Asegurar formato de fecha
        if isinstance(datetime_utc, str):
            date_part = datetime_utc.split('T')[0]
        else:
            date_part = str(datetime_utc).split('T')[0]
        
        result = {
            'date': date_part,
            'datetime': str(datetime_utc),
            'city': city_name,
            'parameter': sensor['parameter'],
            'value': value,
            'unit': sensor['units'],
            'location_name': sensor['location_name'],
            'location_id': sensor['location_id'],
            'sensor_id': sensor['id'],
            'source': 'openaq_sdk_v3'
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Error procesando medición de sensor {sensor['parameter']}: {e}")
        return None

def save_historical_data(df):
    """
    Procesa y guarda los datos históricos de múltiples años
    """
    logger.info("Procesando datos históricos de múltiples años")
    logger.info(f"Datos iniciales: {len(df)} registros")
    
    # Limpiar datos
    df = df.dropna(subset=['date', 'value'])
    logger.info(f"Después de eliminar nulos: {len(df)} registros")
    
    # Asegurar que value sea numérico
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['value'])
    logger.info(f"Después de validar valores numéricos: {len(df)} registros")
    
    # Filtrar valores válidos
    df = df[df['value'] > 0]
    df = df[df['value'] < 1000]
    logger.info(f"Después de filtrar rango: {len(df)} registros")
    
    # Convertir fechas
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    logger.info(f"Después de convertir fechas: {len(df)} registros")
    
    # Filtrar rango temporal válido (2020-2024)
    start_date = pd.to_datetime('2020-01-01')
    end_date = pd.to_datetime('2024-12-31')
    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
    logger.info(f"Después de filtrar fechas válidas: {len(df)} registros")
    
    # Promedios diarios por ciudad y parámetro
    daily_avg = df.groupby(['date', 'city', 'parameter']).agg({
        'value': 'mean',
        'unit': 'first'
    }).reset_index()
    
    daily_avg['value'] = daily_avg['value'].round(2)
    
    # Guardar datos
    os.makedirs('data/raw', exist_ok=True)
    output_path = 'data/raw/air_quality_data_2020_2024.csv'  # Nuevo nombre
    daily_avg.to_csv(output_path, index=False)
    
    print_data_report(daily_avg, output_path)
    return daily_avg

def print_data_report(df, output_path):
    """
    Genera reporte final de los datos descargados de múltiples años
    """
    print("\n" + "="*60)
    print("DATOS HISTÓRICOS REALES DESCARGADOS - MÚLTIPLES AÑOS")
    print("="*60)
    print(f"Fuente: OpenAQ API v3 (SDK Oficial)")
    print(f"Archivo: {output_path}")
    print(f"Total registros: {len(df):,}")
    
    if len(df) > 0:
        print(f"\nCiudades con datos ({df['city'].nunique()}):")
        for city in sorted(df['city'].unique()):
            city_data = df[df['city'] == city]
            count = len(city_data)
            date_range = f"{city_data['date'].min().date()} a {city_data['date'].max().date()}"
            years = sorted(city_data['date'].dt.year.unique())
            print(f"  - {city}: {count:,} registros ({date_range}) - Años: {years}")
        
        print(f"\nContaminantes disponibles:")
        for param in sorted(df['parameter'].unique()):
            param_data = df[df['parameter'] == param]
            count = len(param_data)
            cities = param_data['city'].nunique()
            years = sorted(param_data['date'].dt.year.unique())
            print(f"  - {param.upper()}: {count:,} registros ({cities} ciudades) - Años: {years}")
        
        print(f"\nCobertura temporal por año:")
        year_stats = df.groupby(df['date'].dt.year).agg({
            'date': 'count',
            'city': 'nunique',
            'parameter': 'nunique'
        }).rename(columns={'date': 'registros', 'city': 'ciudades', 'parameter': 'contaminantes'})
        print(year_stats)
        
        print(f"\nRango temporal completo:")
        print(f"  Desde: {df['date'].min().date()}")
        print(f"  Hasta: {df['date'].max().date()}")
        print(f"  Total días: {(df['date'].max() - df['date'].min()).days}")
        
        print(f"\nPromedios por contaminante (todo el periodo):")
        for param in sorted(df['parameter'].unique()):
            param_data = df[df['parameter'] == param]['value']
            print(f"  {param.upper()}: {param_data.mean():.1f} µg/m³ "
                  f"(rango: {param_data.min():.1f}-{param_data.max():.1f})")
    else:
        print("\nNo se obtuvieron datos válidos")

if __name__ == "__main__":
    try:
        df = download_openaq_historical_data()
        if df is not None:
            logger.info("Descarga de datos históricos completada")
        else:
            logger.error("No se pudieron descargar datos históricos")
    except KeyboardInterrupt:
        logger.info("Descarga cancelada")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")