#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VALIDATEURS DE DONNÉES - GÉNÉRATEUR MALAYSIA
Fichier: app/utils/validators.py

Fonctions de validation pour les requêtes, données et paramètres
du générateur de données énergétiques Malaysia.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Validateurs modulaires
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional
import re


logger = logging.getLogger(__name__)


def validate_generation_request(data: Dict[str, Any], config: Any) -> Dict[str, Any]:
    """
    Valide une requête de génération de données.
    
    Args:
        data: Données de la requête
        config: Configuration de l'application
        
    Returns:
        Résultat de validation avec erreurs éventuelles
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    # Validation du nombre de bâtiments
    num_buildings = data.get('num_buildings')
    if num_buildings is None:
        validation_result['errors'].append("num_buildings requis")
    elif not isinstance(num_buildings, int):
        validation_result['errors'].append("num_buildings doit être un entier")
    elif num_buildings <= 0:
        validation_result['errors'].append("num_buildings doit être positif")
    elif hasattr(config, 'MAX_BUILDINGS') and num_buildings > config.MAX_BUILDINGS:
        validation_result['errors'].append(f"num_buildings ne peut pas dépasser {config.MAX_BUILDINGS}")
    elif num_buildings > 10000:
        validation_result['warnings'].append("Nombre de bâtiments très élevé - génération longue")
    
    # Validation des dates
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    if start_date and end_date:
        date_validation = validate_date_range(start_date, end_date, config)
        if not date_validation['valid']:
            validation_result['errors'].extend(date_validation['errors'])
    
    validation_result['valid'] = len(validation_result['errors']) == 0
    return validation_result


def validate_date_range(start_date: str, end_date: str, config: Any) -> Dict[str, Any]:
    """
    Valide une plage de dates.
    
    Args:
        start_date: Date de début (format YYYY-MM-DD)
        end_date: Date de fin (format YYYY-MM-DD)
        config: Configuration de l'application
        
    Returns:
        Résultat de validation
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    # Validation du format des dates
    if not start_date:
        validation_result['errors'].append("start_date requis")
        return validation_result
    
    if not end_date:
        validation_result['errors'].append("end_date requis")
        return validation_result
    
    # Parser les dates
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    except ValueError:
        validation_result['errors'].append("start_date doit être au format YYYY-MM-DD")
        return validation_result
    
    try:
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        validation_result['errors'].append("end_date doit être au format YYYY-MM-DD")
        return validation_result
    
    # Validation de la logique des dates
    if start_dt >= end_dt:
        validation_result['errors'].append("end_date doit être après start_date")
    
    # Validation de la période
    period_days = (end_dt - start_dt).days
    
    if hasattr(config, 'MAX_PERIOD_DAYS') and period_days > config.MAX_PERIOD_DAYS:
        validation_result['errors'].append(f"Période maximum: {config.MAX_PERIOD_DAYS} jours")
    elif period_days > 365:
        validation_result['warnings'].append("Période très longue - dataset volumineux")
    elif period_days < 1:
        validation_result['warnings'].append("Période très courte - peu de données")
    
    # Validation que les dates ne sont pas trop dans le futur
    now = datetime.now()
    if start_dt > now + timedelta(days=365):
        validation_result['warnings'].append("Date de début très dans le futur")
    
    return validation_result


def validate_frequency(frequency: str, config: Any) -> Dict[str, Any]:
    """
    Valide une fréquence de données.
    
    Args:
        frequency: Fréquence (H, D, W, M, 30T, 15T, etc.)
        config: Configuration de l'application
        
    Returns:
        Résultat de validation
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    if not frequency:
        validation_result['errors'].append("frequency requis")
        return validation_result
    
    # Fréquences supportées
    if hasattr(config, 'SUPPORTED_FREQUENCIES'):
        supported = config.SUPPORTED_FREQUENCIES
    else:
        supported = ['H', 'D', 'W', 'M', '30T', '15T']
    
    if frequency not in supported:
        validation_result['errors'].append(f"Fréquence supportées: {supported}")
    
    # Avertissements pour des fréquences particulières
    if frequency in ['15T', '5T', '1T']:
        validation_result['warnings'].append("Fréquence très fine - dataset volumineux")
    elif frequency in ['M', 'Q', 'Y']:
        validation_result['warnings'].append("Fréquence très large - peu de points de données")
    
    return validation_result


def validate_coordinates(latitude: float, longitude: float) -> Dict[str, Any]:
    """
    Valide des coordonnées géographiques.
    
    Args:
        latitude: Latitude (-90 à 90)
        longitude: Longitude (-180 à 180)
        
    Returns:
        Résultat de validation
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    # Validation des types
    if not isinstance(latitude, (int, float)):
        validation_result['errors'].append("latitude doit être un nombre")
    elif not (-90 <= latitude <= 90):
        validation_result['errors'].append("latitude doit être entre -90 et 90")
    
    if not isinstance(longitude, (int, float)):
        validation_result['errors'].append("longitude doit être un nombre")
    elif not (-180 <= longitude <= 180):
        validation_result['errors'].append("longitude doit être entre -180 et 180")
    
    # Validation spécifique Malaysia
    if validation_result['valid']:
        if not (0.5 <= latitude <= 7.5):
            validation_result['warnings'].append("Latitude hors Malaysia (0.5° à 7.5°)")
        
        if not (99.0 <= longitude <= 120.0):
            validation_result['warnings'].append("Longitude hors Malaysia (99° à 120°)")
    
    validation_result['valid'] = len(validation_result['errors']) == 0
    return validation_result


def validate_coordinates_dict(coordinates: Dict) -> Dict[str, Any]:
    """
    Valide un dictionnaire de coordonnées.
    
    Args:
        coordinates: Dictionnaire avec 'lat' et 'lon'
        
    Returns:
        Résultat de validation
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    if not isinstance(coordinates, dict):
        validation_result['errors'].append("coordinates doit être un objet")
        return validation_result
    
    # Vérifier les clés requises
    if 'lat' not in coordinates:
        validation_result['errors'].append("coordinates.lat requis")
    
    if 'lon' not in coordinates:
        validation_result['errors'].append("coordinates.lon requis")
    
    if validation_result['errors']:
        return validation_result
    
    # Valider les coordonnées
    coords_validation = validate_coordinates(coordinates['lat'], coordinates['lon'])
    validation_result['errors'].extend(coords_validation['errors'])
    validation_result['warnings'].extend(coords_validation['warnings'])
    
    validation_result['valid'] = len(validation_result['errors']) == 0
    return validation_result


def validate_bbox(bbox: List[float]) -> Dict[str, Any]:
    """
    Valide une bounding box.
    
    Args:
        bbox: Liste [sud, ouest, nord, est]
        
    Returns:
        Résultat de validation
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    if not isinstance(bbox, list):
        validation_result['errors'].append("bbox doit être une liste")
        return validation_result
    
    if len(bbox) != 4:
        validation_result['errors'].append("bbox doit contenir 4 éléments [sud, ouest, nord, est]")
        return validation_result
    
    try:
        south, west, north, east = [float(x) for x in bbox]
    except (ValueError, TypeError):
        validation_result['errors'].append("bbox doit contenir des nombres")
        return validation_result
    
    # Validation des coordonnées individuelles
    for coord in [south, north]:
        if not (-90 <= coord <= 90):
            validation_result['errors'].append("Latitudes (sud/nord) doivent être entre -90 et 90")
            break
    
    for coord in [west, east]:
        if not (-180 <= coord <= 180):
            validation_result['errors'].append("Longitudes (ouest/est) doivent être entre -180 et 180")
            break
    
    # Validation de la logique de la bbox
    if south >= north:
        validation_result['errors'].append("Sud doit être inférieur au nord")
    
    if west >= east:
        validation_result['errors'].append("Ouest doit être inférieur à l'est")
    
    # Vérification de la taille de la bbox
    if validation_result['valid']:
        lat_span = north - south
        lon_span = east - west
        
        if lat_span > 10 or lon_span > 10:
            validation_result['warnings'].append("Bbox très large - beaucoup de bâtiments possibles")
        elif lat_span < 0.01 or lon_span < 0.01:
            validation_result['warnings'].append("Bbox très petite - peu de bâtiments possibles")
        
        # Vérification spécifique Malaysia
        malaysia_south, malaysia_north = 0.5, 7.5
        malaysia_west, malaysia_east = 99.0, 120.0
        
        if not (malaysia_south <= south <= malaysia_north and 
                malaysia_south <= north <= malaysia_north):
            validation_result['warnings'].append("Bbox partiellement hors Malaysia (latitudes)")
        
        if not (malaysia_west <= west <= malaysia_east and 
                malaysia_west <= east <= malaysia_east):
            validation_result['warnings'].append("Bbox partiellement hors Malaysia (longitudes)")
    
    validation_result['valid'] = len(validation_result['errors']) == 0
    return validation_result


def validate_city_name(city: str) -> Dict[str, Any]:
    """
    Valide un nom de ville.
    
    Args:
        city: Nom de la ville
        
    Returns:
        Résultat de validation
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    if not city:
        validation_result['errors'].append("Nom de ville requis")
        return validation_result
    
    if not isinstance(city, str):
        validation_result['errors'].append("Nom de ville doit être une chaîne")
        return validation_result
    
    # Validation de base du format
    if len(city.strip()) < 2:
        validation_result['errors'].append("Nom de ville trop court")
    elif len(city) > 100:
        validation_result['errors'].append("Nom de ville trop long")
    
    # Validation des caractères (lettres, espaces, tirets, apostrophes)
    if not re.match(r"^[a-zA-Z\s\-']+$", city):
        validation_result['errors'].append("Nom de ville contient des caractères invalides")
    
    # Villes malaysiennes connues
    known_malaysia_cities = [
        'Kuala Lumpur', 'George Town', 'Ipoh', 'Shah Alam', 'Petaling Jaya',
        'Johor Bahru', 'Kota Kinabalu', 'Kuching', 'Malacca', 'Alor Setar',
        'Seremban', 'Kuantan', 'Kuala Terengganu', 'Kota Bharu', 'Kangar',
        'Sandakan', 'Tawau', 'Miri', 'Sibu', 'Putrajaya', 'Labuan'
    ]
    
    if city not in known_malaysia_cities:
        validation_result['warnings'].append("Ville non reconnue dans la base Malaysia")
    
    validation_result['valid'] = len(validation_result['errors']) == 0
    return validation_result


def validate_building_types(building_types: List[str]) -> Dict[str, Any]:
    """
    Valide une liste de types de bâtiments.
    
    Args:
        building_types: Liste des types de bâtiments
        
    Returns:
        Résultat de validation
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    if not isinstance(building_types, list):
        validation_result['errors'].append("building_types doit être une liste")
        return validation_result
    
    if not building_types:
        validation_result['warnings'].append("Liste de types de bâtiments vide")
        return validation_result
    
    # Types supportés
    supported_types = ['residential', 'commercial', 'industrial', 'public']
    
    for building_type in building_types:
        if not isinstance(building_type, str):
            validation_result['errors'].append("Types de bâtiments doivent être des chaînes")
        elif building_type not in supported_types:
            validation_result['errors'].append(f"Type '{building_type}' non supporté. Types supportés: {supported_types}")
    
    # Vérifier les doublons
    if len(building_types) != len(set(building_types)):
        validation_result['warnings'].append("Types de bâtiments dupliqués détectés")
    
    validation_result['valid'] = len(validation_result['errors']) == 0
    return validation_result


def validate_location_filter(location_filter: Dict) -> Dict[str, Any]:
    """
    Valide un filtre de localisation.
    
    Args:
        location_filter: Filtre de localisation
        
    Returns:
        Résultat de validation
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    if not isinstance(location_filter, dict):
        validation_result['errors'].append("location_filter doit être un objet")
        return validation_result
    
    # Clés valides pour le filtre
    valid_keys = ['state', 'region', 'type', 'urban_level', 'min_population', 'city']
    
    for key in location_filter.keys():
        if key not in valid_keys:
            validation_result['warnings'].append(f"Clé de filtre '{key}' non reconnue. Clés valides: {valid_keys}")
    
    # Validation des valeurs spécifiques
    if 'state' in location_filter:
        state = location_filter['state']
        malaysia_states = [
            'Johor', 'Kedah', 'Kelantan', 'Malacca', 'Negeri Sembilan', 'Pahang',
            'Penang', 'Perak', 'Perlis', 'Selangor', 'Terengganu', 'Sabah', 'Sarawak',
            'Federal Territory'
        ]
        if state not in malaysia_states:
            validation_result['warnings'].append(f"État '{state}' non reconnu en Malaysia")
    
    if 'min_population' in location_filter:
        min_pop = location_filter['min_population']
        if not isinstance(min_pop, int) or min_pop < 0:
            validation_result['errors'].append("min_population doit être un entier positif")
        elif min_pop > 5000000:
            validation_result['warnings'].append("min_population très élevé - peu de villes correspondantes")
    
    if 'urban_level' in location_filter:
        urban_level = location_filter['urban_level']
        valid_levels = ['metropolitan', 'urban', 'small_town', 'rural']
        if urban_level not in valid_levels:
            validation_result['errors'].append(f"urban_level doit être dans {valid_levels}")
    
    validation_result['valid'] = len(validation_result['errors']) == 0
    return validation_result


def validate_export_format(export_format: str, config: Any) -> Dict[str, Any]:
    """
    Valide un format d'export.
    
    Args:
        export_format: Format d'export demandé
        config: Configuration de l'application
        
    Returns:
        Résultat de validation
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    if not export_format:
        validation_result['errors'].append("export_format requis")
        return validation_result
    
    # Formats supportés
    if hasattr(config, 'SUPPORTED_EXPORT_FORMATS'):
        supported = config.SUPPORTED_EXPORT_FORMATS
    else:
        supported = ['parquet', 'csv', 'json', 'excel']
    
    if export_format not in supported:
        validation_result['errors'].append(f"Formats supportés: {supported}")
    
    # Avertissements pour certains formats
    if export_format == 'json':
        validation_result['warnings'].append("Format JSON peut être volumineux pour de gros datasets")
    elif export_format == 'excel':
        validation_result['warnings'].append("Format Excel limité à 1M lignes par feuille")
    
    validation_result['valid'] = len(validation_result['errors']) == 0
    return validation_result


def validate_osm_data(osm_data: Dict) -> Dict[str, Any]:
    """
    Valide des données OSM retournées par l'API Overpass.
    
    Args:
        osm_data: Données brutes OSM
        
    Returns:
        Résultat de validation
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    if not isinstance(osm_data, dict):
        validation_result['errors'].append("Données OSM doivent être un objet JSON")
        return validation_result
    
    # Vérifier la structure de base
    if 'elements' not in osm_data:
        validation_result['errors'].append("Clé 'elements' manquante dans les données OSM")
        return validation_result
    
    elements = osm_data['elements']
    if not isinstance(elements, list):
        validation_result['errors'].append("'elements' doit être une liste")
        return validation_result
    
    # Validation des éléments individuels
    valid_elements = 0
    for i, element in enumerate(elements):
        if not isinstance(element, dict):
            validation_result['warnings'].append(f"Élément {i} invalide (pas un objet)")
            continue
        
        # Vérifier les champs requis
        if 'id' not in element:
            validation_result['warnings'].append(f"Élément {i} sans ID")
            continue
        
        if 'type' not in element:
            validation_result['warnings'].append(f"Élément {i} sans type")
            continue
        
        # Vérifier la géométrie pour les ways et relations
        if element['type'] in ['way', 'relation']:
            if 'geometry' not in element or not element['geometry']:
                validation_result['warnings'].append(f"Élément {i} sans géométrie")
                continue
        
        valid_elements += 1
    
    if valid_elements == 0 and len(elements) > 0:
        validation_result['errors'].append("Aucun élément OSM valide trouvé")
    elif valid_elements < len(elements) * 0.5:
        validation_result['warnings'].append("Moins de 50% des éléments OSM sont valides")
    
    validation_result['valid'] = len(validation_result['errors']) == 0
    return validation_result


def validate_timeseries_data(timeseries_data: List[Dict]) -> Dict[str, Any]:
    """
    Valide des données de séries temporelles.
    
    Args:
        timeseries_data: Liste des observations
        
    Returns:
        Résultat de validation
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'statistics': {}
    }
    
    if not isinstance(timeseries_data, list):
        validation_result['errors'].append("Données de séries temporelles doivent être une liste")
        return validation_result
    
    if not timeseries_data:
        validation_result['warnings'].append("Liste de séries temporelles vide")
        return validation_result
    
    # Validation des observations individuelles
    valid_observations = 0
    consumption_values = []
    timestamps = []
    
    for i, observation in enumerate(timeseries_data):
        if not isinstance(observation, dict):
            validation_result['warnings'].append(f"Observation {i} invalide (pas un objet)")
            continue
        
        # Vérifier les champs requis
        required_fields = ['unique_id', 'timestamp', 'y']
        missing_fields = [field for field in required_fields if field not in observation]
        
        if missing_fields:
            validation_result['warnings'].append(f"Observation {i} - champs manquants: {missing_fields}")
            continue
        
        # Valider la consommation
        try:
            consumption = float(observation['y'])
            if consumption < 0:
                validation_result['warnings'].append(f"Observation {i} - consommation négative")
            elif consumption > 10000:
                validation_result['warnings'].append(f"Observation {i} - consommation très élevée")
            else:
                consumption_values.append(consumption)
        except (ValueError, TypeError):
            validation_result['warnings'].append(f"Observation {i} - consommation invalide")
            continue
        
        # Valider le timestamp
        try:
            if isinstance(observation['timestamp'], str):
                timestamp = datetime.fromisoformat(observation['timestamp'].replace('Z', '+00:00'))
            else:
                timestamp = observation['timestamp']
            timestamps.append(timestamp)
        except (ValueError, TypeError):
            validation_result['warnings'].append(f"Observation {i} - timestamp invalide")
            continue
        
        valid_observations += 1
    
    # Statistiques de validation
    total_observations = len(timeseries_data)
    validation_rate = (valid_observations / total_observations * 100) if total_observations > 0 else 0
    
    validation_result['statistics'] = {
        'total_observations': total_observations,
        'valid_observations': valid_observations,
        'validation_rate': round(validation_rate, 1)
    }
    
    if consumption_values:
        validation_result['statistics']['consumption_stats'] = {
            'mean': round(sum(consumption_values) / len(consumption_values), 2),
            'min': round(min(consumption_values), 2),
            'max': round(max(consumption_values), 2)
        }
    
    if timestamps:
        validation_result['statistics']['time_range'] = {
            'start': min(timestamps).isoformat(),
            'end': max(timestamps).isoformat(),
            'span_days': (max(timestamps) - min(timestamps)).days
        }
    
    # Déterminer la validité globale
    if validation_rate < 50:
        validation_result['errors'].append("Moins de 50% des observations sont valides")
    elif validation_rate < 80:
        validation_result['warnings'].append("Moins de 80% des observations sont valides")
    
    validation_result['valid'] = len(validation_result['errors']) == 0
    return validation_result


# Export des fonctions principales
__all__ = [
    'validate_generation_request',
    'validate_osm_request', 
    'validate_date_range',
    'validate_frequency',
    'validate_coordinates',
    'validate_coordinates_dict',
    'validate_bbox',
    'validate_city_name',
    'validate_building_types',
    'validate_location_filter',
    'validate_export_format',
    'validate_osm_data',
    'validate_timeseries_data'
]
    
    date_validation = validate_date_range(start_date, end_date, config)
    if not date_validation['valid']:
        validation_result['errors'].extend(date_validation['errors'])
        validation_result['warnings'].extend(date_validation['warnings'])
    
    # Validation de la fréquence
    frequency = data.get('frequency', 'D')
    freq_validation = validate_frequency(frequency, config)
    if not freq_validation['valid']:
        validation_result['errors'].extend(freq_validation['errors'])
    
    # Validation du filtre de localisation
    location_filter = data.get('location_filter')
    if location_filter:
        filter_validation = validate_location_filter(location_filter)
        if not filter_validation['valid']:
            validation_result['errors'].extend(filter_validation['errors'])
    
    # Validation des types de bâtiments
    building_types = data.get('building_types')
    if building_types:
        types_validation = validate_building_types(building_types)
        if not types_validation['valid']:
            validation_result['errors'].extend(types_validation['errors'])
    
    # Validation du format d'export
    export_format = data.get('export_format', 'parquet')
    format_validation = validate_export_format(export_format, config)
    if not format_validation['valid']:
        validation_result['errors'].extend(format_validation['errors'])
    
    # Déterminer la validité globale
    validation_result['valid'] = len(validation_result['errors']) == 0
    
    return validation_result


def validate_osm_request(data: Dict[str, Any], config: Any) -> Dict[str, Any]:
    """
    Valide une requête de génération basée sur OSM.
    
    Args:
        data: Données de la requête OSM
        config: Configuration de l'application
        
    Returns:
        Résultat de validation
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    # Au moins un paramètre de localisation requis
    has_city = 'city' in data and data['city']
    has_bbox = 'bbox' in data and data['bbox']
    has_coordinates = 'coordinates' in data and data['coordinates']
    
    if not (has_city or has_bbox or has_coordinates):
        validation_result['errors'].append("Paramètre de localisation requis (city, bbox, ou coordinates)")
    
    # Validation de la ville
    if has_city:
        city_validation = validate_city_name(data['city'])
        if not city_validation['valid']:
            validation_result['errors'].extend(city_validation['errors'])
    
    # Validation de la bbox
    if has_bbox:
        bbox_validation = validate_bbox(data['bbox'])
        if not bbox_validation['valid']:
            validation_result['errors'].extend(bbox_validation['errors'])
    
    # Validation des coordonnées
    if has_coordinates:
        coords_validation = validate_coordinates_dict(data['coordinates'])
        if not coords_validation['valid']:
            validation_result['errors'].extend(coords_validation['errors'])
    
    # Validation de la limite
    limit = data.get('limit')
    if limit is not None:
        if not isinstance(limit, int) or limit <= 0:
            validation_result['errors'].append("limit doit être un entier positif")
        elif limit > 10000:
            validation_result['warnings'].append("Limite très élevée - requête OSM longue")
    
    # Validation du rayon (pour coordinates)
    radius = data.get('radius')
    if radius is not None:
        if not isinstance(radius, (int, float)) or radius <= 0:
            validation_result['errors'].append("radius doit être un nombre positif")
        elif radius > 50000:  # 50km
            validation_result['warnings'].append("Rayon très large - beaucoup de bâtiments possibles")
    
    # Validation des types de bâtiments
    building_types = data.get('building_types')
    if building_types:
        types_validation = validate_building_types(building_types)
        if not types_validation['valid']:
            validation_result['errors'].extend(types_validation['errors'])
    
    # Validation des dates (optionnelles pour OSM)
    start_date = data.get('start_date')
    end_date = data.get('end_date