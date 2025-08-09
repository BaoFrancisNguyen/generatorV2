#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROUTES DE GÉNÉRATION OSM AUTOMATIQUE - GÉNÉRATEUR MALAYSIA
Fichier: app/routes/osm_generation.py

Nouvelles routes pour la génération automatique basée sur les bâtiments réels OSM.
Le nombre de bâtiments est déterminé automatiquement par OSM, pas par l'utilisateur.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Génération OSM automatique
"""

import logging
from datetime import datetime
from flask import Blueprint, request, jsonify, current_app
from werkzeug.exceptions import BadRequest

# Créer le blueprint pour les routes de génération OSM
osm_generation_bp = Blueprint('osm_generation', __name__)
logger = logging.getLogger(__name__)


@osm_generation_bp.route('/preview', methods=['POST'])
def preview_osm_buildings():
    """
    Aperçu des bâtiments disponibles dans une zone via OSM.
    Ne génère pas encore les données, juste la liste des bâtiments.
    
    Body JSON:
    {
        "zone_type": "country|state|city|custom",
        "state": "Selangor",  // si zone_type=state
        "city": "Kuala Lumpur",  // si zone_type=city
        "bbox": [south, west, north, east],  // si zone_type=custom
        "building_types": ["residential", "commercial", "industrial", "public"]
    }
    
    Returns:
        JSON avec la liste des bâtiments et statistiques
    """
    logger.info("🔍 Aperçu des bâtiments OSM demandé")
    
    try:
        if not request.is_json:
            raise BadRequest("Content-Type application/json requis")
        
        data = request.get_json()
        if not data:
            raise BadRequest("Corps de requête JSON vide")
        
        zone_type = data.get('zone_type')
        building_types = data.get('building_types', ['residential', 'commercial', 'industrial', 'public'])
        
        if not zone_type:
            raise BadRequest("zone_type requis")
        
        # Construire la requête OSM selon le type de zone
        osm_query_params = _build_osm_query_params(zone_type, data, building_types)
        
        # Récupérer les bâtiments via le service OSM
        start_time = datetime.now()
        buildings = _fetch_buildings_from_osm(osm_query_params)
        fetch_time = (datetime.now() - start_time).total_seconds()
        
        # Calculer les statistiques
        stats = _calculate_building_statistics(buildings)
        
        # Préparer la réponse
        response = {
            'success': True,
            'zone_info': {
                'zone_type': zone_type,
                'building_types_requested': building_types,
                'fetch_time_seconds': round(fetch_time, 2)
            },
            'buildings_found': len(buildings),
            'statistics': stats,
            'buildings_sample': buildings[:50] if len(buildings) > 50 else buildings,  # Échantillon pour la carte
            'ready_for_generation': len(buildings) > 0,
            'estimated_generation_time': _estimate_generation_time(len(buildings)),
            'warnings': _get_preview_warnings(len(buildings), zone_type)
        }
        
        # Sauvegarder les bâtiments en session pour la génération
        # Dans une vraie implémentation, utiliser Redis ou base de données
        current_app._last_buildings_preview = {
            'buildings': buildings,
            'timestamp': datetime.now(),
            'zone_type': zone_type
}
        
        logger.info(f"✅ Aperçu OSM terminé: {len(buildings)} bâtiments trouvés en {fetch_time:.2f}s")
        
        return jsonify(response)
        
    except BadRequest as e:
        logger.warning(f"⚠️ Requête d'aperçu invalide: {e}")
        return jsonify({
            'success': False,
            'error': 'Requête invalide',
            'details': str(e)
        }), 400
    
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'aperçu OSM: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la récupération des bâtiments',
            'details': str(e)
        }), 500


@osm_generation_bp.route('/generate', methods=['POST'])
def generate_from_osm_preview():
    """
    Génère les données énergétiques basées sur l'aperçu OSM précédent.
    Utilise les bâtiments récupérés lors de l'aperçu.
    
    Body JSON:
    {
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "frequency": "D",
        "export_format": "parquet",
        "include_weather": true,
        "include_metadata": true
    }
    
    Returns:
        JSON avec les données générées ou les fichiers d'export
    """
    logger.info("🏗️ Génération basée sur aperçu OSM")
    
    try:
        if not request.is_json:
            raise BadRequest("Content-Type application/json requis")
        
        data = request.get_json()
        if not data:
            raise BadRequest("Corps de requête JSON vide")
        
        # Vérifier qu'un aperçu a été fait
        if not hasattr(current_app, '_last_buildings_preview') or not current_app._last_buildings_preview:
            raise BadRequest("Aucun aperçu disponible. Effectuez d'abord un aperçu des bâtiments.")
        
        preview_data = current_app._last_buildings_preview
        buildings_list = preview_data['buildings']
        
        if not buildings_list:
            raise BadRequest("Aucun bâtiment disponible pour la génération")
        
        # Vérifier que l'aperçu n'est pas trop ancien (30 minutes max)
        age_minutes = (datetime.now() - preview_data['timestamp']).total_seconds() / 60
        if age_minutes > 30:
            raise BadRequest("Aperçu trop ancien. Veuillez refaire un aperçu.")
        
        # Paramètres de génération
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        frequency = data.get('frequency', 'D')
        export_format = data.get('export_format', 'json')
        include_weather = data.get('include_weather', False)
        include_metadata = data.get('include_metadata', True)
        
        # Validation des paramètres
        _validate_generation_params(start_date, end_date, frequency)
        
        logger.info(f"📊 Génération pour {len(buildings_list)} bâtiments du {start_date} au {end_date}")
        
        # Convertir les bâtiments OSM en format générateur
        start_time = datetime.now()
        buildings_df = _convert_osm_buildings_to_dataframe(buildings_list)
        
        # Générer les séries temporelles
        timeseries_df = current_app.data_generator.generate_timeseries_data(
            buildings_df=buildings_df,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency
        )
        
        generation_time = (datetime.now() - start_time).total_seconds()
        
        # Ajouter le contexte météorologique si demandé
        if include_weather:
            timeseries_df = _add_weather_context(timeseries_df, buildings_df)
        
        # Créer le dataset complet
        dataset = {
            'buildings': buildings_df,
            'timeseries': timeseries_df,
            'metadata': _generate_osm_metadata(
                buildings_df, timeseries_df, start_date, end_date, 
                frequency, preview_data, generation_time
            )
        }
        
        # Validation des données générées
        validation_results = current_app.validation_service.validate_complete_dataset(
            buildings_df, timeseries_df
        )
        
        # Préparer la réponse selon le format
        if export_format == 'json' and data.get('return_data', False):
            # Retour direct des données
            response = {
                'success': True,
                'generation_info': {
                    'total_buildings': len(buildings_df),
                    'total_observations': len(timeseries_df),
                    'generation_time_seconds': round(generation_time, 2),
                    'osm_source': True,
                    'zone_type': preview_data['zone_type']
                },
                'validation': validation_results,
                'data': {
                    'buildings': buildings_df.to_dict('records'),
                    'timeseries': timeseries_df.head(1000).to_dict('records')  # Limiter pour éviter surcharge
                },
                'metadata': dataset['metadata']
            }
        else:
            # Export vers fichiers
            export_result = current_app.export_service.export_dataset(
                dataset, 
                export_format,
                include_metadata=include_metadata,
                filename_prefix='malaysia_osm'
            )
            
            response = {
                'success': True,
                'generation_info': {
                    'total_buildings': len(buildings_df),
                    'total_observations': len(timeseries_df),
                    'generation_time_seconds': round(generation_time, 2),
                    'osm_source': True,
                    'zone_type': preview_data['zone_type']
                },
                'validation': validation_results,
                'export': {
                    'format': export_format,
                    'files': export_result['files'],
                    'total_size_bytes': export_result['total_size']
                },
                'metadata': dataset['metadata']
            }
        
        # Nettoyer l'aperçu après utilisation
        current_app._last_buildings_preview = None
        
        logger.info(f"✅ Génération OSM terminée: {len(timeseries_df)} observations en {generation_time:.2f}s")
        
        return jsonify(response)
        
    except BadRequest as e:
        logger.warning(f"⚠️ Requête de génération invalide: {e}")
        return jsonify({
            'success': False,
            'error': 'Requête invalide',
            'details': str(e)
        }), 400
    
    except Exception as e:
        logger.error(f"❌ Erreur lors de la génération OSM: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la génération',
            'details': str(e)
        }), 500


@osm_generation_bp.route('/zones/available', methods=['GET'])
def get_available_zones():
    """
    Retourne les zones géographiques disponibles pour la génération OSM.
    
    Returns:
        JSON avec les pays, états, et villes supportés
    """
    logger.info("🗺️ Demande des zones disponibles")
    
    try:
        zones = {
            'success': True,
            'zones': {
                'countries': ['Malaysia'],
                'states': [
                    'Johor', 'Kedah', 'Kelantan', 'Malacca', 'Negeri Sembilan',
                    'Pahang', 'Penang', 'Perak', 'Perlis', 'Selangor',
                    'Terengganu', 'Sabah', 'Sarawak', 'Federal Territory'
                ],
                'major_cities': [
                    'Kuala Lumpur', 'George Town', 'Ipoh', 'Shah Alam',
                    'Petaling Jaya', 'Johor Bahru', 'Kota Kinabalu',
                    'Kuching', 'Malacca', 'Alor Setar', 'Seremban',
                    'Kuantan', 'Kuala Terengganu', 'Kota Bharu'
                ]
            },
            'building_types': ['residential', 'commercial', 'industrial', 'public'],
            'supported_frequencies': ['H', 'D', 'W'],
            'export_formats': ['json', 'csv', 'parquet', 'excel']
        }
        
        return jsonify(zones)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des zones: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la récupération des zones',
            'details': str(e)
        }), 500


# === FONCTIONS UTILITAIRES ===

def _build_osm_query_params(zone_type, data, building_types):
    """
    Construit les paramètres de requête OSM selon le type de zone.
    """
    params = {
        'building_types': building_types
    }
    
    if zone_type == 'country':
        # Toute la Malaysia - grande bbox
        params['bbox'] = [0.5, 99.0, 7.5, 120.0]
        
    elif zone_type == 'state':
        state = data.get('state')
        if not state:
            raise BadRequest("État requis pour zone_type=state")
        params['state'] = state
        
    elif zone_type == 'city':
        city = data.get('city')
        if not city:
            raise BadRequest("Ville requise pour zone_type=city")
        params['city'] = city
        
    elif zone_type == 'custom':
        bbox = data.get('bbox')
        if not bbox or len(bbox) != 4:
            raise BadRequest("Bbox requise pour zone_type=custom [south, west, north, east]")
        params['bbox'] = bbox
    
    else:
        raise BadRequest(f"zone_type '{zone_type}' non supporté")
    
    return params


def _fetch_buildings_from_osm(params):
    """
    Récupère les bâtiments depuis OSM selon les paramètres.
    """
    osm_service = current_app.osm_service
    
    if 'city' in params:
        buildings = osm_service.get_buildings_for_city(
            city_name=params['city'],
            limit=None  # Pas de limite, récupérer tous les bâtiments
        )
    elif 'bbox' in params:
        buildings = osm_service.get_buildings_in_bbox(
            bbox=params['bbox'],
            building_types=params.get('building_types')
        )
    elif 'state' in params:
        # Pour l'état, utiliser une bbox approximative
        state_bbox = _get_state_bbox(params['state'])
        buildings = osm_service.get_buildings_in_bbox(
            bbox=state_bbox,
            building_types=params.get('building_types')
        )
    else:
        raise ValueError("Paramètres OSM insuffisants")
    
    return buildings


def _get_state_bbox(state_name):
    """
    Retourne la bbox approximative d'un état malaysien.
    """
    state_bboxes = {
        'Johor': [1.2, 102.5, 2.8, 104.8],
        'Kedah': [5.0, 99.6, 6.8, 101.0],
        'Kelantan': [4.5, 101.8, 6.3, 102.8],
        'Malacca': [2.0, 102.0, 2.5, 102.6],
        'Negeri Sembilan': [2.4, 101.8, 3.2, 102.8],
        'Pahang': [2.8, 101.8, 4.8, 104.0],
        'Penang': [5.2, 100.1, 5.6, 100.6],
        'Perak': [3.8, 100.5, 5.8, 101.8],
        'Perlis': [6.3, 100.0, 6.8, 100.6],
        'Selangor': [2.8, 101.0, 3.8, 102.0],
        'Terengganu': [4.0, 102.8, 6.0, 103.8],
        'Sabah': [4.0, 115.0, 7.5, 119.5],
        'Sarawak': [0.8, 109.5, 5.0, 115.5],
        'Federal Territory': [3.0, 101.5, 3.3, 101.8]
    }
    
    return state_bboxes.get(state_name, [0.5, 99.0, 7.5, 120.0])


def _calculate_building_statistics(buildings):
    """
    Calcule les statistiques des bâtiments récupérés.
    """
    stats = {
        'total': len(buildings),
        'by_type': {},
        'average_area': 0,
        'coordinates_range': {}
    }
    
    if not buildings:
        return stats
    
    # Statistiques par type
    for building in buildings:
        building_type = building.get('building_type', 'unknown')
        stats['by_type'][building_type] = stats['by_type'].get(building_type, 0) + 1
    
    # Surface moyenne
    areas = [b.get('area_sqm', 0) for b in buildings if b.get('area_sqm')]
    if areas:
        stats['average_area'] = round(sum(areas) / len(areas), 1)
    
    # Plage de coordonnées
    lats = [b['latitude'] for b in buildings]
    lons = [b['longitude'] for b in buildings]
    
    if lats and lons:
        stats['coordinates_range'] = {
            'lat_min': round(min(lats), 4),
            'lat_max': round(max(lats), 4),
            'lon_min': round(min(lons), 4),
            'lon_max': round(max(lons), 4)
        }
    
    return stats


def _estimate_generation_time(num_buildings):
    """
    Estime le temps de génération selon le nombre de bâtiments.
    """
    # Estimation basée sur des benchmarks
    # ~100 bâtiments par seconde pour des données quotidiennes
    base_time = 2  # 2 secondes de base
    building_time = num_buildings / 100  # 1 seconde pour 100 bâtiments
    
    total_seconds = base_time + building_time
    
    if total_seconds < 60:
        return f"{int(total_seconds)} secondes"
    else:
        minutes = int(total_seconds / 60)
        seconds = int(total_seconds % 60)
        return f"{minutes}m {seconds}s"


def _get_preview_warnings(num_buildings, zone_type):
    """
    Génère des avertissements basés sur le nombre de bâtiments.
    """
    warnings = []
    
    if num_buildings == 0:
        warnings.append("Aucun bâtiment trouvé dans cette zone")
    elif num_buildings > 10000:
        warnings.append("Très grand nombre de bâtiments - génération longue attendue")
    elif num_buildings > 5000:
        warnings.append("Grand nombre de bâtiments - temps de génération élevé")
    
    if zone_type == 'country' and num_buildings < 1000:
        warnings.append("Peu de bâtiments pour tout le pays - vérifiez les filtres")
    
    return warnings


def _validate_generation_params(start_date, end_date, frequency):
    """
    Valide les paramètres de génération.
    """
    if not start_date or not end_date:
        raise BadRequest("start_date et end_date requis")
    
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        raise BadRequest("Format de date invalide. Utilisez YYYY-MM-DD")
    
    if start_dt >= end_dt:
        raise BadRequest("end_date doit être après start_date")
    
    period_days = (end_dt - start_dt).days
    if period_days > 365:
        raise BadRequest("Période maximum: 365 jours")
    
    valid_frequencies = ['H', 'D', 'W']
    if frequency not in valid_frequencies:
        raise BadRequest(f"Fréquence doit être dans {valid_frequencies}")


def _convert_osm_buildings_to_dataframe(osm_buildings):
    """
    Convertit les bâtiments OSM en DataFrame compatible avec le générateur.
    """
    import pandas as pd
    import uuid
    
    buildings_data = []
    
    for osm_building in osm_buildings:
        # Générer un ID unique pour le générateur
        unique_id = str(uuid.uuid4()).replace('-', '')[:16]
        
        building_data = {
            'unique_id': unique_id,
            'building_id': f"OSM_{osm_building.get('osm_id', unique_id)}",
            'latitude': osm_building['latitude'],
            'longitude': osm_building['longitude'],
            'location': osm_building.get('city', 'Malaysia'),
            'state': 'Unknown',  # Pourrait être déterminé par géocodage
            'region': 'Unknown',
            'building_class': osm_building['building_type'],
            'osm_source': True,
            'osm_id': osm_building.get('osm_id'),
            'area_sqm': osm_building.get('area_sqm'),
            'levels': osm_building.get('levels'),
            'height': osm_building.get('height'),
            'characteristics': {
                'osm_tags': osm_building.get('tags', {}),
                'osm_type': osm_building.get('osm_type', 'way')
            }
        }
        
        buildings_data.append(building_data)
    
    return pd.DataFrame(buildings_data)


def _add_weather_context(timeseries_df, buildings_df):
    """
    Ajoute un contexte météorologique aux séries temporelles.
    """
    # Implementation simplifiée - dans la vraie version,
    # intégrer avec une API météo réelle
    
    import numpy as np
    import pandas as pd
    
    def generate_weather_context(timestamp, location):
        """Génère un contexte météo approximatif pour Malaysia"""
        month = timestamp.month
        hour = timestamp.hour
        
        # Température de base selon le mois
        base_temps = {
            1: 26, 2: 27, 3: 29, 4: 30, 5: 30, 6: 29,
            7: 28, 8: 28, 9: 29, 10: 29, 11: 28, 12: 26
        }
        
        base_temp = base_temps[month]
        
        # Variation journalière
        if 6 <= hour <= 8:
            temp_variation = -1
        elif 14 <= hour <= 16:
            temp_variation = 3
        elif 22 <= hour or hour <= 5:
            temp_variation = -3
        else:
            temp_variation = 0
        
        temperature = base_temp + temp_variation + np.random.normal(0, 1)
        humidity = max(60, min(95, 80 + np.random.normal(0, 5)))
        
        return {
            'temperature': round(temperature, 1),
            'humidity': round(humidity, 1),
            'is_raining': np.random.random() < 0.3  # 30% chance
        }
    
    # Ajouter le contexte météo à chaque observation
    weather_contexts = []
    
    for _, row in timeseries_df.iterrows():
        timestamp = pd.to_datetime(row['timestamp'])
        location = row['unique_id']  # Simplification
        
        weather = generate_weather_context(timestamp, location)
        weather_contexts.append(weather)
    
    timeseries_df['weather_context'] = weather_contexts
    
    return timeseries_df


def _generate_osm_metadata(buildings_df, timeseries_df, start_date, end_date, 
                          frequency, preview_data, generation_time):
    """
    Génère les métadonnées spécifiques à la génération OSM.
    """
    return {
        'dataset_name': 'malaysia_osm_electricity_v3',
        'generation_timestamp': datetime.now().isoformat(),
        'generator_version': '3.0',
        'data_source': 'OpenStreetMap',
        'osm_fetch_timestamp': preview_data['timestamp'].isoformat(),
        'zone_type': preview_data['zone_type'],
        'total_buildings': len(buildings_df),
        'total_observations': len(timeseries_df),
        'generation_time_seconds': round(generation_time, 2),
        'period': {
            'start_date': start_date,
            'end_date': end_date,
            'frequency': frequency,
            'total_days': (datetime.strptime(end_date, '%Y-%m-%d') - 
                          datetime.strptime(start_date, '%Y-%m-%d')).days + 1
        },
        'geographic_coverage': {
            'country': 'Malaysia',
            'osm_buildings_used': True,
            'coordinates_range': {
                'lat_min': buildings_df['latitude'].min(),
                'lat_max': buildings_df['latitude'].max(),
                'lon_min': buildings_df['longitude'].min(),
                'lon_max': buildings_df['longitude'].max()
            }
        },
        'building_distribution': buildings_df['building_class'].value_counts().to_dict(),
        'data_quality': {
            'osm_source': True,
            'real_coordinates': True,
            'synthetic_consumption': True
        }
    }


# Export du blueprint
__all__ = ['osm_generation_bp']