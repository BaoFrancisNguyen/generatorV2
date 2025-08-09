#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROUTES DE GÉNÉRATION - GÉNÉRATEUR MALAYSIA
Fichier: app/routes/generation.py

Routes pour la génération de données énergétiques:
- Génération standard avec paramètres personnalisés
- Génération basée sur OpenStreetMap
- Aperçu et validation des paramètres
- Export dans différents formats

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Routes modulaires
"""

import logging
from datetime import datetime
from flask import Blueprint, request, jsonify, current_app, send_file
from werkzeug.exceptions import BadRequest

from app.utils.validators import validate_generation_request, validate_osm_request

# Créer le blueprint pour les routes de génération
generation_bp = Blueprint('generation', __name__)
logger = logging.getLogger(__name__)


@generation_bp.route('/', methods=['POST'])
def generate_standard():
    """
    Génération standard de données énergétiques avec paramètres personnalisés.
    
    Body JSON attendu:
    {
        "num_buildings": 100,
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "frequency": "D",
        "location_filter": {...},
        "building_types": [...],
        "export_format": "parquet"
    }
    
    Returns:
        JSON avec les données générées ou le chemin du fichier d'export
    """
    logger.info("🏗️ Demande de génération standard")
    
    try:
        # Validation de la requête
        if not request.is_json:
            raise BadRequest("Content-Type application/json requis")
        
        data = request.get_json()
        if not data:
            raise BadRequest("Corps de requête JSON vide")
        
        # Valider les paramètres
        validation_result = validate_generation_request(data, current_app.config)
        if not validation_result['valid']:
            return jsonify({
                'success': False,
                'error': 'Paramètres invalides',
                'details': validation_result['errors']
            }), 400
        
        # Extraire les paramètres
        generation_params = {
            'num_buildings': data.get('num_buildings', current_app.config['DEFAULT_NUM_BUILDINGS']),
            'start_date': data.get('start_date', current_app.config['DEFAULT_START_DATE']),
            'end_date': data.get('end_date', current_app.config['DEFAULT_END_DATE']),
            'frequency': data.get('frequency', current_app.config['DEFAULT_FREQUENCY']),
            'location_filter': data.get('location_filter'),
            'building_types': data.get('building_types')
        }
        
        export_format = data.get('export_format', 'json')
        return_data = data.get('return_data', True)
        
        logger.info(f"📊 Génération de {generation_params['num_buildings']} bâtiments "
                   f"du {generation_params['start_date']} au {generation_params['end_date']}")
        
        # Génération des données
        start_time = datetime.now()
        dataset = current_app.data_generator.generate_complete_dataset(**generation_params)
        generation_time = (datetime.now() - start_time).total_seconds()
        
        # Validation des données générées
        validation_results = current_app.validation_service.validate_complete_dataset(
            dataset['buildings'], 
            dataset['timeseries']
        )
        
        # Préparer la réponse
        response_data = {
            'success': True,
            'generation_time_seconds': round(generation_time, 2),
            'metadata': dataset['metadata'],
            'validation': validation_results,
            'statistics': _calculate_dataset_statistics(dataset)
        }
        
        # Retourner les données ou exporter selon le format demandé
        if return_data and export_format == 'json':
            # Retourner directement les données en JSON
            response_data['data'] = {
                'buildings': dataset['buildings'].to_dict('records'),
                'timeseries': dataset['timeseries'].to_dict('records')
            }
            return jsonify(response_data)
        
        else:
            # Exporter vers un fichier
            export_result = current_app.export_service.export_dataset(
                dataset, 
                export_format,
                include_metadata=True
            )
            
            response_data['export'] = {
                'format': export_format,
                'files': export_result['files'],
                'total_size_bytes': export_result['total_size']
            }
            
            if data.get('download_immediately', False):
                # Téléchargement immédiat du premier fichier
                return send_file(
                    export_result['files'][0]['path'],
                    as_attachment=True,
                    download_name=export_result['files'][0]['filename']
                )
            
            return jsonify(response_data)
        
    except BadRequest as e:
        logger.warning(f"⚠️ Requête invalide: {e}")
        return jsonify({
            'success': False,
            'error': 'Requête invalide',
            'details': str(e)
        }), 400
    
    except Exception as e:
        logger.error(f"❌ Erreur lors de la génération standard: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la génération',
            'details': str(e)
        }), 500


@generation_bp.route('/osm', methods=['POST'])
def generate_from_osm():
    """
    Génération basée sur des bâtiments réels d'OpenStreetMap.
    
    Body JSON attendu:
    {
        "city": "Kuala Lumpur",
        "bbox": [south, west, north, east],  // Alternative à city
        "building_types": ["residential", "commercial"],
        "limit": 1000,
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "frequency": "D",
        "export_format": "parquet"
    }
    
    Returns:
        JSON avec les données générées basées sur OSM
    """
    logger.info("🗺️ Demande de génération basée sur OSM")
    
    try:
        # Validation de la requête
        if not request.is_json:
            raise BadRequest("Content-Type application/json requis")
        
        data = request.get_json()
        if not data:
            raise BadRequest("Corps de requête JSON vide")
        
        # Valider les paramètres OSM
        validation_result = validate_osm_request(data, current_app.config)
        if not validation_result['valid']:
            return jsonify({
                'success': False,
                'error': 'Paramètres OSM invalides',
                'details': validation_result['errors']
            }), 400
        
        # Récupérer les bâtiments OSM
        start_time = datetime.now()
        
        if 'city' in data:
            # Récupération par ville
            osm_buildings = current_app.osm_service.get_buildings_for_city(
                city_name=data['city'],
                limit=data.get('limit')
            )
        elif 'bbox' in data:
            # Récupération par bbox
            osm_buildings = current_app.osm_service.get_buildings_in_bbox(
                bbox=data['bbox'],
                building_types=data.get('building_types')
            )
        elif 'coordinates' in data:
            # Récupération autour d'un point
            coords = data['coordinates']
            osm_buildings = current_app.osm_service.get_buildings_around_point(
                lat=coords['lat'],
                lon=coords['lon'],
                radius=data.get('radius', 1000),
                building_types=data.get('building_types')
            )
        else:
            raise BadRequest("Paramètre de localisation requis (city, bbox, ou coordinates)")
        
        osm_fetch_time = (datetime.now() - start_time).total_seconds()
        
        if not osm_buildings:
            return jsonify({
                'success': False,
                'error': 'Aucun bâtiment trouvé',
                'details': 'Aucun bâtiment OSM trouvé pour les critères spécifiés'
            }), 404
        
        logger.info(f"📍 {len(osm_buildings)} bâtiments OSM récupérés en {osm_fetch_time:.2f}s")
        
        # Convertir les bâtiments OSM en format générateur
        buildings_df = _convert_osm_to_generator_format(osm_buildings)
        
        # Génération des séries temporelles
        generation_start = datetime.now()
        timeseries_df = current_app.data_generator.generate_timeseries_data(
            buildings_df=buildings_df,
            start_date=data.get('start_date', current_app.config['DEFAULT_START_DATE']),
            end_date=data.get('end_date', current_app.config['DEFAULT_END_DATE']),
            frequency=data.get('frequency', current_app.config['DEFAULT_FREQUENCY'])
        )
        generation_time = (datetime.now() - generation_start).total_seconds()
        
        # Créer le dataset complet
        dataset = {
            'buildings': buildings_df,
            'timeseries': timeseries_df,
            'metadata': {
                'osm_source': True,
                'osm_fetch_time_seconds': round(osm_fetch_time, 2),
                'generation_time_seconds': round(generation_time, 2),
                'total_buildings': len(buildings_df),
                'total_observations': len(timeseries_df),
                'osm_query_info': {
                    'city': data.get('city'),
                    'bbox': data.get('bbox'),
                    'building_types_filter': data.get('building_types'),
                    'limit': data.get('limit')
                }
            }
        }
        
        # Validation des données
        validation_results = current_app.validation_service.validate_complete_dataset(
            buildings_df, timeseries_df
        )
        
        # Préparer la réponse
        response_data = {
            'success': True,
            'osm_buildings_found': len(osm_buildings),
            'osm_fetch_time_seconds': round(osm_fetch_time, 2),
            'generation_time_seconds': round(generation_time, 2),
            'total_time_seconds': round(osm_fetch_time + generation_time, 2),
            'metadata': dataset['metadata'],
            'validation': validation_results,
            'statistics': _calculate_dataset_statistics(dataset)
        }
        
        # Export ou retour des données
        export_format = data.get('export_format', 'json')
        
        if data.get('return_data', True) and export_format == 'json':
            response_data['data'] = {
                'buildings': buildings_df.to_dict('records'),
                'timeseries': timeseries_df.to_dict('records')
            }
            return jsonify(response_data)
        else:
            # Export vers fichier
            export_result = current_app.export_service.export_dataset(
                dataset, 
                export_format,
                include_metadata=True,
                filename_prefix='osm_generated'
            )
            
            response_data['export'] = {
                'format': export_format,
                'files': export_result['files'],
                'total_size_bytes': export_result['total_size']
            }
            
            return jsonify(response_data)
        
    except BadRequest as e:
        logger.warning(f"⚠️ Requête OSM invalide: {e}")
        return jsonify({
            'success': False,
            'error': 'Requête invalide',
            'details': str(e)
        }), 400
    
    except Exception as e:
        logger.error(f"❌ Erreur lors de la génération OSM: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la génération OSM',
            'details': str(e)
        }), 500


@generation_bp.route('/preview', methods=['POST'])
def preview_generation():
    """
    Aperçu des paramètres de génération avec estimations.
    
    Returns:
        JSON avec les estimations de temps, taille, et caractéristiques
    """
    logger.info("👁️ Demande d'aperçu de génération")
    
    try:
        data = request.get_json() or {}
        
        # Paramètres par défaut
        num_buildings = data.get('num_buildings', current_app.config['DEFAULT_NUM_BUILDINGS'])
        start_date = data.get('start_date', current_app.config['DEFAULT_START_DATE'])
        end_date = data.get('end_date', current_app.config['DEFAULT_END_DATE'])
        frequency = data.get('frequency', current_app.config['DEFAULT_FREQUENCY'])
        
        # Calculs d'estimation
        estimations = _calculate_generation_estimations(
            num_buildings, start_date, end_date, frequency
        )
        
        # Recommandations
        recommendations = _get_generation_recommendations(estimations)
        
        preview_data = {
            'success': True,
            'parameters': {
                'num_buildings': num_buildings,
                'start_date': start_date,
                'end_date': end_date,
                'frequency': frequency,
                'location_filter': data.get('location_filter'),
                'building_types': data.get('building_types')
            },
            'estimations': estimations,
            'recommendations': recommendations,
            'warnings': _get_generation_warnings(estimations),
            'compatibility': {
                'formats_supported': current_app.config.get('SUPPORTED_EXPORT_FORMATS', []),
                'max_buildings': current_app.config.get('MAX_BUILDINGS'),
                'max_period_days': current_app.config.get('MAX_PERIOD_DAYS')
            }
        }
        
        return jsonify(preview_data)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'aperçu: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de l\'aperçu',
            'details': str(e)
        }), 500


@generation_bp.route('/sample', methods=['GET'])
def generate_sample():
    """
    Génère un échantillon de données pour démonstration.
    
    Returns:
        JSON avec un petit dataset d'exemple
    """
    logger.info("🧪 Génération d'échantillon de démonstration")
    
    try:
        # Paramètres fixes pour l'échantillon
        sample_params = {
            'num_buildings': 5,
            'start_date': '2024-01-01',
            'end_date': '2024-01-07',  # 1 semaine
            'frequency': 'H'  # Données horaires
        }
        
        # Génération rapide
        start_time = datetime.now()
        dataset = current_app.data_generator.generate_complete_dataset(**sample_params)
        generation_time = (datetime.now() - start_time).total_seconds()
        
        sample_data = {
            'success': True,
            'sample_type': 'demonstration',
            'generation_time_seconds': round(generation_time, 2),
            'metadata': dataset['metadata'],
            'data': {
                'buildings': dataset['buildings'].to_dict('records'),
                'timeseries': dataset['timeseries'].head(50).to_dict('records')  # Limiter à 50 observations
            },
            'statistics': _calculate_dataset_statistics(dataset),
            'note': 'Ceci est un échantillon de démonstration avec des données limitées'
        }
        
        return jsonify(sample_data)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la génération d'échantillon: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la génération d\'échantillon',
            'details': str(e)
        }), 500


# Fonctions utilitaires privées

def _convert_osm_to_generator_format(osm_buildings):
    """
    Convertit les bâtiments OSM au format attendu par le générateur.
    
    Args:
        osm_buildings: Liste des bâtiments OSM
        
    Returns:
        DataFrame au format générateur
    """
    import pandas as pd
    
    converted_buildings = []
    
    for osm_building in osm_buildings:
        # Générer un ID unique pour le générateur
        unique_id = current_app.data_generator._generate_unique_id()
        
        # Convertir au format générateur
        building_data = {
            'unique_id': unique_id,
            'building_id': f"OSM_{osm_building.get('osm_id', unique_id)}",
            'latitude': osm_building['latitude'],
            'longitude': osm_building['longitude'],
            'location': osm_building.get('city', 'Unknown'),
            'building_class': osm_building['building_type'],
            'osm_source': True,
            'osm_id': osm_building.get('osm_id'),
            'osm_tags': osm_building.get('tags', {}),
            'area_sqm': osm_building.get('area_sqm'),
            'levels': osm_building.get('levels'),
            'height': osm_building.get('height')
        }
        
        converted_buildings.append(building_data)
    
    return pd.DataFrame(converted_buildings)


def _calculate_dataset_statistics(dataset):
    """
    Calcule les statistiques d'un dataset généré.
    
    Args:
        dataset: Dataset avec 'buildings' et 'timeseries'
        
    Returns:
        Dictionnaire des statistiques
    """
    buildings_df = dataset['buildings']
    timeseries_df = dataset['timeseries']
    
    # Statistiques de base
    stats = {
        'total_buildings': len(buildings_df),
        'total_observations': len(timeseries_df),
        'unique_locations': buildings_df['location'].nunique() if 'location' in buildings_df.columns else 0,
        'building_types': buildings_df['building_class'].value_counts().to_dict() if 'building_class' in buildings_df.columns else {},
        'consumption_stats': {}
    }
    
    # Statistiques de consommation
    if 'y' in timeseries_df.columns:
        consumption_col = 'y'
    elif 'consumption_kwh' in timeseries_df.columns:
        consumption_col = 'consumption_kwh'
    else:
        consumption_col = None
    
    if consumption_col:
        consumption_data = timeseries_df[consumption_col]
        stats['consumption_stats'] = {
            'mean_kwh': round(consumption_data.mean(), 2),
            'median_kwh': round(consumption_data.median(), 2),
            'max_kwh': round(consumption_data.max(), 2),
            'min_kwh': round(consumption_data.min(), 2),
            'std_kwh': round(consumption_data.std(), 2),
            'total_kwh': round(consumption_data.sum(), 2)
        }
    
    # Période temporelle
    if 'timestamp' in timeseries_df.columns:
        timestamps = pd.to_datetime(timeseries_df['timestamp'])
        stats['temporal_coverage'] = {
            'start_date': timestamps.min().isoformat(),
            'end_date': timestamps.max().isoformat(),
            'total_days': (timestamps.max() - timestamps.min()).days + 1
        }
    
    return stats


def _calculate_generation_estimations(num_buildings, start_date, end_date, frequency):
    """
    Calcule les estimations pour une génération.
    
    Returns:
        Dictionnaire avec les estimations
    """
    from datetime import datetime
    import pandas as pd
    
    # Calculer le nombre d'observations
    date_range = pd.date_range(start=start_date, end=end_date, freq=frequency)
    total_observations = len(date_range) * num_buildings
    
    # Estimations de temps (basées sur des benchmarks)
    base_time_per_building = 0.01  # 10ms par bâtiment
    base_time_per_observation = 0.001  # 1ms par observation
    
    estimated_time = (num_buildings * base_time_per_building) + (total_observations * base_time_per_observation)
    
    # Estimation de taille (approximative)
    bytes_per_observation = 150  # Estimation basée sur le format parquet
    estimated_size_bytes = total_observations * bytes_per_observation
    
    return {
        'total_observations': total_observations,
        'estimated_time_seconds': round(estimated_time, 2),
        'estimated_size_bytes': estimated_size_bytes,
        'estimated_size_mb': round(estimated_size_bytes / (1024 * 1024), 2),
        'complexity_level': _assess_complexity_level(num_buildings, total_observations)
    }


def _assess_complexity_level(num_buildings, total_observations):
    """Évalue le niveau de complexité d'une génération."""
    if total_observations < 10000:
        return 'low'
    elif total_observations < 100000:
        return 'medium'
    elif total_observations < 1000000:
        return 'high'
    else:
        return 'very_high'


def _get_generation_recommendations(estimations):
    """Génère des recommandations basées sur les estimations."""
    recommendations = []
    
    complexity = estimations['complexity_level']
    
    if complexity == 'low':
        recommendations.append("Configuration idéale pour les tests et le développement")
    elif complexity == 'medium':
        recommendations.append("Configuration appropriée pour l'entraînement de modèles")
    elif complexity == 'high':
        recommendations.append("Configuration pour l'analyse de production - temps de génération élevé")
    else:
        recommendations.append("Configuration très lourde - considérez réduire la période ou le nombre de bâtiments")
    
    if estimations['estimated_time_seconds'] > 300:  # 5 minutes
        recommendations.append("Temps de génération élevé - considérez l'export direct en fichier")
    
    if estimations['estimated_size_mb'] > 100:  # 100MB
        recommendations.append("Dataset volumineux - format Parquet recommandé pour l'efficacité")
    
    return recommendations


def _get_generation_warnings(estimations):
    """Génère des avertissements basés sur les estimations."""
    warnings = []
    
    if estimations['estimated_time_seconds'] > 600:  # 10 minutes
        warnings.append("Temps de génération très élevé (>10 minutes)")
    
    if estimations['estimated_size_mb'] > 500:  # 500MB
        warnings.append("Dataset très volumineux (>500MB)")
    
    if estimations['total_observations'] > 5000000:  # 5M observations
        warnings.append("Nombre d'observations très élevé - peut impacter les performances")
    
    return warnings


# Export du blueprint
__all__ = ['generation_bp']
