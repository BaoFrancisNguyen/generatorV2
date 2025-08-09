#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROUTES API REST - GÉNÉRATEUR MALAYSIA
Fichier: app/routes/api.py

Routes API REST pour l'accès programmatique aux fonctionnalités
du générateur de données énergétiques Malaysia.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - API structurée
"""

import logging
from datetime import datetime
from flask import Blueprint, request, jsonify, current_app
from werkzeug.exceptions import BadRequest, NotFound

# Créer le blueprint pour les routes API
api_bp = Blueprint('api', __name__)
logger = logging.getLogger(__name__)


@api_bp.route('/cities', methods=['GET'])
def get_cities():
    """
    Retourne la liste des villes malaysiennes disponibles.
    
    Query Parameters:
        state (str): Filtrer par état
        min_population (int): Population minimale
        format (str): Format de réponse ('simple' ou 'detailed')
    
    Returns:
        JSON avec la liste des villes et leurs informations
    """
    logger.info("📍 Demande de liste des villes")
    
    try:
        # Paramètres de requête
        state_filter = request.args.get('state')
        min_population = request.args.get('min_population', type=int)
        response_format = request.args.get('format', 'simple')
        
        # Récupérer les données Malaysia
        malaysia_data = current_app.data_generator.malaysia_data
        all_locations = malaysia_data.get_available_locations()
        
        # Appliquer les filtres
        filtered_locations = all_locations.copy()
        
        if state_filter:
            filtered_locations = {
                k: v for k, v in filtered_locations.items()
                if v.get('state') == state_filter
            }
        
        if min_population:
            filtered_locations = {
                k: v for k, v in filtered_locations.items()
                if v.get('population', 0) >= min_population
            }
        
        # Formater la réponse
        if response_format == 'simple':
            cities = [
                {
                    'city': city,
                    'state': data['state'],
                    'population': data['population']
                }
                for city, data in filtered_locations.items()
            ]
        else:  # detailed
            cities = [
                {
                    'city': city,
                    'state': data['state'],
                    'state_code': data.get('state_code'),
                    'region': data['region'],
                    'population': data['population'],
                    'coordinates': {
                        'latitude': data['latitude'],
                        'longitude': data['longitude']
                    },
                    'urban_level': data.get('urban_level'),
                    'type': data.get('type'),
                    'timezone': data.get('timezone'),
                    'economic_level': data.get('economic_level')
                }
                for city, data in filtered_locations.items()
            ]
        
        # Trier par population (descendant)
        cities.sort(key=lambda x: x['population'], reverse=True)
        
        response = {
            'success': True,
            'total_cities': len(cities),
            'filters_applied': {
                'state': state_filter,
                'min_population': min_population
            },
            'cities': cities
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des villes: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la récupération des villes',
            'details': str(e)
        }), 500


@api_bp.route('/building-types', methods=['GET'])
def get_building_types():
    """
    Retourne les types de bâtiments supportés avec leurs caractéristiques.
    
    Returns:
        JSON avec les types de bâtiments et leurs propriétés
    """
    logger.info("🏢 Demande des types de bâtiments")
    
    try:
        # Récupérer les patterns énergétiques
        pattern_generator = current_app.data_generator.pattern_generator
        
        building_types_info = {}
        
        for building_type in ['residential', 'commercial', 'industrial', 'public']:
            stats = pattern_generator.get_pattern_statistics(building_type)
            
            building_types_info[building_type] = {
                'name': building_type,
                'display_name': {
                    'residential': 'Résidentiel',
                    'commercial': 'Commercial', 
                    'industrial': 'Industriel',
                    'public': 'Public'
                }[building_type],
                'description': {
                    'residential': 'Maisons, appartements, logements',
                    'commercial': 'Bureaux, magasins, centres commerciaux',
                    'industrial': 'Usines, entrepôts, installations industrielles',
                    'public': 'Hôpitaux, écoles, bâtiments gouvernementaux'
                }[building_type],
                'base_consumption_kwh': stats.get('base_consumption_kwh', 0),
                'peak_hours': stats.get('peak_hours', []),
                'low_hours': stats.get('low_hours', []),
                'characteristics': {
                    'ac_dependency': stats.get('ac_dependency', 0),
                    'seasonal_sensitivity': stats.get('seasonal_variation_range', {}),
                    'noise_level': stats.get('noise_level', 0)
                }
            }
        
        response = {
            'success': True,
            'total_types': len(building_types_info),
            'building_types': building_types_info
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des types de bâtiments: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la récupération des types de bâtiments',
            'details': str(e)
        }), 500


@api_bp.route('/statistics', methods=['GET'])
def get_statistics():
    """
    Retourne les statistiques globales de l'application.
    
    Returns:
        JSON avec les statistiques de tous les services
    """
    logger.info("📊 Demande de statistiques globales")
    
    try:
        # Statistiques du générateur
        generator_stats = current_app.data_generator.get_generation_statistics()
        
        # Statistiques OSM
        osm_stats = current_app.osm_service.get_statistics()
        
        # Statistiques de validation
        validation_stats = current_app.validation_service.get_validation_statistics()
        
        # Statistiques d'export
        export_stats = current_app.export_service.get_export_statistics()
        
        # Statistiques des données Malaysia
        malaysia_stats = current_app.data_generator.malaysia_data.get_statistics()
        
        response = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'application': {
                'name': 'Malaysia Energy Generator',
                'version': '3.0',
                'uptime_seconds': (datetime.now() - getattr(current_app, '_start_time', datetime.now())).total_seconds()
            },
            'services': {
                'data_generator': generator_stats,
                'osm_service': osm_stats,
                'validation_service': validation_stats,
                'export_service': export_stats
            },
            'malaysia_data': malaysia_stats
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des statistiques: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la récupération des statistiques',
            'details': str(e)
        }), 500


@api_bp.route('/validate', methods=['POST'])
def validate_data():
    """
    Valide des données de bâtiments ou de séries temporelles.
    
    Body JSON:
    {
        "type": "buildings" | "timeseries" | "generation_request",
        "data": [...] | {...}
    }
    
    Returns:
        JSON avec les résultats de validation
    """
    logger.info("✅ Demande de validation de données")
    
    try:
        if not request.is_json:
            raise BadRequest("Content-Type application/json requis")
        
        data = request.get_json()
        if not data:
            raise BadRequest("Corps de requête JSON vide")
        
        validation_type = data.get('type')
        validation_data = data.get('data')
        
        if not validation_type:
            raise BadRequest("Type de validation requis")
        
        if not validation_data:
            raise BadRequest("Données à valider requises")
        
        validation_service = current_app.validation_service
        
        if validation_type == 'buildings':
            # Validation de métadonnées de bâtiments
            if isinstance(validation_data, list):
                import pandas as pd
                buildings_df = pd.DataFrame(validation_data)
                validation_results = validation_service.validate_buildings_metadata(buildings_df)
            else:
                raise BadRequest("Données de bâtiments doivent être une liste")
        
        elif validation_type == 'timeseries':
            # Validation de séries temporelles
            from app.utils.validators import validate_timeseries_data
            validation_results = validate_timeseries_data(validation_data)
        
        elif validation_type == 'generation_request':
            # Validation de requête de génération
            from app.utils.validators import validate_generation_request
            validation_results = validate_generation_request(validation_data, current_app.config)
        
        else:
            raise BadRequest(f"Type de validation non supporté: {validation_type}")
        
        response = {
            'success': True,
            'validation_type': validation_type,
            'validation_results': validation_results,
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(response)
        
    except BadRequest as e:
        logger.warning(f"⚠️ Requête de validation invalide: {e}")
        return jsonify({
            'success': False,
            'error': 'Requête de validation invalide',
            'details': str(e)
        }), 400
    
    except Exception as e:
        logger.error(f"❌ Erreur lors de la validation: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la validation',
            'details': str(e)
        }), 500


@api_bp.route('/export-formats', methods=['GET'])
def get_export_formats():
    """
    Retourne les formats d'export supportés avec leurs caractéristiques.
    
    Returns:
        JSON avec les formats d'export disponibles
    """
    logger.info("📁 Demande des formats d'export")
    
    try:
        export_service = current_app.export_service
        supported_formats = export_service.supported_formats
        
        formats_info = {}
        
        for format_name, config in supported_formats.items():
            formats_info[format_name] = {
                'name': format_name,
                'extension': config['extension'],
                'description': {
                    'parquet': 'Format optimisé pour l\'analyse (recommandé)',
                    'csv': 'Format universel compatible Excel',
                    'json': 'Format pour intégrations API',
                    'excel': 'Format Excel avec feuilles multiples'
                }.get(format_name, 'Format de données'),
                'advantages': {
                    'parquet': ['Très compact', 'Lecture rapide', 'Types préservés'],
                    'csv': ['Compatible partout', 'Lisible par humains', 'Simple'],
                    'json': ['Structure flexible', 'API friendly', 'Métadonnées incluses'],
                    'excel': ['Interface familière', 'Graphiques possibles', 'Feuilles multiples']
                }.get(format_name, []),
                'use_cases': {
                    'parquet': 'Analyse de données, Machine Learning, Big Data',
                    'csv': 'Excel, tableaux, compatibilité universelle',
                    'json': 'APIs, applications web, intégrations',
                    'excel': 'Reporting, présentations, analyse business'
                }.get(format_name, 'Usage général'),
                'configuration': config
            }
        
        response = {
            'success': True,
            'total_formats': len(formats_info),
            'export_formats': formats_info,
            'recommendations': {
                'performance': 'parquet',
                'compatibility': 'csv', 
                'integration': 'json',
                'reporting': 'excel'
            }
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des formats: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la récupération des formats d\'export',
            'details': str(e)
        }), 500


@api_bp.route('/climate-zones', methods=['GET'])
def get_climate_zones():
    """
    Retourne les zones climatiques de Malaysia avec leurs caractéristiques.
    
    Returns:
        JSON avec les zones climatiques
    """
    logger.info("🌡️ Demande des zones climatiques")
    
    try:
        # Récupérer les zones climatiques depuis les patterns énergétiques
        pattern_generator = current_app.data_generator.pattern_generator
        climate_zones = pattern_generator._climate_zones if hasattr(pattern_generator, '_climate_zones') else {}
        
        if not climate_zones:
            # Fallback avec données par défaut
            climate_zones = {
                'tropical_urban': {
                    'temperature_range': (26, 35),
                    'humidity_range': (65, 90),
                    'description': 'Zones urbaines avec îlot de chaleur'
                },
                'tropical_coastal': {
                    'temperature_range': (24, 33),
                    'humidity_range': (75, 95),
                    'description': 'Zones côtières avec brise marine'
                },
                'tropical_inland': {
                    'temperature_range': (22, 36),
                    'humidity_range': (60, 85),
                    'description': 'Zones intérieures plus chaudes'
                },
                'tropical_highland': {
                    'temperature_range': (18, 28),
                    'humidity_range': (70, 90),
                    'description': 'Zones montagneuses plus fraîches'
                }
            }
        
        # Enrichir avec des informations supplémentaires
        enhanced_zones = {}
        for zone_name, zone_data in climate_zones.items():
            enhanced_zones[zone_name] = {
                'name': zone_name,
                'display_name': {
                    'tropical_urban': 'Tropical Urbain',
                    'tropical_coastal': 'Tropical Côtier',
                    'tropical_inland': 'Tropical Intérieur',
                    'tropical_highland': 'Tropical Montagnard'
                }.get(zone_name, zone_name.replace('_', ' ').title()),
                'temperature_range_celsius': zone_data.get('temperature_range', (25, 32)),
                'humidity_range_percent': zone_data.get('humidity_range', (70, 90)),
                'annual_rainfall_mm': zone_data.get('rainfall_mm_year', 2500),
                'energy_impact': {
                    'cooling_factor': zone_data.get('cooling_factor', 1.0),
                    'baseline_multiplier': zone_data.get('energy_baseline_multiplier', 1.0)
                },
                'characteristics': zone_data.get('description', 'Zone climatique tropicale'),
                'cities_examples': {
                    'tropical_urban': ['Kuala Lumpur', 'Petaling Jaya', 'Shah Alam'],
                    'tropical_coastal': ['George Town', 'Johor Bahru', 'Kota Kinabalu'],
                    'tropical_inland': ['Ipoh', 'Seremban', 'Temerloh'],
                    'tropical_highland': ['Cameron Highlands', 'Genting Highlands']
                }.get(zone_name, [])
            }
        
        response = {
            'success': True,
            'total_zones': len(enhanced_zones),
            'climate_zones': enhanced_zones,
            'general_info': {
                'country': 'Malaysia',
                'climate_type': 'Tropical Equatorial',
                'seasons': {
                    'northeast_monsoon': 'Novembre - Février (plus frais)',
                    'southwest_monsoon': 'Juin - Août (mousson)',
                    'inter_monsoon': 'Mars - Mai, Septembre - Octobre (chaud)'
                }
            }
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des zones climatiques: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la récupération des zones climatiques',
            'details': str(e)
        }), 500


@api_bp.route('/estimate', methods=['POST'])
def estimate_generation():
    """
    Estime les ressources nécessaires pour une génération de données.
    
    Body JSON:
    {
        "num_buildings": 1000,
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "frequency": "H"
    }
    
    Returns:
        JSON avec les estimations de temps, taille et complexité
    """
    logger.info("🔮 Demande d'estimation de génération")
    
    try:
        if not request.is_json:
            raise BadRequest("Content-Type application/json requis")
        
        data = request.get_json()
        if not data:
            raise BadRequest("Corps de requête JSON vide")
        
        # Paramètres par défaut
        num_buildings = data.get('num_buildings', 100)
        start_date = data.get('start_date', '2024-01-01')
        end_date = data.get('end_date', '2024-01-31')
        frequency = data.get('frequency', 'D')
        
        # Validation de base
        from app.utils.validators import validate_generation_request
        validation = validate_generation_request(data, current_app.config)
        
        if not validation['valid']:
            return jsonify({
                'success': False,
                'error': 'Paramètres invalides',
                'details': validation['errors']
            }), 400
        
        # Calculs d'estimation
        from datetime import datetime
        import pandas as pd
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Calculer le nombre d'observations
        date_range = pd.date_range(start=start_date, end=end_date, freq=frequency)
        total_observations = len(date_range) * num_buildings
        
        # Estimations de temps (basées sur des benchmarks)
        base_time_per_building = 0.015  # 15ms par bâtiment
        base_time_per_observation = 0.0008  # 0.8ms par observation
        
        estimated_time_seconds = (num_buildings * base_time_per_building) + (total_observations * base_time_per_observation)
        
        # Estimation de taille de fichier
        bytes_per_observation = {
            'parquet': 120,  # Avec compression
            'csv': 180,
            'json': 250,
            'excel': 200
        }
        
        file_sizes = {}
        for format_name, bytes_per_obs in bytes_per_observation.items():
            size_bytes = total_observations * bytes_per_obs
            file_sizes[format_name] = {
                'bytes': size_bytes,
                'mb': round(size_bytes / (1024 * 1024), 2),
                'gb': round(size_bytes / (1024 * 1024 * 1024), 3)
            }
        
        # Niveau de complexité
        if total_observations < 10000:
            complexity = 'low'
            complexity_desc = 'Génération rapide et simple'
        elif total_observations < 100000:
            complexity = 'medium'
            complexity_desc = 'Génération standard'
        elif total_observations < 1000000:
            complexity = 'high'
            complexity_desc = 'Génération lourde - patience requise'
        else:
            complexity = 'very_high'
            complexity_desc = 'Génération très lourde - considérer réduire les paramètres'
        
        # Recommandations
        recommendations = []
        if estimated_time_seconds > 300:  # 5 minutes
            recommendations.append("Temps de génération élevé - considérer l'export direct en fichier")
        
        if total_observations > 5000000:
            recommendations.append("Dataset très volumineux - considérer réduire la période ou augmenter la fréquence")
        
        if frequency in ['15T', '5T', '1T']:
            recommendations.append("Fréquence très fine - dataset volumineux résultant")
        
        recommendations.append(f"Format Parquet recommandé pour {file_sizes['parquet']['mb']} MB")
        
        response = {
            'success': True,
            'parameters': {
                'num_buildings': num_buildings,
                'start_date': start_date,
                'end_date': end_date,
                'frequency': frequency,
                'period_days': (end_dt - start_dt).days + 1
            },
            'estimations': {
                'total_observations': total_observations,
                'estimated_time': {
                    'seconds': round(estimated_time_seconds, 1),
                    'minutes': round(estimated_time_seconds / 60, 1),
                    'formatted': f"{int(estimated_time_seconds // 60)}m {int(estimated_time_seconds % 60)}s"
                },
                'file_sizes': file_sizes,
                'memory_usage_mb': round(total_observations * 0.001, 1),  # Estimation mémoire
                'complexity': {
                    'level': complexity,
                    'description': complexity_desc
                }
            },
            'recommendations': recommendations,
            'warnings': validation.get('warnings', [])
        }
        
        return jsonify(response)
        
    except BadRequest as e:
        logger.warning(f"⚠️ Requête d'estimation invalide: {e}")
        return jsonify({
            'success': False,
            'error': 'Requête d\'estimation invalide',
            'details': str(e)
        }), 400
    
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'estimation: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de l\'estimation',
            'details': str(e)
        }), 500


# Gestionnaires d'erreurs spécifiques au blueprint API
@api_bp.errorhandler(404)
def api_not_found(error):
    """Gestionnaire d'erreur 404 pour l'API."""
    return jsonify({
        'success': False,
        'error': 'Endpoint API non trouvé',
        'available_endpoints': [
            '/api/cities - Liste des villes malaysiennes',
            '/api/building-types - Types de bâtiments supportés',
            '/api/statistics - Statistiques de l\'application',
            '/api/validate - Validation de données',
            '/api/export-formats - Formats d\'export disponibles',
            '/api/climate-zones - Zones climatiques Malaysia',
            '/api/estimate - Estimation de génération'
        ]
    }), 404


@api_bp.errorhandler(500)
def api_internal_error(error):
    """Gestionnaire d'erreur 500 pour l'API."""
    logger.error(f"💥 Erreur interne API: {error}")
    return jsonify({
        'success': False,
        'error': 'Erreur interne de l\'API',
        'timestamp': datetime.now().isoformat()
    }), 500


# Export du blueprint
__all__ = ['api_bp']