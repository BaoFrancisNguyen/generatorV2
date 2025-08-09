#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROUTES OPENSTREETMAP - GÉNÉRATEUR MALAYSIA
Fichier: app/routes/osm.py

Routes spécialisées pour l'intégration OpenStreetMap:
récupération de bâtiments, gestion du cache, statistiques OSM.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Routes OSM modulaires
"""

import logging
from datetime import datetime
from flask import Blueprint, request, jsonify, current_app
from werkzeug.exceptions import BadRequest, NotFound

from app.utils.validators import validate_coordinates, validate_bbox, validate_city_name

# Créer le blueprint pour les routes OSM
osm_bp = Blueprint('osm', __name__)
logger = logging.getLogger(__name__)


@osm_bp.route('/buildings', methods=['GET'])
def get_buildings():
    """
    Récupère des bâtiments depuis OpenStreetMap.
    
    Query Parameters:
        city (str): Nom de la ville malaysienne
        bbox (str): Bounding box "sud,ouest,nord,est"
        lat (float): Latitude (avec lon et radius)
        lon (float): Longitude (avec lat et radius)
        radius (int): Rayon en mètres (défaut: 1000)
        building_types (str): Types séparés par virgules
        limit (int): Limite du nombre de bâtiments
        format (str): Format de réponse ('geojson' ou 'simple')
    
    Returns:
        JSON avec les bâtiments OSM récupérés
    """
    logger.info("🏢 Demande de bâtiments OSM")
    
    try:
        # Paramètres de requête
        city = request.args.get('city')
        bbox_str = request.args.get('bbox')
        lat = request.args.get('lat', type=float)
        lon = request.args.get('lon', type=float)
        radius = request.args.get('radius', type=int, default=1000)
        building_types_str = request.args.get('building_types')
        limit = request.args.get('limit', type=int)
        response_format = request.args.get('format', 'simple')
        
        # Validation des paramètres
        if not any([city, bbox_str, (lat and lon)]):
            raise BadRequest("Paramètre de localisation requis: city, bbox, ou lat+lon")
        
        # Préparer les types de bâtiments
        building_types = None
        if building_types_str:
            building_types = [bt.strip() for bt in building_types_str.split(',')]
        
        osm_service = current_app.osm_service
        buildings = []
        query_info = {}
        
        # Récupération selon le type de requête
        if city:
            # Validation de la ville
            city_validation = validate_city_name(city)
            if not city_validation['valid']:
                raise BadRequest(f"Ville invalide: {city_validation['errors']}")
            
            logger.info(f"🏙️ Recherche de bâtiments à {city}")
            buildings = osm_service.get_buildings_for_city(city, limit)
            query_info = {
                'type': 'city',
                'city': city,
                'limit': limit
            }
        
        elif bbox_str:
            # Parser et valider la bbox
            try:
                bbox_parts = [float(x.strip()) for x in bbox_str.split(',')]
                if len(bbox_parts) != 4:
                    raise ValueError("4 valeurs requises")
                bbox = tuple(bbox_parts)
            except ValueError:
                raise BadRequest("Format bbox invalide. Utilisez: sud,ouest,nord,est")
            
            bbox_validation = validate_bbox(list(bbox))
            if not bbox_validation['valid']:
                raise BadRequest(f"Bbox invalide: {bbox_validation['errors']}")
            
            logger.info(f"📦 Recherche de bâtiments dans bbox: {bbox}")
            buildings = osm_service.get_buildings_in_bbox(bbox, building_types)
            query_info = {
                'type': 'bbox',
                'bbox': bbox,
                'building_types': building_types
            }
        
        elif lat and lon:
            # Validation des coordonnées
            coords_validation = validate_coordinates(lat, lon)
            if not coords_validation['valid']:
                raise BadRequest(f"Coordonnées invalides: {coords_validation['errors']}")
            
            logger.info(f"📍 Recherche de bâtiments autour de ({lat}, {lon})")
            buildings = osm_service.get_buildings_around_point(lat, lon, radius, building_types)
            query_info = {
                'type': 'around',
                'coordinates': {'lat': lat, 'lon': lon},
                'radius': radius,
                'building_types': building_types
            }
        
        # Appliquer la limite si spécifiée et pas déjà appliquée
        if limit and len(buildings) > limit:
            buildings = buildings[:limit]
            logger.info(f"✂️ Limitation appliquée: {limit} bâtiments retenus")
        
        # Formater la réponse selon le format demandé
        if response_format == 'geojson':
            formatted_buildings = _format_buildings_as_geojson(buildings)
        else:  # simple
            formatted_buildings = _format_buildings_simple(buildings)
        
        # Statistiques
        building_type_counts = {}
        for building in buildings:
            building_type = building.get('building_type', 'unknown')
            building_type_counts[building_type] = building_type_counts.get(building_type, 0) + 1
        
        response = {
            'success': True,
            'query_info': query_info,
            'total_buildings': len(buildings),
            'buildings': formatted_buildings,
            'building_type_distribution': building_type_counts,
            'timestamp': datetime.now().isoformat(),
            'format': response_format
        }
        
        return jsonify(response)
        
    except BadRequest as e:
        logger.warning(f"⚠️ Requête OSM invalide: {e}")
        return jsonify({
            'success': False,
            'error': 'Requête invalide',
            'details': str(e)
        }), 400
    
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des bâtiments OSM: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la récupération des bâtiments OSM',
            'details': str(e)
        }), 500


@osm_bp.route('/statistics', methods=['GET'])
def get_osm_statistics():
    """
    Retourne les statistiques du service OSM.
    
    Returns:
        JSON avec les statistiques détaillées
    """
    logger.info("📊 Demande de statistiques OSM")
    
    try:
        osm_service = current_app.osm_service
        stats = osm_service.get_statistics()
        
        # Enrichir avec des informations supplémentaires
        enhanced_stats = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'service_info': {
                'overpass_url': stats['configuration']['overpass_url'],
                'timeout_seconds': stats['configuration']['timeout'],
                'max_retries': stats['configuration']['max_retries']
            },
            'request_statistics': stats['request_stats'],
            'cache_statistics': stats['cache_info'],
            'performance_metrics': {
                'total_requests': stats['request_stats']['total_requests'],
                'success_rate': round(
                    (stats['request_stats']['total_requests'] - stats['request_stats']['failed_requests']) / 
                    max(1, stats['request_stats']['total_requests']) * 100, 2
                ) if stats['request_stats']['total_requests'] > 0 else 0,
                'cache_hit_rate': round(
                    stats['request_stats']['cache_hits'] / 
                    max(1, stats['request_stats']['cache_hits'] + stats['request_stats']['cache_misses']) * 100, 2
                ) if (stats['request_stats']['cache_hits'] + stats['request_stats']['cache_misses']) > 0 else 0
            }
        }
        
        return jsonify(enhanced_stats)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des statistiques OSM: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la récupération des statistiques OSM',
            'details': str(e)
        }), 500


@osm_bp.route('/cache/clear', methods=['POST'])
def clear_osm_cache():
    """
    Vide le cache OSM.
    
    Returns:
        JSON confirmant la suppression du cache
    """
    logger.info("🗑️ Demande de vidage du cache OSM")
    
    try:
        osm_service = current_app.osm_service
        
        # Obtenir les statistiques avant suppression
        stats_before = osm_service.get_statistics()
        cache_files_before = stats_before['cache_info'].get('files_count', 0)
        
        # Vider le cache
        osm_service.clear_cache()
        
        # Obtenir les statistiques après suppression
        stats_after = osm_service.get_statistics()
        cache_files_after = stats_after['cache_info'].get('files_count', 0)
        
        response = {
            'success': True,
            'message': 'Cache OSM vidé avec succès',
            'cache_info': {
                'files_removed': cache_files_before - cache_files_after,
                'files_before': cache_files_before,
                'files_after': cache_files_after
            },
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"✅ Cache OSM vidé: {cache_files_before - cache_files_after} fichiers supprimés")
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors du vidage du cache OSM: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors du vidage du cache OSM',
            'details': str(e)
        }), 500


@osm_bp.route('/cache/info', methods=['GET'])
def get_cache_info():
    """
    Retourne des informations détaillées sur le cache OSM.
    
    Returns:
        JSON avec les informations du cache
    """
    logger.info("📋 Demande d'informations sur le cache OSM")
    
    try:
        osm_service = current_app.osm_service
        stats = osm_service.get_statistics()
        cache_info = stats['cache_info']
        
        # Informations détaillées du cache
        detailed_cache_info = {
            'success': True,
            'cache_enabled': cache_info['enabled'],
            'cache_directory': cache_info['directory'],
            'cache_duration_hours': cache_info['duration_hours'],
            'cache_files_count': cache_info['files_count'],
            'cache_statistics': {
                'hits': stats['request_stats']['cache_hits'],
                'misses': stats['request_stats']['cache_misses'],
                'hit_rate_percent': round(
                    stats['request_stats']['cache_hits'] / 
                    max(1, stats['request_stats']['cache_hits'] + stats['request_stats']['cache_misses']) * 100, 2
                ) if (stats['request_stats']['cache_hits'] + stats['request_stats']['cache_misses']) > 0 else 0
            },
            'recommendations': _get_cache_recommendations(cache_info, stats['request_stats']),
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(detailed_cache_info)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des informations de cache: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la récupération des informations de cache',
            'details': str(e)
        }), 500


@osm_bp.route('/validate', methods=['POST'])
def validate_osm_query():
    """
    Valide une requête OSM sans l'exécuter.
    
    Body JSON:
    {
        "type": "city" | "bbox" | "around",
        "city": "Kuala Lumpur",  // pour type=city
        "bbox": [south, west, north, east],  // pour type=bbox
        "lat": 3.139, "lon": 101.687, "radius": 1000,  // pour type=around
        "building_types": ["residential", "commercial"],
        "limit": 1000
    }
    
    Returns:
        JSON avec les résultats de validation
    """
    logger.info("✅ Demande de validation de requête OSM")
    
    try:
        if not request.is_json:
            raise BadRequest("Content-Type application/json requis")
        
        data = request.get_json()
        if not data:
            raise BadRequest("Corps de requête JSON vide")
        
        # Validation de la requête OSM
        from app.utils.validators import validate_osm_request
        validation_result = validate_osm_request(data, current_app.config)
        
        # Ajouter des estimations si la validation est réussie
        if validation_result['valid']:
            # Estimer le nombre de bâtiments potentiels
            estimated_buildings = _estimate_buildings_count(data)
            validation_result['estimations'] = {
                'estimated_buildings': estimated_buildings,
                'estimated_request_time_seconds': _estimate_request_time(estimated_buildings),
                'complexity_level': _assess_query_complexity(data, estimated_buildings)
            }
        
        response = {
            'success': True,
            'validation_results': validation_result,
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(response)
        
    except BadRequest as e:
        logger.warning(f"⚠️ Requête de validation OSM invalide: {e}")
        return jsonify({
            'success': False,
            'error': 'Requête de validation invalide',
            'details': str(e)
        }), 400
    
    except Exception as e:
        logger.error(f"❌ Erreur lors de la validation OSM: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la validation OSM',
            'details': str(e)
        }), 500


# === FONCTIONS UTILITAIRES PRIVÉES ===

def _format_buildings_as_geojson(buildings):
    """
    Formate les bâtiments au format GeoJSON.
    
    Args:
        buildings: Liste des bâtiments OSM
        
    Returns:
        Structure GeoJSON
    """
    features = []
    
    for building in buildings:
        if 'geometry' in building and building['geometry']:
            # Convertir la géométrie OSM en format GeoJSON
            coordinates = []
            for point in building['geometry']:
                if 'lat' in point and 'lon' in point:
                    coordinates.append([point['lon'], point['lat']])
            
            if len(coordinates) >= 3:
                # Fermer le polygon si nécessaire
                if coordinates[0] != coordinates[-1]:
                    coordinates.append(coordinates[0])
                
                feature = {
                    'type': 'Feature',
                    'properties': {
                        'osm_id': building.get('osm_id'),
                        'building_type': building.get('building_type'),
                        'osm_building_tag': building.get('osm_building_tag'),
                        'area_sqm': building.get('area_sqm'),
                        'levels': building.get('levels'),
                        'height': building.get('height'),
                        'name': building.get('name'),
                        'city': building.get('city')
                    },
                    'geometry': {
                        'type': 'Polygon',
                        'coordinates': [coordinates]
                    }
                }
                features.append(feature)
    
    return {
        'type': 'FeatureCollection',
        'features': features
    }


def _format_buildings_simple(buildings):
    """
    Formate les bâtiments au format simple.
    
    Args:
        buildings: Liste des bâtiments OSM
        
    Returns:
        Liste de bâtiments formatés
    """
    formatted = []
    
    for building in buildings:
        formatted_building = {
            'osm_id': building.get('osm_id'),
            'coordinates': {
                'latitude': building.get('latitude'),
                'longitude': building.get('longitude')
            },
            'building_type': building.get('building_type'),
            'properties': {
                'area_sqm': building.get('area_sqm'),
                'levels': building.get('levels'),
                'height': building.get('height'),
                'name': building.get('name')
            },
            'location': {
                'city': building.get('city'),
                'address': {
                    'street': building.get('addr_street'),
                    'housenumber': building.get('addr_housenumber'),
                    'postcode': building.get('addr_postcode')
                }
            },
            'osm_metadata': {
                'osm_type': building.get('osm_type'),
                'osm_building_tag': building.get('osm_building_tag'),
                'tags': building.get('tags', {})
            }
        }
        formatted.append(formatted_building)
    
    return formatted


def _get_cache_recommendations(cache_info, request_stats):
    """
    Génère des recommandations pour l'optimisation du cache OSM.
    
    Args:
        cache_info: Informations du cache
        request_stats: Statistiques des requêtes
        
    Returns:
        Liste de recommandations
    """
    recommendations = []
    
    total_requests = request_stats['cache_hits'] + request_stats['cache_misses']
    
    if total_requests == 0:
        recommendations.append("Aucune requête OSM effectuée pour le moment")
        return recommendations
    
    hit_rate = request_stats['cache_hits'] / total_requests * 100
    
    if hit_rate < 30:
        recommendations.append("Taux de cache faible (<30%) - Considérer augmenter la durée de cache")
    elif hit_rate < 60:
        recommendations.append("Taux de cache moyen - Performances correctes")
    else:
        recommendations.append("Excellent taux de cache (>60%) - Optimisation réussie")
    
    if cache_info['files_count'] > 1000:
        recommendations.append("Beaucoup de fichiers en cache - Considérer un nettoyage périodique")
    
    if request_stats['failed_requests'] > total_requests * 0.1:
        recommendations.append("Taux d'échec élevé (>10%) - Vérifier la connectivité Overpass")
    
    return recommendations


def _estimate_buildings_count(query_data):
    """
    Estime le nombre de bâtiments pour une requête OSM.
    
    Args:
        query_data: Données de la requête
        
    Returns:
        Estimation du nombre de bâtiments
    """
    query_type = query_data.get('type')
    
    if query_type == 'city':
        # Estimations basées sur les villes malaysiennes connues
        city = query_data.get('city', '')
        city_estimates = {
            'Kuala Lumpur': 15000,
            'George Town': 8000,
            'Johor Bahru': 6000,
            'Ipoh': 5000,
            'Petaling Jaya': 7000,
            'Shah Alam': 6000,
            'Kota Kinabalu': 4000,
            'Kuching': 3000
        }
        base_estimate = city_estimates.get(city, 2000)
        
    elif query_type == 'bbox':
        # Estimation basée sur la taille de la bbox
        bbox = query_data.get('bbox', [0, 0, 0, 0])
        if len(bbox) == 4:
            south, west, north, east = bbox
            area_deg_sq = (north - south) * (east - west)
            # Approximation: 1000 bâtiments par degré carré en zone urbaine
            base_estimate = int(area_deg_sq * 1000)
        else:
            base_estimate = 1000
            
    elif query_type == 'around':
        # Estimation basée sur le rayon
        radius = query_data.get('radius', 1000)
        # Approximation: 1 bâtiment par 100m² en zone urbaine
        area_m2 = 3.14159 * (radius ** 2)
        base_estimate = int(area_m2 / 100)
        
    else:
        base_estimate = 1000
    
    # Appliquer la limite si spécifiée
    limit = query_data.get('limit')
    if limit and limit < base_estimate:
        return limit
    
    return min(base_estimate, 10000)  # Maximum 10k bâtiments


def _estimate_request_time(estimated_buildings):
    """
    Estime le temps de requête OSM.
    
    Args:
        estimated_buildings: Nombre estimé de bâtiments
        
    Returns:
        Temps estimé en secondes
    """
    # Temps de base + temps proportionnel au nombre de bâtiments
    base_time = 2.0  # 2 secondes de base
    time_per_building = 0.001  # 1ms par bâtiment
    
    return base_time + (estimated_buildings * time_per_building)


def _assess_query_complexity(query_data, estimated_buildings):
    """
    Évalue la complexité d'une requête OSM.
    
    Args:
        query_data: Données de la requête
        estimated_buildings: Nombre estimé de bâtiments
        
    Returns:
        Niveau de complexité
    """
    if estimated_buildings < 100:
        return 'low'
    elif estimated_buildings < 1000:
        return 'medium'
    elif estimated_buildings < 5000:
        return 'high'
    else:
        return 'very_high'


# Gestionnaires d'erreurs spécifiques au blueprint OSM
@osm_bp.errorhandler(404)
def osm_not_found(error):
    """Gestionnaire d'erreur 404 pour les routes OSM."""
    return jsonify({
        'success': False,
        'error': 'Endpoint OSM non trouvé',
        'available_endpoints': [
            '/osm/buildings - Récupération de bâtiments OSM',
            '/osm/statistics - Statistiques du service OSM',
            '/osm/cache/clear - Vidage du cache OSM',
            '/osm/cache/info - Informations sur le cache',
            '/osm/validate - Validation de requête OSM'
        ]
    }), 404


@osm_bp.errorhandler(500)
def osm_internal_error(error):
    """Gestionnaire d'erreur 500 pour les routes OSM."""
    logger.error(f"💥 Erreur interne OSM: {error}")
    return jsonify({
        'success': False,
        'error': 'Erreur interne du service OSM',
        'timestamp': datetime.now().isoformat(),
        'suggestion': 'Vérifiez la connectivité avec l\'API Overpass'
    }), 500


# Export du blueprint
__all__ = ['osm_bp']