#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROUTES OPENSTREETMAP ULTRA-OPTIMISÉES - GÉNÉRATEUR MALAYSIA
Fichier: app/routes/osm.py

Routes optimisées pour récupération exhaustive de bâtiments OSM:
- Endpoint pour récupération complète Malaysia
- Download direct des données
- Monitoring des performances
- Cache management

Auteur: Équipe Développement
Date: 2025
Version: 4.0 - Ultra-optimisé
"""

import logging
import json
import asyncio
from datetime import datetime
from flask import Blueprint, request, jsonify, current_app, send_file
from werkzeug.exceptions import BadRequest, NotFound
import pandas as pd
import tempfile
import gzip
from pathlib import Path

from app.utils.validators import validate_coordinates, validate_bbox, validate_city_name

# Créer le blueprint pour les routes OSM
osm_bp = Blueprint('osm', __name__)
logger = logging.getLogger(__name__)


@osm_bp.route('/buildings/all', methods=['GET'])
def get_all_buildings():
    """
    🚀 ENDPOINT PRINCIPAL: Récupère TOUS les bâtiments de Malaysia.
    
    Query Parameters:
        format (str): Format de réponse ('json', 'geojson', 'csv', 'parquet')
        download (bool): Télécharger directement le fichier
        sample (int): Nombre d'échantillons pour aperçu (défaut: tous)
        include_geometry (bool): Inclure la géométrie complète
        compress (bool): Compresser la réponse
    
    Returns:
        JSON avec tous les bâtiments ou fichier en téléchargement
    """
    logger.info("🇲🇾 Demande de récupération EXHAUSTIVE des bâtiments Malaysia")
    
    try:
        # Paramètres
        response_format = request.args.get('format', 'json').lower()
        download = request.args.get('download', 'false').lower() == 'true'
        sample_size = request.args.get('sample', type=int)
        include_geometry = request.args.get('include_geometry', 'false').lower() == 'true'
        compress = request.args.get('compress', 'false').lower() == 'true'
        
        # Validation du format
        valid_formats = ['json', 'geojson', 'csv', 'parquet', 'xlsx']
        if response_format not in valid_formats:
            raise BadRequest(f"Format invalide. Formats supportés: {', '.join(valid_formats)}")
        
        osm_service = current_app.osm_service
        
        # 🚀 RÉCUPÉRATION EXHAUSTIVE
        logger.info("⚡ Début récupération exhaustive...")
        start_time = datetime.now()
        
        buildings = osm_service.get_all_buildings_malaysia()
        
        fetch_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"✅ Récupération terminée: {len(buildings)} bâtiments en {fetch_time:.1f}s")
        
        # Échantillonnage si demandé
        if sample_size and sample_size < len(buildings):
            buildings = buildings[:sample_size]
            logger.info(f"📊 Échantillon réduit à {len(buildings)} bâtiments")
        
        # Statistiques
        stats = _calculate_building_statistics(buildings)
        stats['fetch_time_seconds'] = round(fetch_time, 2)
        stats['osm_stats'] = osm_service.get_stats()
        
        # Génération de la réponse selon le format
        if download or response_format != 'json':
            # Génération de fichier pour téléchargement
            file_path, filename = _generate_download_file(
                buildings, 
                response_format, 
                include_geometry, 
                compress
            )
            
            if download:
                logger.info(f"📥 Téléchargement direct: {filename}")
                return send_file(
                    file_path,
                    as_attachment=True,
                    download_name=filename,
                    mimetype=_get_mimetype(response_format, compress)
                )
            else:
                # Retourner les informations du fichier généré
                file_size = Path(file_path).stat().st_size
                return jsonify({
                    'success': True,
                    'buildings_count': len(buildings),
                    'statistics': stats,
                    'file_info': {
                        'filename': filename,
                        'format': response_format,
                        'size_bytes': file_size,
                        'size_mb': round(file_size / (1024 * 1024), 2),
                        'compressed': compress,
                        'download_url': f'/osm/download/{filename}'
                    }
                })
        else:
            # Réponse JSON directe
            return jsonify({
                'success': True,
                'buildings_count': len(buildings),
                'statistics': stats,
                'buildings': buildings,
                'meta': {
                    'generated_at': datetime.now().isoformat(),
                    'total_fetch_time_seconds': fetch_time,
                    'includes_geometry': include_geometry,
                    'coverage': 'complete_malaysia'
                }
            })
        
    except Exception as e:
        logger.error(f"❌ Erreur récupération exhaustive: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la récupération des bâtiments',
            'details': str(e)
        }), 500


@osm_bp.route('/buildings/by-state/<state_name>', methods=['GET'])
def get_buildings_by_state(state_name):
    """
    Récupère les bâtiments pour un état spécifique de Malaysia.
    
    Path Parameters:
        state_name (str): Nom de l'état (johor, selangor, etc.)
    
    Query Parameters:
        format (str): Format de réponse
        download (bool): Télécharger directement
    """
    logger.info(f"🏛️ Demande bâtiments pour l'état: {state_name}")
    
    try:
        response_format = request.args.get('format', 'json').lower()
        download = request.args.get('download', 'false').lower() == 'true'
        
        osm_service = current_app.osm_service
        
        # Vérifier que l'état existe
        state_lower = state_name.lower().replace(' ', '_')
        if state_lower not in osm_service.malaysia_states:
            available_states = list(osm_service.malaysia_states.keys())
            raise BadRequest(f"État '{state_name}' non trouvé. États disponibles: {', '.join(available_states)}")
        
        # Récupération pour l'état
        start_time = datetime.now()
        buildings = osm_service.get_buildings_for_city(state_name)
        fetch_time = (datetime.now() - start_time).total_seconds()
        
        stats = _calculate_building_statistics(buildings)
        stats['fetch_time_seconds'] = round(fetch_time, 2)
        stats['state'] = state_name
        
        if download or response_format != 'json':
            file_path, filename = _generate_download_file(buildings, response_format, True, False)
            
            if download:
                return send_file(file_path, as_attachment=True, download_name=filename)
        
        return jsonify({
            'success': True,
            'state': state_name,
            'buildings_count': len(buildings),
            'statistics': stats,
            'buildings': buildings
        })
        
    except BadRequest as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        logger.error(f"❌ Erreur récupération état {state_name}: {e}")
        return jsonify({
            'success': False,
            'error': f'Erreur récupération état {state_name}',
            'details': str(e)
        }), 500


@osm_bp.route('/buildings/progress', methods=['GET'])
def get_progress():
    """
    Retourne le progrès de la récupération en cours.
    Utile pour les requêtes longues.
    """
    try:
        osm_service = current_app.osm_service
        stats = osm_service.get_stats()
        
        # Calculer le progrès approximatif
        total_zones = len(osm_service.malaysia_states) + 4  # États + zones supplémentaires
        progress_percent = min(100, (stats['zones_processed'] / total_zones) * 100)
        
        return jsonify({
            'success': True,
            'progress': {
                'percent': round(progress_percent, 1),
                'zones_processed': stats['zones_processed'],
                'total_zones': total_zones,
                'buildings_found': stats['total_buildings'],
                'requests_made': stats['total_requests'],
                'cache_hits': stats['cache_hits'],
                'failed_requests': stats['failed_requests']
            },
            'status': 'in_progress' if progress_percent < 100 else 'completed'
        })
        
    except Exception as e:
        logger.error(f"❌ Erreur récupération progrès: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur récupération progrès',
            'details': str(e)
        }), 500


@osm_bp.route('/cache/info', methods=['GET'])
def get_cache_info():
    """
    Retourne les informations détaillées du cache OSM.
    """
    try:
        osm_service = current_app.osm_service
        cache_info = osm_service.get_cache_info()
        stats = osm_service.get_stats()
        
        # Calculer le taux de cache hit
        total_requests = stats['cache_hits'] + stats['cache_misses']
        hit_rate = (stats['cache_hits'] / total_requests * 100) if total_requests > 0 else 0
        
        return jsonify({
            'success': True,
            'cache': cache_info,
            'performance': {
                'hit_rate_percent': round(hit_rate, 1),
                'total_requests': total_requests,
                'cache_hits': stats['cache_hits'],
                'cache_misses': stats['cache_misses']
            },
            'recommendations': _get_cache_recommendations(cache_info, stats)
        })
        
    except Exception as e:
        logger.error(f"❌ Erreur info cache: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur récupération info cache',
            'details': str(e)
        }), 500


@osm_bp.route('/cache/clear', methods=['POST'])
def clear_cache():
    """
    Vide le cache OSM.
    """
    try:
        osm_service = current_app.osm_service
        osm_service.clear_cache()
        
        return jsonify({
            'success': True,
            'message': 'Cache OSM vidé avec succès'
        })
        
    except Exception as e:
        logger.error(f"❌ Erreur vidage cache: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors du vidage du cache',
            'details': str(e)
        }), 500


@osm_bp.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    """
    Télécharge un fichier généré précédemment.
    """
    try:
        # Répertoire de téléchargement temporaire
        temp_dir = Path(tempfile.gettempdir()) / 'osm_downloads'
        file_path = temp_dir / filename
        
        if not file_path.exists():
            raise NotFound(f"Fichier '{filename}' non trouvé")
        
        # Déterminer le mimetype
        if filename.endswith('.json.gz'):
            mimetype = 'application/gzip'
        elif filename.endswith('.json'):
            mimetype = 'application/json'
        elif filename.endswith('.csv.gz'):
            mimetype = 'application/gzip'
        elif filename.endswith('.csv'):
            mimetype = 'text/csv'
        elif filename.endswith('.parquet'):
            mimetype = 'application/octet-stream'
        elif filename.endswith('.xlsx'):
            mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        else:
            mimetype = 'application/octet-stream'
        
        logger.info(f"📥 Téléchargement fichier: {filename}")
        return send_file(
            file_path,
            as_attachment=True,
            download_name=filename,
            mimetype=mimetype
        )
        
    except NotFound as e:
        return jsonify({'success': False, 'error': str(e)}), 404
    except Exception as e:
        logger.error(f"❌ Erreur téléchargement {filename}: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors du téléchargement',
            'details': str(e)
        }), 500


@osm_bp.route('/stats', methods=['GET'])
def get_stats():
    """
    Retourne les statistiques complètes du service OSM.
    """
    try:
        osm_service = current_app.osm_service
        stats = osm_service.get_stats()
        cache_info = osm_service.get_cache_info()
        
        # Enrichir avec des métriques calculées
        if stats['start_time'] and stats['end_time']:
            total_time = (stats['end_time'] - stats['start_time']).total_seconds()
            buildings_per_second = stats['total_buildings'] / total_time if total_time > 0 else 0
        else:
            total_time = 0
            buildings_per_second = 0
        
        return jsonify({
            'success': True,
            'statistics': {
                'performance': {
                    'total_buildings': stats['total_buildings'],
                    'total_time_seconds': round(total_time, 2),
                    'buildings_per_second': round(buildings_per_second, 2),
                    'zones_processed': stats['zones_processed'],
                    'total_requests': stats['total_requests'],
                    'parallel_requests': stats['parallel_requests']
                },
                'cache': {
                    'enabled': cache_info['cache_enabled'],
                    'hits': stats['cache_hits'],
                    'misses': stats['cache_misses'],
                    'hit_rate_percent': round(
                        stats['cache_hits'] / (stats['cache_hits'] + stats['cache_misses']) * 100
                        if (stats['cache_hits'] + stats['cache_misses']) > 0 else 0, 1
                    ),
                    'files_count': cache_info['files_count'],
                    'total_size_mb': cache_info['total_size_mb']
                },
                'errors': {
                    'failed_requests': stats['failed_requests'],
                    'error_rate_percent': round(
                        stats['failed_requests'] / stats['total_requests'] * 100
                        if stats['total_requests'] > 0 else 0, 1
                    )
                }
            },
            'system_info': {
                'available_states': list(osm_service.malaysia_states.keys()),
                'overpass_urls': osm_service.overpass_urls,
                'max_concurrent_requests': osm_service.max_concurrent_requests,
                'timeout_seconds': osm_service.timeout
            }
        })
        
    except Exception as e:
        logger.error(f"❌ Erreur récupération stats: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur récupération statistiques',
            'details': str(e)
        }), 500


# Fonctions utilitaires

def _calculate_building_statistics(buildings: list) -> dict:
    """Calcule les statistiques des bâtiments."""
    if not buildings:
        return {
            'total_count': 0,
            'by_type': {},
            'by_state': {},
            'has_geometry': 0,
            'average_area_sqm': 0
        }
    
    # Statistiques par type
    type_counts = {}
    state_counts = {}
    geometry_count = 0
    total_area = 0
    area_count = 0
    
    for building in buildings:
        # Par type
        building_type = building.get('building_type', 'unknown')
        type_counts[building_type] = type_counts.get(building_type, 0) + 1
        
        # Par état (approximatif depuis addr_city)
        state = building.get('addr_city', 'unknown')
        state_counts[state] = state_counts.get(state, 0) + 1
        
        # Géométrie
        if building.get('geometry_type') or building.get('coordinates'):
            geometry_count += 1
        
        # Surface
        area = building.get('area_sqm')
        if area and area > 0:
            total_area += area
            area_count += 1
    
    return {
        'total_count': len(buildings),
        'by_type': type_counts,
        'by_state': state_counts,
        'has_geometry': geometry_count,
        'geometry_percentage': round(geometry_count / len(buildings) * 100, 1),
        'average_area_sqm': round(total_area / area_count, 1) if area_count > 0 else 0,
        'buildings_with_area': area_count
    }


def _generate_download_file(buildings: list, format_type: str, include_geometry: bool, compress: bool) -> tuple:
    """Génère un fichier de téléchargement dans le format demandé."""
    temp_dir = Path(tempfile.gettempdir()) / 'osm_downloads'
    temp_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_filename = f"malaysia_buildings_{timestamp}"
    
    if format_type == 'json':
        filename = f"{base_filename}.json"
        if compress:
            filename += ".gz"
        file_path = temp_dir / filename
        
        data = {
            'buildings': buildings,
            'count': len(buildings),
            'generated_at': datetime.now().isoformat(),
            'includes_geometry': include_geometry
        }
        
        if compress:
            with gzip.open(file_path, 'wt', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        else:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
    
    elif format_type == 'geojson':
        filename = f"{base_filename}.geojson"
        if compress:
            filename += ".gz"
        file_path = temp_dir / filename
        
        # Convertir en GeoJSON
        features = []
        for building in buildings:
            if building.get('coordinates') and include_geometry:
                geometry = {
                    "type": "Polygon" if building.get('geometry_type') == 'polygon' else "Point",
                    "coordinates": building['coordinates'] if building.get('geometry_type') == 'polygon' else [building.get('longitude', 0), building.get('latitude', 0)]
                }
            else:
                geometry = {
                    "type": "Point",
                    "coordinates": [building.get('longitude', 0), building.get('latitude', 0)]
                }
            
            feature = {
                "type": "Feature",
                "geometry": geometry,
                "properties": {k: v for k, v in building.items() if k not in ['coordinates', 'latitude', 'longitude']}
            }
            features.append(feature)
        
        geojson_data = {
            "type": "FeatureCollection",
            "features": features
        }
        
        if compress:
            with gzip.open(file_path, 'wt', encoding='utf-8') as f:
                json.dump(geojson_data, f, ensure_ascii=False)
        else:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(geojson_data, f, ensure_ascii=False)
    
    elif format_type == 'csv':
        filename = f"{base_filename}.csv"
        if compress:
            filename += ".gz"
        file_path = temp_dir / filename
        
        # Convertir en DataFrame
        df_data = []
        for building in buildings:
            row = building.copy()
            # Aplatir les coordonnées si pas de géométrie complète
            if not include_geometry and 'coordinates' in row:
                del row['coordinates']
            if 'tags' in row:
                row['tags'] = json.dumps(row['tags'])
            df_data.append(row)
        
        df = pd.DataFrame(df_data)
        
        if compress:
            df.to_csv(file_path, index=False, encoding='utf-8', compression='gzip')
        else:
            df.to_csv(file_path, index=False, encoding='utf-8')
    
    elif format_type == 'parquet':
        filename = f"{base_filename}.parquet"
        file_path = temp_dir / filename
        
        # Convertir en DataFrame pour Parquet
        df_data = []
        for building in buildings:
            row = building.copy()
            # Convertir les objets complexes en JSON strings pour Parquet
            if 'coordinates' in row and not include_geometry:
                del row['coordinates']
            if 'tags' in row:
                row['tags'] = json.dumps(row['tags'])
            df_data.append(row)
        
        df = pd.DataFrame(df_data)
        df.to_parquet(file_path, index=False)
    
    elif format_type == 'xlsx':
        filename = f"{base_filename}.xlsx"
        file_path = temp_dir / filename
        
        # Convertir en DataFrame pour Excel
        df_data = []
        for building in buildings:
            row = building.copy()
            if 'coordinates' in row and not include_geometry:
                del row['coordinates']
            if 'tags' in row:
                row['tags'] = json.dumps(row['tags'])
            df_data.append(row)
        
        df = pd.DataFrame(df_data)
        df.to_excel(file_path, index=False)
    
    return file_path, filename


def _get_mimetype(format_type: str, compress: bool) -> str:
    """Retourne le mimetype approprié."""
    mimetypes = {
        'json': 'application/json',
        'geojson': 'application/geo+json',
        'csv': 'text/csv',
        'parquet': 'application/octet-stream',
        'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }
    
    if compress and format_type in ['json', 'geojson', 'csv']:
        return 'application/gzip'
    
    return mimetypes.get(format_type, 'application/octet-stream')


def _get_cache_recommendations(cache_info: dict, stats: dict) -> list:
    """Génère des recommandations pour optimiser le cache."""
    recommendations = []
    
    total_requests = stats['cache_hits'] + stats['cache_misses']
    
    if total_requests == 0:
        recommendations.append("Aucune requête effectuée pour le moment")
        return recommendations
    
    hit_rate = stats['cache_hits'] / total_requests * 100
    
    if hit_rate < 30:
        recommendations.append("🔴 Taux de cache faible (<30%) - Considérer augmenter la durée de cache")
    elif hit_rate < 60:
        recommendations.append("🟡 Taux de cache moyen (30-60%) - Performances correctes")
    else:
        recommendations.append("🟢 Excellent taux de cache (>60%) - Optimisation réussie")
    
    if cache_info['files_count'] > 1000:
        recommendations.append("⚠️ Beaucoup de fichiers en cache - Considérer un nettoyage périodique")
    
    if cache_info['total_size_mb'] > 1000:
        recommendations.append("💾 Cache volumineux (>1GB) - Surveiller l'espace disque")
    
    if stats['failed_requests'] > total_requests * 0.1:
        recommendations.append("🔥 Taux d'échec élevé (>10%) - Vérifier la connectivité Overpass")
    
    return recommendations