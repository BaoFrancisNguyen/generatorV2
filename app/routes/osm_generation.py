#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROUTES DE GÉNÉRATION OSM ULTRA-OPTIMISÉES - GÉNÉRATEUR MALAYSIA
Fichier: app/routes/osm_generation.py

Routes optimisées pour génération de données énergétiques basées sur OSM:
- Génération exhaustive Malaysia
- Preview rapide avec échantillonnage
- Export direct avec tous les formats
- Monitoring en temps réel

Auteur: Équipe Développement
Date: 2025
Version: 4.0 - Ultra-optimisé pour récupération complète
"""

import logging
import asyncio
from datetime import datetime
from flask import Blueprint, request, jsonify, current_app, send_file
from werkzeug.exceptions import BadRequest, NotFound
import pandas as pd
import tempfile
from pathlib import Path

from app.utils.validators import validate_osm_request, validate_generation_request

# Créer le blueprint pour les routes de génération OSM
osm_generation_bp = Blueprint('osm_generation', __name__)
logger = logging.getLogger(__name__)


@osm_generation_bp.route('/preview/all', methods=['GET'])
def preview_all_malaysia():
    """
    🚀 PREVIEW ULTRA-RAPIDE: Aperçu de TOUS les bâtiments Malaysia.
    
    Utilise un échantillonnage intelligent pour donner un aperçu rapide
    de la distribution des bâtiments sans récupérer toutes les données.
    
    Query Parameters:
        sample_size (int): Taille de l'échantillon (défaut: 1000)
        include_stats (bool): Inclure les statistiques détaillées
        include_map_data (bool): Inclure les données pour la carte
    
    Returns:
        JSON avec aperçu des bâtiments et estimations
    """
    logger.info("👀 Preview ULTRA-RAPIDE Malaysia")
    
    try:
        # Paramètres
        sample_size = request.args.get('sample_size', type=int, default=1000)
        include_stats = request.args.get('include_stats', 'true').lower() == 'true'
        include_map_data = request.args.get('include_map_data', 'true').lower() == 'true'
        
        # Validation
        if sample_size < 10 or sample_size > 10000:
            raise BadRequest("sample_size doit être entre 10 et 10000")
        
        osm_service = current_app.osm_service
        
        # STRATÉGIE: Récupération par échantillonnage de zones
        start_time = datetime.now()
        
        # Sélectionner quelques états représentatifs pour l'échantillon
        sample_states = ['selangor', 'johor', 'penang', 'kuala_lumpur', 'sabah']
        
        logger.info(f"🎯 Échantillonnage sur {len(sample_states)} états...")
        
        sample_buildings = []
        states_data = {}
        
        for state in sample_states:
            try:
                state_buildings = osm_service.get_buildings_for_city(state, limit=sample_size // len(sample_states))
                sample_buildings.extend(state_buildings)
                states_data[state] = {
                    'count': len(state_buildings),
                    'sample_taken': True
                }
                logger.info(f"✅ {state}: {len(state_buildings)} bâtiments échantillonnés")
                
            except Exception as e:
                logger.warning(f"⚠️ Échec échantillonnage {state}: {e}")
                states_data[state] = {
                    'count': 0,
                    'sample_taken': False,
                    'error': str(e)
                }
        
        preview_time = (datetime.now() - start_time).total_seconds()
        
        # Limiter l'échantillon à la taille demandée
        if len(sample_buildings) > sample_size:
            sample_buildings = sample_buildings[:sample_size]
        
        # Calculs d'estimation
        total_estimated = _estimate_total_buildings_malaysia(sample_buildings, states_data)
        
        # Statistiques de l'échantillon
        sample_stats = _calculate_sample_statistics(sample_buildings) if include_stats else {}
        
        # Données pour la carte
        map_data = _prepare_map_data(sample_buildings) if include_map_data else []
        
        # Estimation du temps de récupération complète
        estimated_full_time = _estimate_full_retrieval_time(total_estimated, preview_time)
        
        response = {
            'success': True,
            'preview': {
                'sample_size': len(sample_buildings),
                'sample_time_seconds': round(preview_time, 2),
                'states_sampled': len([s for s in states_data.values() if s['sample_taken']]),
                'states_data': states_data
            },
            'estimations': {
                'total_buildings_estimated': total_estimated,
                'confidence_level': 'medium',  # Basé sur échantillonnage partiel
                'full_retrieval_time_estimated_seconds': estimated_full_time,
                'full_retrieval_time_estimated_minutes': round(estimated_full_time / 60, 1)
            },
            'sample_statistics': sample_stats,
            'map_data': map_data,
            'recommendations': _get_retrieval_recommendations(total_estimated, estimated_full_time),
            'ready_for_full_retrieval': True
        }
        
        logger.info(f"👀 Preview terminé: {len(sample_buildings)} échantillons, {total_estimated} estimés")
        return jsonify(response)
        
    except BadRequest as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        logger.error(f"❌ Erreur preview: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors du preview',
            'details': str(e)
        }), 500


@osm_generation_bp.route('/generate/all', methods=['POST'])
def generate_all_malaysia():
    """
    🚀 GÉNÉRATION EXHAUSTIVE: Récupère TOUS les bâtiments et génère les données énergétiques.
    
    Body JSON:
    {
        "export_format": "parquet|csv|excel|json",
        "download_immediately": true|false,
        "energy_params": {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "frequency": "D",
            "include_weather": true,
            "custom_profiles": {...}
        },
        "building_filters": {
            "types": ["residential", "commercial"],
            "min_area": 50,
            "states": ["selangor", "johor"]
        }
    }
    """
    logger.info("🚀 Génération EXHAUSTIVE Malaysia")
    
    try:
        # Validation de la requête
        if not request.is_json:
            raise BadRequest("Content-Type application/json requis")
        
        data = request.get_json()
        if not data:
            raise BadRequest("Corps JSON requis")
        
        # Paramètres par défaut
        export_format = data.get('export_format', 'parquet').lower()
        download_immediately = data.get('download_immediately', False)
        energy_params = data.get('energy_params', {})
        building_filters = data.get('building_filters', {})
        
        # Validation des paramètres
        valid_formats = ['parquet', 'csv', 'excel', 'json']
        if export_format not in valid_formats:
            raise BadRequest(f"Format invalide. Formats supportés: {', '.join(valid_formats)}")
        
        osm_service = current_app.osm_service
        generation_service = current_app.generation_service
        
        # ÉTAPE 1: Récupération exhaustive des bâtiments OSM
        logger.info("🏢 ÉTAPE 1: Récupération exhaustive des bâtiments...")
        
        buildings_start_time = datetime.now()
        all_buildings = osm_service.get_all_buildings_malaysia()
        buildings_fetch_time = (datetime.now() - buildings_start_time).total_seconds()
        
        logger.info(f"✅ Récupération terminée: {len(all_buildings)} bâtiments en {buildings_fetch_time:.1f}s")
        
        # ÉTAPE 2: Filtrage des bâtiments
        filtered_buildings = _apply_building_filters(all_buildings, building_filters)
        logger.info(f"🔍 Filtrage: {len(filtered_buildings)} bâtiments retenus (sur {len(all_buildings)})")
        
        # ÉTAPE 3: Génération des données énergétiques
        logger.info("⚡ ÉTAPE 2: Génération des données énergétiques...")
        
        generation_start_time = datetime.now()
        
        # Préparer les paramètres de génération
        generation_config = {
            'buildings': filtered_buildings,
            'start_date': energy_params.get('start_date', '2024-01-01'),
            'end_date': energy_params.get('end_date', '2024-12-31'),
            'frequency': energy_params.get('frequency', 'D'),
            'include_weather': energy_params.get('include_weather', True),
            'custom_profiles': energy_params.get('custom_profiles', {}),
            'source': 'osm_exhaustive'
        }
        
        # Générer les données énergétiques
        energy_dataset = generation_service.generate_from_buildings(generation_config)
        
        generation_time = (datetime.now() - generation_start_time).total_seconds()
        logger.info(f"⚡ Génération terminée en {generation_time:.1f}s")
        
        # ÉTAPE 3: Export et réponse
        total_time = buildings_fetch_time + generation_time
        
        response_data = {
            'success': True,
            'process_summary': {
                'total_time_seconds': round(total_time, 2),
                'total_time_minutes': round(total_time / 60, 2),
                'buildings_fetched': len(all_buildings),
                'buildings_used': len(filtered_buildings),
                'buildings_fetch_time_seconds': round(buildings_fetch_time, 2),
                'energy_generation_time_seconds': round(generation_time, 2)
            },
            'dataset_info': {
                'buildings_count': len(energy_dataset['buildings']),
                'timeseries_records': len(energy_dataset['timeseries']),
                'date_range': {
                    'start': generation_config['start_date'],
                    'end': generation_config['end_date']
                },
                'frequency': generation_config['frequency']
            },
            'osm_stats': osm_service.get_stats(),
            'generated_at': datetime.now().isoformat()
        }
        
        # Export selon le format
        if export_format != 'json' or download_immediately:
            export_result = _export_energy_dataset(energy_dataset, export_format, generation_config)
            
            response_data['export'] = {
                'format': export_format,
                'files': export_result['files'],
                'total_size_bytes': export_result['total_size'],
                'total_size_mb': round(export_result['total_size'] / (1024 * 1024), 2)
            }
            
            if download_immediately and export_result['files']:
                first_file = export_result['files'][0]
                logger.info(f"📥 Téléchargement immédiat: {first_file['name']}")
                return send_file(
                    first_file['path'],
                    as_attachment=True,
                    download_name=first_file['name']
                )
        else:
            # Inclure les données dans la réponse JSON
            response_data['data'] = {
                'buildings': energy_dataset['buildings'].to_dict('records') if hasattr(energy_dataset['buildings'], 'to_dict') else energy_dataset['buildings'],
                'timeseries': energy_dataset['timeseries'].to_dict('records') if hasattr(energy_dataset['timeseries'], 'to_dict') else energy_dataset['timeseries']
            }
        
        logger.info(f"🎉 Génération exhaustive terminée: {len(filtered_buildings)} bâtiments en {total_time:.1f}s")
        return jsonify(response_data)
        
    except BadRequest as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        logger.error(f"❌ Erreur génération exhaustive: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la génération exhaustive',
            'details': str(e)
        }), 500


@osm_generation_bp.route('/generate/by-state/<state_name>', methods=['POST'])
def generate_by_state(state_name):
    """
    Génère les données énergétiques pour un état spécifique.
    
    Path Parameters:
        state_name (str): Nom de l'état Malaysia
    """
    logger.info(f"🏛️ Génération pour l'état: {state_name}")
    
    try:
        data = request.get_json() or {}
        
        export_format = data.get('export_format', 'parquet').lower()
        download_immediately = data.get('download_immediately', False)
        energy_params = data.get('energy_params', {})
        
        osm_service = current_app.osm_service
        generation_service = current_app.generation_service
        
        # Récupération pour l'état
        buildings = osm_service.get_buildings_for_city(state_name)
        
        if not buildings:
            return jsonify({
                'success': False,
                'error': f'Aucun bâtiment trouvé pour {state_name}'
            }), 404
        
        # Génération énergétique
        generation_config = {
            'buildings': buildings,
            'start_date': energy_params.get('start_date', '2024-01-01'),
            'end_date': energy_params.get('end_date', '2024-12-31'),
            'frequency': energy_params.get('frequency', 'D'),
            'source': f'osm_state_{state_name}'
        }
        
        energy_dataset = generation_service.generate_from_buildings(generation_config)
        
        # Export et réponse
        if export_format != 'json' or download_immediately:
            export_result = _export_energy_dataset(energy_dataset, export_format, generation_config)
            
            if download_immediately and export_result['files']:
                first_file = export_result['files'][0]
                return send_file(first_file['path'], as_attachment=True, download_name=first_file['name'])
        
        return jsonify({
            'success': True,
            'state': state_name,
            'buildings_count': len(buildings),
            'dataset_info': {
                'buildings_count': len(energy_dataset['buildings']),
                'timeseries_records': len(energy_dataset['timeseries'])
            }
        })
        
    except Exception as e:
        logger.error(f"❌ Erreur génération état {state_name}: {e}")
        return jsonify({
            'success': False,
            'error': f'Erreur génération {state_name}',
            'details': str(e)
        }), 500


@osm_generation_bp.route('/status', methods=['GET'])
def get_generation_status():
    """
    Retourne le statut en temps réel de la génération en cours.
    """
    try:
        osm_service = current_app.osm_service
        osm_stats = osm_service.get_stats()
        
        # Déterminer le statut
        if osm_stats['start_time'] and not osm_stats['end_time']:
            status = 'in_progress'
        elif osm_stats['end_time']:
            status = 'completed'
        else:
            status = 'idle'
        
        # Calculer le progrès
        total_zones = len(osm_service.malaysia_states) + 4
        progress_percent = min(100, (osm_stats['zones_processed'] / total_zones) * 100) if total_zones > 0 else 0
        
        return jsonify({
            'success': True,
            'status': status,
            'progress': {
                'percent': round(progress_percent, 1),
                'zones_processed': osm_stats['zones_processed'],
                'total_zones': total_zones,
                'buildings_found': osm_stats['total_buildings'],
                'current_phase': _determine_current_phase(osm_stats)
            },
            'performance': {
                'requests_made': osm_stats['total_requests'],
                'parallel_requests': osm_stats['parallel_requests'],
                'cache_hits': osm_stats['cache_hits'],
                'failed_requests': osm_stats['failed_requests']
            },
            'timing': _calculate_timing_info(osm_stats)
        })
        
    except Exception as e:
        logger.error(f"❌ Erreur statut génération: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur récupération statut',
            'details': str(e)
        }), 500


# Fonctions utilitaires

def _estimate_total_buildings_malaysia(sample_buildings: list, states_data: dict) -> int:
    """Estime le nombre total de bâtiments en Malaysia."""
    if not sample_buildings:
        return 0
    
    # Estimation basée sur la densité de l'échantillon
    successful_states = [s for s in states_data.values() if s['sample_taken']]
    
    if not successful_states:
        return len(sample_buildings) * 50  # Estimation très approximative
    
    # Moyenne de bâtiments par état échantillonné
    avg_per_sampled_state = sum(s['count'] for s in successful_states) / len(successful_states)
    
    # Estimation pour tous les états (15 états + territoires fédéraux)
    total_states = 15
    
    # Facteur de correction pour les états non échantillonnés
    correction_factor = 1.2  # Les petits états ont généralement moins de bâtiments
    
    estimated_total = int(avg_per_sampled_state * total_states * correction_factor)
    
    # Limites de sécurité
    min_estimate = len(sample_buildings) * 10
    max_estimate = 2000000  # 2 millions max
    
    return max(min_estimate, min(estimated_total, max_estimate))


def _estimate_full_retrieval_time(total_buildings: int, sample_time: float) -> float:
    """Estime le temps de récupération complète."""
    if total_buildings <= 0:
        return 0
    
    # Estimation basée sur la performance de l'échantillon
    # Facteur de parallélisation (10 requêtes simultanées)
    parallelization_factor = 0.3  # 70% de gain grâce au parallélisme
    
    # Facteur de cache (amélioration avec le cache)
    cache_factor = 0.7  # 30% de gain grâce au cache
    
    # Estimation de base
    base_estimate = sample_time * (total_buildings / 1000) * parallelization_factor * cache_factor
    
    # Minimum de 30 secondes, maximum de 30 minutes
    return max(30, min(base_estimate, 1800))


def _calculate_sample_statistics(sample_buildings: list) -> dict:
    """Calcule les statistiques de l'échantillon."""
    if not sample_buildings:
        return {}
    
    # Distribution par type
    types_count = {}
    areas = []
    states_count = {}
    
    for building in sample_buildings:
        # Types
        building_type = building.get('building_type', 'unknown')
        types_count[building_type] = types_count.get(building_type, 0) + 1
        
        # Aires
        area = building.get('area_sqm')
        if area and area > 0:
            areas.append(area)
        
        # États
        state = building.get('addr_city', 'unknown')
        states_count[state] = states_count.get(state, 0) + 1
    
    return {
        'types_distribution': types_count,
        'states_distribution': states_count,
        'area_statistics': {
            'count_with_area': len(areas),
            'average_area_sqm': round(sum(areas) / len(areas), 1) if areas else 0,
            'min_area_sqm': min(areas) if areas else 0,
            'max_area_sqm': max(areas) if areas else 0
        }
    }


def _prepare_map_data(sample_buildings: list) -> list:
    """Prépare les données pour l'affichage sur la carte."""
    map_data = []
    
    for building in sample_buildings[:100]:  # Limiter à 100 pour la carte
        if building.get('latitude') and building.get('longitude'):
            map_data.append({
                'lat': building['latitude'],
                'lng': building['longitude'],
                'type': building.get('building_type', 'unknown'),
                'name': building.get('name', 'Bâtiment sans nom'),
                'area': building.get('area_sqm')
            })
    
    return map_data


def _get_retrieval_recommendations(total_estimated: int, estimated_time: float) -> list:
    """Génère des recommandations pour la récupération."""
    recommendations = []
    
    if total_estimated > 500000:
        recommendations.append("🔶 Volume très important (>500k bâtiments) - Prévoir du temps")
    elif total_estimated > 100000:
        recommendations.append("🔸 Volume important (>100k bâtiments) - Récupération recommandée")
    else:
        recommendations.append("🟢 Volume modéré - Récupération rapide")
    
    if estimated_time > 600:  # Plus de 10 minutes
        recommendations.append("⏰ Temps estimé long - Lancer en arrière-plan")
        recommendations.append("💾 Utiliser le cache pour les prochaines fois")
    elif estimated_time > 120:  # Plus de 2 minutes
        recommendations.append("⌛ Temps modéré - Patience recommandée")
    else:
        recommendations.append("⚡ Récupération rapide attendue")
    
    recommendations.append("📊 Utiliser l'endpoint /status pour suivre le progrès")
    recommendations.append("💾 Les données seront automatiquement mises en cache")
    
    return recommendations


def _apply_building_filters(buildings: list, filters: dict) -> list:
    """Applique les filtres aux bâtiments."""
    if not filters:
        return buildings
    
    filtered = buildings
    
    # Filtre par types
    if 'types' in filters and filters['types']:
        allowed_types = set(filters['types'])
        filtered = [b for b in filtered if b.get('building_type') in allowed_types]
    
    # Filtre par surface minimale
    if 'min_area' in filters and filters['min_area']:
        min_area = filters['min_area']
        filtered = [b for b in filtered if b.get('area_sqm', 0) >= min_area]
    
    # Filtre par états
    if 'states' in filters and filters['states']:
        allowed_states = set(s.lower().replace(' ', '_') for s in filters['states'])
        filtered = [b for b in filtered if any(state in str(b.get('addr_city', '')).lower() for state in allowed_states)]
    
    return filtered


def _export_energy_dataset(dataset: dict, format_type: str, config: dict) -> dict:
    """Exporte le dataset énergétique dans le format demandé."""
    temp_dir = Path(tempfile.gettempdir()) / 'energy_exports'
    temp_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"malaysia_energy_{timestamp}"
    
    exported_files = []
    total_size = 0
    
    try:
        if format_type == 'parquet':
            # Export Parquet (recommandé pour gros volumes)
            buildings_file = temp_dir / f"{base_name}_buildings.parquet"
            timeseries_file = temp_dir / f"{base_name}_timeseries.parquet"
            
            if hasattr(dataset['buildings'], 'to_parquet'):
                dataset['buildings'].to_parquet(buildings_file, index=False)
                dataset['timeseries'].to_parquet(timeseries_file, index=False)
            else:
                # Si ce sont des listes, convertir en DataFrame
                pd.DataFrame(dataset['buildings']).to_parquet(buildings_file, index=False)
                pd.DataFrame(dataset['timeseries']).to_parquet(timeseries_file, index=False)
            
            exported_files.extend([
                {'name': f"{base_name}_buildings.parquet", 'path': buildings_file, 'type': 'buildings'},
                {'name': f"{base_name}_timeseries.parquet", 'path': timeseries_file, 'type': 'timeseries'}
            ])
        
        elif format_type == 'csv':
            # Export CSV
            buildings_file = temp_dir / f"{base_name}_buildings.csv"
            timeseries_file = temp_dir / f"{base_name}_timeseries.csv"
            
            if hasattr(dataset['buildings'], 'to_csv'):
                dataset['buildings'].to_csv(buildings_file, index=False)
                dataset['timeseries'].to_csv(timeseries_file, index=False)
            else:
                pd.DataFrame(dataset['buildings']).to_csv(buildings_file, index=False)
                pd.DataFrame(dataset['timeseries']).to_csv(timeseries_file, index=False)
            
            exported_files.extend([
                {'name': f"{base_name}_buildings.csv", 'path': buildings_file, 'type': 'buildings'},
                {'name': f"{base_name}_timeseries.csv", 'path': timeseries_file, 'type': 'timeseries'}
            ])
        
        elif format_type == 'excel':
            # Export Excel avec onglets multiples
            excel_file = temp_dir / f"{base_name}_complete.xlsx"
            
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                if hasattr(dataset['buildings'], 'to_excel'):
                    dataset['buildings'].to_excel(writer, sheet_name='Buildings', index=False)
                    dataset['timeseries'].to_excel(writer, sheet_name='Timeseries', index=False)
                else:
                    pd.DataFrame(dataset['buildings']).to_excel(writer, sheet_name='Buildings', index=False)
                    pd.DataFrame(dataset['timeseries']).to_excel(writer, sheet_name='Timeseries', index=False)
                
                # Ajouter une feuille de métadonnées
                metadata_df = pd.DataFrame([
                    ['Generated at', datetime.now().isoformat()],
                    ['Source', config.get('source', 'osm')],
                    ['Buildings count', len(dataset['buildings'])],
                    ['Date range', f"{config.get('start_date')} to {config.get('end_date')}"],
                    ['Frequency', config.get('frequency', 'D')]
                ], columns=['Parameter', 'Value'])
                metadata_df.to_excel(writer, sheet_name='Metadata', index=False)
            
            exported_files.append({
                'name': f"{base_name}_complete.xlsx",
                'path': excel_file,
                'type': 'complete'
            })
        
        elif format_type == 'json':
            # Export JSON
            json_file = temp_dir / f"{base_name}_complete.json"
            
            export_data = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'source': config.get('source', 'osm'),
                    'buildings_count': len(dataset['buildings']),
                    'config': config
                },
                'buildings': dataset['buildings'].to_dict('records') if hasattr(dataset['buildings'], 'to_dict') else dataset['buildings'],
                'timeseries': dataset['timeseries'].to_dict('records') if hasattr(dataset['timeseries'], 'to_dict') else dataset['timeseries']
            }
            
            import json
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)
            
            exported_files.append({
                'name': f"{base_name}_complete.json",
                'path': json_file,
                'type': 'complete'
            })
        
        # Calculer la taille totale
        for file_info in exported_files:
            file_size = Path(file_info['path']).stat().st_size
            file_info['size_bytes'] = file_size
            file_info['size_mb'] = round(file_size / (1024 * 1024), 2)
            total_size += file_size
        
        return {
            'files': exported_files,
            'total_size': total_size,
            'format': format_type
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur export {format_type}: {e}")
        raise


def _determine_current_phase(osm_stats: dict) -> str:
    """Détermine la phase actuelle du processus."""
    if not osm_stats['start_time']:
        return 'idle'
    elif not osm_stats['end_time']:
        if osm_stats['zones_processed'] == 0:
            return 'initializing'
        elif osm_stats['zones_processed'] < 5:
            return 'fetching_major_states'
        else:
            return 'fetching_remaining_zones'
    else:
        return 'completed'


def _calculate_timing_info(osm_stats: dict) -> dict:
    """Calcule les informations de timing."""
    timing_info = {}
    
    if osm_stats['start_time']:
        start_time = osm_stats['start_time']
        end_time = osm_stats['end_time'] or datetime.now()
        
        elapsed = (end_time - start_time).total_seconds()
        
        timing_info.update({
            'elapsed_seconds': round(elapsed, 1),
            'elapsed_minutes': round(elapsed / 60, 1),
            'started_at': start_time.isoformat(),
            'ended_at': end_time.isoformat() if osm_stats['end_time'] else None
        })
        
        # Estimation du temps restant
        if not osm_stats['end_time'] and osm_stats['zones_processed'] > 0:
            total_zones = 19  # 15 états + 4 zones supplémentaires
            avg_time_per_zone = elapsed / osm_stats['zones_processed']
            remaining_zones = total_zones - osm_stats['zones_processed']
            estimated_remaining = remaining_zones * avg_time_per_zone
            
            timing_info.update({
                'estimated_remaining_seconds': round(estimated_remaining, 1),
                'estimated_remaining_minutes': round(estimated_remaining / 60, 1),
                'estimated_completion': (datetime.now() + pd.Timedelta(seconds=estimated_remaining)).isoformat()
            })
    
    return timing_info