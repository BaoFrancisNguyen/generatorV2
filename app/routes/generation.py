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
- Téléchargement de fichiers générés

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Routes modulaires
"""

import logging
import os
from datetime import datetime
from pathlib import Path
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
        "export_format": "parquet",
        "download_immediately": false,
        "return_data": true
    }
    
    Returns:
        JSON avec les données générées ou fichier en téléchargement
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
            'num_buildings': data.get('num_buildings', current_app.config.get('DEFAULT_NUM_BUILDINGS', 100)),
            'start_date': data.get('start_date', current_app.config.get('DEFAULT_START_DATE', '2024-01-01')),
            'end_date': data.get('end_date', current_app.config.get('DEFAULT_END_DATE', '2024-01-31')),
            'frequency': data.get('frequency', current_app.config.get('DEFAULT_FREQUENCY', 'D')),
            'location_filter': data.get('location_filter'),
            'building_types': data.get('building_types')
        }
        
        logger.info(f"📊 Génération: {generation_params['num_buildings']} bâtiments, "
                   f"{generation_params['start_date']} → {generation_params['end_date']}")
        
        # Mesurer le temps de génération
        start_time = datetime.now()
        
        # Générer le dataset
        dataset = current_app.data_generator.generate_complete_dataset(**generation_params)
        
        # Calculer les statistiques
        generation_duration = (datetime.now() - start_time).total_seconds()
        
        # Préparer la réponse de base
        response_data = {
            'success': True,
            'generation_time': round(generation_duration, 3),
            'parameters': generation_params,
            'statistics': {
                'buildings_generated': len(dataset['buildings']),
                'timeseries_observations': len(dataset['timeseries']),
                'period_days': (datetime.strptime(generation_params['end_date'], '%Y-%m-%d') - 
                              datetime.strptime(generation_params['start_date'], '%Y-%m-%d')).days,
                'frequency': generation_params['frequency']
            },
            'metadata': dataset.get('metadata', {})
        }
        
        # Gestion de l'export et du téléchargement
        export_format = data.get('export_format', 'json')
        download_immediately = data.get('download_immediately', False)
        return_data = data.get('return_data', True)
        
        if export_format != 'json' or not return_data:
            # Export vers fichier
            logger.info(f"📁 Export en format {export_format}")
            
            export_result = current_app.export_service.export_dataset(
                dataset, 
                export_format,
                include_metadata=True,
                filename_prefix='malaysia_energy'
            )
            
            response_data['export'] = {
                'format': export_format,
                'files': export_result['files'],
                'total_size_bytes': export_result['total_size'],
                'total_size_mb': round(export_result['total_size'] / (1024 * 1024), 2),
                'download_urls': [f"/generate/download/{file['name']}" for file in export_result['files']]
            }
            
            # Si téléchargement immédiat demandé
            if download_immediately and export_result['files']:
                first_file = export_result['files'][0]
                logger.info(f"📥 Téléchargement immédiat: {first_file['name']}")
                return send_file(
                    first_file['path'],
                    as_attachment=True,
                    download_name=first_file['name']
                )
            
            return jsonify(response_data)
        else:
            # Retour JSON standard avec données incluses
            response_data['data'] = {
                'buildings': dataset['buildings'].to_dict('records'),
                'timeseries': dataset['timeseries'].to_dict('records')
            }
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
        
        osm_buildings = []
        if 'city' in data:
            # Récupération par ville
            osm_buildings = current_app.osm_service.get_buildings_for_city(
                city_name=data['city'],
                limit=data.get('limit', 1000)
            )
        elif 'bbox' in data:
            # Récupération par bbox
            osm_buildings = current_app.osm_service.get_buildings_in_bbox(
                bbox=data['bbox'],
                limit=data.get('limit', 1000)
            )
        
        if not osm_buildings:
            return jsonify({
                'success': False,
                'error': 'Aucun bâtiment trouvé',
                'message': 'Aucun bâtiment OSM trouvé pour les critères spécifiés'
            }), 404
        
        logger.info(f"🏢 {len(osm_buildings)} bâtiments OSM récupérés")
        
        # Générer les données énergétiques pour ces bâtiments
        generation_params = {
            'start_date': data.get('start_date', '2024-01-01'),
            'end_date': data.get('end_date', '2024-01-31'),
            'frequency': data.get('frequency', 'D')
        }
        
        # Convertir les bâtiments OSM en dataset
        dataset = current_app.data_generator.generate_from_osm_buildings(
            osm_buildings=osm_buildings,
            **generation_params
        )
        
        generation_duration = (datetime.now() - start_time).total_seconds()
        
        # Préparer la réponse
        response_data = {
            'success': True,
            'generation_time': round(generation_duration, 3),
            'osm_source': True,
            'parameters': {**generation_params, **data},
            'statistics': {
                'osm_buildings_found': len(osm_buildings),
                'buildings_generated': len(dataset['buildings']),
                'timeseries_observations': len(dataset['timeseries'])
            }
        }
        
        # Export ou retour des données
        export_format = data.get('export_format', 'json')
        
        if data.get('return_data', True) and export_format == 'json':
            response_data['data'] = {
                'buildings': dataset['buildings'].to_dict('records'),
                'timeseries': dataset['timeseries'].to_dict('records')
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
                'total_size_bytes': export_result['total_size'],
                'download_urls': [f"/generate/download/{file['name']}" for file in export_result['files']]
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
    
    Body JSON:
    {
        "num_buildings": 100,
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "frequency": "D"
    }
    
    Returns:
        JSON avec les estimations de temps, taille, et caractéristiques
    """
    logger.info("👁️ Demande d'aperçu de génération")
    
    try:
        data = request.get_json() or {}
        
        # Paramètres par défaut
        num_buildings = data.get('num_buildings', current_app.config.get('DEFAULT_NUM_BUILDINGS', 100))
        start_date = data.get('start_date', current_app.config.get('DEFAULT_START_DATE', '2024-01-01'))
        end_date = data.get('end_date', current_app.config.get('DEFAULT_END_DATE', '2024-01-31'))
        frequency = data.get('frequency', current_app.config.get('DEFAULT_FREQUENCY', 'D'))
        
        # Calculs d'estimation
        import pandas as pd
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
        
        recommendations.append(f"Format Parquet recommandé pour la performance ({file_sizes['parquet']['mb']} MB)")
        
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
            'estimations': {
                'total_observations': total_observations,
                'estimated_time_seconds': round(estimated_time_seconds, 2),
                'estimated_time_formatted': _format_duration(estimated_time_seconds),
                'complexity': complexity,
                'complexity_description': complexity_desc,
                'file_sizes': file_sizes
            },
            'recommendations': recommendations,
            'warnings': _get_generation_warnings(total_observations, estimated_time_seconds),
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
    logger.info("🎯 Génération d'échantillon de démonstration")
    
    try:
        # Paramètres fixes pour l'échantillon
        sample_params = {
            'num_buildings': 5,
            'start_date': '2024-01-01',
            'end_date': '2024-01-07',
            'frequency': 'D'
        }
        
        # Générer l'échantillon
        start_time = datetime.now()
        dataset = current_app.data_generator.generate_complete_dataset(**sample_params)
        generation_duration = (datetime.now() - start_time).total_seconds()
        
        return jsonify({
            'success': True,
            'sample': True,
            'generation_time': round(generation_duration, 3),
            'parameters': sample_params,
            'data': {
                'buildings': dataset['buildings'].to_dict('records'),
                'timeseries': dataset['timeseries'].to_dict('records')
            },
            'statistics': {
                'buildings_count': len(dataset['buildings']),
                'observations_count': len(dataset['timeseries'])
            }
        })
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la génération d'échantillon: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la génération d\'échantillon',
            'details': str(e)
        }), 500


@generation_bp.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    """
    Télécharge un fichier généré.
    
    Args:
        filename: Nom du fichier à télécharger
        
    Returns:
        Fichier en téléchargement ou erreur 404
    """
    logger.info(f"📥 Demande de téléchargement: {filename}")
    
    try:
        # Vérifier que le fichier existe dans le répertoire d'export
        export_dir = current_app.config.get('GENERATED_DATA_DIR', 'data/generated')
        file_path = Path(export_dir) / filename
        
        if not file_path.exists():
            logger.warning(f"⚠️ Fichier non trouvé: {filename}")
            return jsonify({
                'success': False,
                'error': 'Fichier non trouvé',
                'filename': filename
            }), 404
        
        # Vérifier que le fichier est dans le bon répertoire (sécurité)
        if not str(file_path.resolve()).startswith(str(Path(export_dir).resolve())):
            logger.warning(f"🚫 Tentative d'accès non autorisé: {filename}")
            return jsonify({
                'success': False,
                'error': 'Accès non autorisé'
            }), 403
        
        # Déterminer le type MIME
        mimetype = 'application/octet-stream'
        if filename.endswith('.csv'):
            mimetype = 'text/csv'
        elif filename.endswith('.json'):
            mimetype = 'application/json'
        elif filename.endswith('.xlsx'):
            mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        elif filename.endswith('.parquet'):
            mimetype = 'application/octet-stream'
        
        logger.info(f"✅ Téléchargement démarré: {filename}")
        
        return send_file(
            file_path,
            as_attachment=True,
            download_name=filename,
            mimetype=mimetype
        )
        
    except Exception as e:
        logger.error(f"❌ Erreur lors du téléchargement de {filename}: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors du téléchargement',
            'details': str(e)
        }), 500


@generation_bp.route('/files', methods=['GET'])
def list_generated_files():
    """
    Liste tous les fichiers générés disponibles au téléchargement.
    
    Returns:
        JSON avec la liste des fichiers
    """
    logger.info("📋 Demande de liste des fichiers générés")
    
    try:
        export_dir = Path(current_app.config.get('GENERATED_DATA_DIR', 'data/generated'))
        
        if not export_dir.exists():
            return jsonify({
                'success': True,
                'files': [],
                'message': 'Aucun fichier généré trouvé'
            })
        
        files_info = []
        
        # Parcourir tous les fichiers dans le répertoire d'export
        for file_path in export_dir.iterdir():
            if file_path.is_file():
                stat = file_path.stat()
                
                files_info.append({
                    'name': file_path.name,
                    'size_bytes': stat.st_size,
                    'size_mb': round(stat.st_size / (1024 * 1024), 2),
                    'created': datetime.fromtimestamp(stat.st_ctime).isoformat(),
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    'extension': file_path.suffix,
                    'download_url': f"/generate/download/{file_path.name}"
                })
        
        # Trier par date de modification (plus récent en premier)
        files_info.sort(key=lambda x: x['modified'], reverse=True)
        
        return jsonify({
            'success': True,
            'total_files': len(files_info),
            'files': files_info,
            'total_size_mb': round(sum(f['size_bytes'] for f in files_info) / (1024 * 1024), 2)
        })
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la liste des fichiers: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la récupération de la liste des fichiers',
            'details': str(e)
        }), 500


@generation_bp.route('/cleanup', methods=['POST'])
def cleanup_old_files():
    """
    Nettoie les anciens fichiers générés.
    
    Body JSON:
    {
        "days_old": 7,  // Fichiers plus anciens que X jours
        "confirm": true
    }
    
    Returns:
        JSON avec le résultat du nettoyage
    """
    logger.info("🧹 Demande de nettoyage des fichiers")
    
    try:
        data = request.get_json() or {}
        
        if not data.get('confirm', False):
            return jsonify({
                'success': False,
                'error': 'Confirmation requise',
                'message': 'Ajoutez "confirm": true pour confirmer le nettoyage'
            }), 400
        
        days_old = data.get('days_old', 7)
        
        # Utiliser le service d'export pour le nettoyage
        cleanup_result = current_app.export_service.cleanup_old_files(days_old)
        
        return jsonify({
            'success': cleanup_result['success'],
            'files_deleted': cleanup_result['files_deleted'],
            'size_freed_mb': round(cleanup_result['size_freed_bytes'] / (1024 * 1024), 2),
            'days_old': days_old
        })
        
    except Exception as e:
        logger.error(f"❌ Erreur lors du nettoyage: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors du nettoyage',
            'details': str(e)
        }), 500


# Fonctions utilitaires
def _format_duration(seconds):
    """
    Formate une durée en secondes en format lisible.
    
    Args:
        seconds: Durée en secondes
        
    Returns:
        Chaîne formatée (ex: "2m 30s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def _get_generation_warnings(total_observations, estimated_time_seconds):
    """
    Génère des avertissements basés sur les paramètres de génération.
    
    Args:
        total_observations: Nombre total d'observations
        estimated_time_seconds: Temps estimé en secondes
        
    Returns:
        Liste des avertissements
    """
    warnings = []
    
    if total_observations > 1000000:
        warnings.append("Dataset très volumineux - vérifiez que vous avez suffisamment d'espace disque")
    
    if estimated_time_seconds > 600:  # 10 minutes
        warnings.append("Génération longue - considérez utiliser l'export direct plutôt que le retour JSON")
    
    if total_observations > 10000000:
        warnings.append("Risque de dépassement de mémoire - considérez réduire les paramètres")
    
    return warnings


def _calculate_generation_statistics(buildings_df, timeseries_df, start_date, end_date, frequency):
    """
    Calcule les statistiques d'un dataset généré.
    
    Args:
        buildings_df: DataFrame des bâtiments
        timeseries_df: DataFrame des séries temporelles
        start_date: Date de début
        end_date: Date de fin
        frequency: Fréquence
        
    Returns:
        Dictionnaire des statistiques
    """
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
        import pandas as pd
        timestamps = pd.to_datetime(timeseries_df['timestamp'])
        stats['temporal_coverage'] = {
            'start_date': timestamps.min().isoformat(),
            'end_date': timestamps.max().isoformat(),
            'total_days': (timestamps.max() - timestamps.min()).days + 1
        }
    
    return stats


# Export du blueprint
__all__ = ['generation_bp']