#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROUTES PRINCIPALES - GÉNÉRATEUR MALAYSIA
Fichier: app/routes/main.py

Routes pour l'interface utilisateur principale, santé du système,
et fonctionnalités de base de l'application.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Routes modulaires
"""

import logging
from datetime import datetime
from flask import Blueprint, render_template, jsonify, current_app

# Créer le blueprint pour les routes principales
main_bp = Blueprint('main', __name__)
logger = logging.getLogger(__name__)


@main_bp.route('/')
def index():
    """
    Page d'accueil principale de l'application.
    
    Returns:
        Template HTML de l'interface utilisateur
    """
    logger.info("🏠 Accès à la page d'accueil")
    
    try:
        # Récupérer les statistiques de base pour la page d'accueil
        malaysia_data = current_app.data_generator.malaysia_data
        stats = malaysia_data.get_statistics()
        
        # Informations pour l'interface
        app_info = {
            'name': 'Malaysia Energy Generator',
            'version': '3.0',
            'total_cities': stats['total_cities'],
            'total_population': stats['total_population'],
            'largest_city': stats['largest_city'],
            'building_types': stats['building_types'],
            'timestamp': datetime.now().isoformat()
        }
        
        return render_template('index.html', app_info=app_info)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors du chargement de la page d'accueil: {e}")
        # Afficher quand même la page avec des données par défaut
        app_info = {
            'name': 'Malaysia Energy Generator',
            'version': '3.0',
            'total_cities': 0,
            'error': str(e)
        }
        return render_template('index.html', app_info=app_info)


@main_bp.route('/health')
def health_check():
    """
    Endpoint de vérification de santé du système.
    
    Returns:
        JSON avec l'état de santé des services
    """
    logger.info("🏥 Vérification de santé du système")
    
    health_status = {
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '3.0',
        'services': {},
        'warnings': [],
        'errors': []
    }
    
    try:
        # Vérifier le service générateur de données
        try:
            stats = current_app.data_generator.get_generation_statistics()
            health_status['services']['data_generator'] = {
                'status': 'healthy',
                'total_locations': stats['total_locations']
            }
        except Exception as e:
            health_status['services']['data_generator'] = {
                'status': 'error',
                'error': str(e)
            }
            health_status['errors'].append(f"Générateur de données: {e}")
        
        # Vérifier le service OSM
        try:
            osm_stats = current_app.osm_service.get_statistics()
            health_status['services']['osm_service'] = {
                'status': 'healthy',
                'cache_enabled': osm_stats['cache_info']['enabled'],
                'total_requests': osm_stats['request_stats']['total_requests']
            }
        except Exception as e:
            health_status['services']['osm_service'] = {
                'status': 'error',
                'error': str(e)
            }
            health_status['errors'].append(f"Service OSM: {e}")
        
        # Vérifier le service de validation
        try:
            validation_stats = current_app.validation_service.get_validation_statistics()
            health_status['services']['validation_service'] = {
                'status': 'healthy',
                'total_validations': validation_stats['service_stats']['total_validations']
            }
        except Exception as e:
            health_status['services']['validation_service'] = {
                'status': 'error',
                'error': str(e)
            }
            health_status['errors'].append(f"Service de validation: {e}")
        
        # Vérifier le service d'export
        try:
            export_stats = current_app.export_service.get_export_statistics()
            health_status['services']['export_service'] = {
                'status': 'healthy',
                'supported_formats': len(current_app.export_service.supported_formats)
            }
        except Exception as e:
            health_status['services']['export_service'] = {
                'status': 'error',
                'error': str(e)
            }
            health_status['errors'].append(f"Service d'export: {e}")
        
        # Déterminer le statut global
        if health_status['errors']:
            health_status['status'] = 'unhealthy'
        elif len([s for s in health_status['services'].values() if s['status'] == 'error']) > 1:
            health_status['status'] = 'degraded'
        
        # Code de réponse HTTP selon le statut
        status_code = 200
        if health_status['status'] == 'unhealthy':
            status_code = 503
        elif health_status['status'] == 'degraded':
            status_code = 200  # Dégradé mais fonctionnel
        
        return jsonify(health_status), status_code
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la vérification de santé: {e}")
        return jsonify({
            'status': 'error',
            'timestamp': datetime.now().isoformat(),
            'error': 'Impossible de vérifier l\'état du système',
            'details': str(e)
        }), 500


@main_bp.route('/info')
def app_info():
    """
    Informations détaillées sur l'application.
    
    Returns:
        JSON avec les informations système
    """
    logger.info("ℹ️ Demande d'informations sur l'application")
    
    try:
        # Informations de base
        app_info = {
            'name': 'Malaysia Energy Generator',
            'version': '3.0',
            'description': 'Générateur de données énergétiques synthétiques pour la Malaysia',
            'author': 'Équipe Développement',
            'timestamp': datetime.now().isoformat()
        }
        
        # Configuration
        config_info = {
            'debug_mode': current_app.config['DEBUG'],
            'environment': current_app.config.get('ENV', 'unknown'),
            'max_buildings': current_app.config['MAX_BUILDINGS'],
            'supported_frequencies': current_app.config['SUPPORTED_FREQUENCIES'],
            'supported_export_formats': current_app.config['SUPPORTED_EXPORT_FORMATS'],
            'timezone': current_app.config['TIMEZONE']
        }
        
        # Statistiques des services
        services_info = {}
        try:
            malaysia_stats = current_app.data_generator.malaysia_data.get_statistics()
            services_info['malaysia_data'] = {
                'total_cities': malaysia_stats['total_cities'],
                'total_population': malaysia_stats['total_population'],
                'states_count': malaysia_stats['states_count'],
                'largest_city': malaysia_stats['largest_city']
            }
        except Exception as e:
            services_info['malaysia_data'] = {'error': str(e)}
        
        # Endpoints disponibles
        endpoints = {
            'web_interface': {
                '/': 'Interface utilisateur principale',
                '/health': 'Vérification de santé du système',
                '/info': 'Informations sur l\'application'
            },
            'api_endpoints': {
                '/api/cities': 'Liste des villes malaysiennes',
                '/api/building-types': 'Types de bâtiments supportés',
                '/api/statistics': 'Statistiques globales',
                '/api/export-formats': 'Formats d\'export disponibles',
                '/api/estimate': 'Estimation de génération'
            },
            'generation_endpoints': {
                '/generate/': 'Génération standard de données',
                '/generate/osm': 'Génération basée sur OpenStreetMap',
                '/generate/preview': 'Aperçu des paramètres',
                '/generate/sample': 'Génération d\'échantillon'
            },
            'osm_endpoints': {
                '/osm/buildings': 'Récupération de bâtiments OSM',
                '/osm/statistics': 'Statistiques du service OSM',
                '/osm/cache/clear': 'Vidage du cache OSM',
                '/osm/cache/info': 'Informations sur le cache'
            }
        }
        
        # Capacités et fonctionnalités
        capabilities = {
            'data_generation': {
                'building_types': ['residential', 'commercial', 'industrial', 'public'],
                'frequencies': current_app.config['SUPPORTED_FREQUENCIES'],
                'export_formats': current_app.config['SUPPORTED_EXPORT_FORMATS'],
                'max_buildings': current_app.config['MAX_BUILDINGS'],
                'max_period_days': current_app.config['MAX_PERIOD_DAYS']
            },
            'osm_integration': {
                'enabled': True,
                'cache_enabled': True,
                'supported_queries': ['city', 'bbox', 'around_point']
            },
            'validation': {
                'real_time_validation': True,
                'data_quality_scoring': True,
                'anomaly_detection': True
            },
            'export': {
                'formats': current_app.config['SUPPORTED_EXPORT_FORMATS'],
                'compression': True,
                'metadata_inclusion': True
            }
        }
        
        return jsonify({
            'success': True,
            'application': app_info,
            'configuration': config_info,
            'services': services_info,
            'endpoints': endpoints,
            'capabilities': capabilities
        })
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des informations: {e}")
        return jsonify({
            'success': False,
            'error': 'Erreur lors de la récupération des informations',
            'details': str(e)
        }), 500


@main_bp.route('/documentation')
def documentation():
    """
    Page de documentation de l'API et d'utilisation.
    
    Returns:
        Template HTML de documentation
    """
    logger.info("📚 Accès à la documentation")
    
    try:
        # Informations pour la documentation
        doc_info = {
            'version': '3.0',
            'last_updated': datetime.now().isoformat(),
            'endpoints_count': 15  # Approximatif
        }
        
        return render_template('documentation.html', doc_info=doc_info)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors du chargement de la documentation: {e}")
        return f"Erreur lors du chargement de la documentation: {e}", 500


@main_bp.route('/status')
def system_status():
    """
    Statut détaillé du système pour monitoring.
    
    Returns:
        JSON avec le statut détaillé
    """
    logger.info("📊 Demande de statut système détaillé")
    
    try:
        # Calculer l'uptime
        uptime_seconds = 0
        if hasattr(current_app, '_start_time'):
            uptime_seconds = (datetime.now() - current_app._start_time).total_seconds()
        
        status = {
            'timestamp': datetime.now().isoformat(),
            'uptime_seconds': uptime_seconds,
            'uptime_formatted': f"{int(uptime_seconds // 3600)}h {int((uptime_seconds % 3600) // 60)}m",
            'environment': current_app.config.get('ENV', 'unknown'),
            'debug_mode': current_app.config['DEBUG'],
            'services': {}
        }
        
        # Statut détaillé des services
        try:
            # Service de génération
            gen_stats = current_app.data_generator.get_generation_statistics()
            status['services']['data_generator'] = {
                'status': 'operational',
                'locations_available': gen_stats['total_locations'],
                'cache_size': gen_stats['cache_size']
            }
        except Exception as e:
            status['services']['data_generator'] = {'status': 'error', 'error': str(e)}
        
        try:
            # Service OSM
            osm_stats = current_app.osm_service.get_statistics()
            status['services']['osm'] = {
                'status': 'operational',
                'total_requests': osm_stats['request_stats']['total_requests'],
                'cache_hits': osm_stats['request_stats']['cache_hits'],
                'cache_enabled': osm_stats['cache_info']['enabled']
            }
        except Exception as e:
            status['services']['osm'] = {'status': 'error', 'error': str(e)}
        
        return jsonify(status)
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération du statut: {e}")
        return jsonify({
            'status': 'error',
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }), 500


# Gestionnaires d'erreurs spécifiques au blueprint principal
@main_bp.errorhandler(404)
def main_not_found(error):
    """Gestionnaire d'erreur 404 pour les routes principales."""
    return render_template('errors/404.html'), 404


@main_bp.errorhandler(500)
def main_internal_error(error):
    """Gestionnaire d'erreur 500 pour les routes principales."""
    logger.error(f"💥 Erreur interne route principale: {error}")
    return render_template('errors/500.html'), 500


# Export du blueprint
__all__ = ['main_bp']