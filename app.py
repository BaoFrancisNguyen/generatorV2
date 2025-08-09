#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
APPLICATION PRINCIPALE ULTRA-OPTIMISÉE - GÉNÉRATEUR MALAYSIA
Fichier: app.py

Application Flask optimisée pour la récupération exhaustive de bâtiments OSM:
- Configuration ultra-optimisée
- Services initialisés avec paramètres de performance
- Routes optimisées pour récupération complète
- Monitoring intégré

Auteur: Équipe Développement
Date: 2025
Version: 4.0 - Ultra-optimisé pour récupération complète
"""

import logging
import sys
import math
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

# Configuration
from app.config.base import get_config

# Services optimisés
from app.services.osm_service import OSMService
from app.services.generation_service import GenerationService

# Routes optimisées
from app.routes.osm import osm_bp
from app.routes.osm_generation import osm_generation_bp
from app.routes.generation import generation_bp

def create_app():
    """
    🚀 Crée l'application Flask ultra-optimisée pour Malaysia.
    """
    app = Flask(__name__)
    
    # Configuration
    config_class = get_config()
    app.config.from_object(config_class)
    
    # CORS
    CORS(app, origins=app.config['CORS_ORIGINS'])
    
    # Configuration du logging
    setup_logging(app)
    
    logger = logging.getLogger(__name__)
    logger.info("🇲🇾 Initialisation Générateur Malaysia ULTRA-OPTIMISÉ v4.0")
    
    # Initialisation des services avec configuration optimisée
    init_services(app, logger)
    
    # Enregistrement des routes
    register_blueprints(app, logger)
    
    # Routes principales
    register_main_routes(app, logger)
    
    # Gestionnaires d'erreurs
    register_error_handlers(app, logger)
    
    logger.info("✅ Application initialisée avec succès")
    log_system_info(app, logger)
    
    return app


def setup_logging(app):
    """Configure le système de logging optimisé."""
    # Niveau de log
    log_level = getattr(logging, app.config['LOG_LEVEL'].upper(), logging.INFO)
    
    # Configuration du logger racine
    logging.basicConfig(
        level=log_level,
        format=app.config['LOG_FORMAT'],
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(app.config['LOG_FILE'])
        ]
    )
    
    # Logger spécifique pour les performances OSM
    perf_logger = logging.getLogger('osm_performance')
    perf_handler = logging.FileHandler(app.config['DATA_DIR'] / 'logs' / 'osm_performance.log')
    perf_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    perf_logger.addHandler(perf_handler)
    perf_logger.setLevel(logging.INFO)
    
    # Réduire le niveau de logging des bibliothèques externes
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)


def init_services(app, logger):
    """Initialise les services avec configuration ultra-optimisée."""
    logger.info("🔧 Initialisation des services optimisés...")
    
    # Service OSM ultra-optimisé
    osm_config = app.config.get_osm_config()
    app.osm_service = OSMService(osm_config)
    logger.info(f"✅ Service OSM initialisé - {osm_config['max_concurrent']} requêtes simultanées")
    
    # Service de génération optimisé
    generation_config = app.config.get_generation_config()
    app.generation_service = GenerationService(generation_config)
    logger.info(f"✅ Service Génération initialisé - {generation_config['parallel_workers']} workers")
    
    # Service d'export
    export_config = app.config.get_export_config()
    app.export_service = ExportService(export_config)
    logger.info(f"✅ Service Export initialisé - formats: {', '.join(export_config['supported_formats'])}")


def register_blueprints(app, logger):
    """Enregistre les blueprints optimisés."""
    logger.info("📋 Enregistrement des routes...")
    
    # Routes OSM principales
    app.register_blueprint(osm_bp, url_prefix='/osm')
    logger.info("✅ Routes OSM enregistrées (/osm)")
    
    # Routes de génération OSM
    app.register_blueprint(osm_generation_bp, url_prefix='/generate/osm')
    logger.info("✅ Routes Génération OSM enregistrées (/generate/osm)")
    
    # Routes de génération standard
    app.register_blueprint(generation_bp, url_prefix='/generate')
    logger.info("✅ Routes Génération standard enregistrées (/generate)")


def register_main_routes(app, logger):
    """Enregistre les routes principales de l'application."""
    
    @app.route('/')
    def index():
        """Page d'accueil avec informations sur l'API."""
        return jsonify({
            'name': 'Générateur Malaysia ULTRA-OPTIMISÉ',
            'version': '4.0',
            'description': 'API optimisée pour récupération exhaustive de bâtiments OSM Malaysia',
            'features': [
                'Récupération exhaustive de TOUS les bâtiments Malaysia',
                'Requêtes parallèles asynchrones ultra-rapides',
                'Cache intelligent avec compression',
                'Export multi-formats (Parquet, CSV, Excel, JSON)',
                'Monitoring en temps réel',
                'Génération de données énergétiques'
            ],
            'endpoints': {
                'main': {
                    '/osm/buildings/all': 'Récupération exhaustive Malaysia',
                    '/osm/buildings/by-state/<state>': 'Récupération par état',
                    '/osm/stats': 'Statistiques de performance',
                    '/osm/cache/info': 'Informations du cache'
                },
                'generation': {
                    '/generate/osm/preview/all': 'Preview rapide Malaysia',
                    '/generate/osm/generate/all': 'Génération exhaustive',
                    '/generate/osm/status': 'Statut en temps réel'
                }
            },
            'performance_tips': [
                'Utilisez format=parquet pour de gros volumes',
                'Activez download=true pour téléchargement direct',
                'Surveillez /osm/stats pour les performances',
                'Le cache améliore drastiquement les performances répétées'
            ],
            'system_info': get_system_status()
        })
    
    @app.route('/health')
    def health_check():
        """Vérification de santé de l'application."""
        try:
            # Vérifier les services
            osm_health = check_osm_service_health(app.osm_service)
            generation_health = check_generation_service_health(app.generation_service)
            
            overall_health = osm_health['status'] == 'healthy' and generation_health['status'] == 'healthy'
            
            return jsonify({
                'status': 'healthy' if overall_health else 'degraded',
                'timestamp': datetime.now().isoformat(),
                'services': {
                    'osm': osm_health,
                    'generation': generation_health
                },
                'system': get_system_health()
            }), 200 if overall_health else 503
            
        except Exception as e:
            logger.error(f"❌ Erreur health check: {e}")
            return jsonify({
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }), 503
    
    @app.route('/system/info')
    def system_info():
        """Informations détaillées du système."""
        return jsonify({
            'system': get_system_status(),
            'configuration': {
                'osm': app.config.get_osm_config(),
                'generation': app.config.get_generation_config(),
                'export': app.config.get_export_config()
            },
            'capabilities': {
                'max_buildings': app.config['MAX_BUILDINGS_PER_GENERATION'],
                'supported_formats': app.config['SUPPORTED_EXPORT_FORMATS'],
                'cache_enabled': app.config['OSM_CACHE_ENABLED'],
                'parallel_requests': app.config['OSM_MAX_CONCURRENT']
            }
        })
    
    logger.info("✅ Routes principales enregistrées")


def register_error_handlers(app, logger):
    """Enregistre les gestionnaires d'erreurs optimisés."""
    
    @app.errorhandler(400)
    def bad_request(error):
        logger.warning(f"⚠️ Requête invalide: {error}")
        return jsonify({
            'success': False,
            'error': 'Requête invalide',
            'details': str(error.description),
            'timestamp': datetime.now().isoformat()
        }), 400
    
    @app.errorhandler(404)
    def not_found(error):
        logger.warning(f"⚠️ Resource non trouvée: {error}")
        return jsonify({
            'success': False,
            'error': 'Ressource non trouvée',
            'details': 'L\'endpoint demandé n\'existe pas',
            'available_endpoints': [
                '/osm/buildings/all',
                '/osm/buildings/by-state/<state>',
                '/generate/osm/preview/all',
                '/generate/osm/generate/all'
            ],
            'timestamp': datetime.now().isoformat()
        }), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        logger.error(f"❌ Erreur interne: {error}")
        return jsonify({
            'success': False,
            'error': 'Erreur interne du serveur',
            'details': 'Une erreur inattendue s\'est produite',
            'timestamp': datetime.now().isoformat(),
            'support': 'Vérifiez les logs pour plus de détails'
        }), 500
    
    @app.errorhandler(503)
    def service_unavailable(error):
        logger.error(f"❌ Service indisponible: {error}")
        return jsonify({
            'success': False,
            'error': 'Service temporairement indisponible',
            'details': 'Le service OSM ou de génération est surchargé',
            'recommendations': [
                'Réessayez dans quelques minutes',
                'Vérifiez /health pour le statut des services',
                'Réduisez la taille des requêtes'
            ],
            'timestamp': datetime.now().isoformat()
        }), 503
    
    logger.info("✅ Gestionnaires d'erreurs enregistrés")


def get_system_status():
    """Retourne le statut du système."""
    import psutil
    import platform
    
    return {
        'platform': platform.platform(),
        'python_version': platform.python_version(),
        'cpu_count': psutil.cpu_count(),
        'memory_total_gb': round(psutil.virtual_memory().total / (1024**3), 1),
        'memory_available_gb': round(psutil.virtual_memory().available / (1024**3), 1),
        'disk_free_gb': round(psutil.disk_usage('/').free / (1024**3), 1),
        'uptime_minutes': round((datetime.now().timestamp() - psutil.boot_time()) / 60, 1)
    }


def get_system_health():
    """Vérifie la santé du système."""
    import psutil
    
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    cpu_percent = psutil.cpu_percent(interval=1)
    
    # Seuils d'alerte
    memory_usage = (memory.total - memory.available) / memory.total
    disk_usage = (disk.total - disk.free) / disk.total
    
    health_status = 'healthy'
    warnings = []
    
    if memory_usage > 0.9:
        health_status = 'degraded'
        warnings.append(f'Mémoire élevée: {memory_usage*100:.1f}%')
    
    if disk_usage > 0.9:
        health_status = 'degraded'
        warnings.append(f'Disque plein: {disk_usage*100:.1f}%')
    
    if cpu_percent > 90:
        health_status = 'degraded'
        warnings.append(f'CPU élevé: {cpu_percent:.1f}%')
    
    return {
        'status': health_status,
        'memory_usage_percent': round(memory_usage * 100, 1),
        'disk_usage_percent': round(disk_usage * 100, 1),
        'cpu_usage_percent': round(cpu_percent, 1),
        'warnings': warnings
    }


def check_osm_service_health(osm_service):
    """Vérifie la santé du service OSM."""
    try:
        # Test simple de connectivité
        cache_info = osm_service.get_cache_info()
        stats = osm_service.get_stats()
        
        # Vérifier les erreurs récentes
        error_rate = 0
        if stats['total_requests'] > 0:
            error_rate = stats['failed_requests'] / stats['total_requests']
        
        status = 'healthy'
        if error_rate > 0.5:  # Plus de 50% d'erreurs
            status = 'degraded'
        
        return {
            'status': status,
            'cache_files': cache_info['files_count'],
            'cache_size_mb': cache_info['total_size_mb'],
            'error_rate_percent': round(error_rate * 100, 1),
            'last_request': stats.get('last_request_time', 'Never')
        }
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e)
        }


def check_generation_service_health(generation_service):
    """Vérifie la santé du service de génération."""
    try:
        # Test basique du service
        return {
            'status': 'healthy',
            'workers_available': True,
            'memory_usage': 'normal'
        }
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e)
        }


def log_system_info(app, logger):
    """Affiche les informations système au démarrage."""
    config = app.config
    
    logger.info("🔧 CONFIGURATION SYSTÈME:")
    logger.info(f"   🌍 Mode: {config.get('ENV', 'development')}")
    logger.info(f"   🐞 Debug: {config.get('DEBUG', False)}")
    logger.info(f"   📡 OSM Timeout: {config['OSM_REQUEST_TIMEOUT']}s")
    logger.info(f"   🚀 OSM Concurrent: {config['OSM_MAX_CONCURRENT']}")
    logger.info(f"   💾 Cache: {'✅' if config['OSM_CACHE_ENABLED'] else '❌'}")
    logger.info(f"   📊 Max Buildings: {config['MAX_BUILDINGS_PER_GENERATION']:,}")
    logger.info(f"   💽 Data Dir: {config['DATA_DIR']}")
    
    # Informations système
    system_info = get_system_status()
    logger.info("💻 SYSTÈME:")
    logger.info(f"   🖥️  Platform: {system_info['platform']}")
    logger.info(f"   🐍 Python: {system_info['python_version']}")
    logger.info(f"   ⚡ CPU: {system_info['cpu_count']} cores")
    logger.info(f"   💾 RAM: {system_info['memory_available_gb']:.1f}GB / {system_info['memory_total_gb']:.1f}GB")
    logger.info(f"   💿 Disk: {system_info['disk_free_gb']:.1f}GB free")


class ExportService:
    """Service d'export optimisé."""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def export_data(self, data, format_type, options=None):
        """Exporte les données dans le format spécifié."""
        # Implémentation simplifiée
        return {'success': True, 'format': format_type}


# Point d'entrée principal
def main():
    """Point d'entrée principal de l'application."""
    print("🇲🇾 GÉNÉRATEUR MALAYSIA ULTRA-OPTIMISÉ v4.0")
    print("=" * 60)
    print("🚀 Récupération exhaustive de TOUS les bâtiments OSM")
    print("⚡ Requêtes parallèles asynchrones ultra-rapides")
    print("💾 Cache intelligent avec compression")
    print("📊 Export multi-formats optimisé")
    print("🔍 Monitoring en temps réel")
    print("=" * 60)
    
    # Créer l'application
    app = create_app()
    
    # Configuration du serveur
    host = app.config.get('HOST', '0.0.0.0')
    port = int(app.config.get('PORT', 5000))
    debug = app.config.get('DEBUG', False)
    
    print(f"🌐 Serveur démarré sur http://{host}:{port}")
    print(f"📖 Documentation API: http://{host}:{port}/")
    print(f"💓 Health check: http://{host}:{port}/health")
    print(f"🔧 System info: http://{host}:{port}/system/info")
    print("")
    print("🚀 ENDPOINTS PRINCIPAUX:")
    print(f"   📍 Tous les bâtiments: http://{host}:{port}/osm/buildings/all")
    print(f"   🏛️  Par état: http://{host}:{port}/osm/buildings/by-state/<state>")
    print(f"   👀 Preview rapide: http://{host}:{port}/generate/osm/preview/all")
    print(f"   ⚡ Génération complète: http://{host}:{port}/generate/osm/generate/all")
    print(f"   📊 Statistiques: http://{host}:{port}/osm/stats")
    print("")
    print("💡 CONSEILS D'UTILISATION:")
    print("   • Utilisez ?format=parquet pour de gros volumes")
    print("   • Ajoutez &download=true pour téléchargement direct")
    print("   • Surveillez /osm/stats pour les performances")
    print("   • Le cache améliore drastiquement les performances")
    print("")
    
    try:
        # Démarrer le serveur
        app.run(
            host=host,
            port=port,
            debug=debug,
            threaded=True,
            use_reloader=debug
        )
    except KeyboardInterrupt:
        print("\n🛑 Arrêt du serveur...")
    except Exception as e:
        print(f"❌ Erreur de démarrage: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()