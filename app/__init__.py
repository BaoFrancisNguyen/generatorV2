#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GÉNÉRATEUR DE DONNÉES ÉNERGÉTIQUES MALAYSIA - APPLICATION FLASK
Fichier: app/__init__.py

Configuration principale de l'application Flask avec factory pattern.
Gestion des extensions, configuration des middlewares et initialisation des services.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Architecture modulaire
"""

import os
import logging
from flask import Flask
from flask_cors import CORS

# Import des configurations
from app.config.base import Config, DevelopmentConfig, ProductionConfig
from app.utils.logger import setup_logger

# Import des services
from app.services.data_generator import ElectricityDataGenerator
from app.services.osm_service import OSMService
from app.services.validation_service import ValidationService
from app.services.export_service import ExportService

def create_app(config_name=None):
    """
    Factory pour créer l'application Flask avec la configuration appropriée.
    
    Args:
        config_name (str): Nom de la configuration ('development', 'production', 'testing')
        
    Returns:
        Flask: Instance de l'application configurée
    """
    
    # ✅ CORRECTION: Calculer les chemins absolus pour éviter les problèmes
    import os
    from pathlib import Path
    
    # Obtenir le répertoire racine du projet
    current_dir = Path(__file__).parent.absolute()  # Dossier app/
    root_dir = current_dir.parent  # Dossier racine du projet
    
    # Vérifier les dossiers templates et static
    templates_path = root_dir / 'templates'
    static_path = root_dir / 'static'
    
    # Si les dossiers n'existent pas dans la racine, chercher dans app/
    if not templates_path.exists():
        templates_path = current_dir / 'templates'
    if not static_path.exists():
        static_path = current_dir / 'static'
    
    # Créer les dossiers s'ils n'existent pas
    templates_path.mkdir(exist_ok=True)
    static_path.mkdir(exist_ok=True)
    
    print(f"🔧 Chemins Flask:")
    print(f"   • Templates: {templates_path}")
    print(f"   • Static: {static_path}")
    
    # Créer l'instance Flask avec les chemins absolus
    app = Flask(__name__, 
                template_folder=str(templates_path),
                static_folder=str(static_path))
    
    # Déterminer la configuration à utiliser
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV', 'development')
    
    # Charger la configuration appropriée
    config_mapping = {
        'development': DevelopmentConfig,
        'production': ProductionConfig,
        'testing': Config  # Configuration de base pour les tests
    }
    
    config_class = config_mapping.get(config_name, DevelopmentConfig)
    app.config.from_object(config_class)
    
    # Configurer le logging
    setup_logger(app)
    logger = logging.getLogger(__name__)
    logger.info(f"🚀 Initialisation de l'application en mode {config_name}")
    
    # Configurer CORS pour le développement
    if app.config['DEBUG']:
        CORS(app, resources={
            r"/api/*": {"origins": "*"},
            r"/generate*": {"origins": "*"}
        })
        logger.info("✅ CORS activé pour le développement")
    
    # Initialiser les services
    initialize_services(app)
    
    # Enregistrer les blueprints (routes)
    register_blueprints(app)
    
    # Configurer les gestionnaires d'erreurs
    configure_error_handlers(app)
    
    # Ajouter les filtres de template
    configure_template_filters(app)
    
    # Créer les dossiers nécessaires
    create_required_directories(app)
    
    logger.info("✅ Application Flask initialisée avec succès")
    return app


def initialize_services(app):
    """
    Initialise les services de l'application et les attache à l'instance Flask.
    
    Args:
        app (Flask): Instance de l'application Flask
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Service générateur de données
        app.data_generator = ElectricityDataGenerator()
        logger.info("✅ Service générateur de données initialisé")
        
        # Service OSM
        app.osm_service = OSMService()
        logger.info("✅ Service OSM initialisé")
        
        # Service de validation
        app.validation_service = ValidationService()
        logger.info("✅ Service de validation initialisé")
        
        # Service d'export
        app.export_service = ExportService(
            output_dir=app.config['GENERATED_DATA_DIR']
        )
        logger.info("✅ Service d'export initialisé")
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'initialisation des services: {e}")
        raise


def register_blueprints(app):
    """
    Enregistre tous les blueprints (routes) de l'application.
    
    Args:
        app (Flask): Instance de l'application Flask
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Routes principales
        from app.routes.main import main_bp
        app.register_blueprint(main_bp)
        
        # Routes de génération
        from app.routes.generation import generation_bp
        app.register_blueprint(generation_bp, url_prefix='/generate')
        
        # Routes API
        from app.routes.api import api_bp
        app.register_blueprint(api_bp, url_prefix='/api')
        
        # Routes OSM
        from app.routes.osm import osm_bp
        app.register_blueprint(osm_bp, url_prefix='/osm')
        
        logger.info("✅ Tous les blueprints enregistrés")
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'enregistrement des blueprints: {e}")
        raise


def configure_error_handlers(app):
    """
    Configure les gestionnaires d'erreurs personnalisés.
    
    Args:
        app (Flask): Instance de l'application Flask
    """
    
    @app.errorhandler(404)
    def not_found_error(error):
        """Gestionnaire pour les erreurs 404"""
        return {
            'success': False,
            'error': 'Ressource non trouvée',
            'code': 404,
            'available_endpoints': [
                '/ - Interface utilisateur principale',
                '/health - Vérification de santé',
                '/api/cities - Liste des villes malaysiennes',
                '/generate - Génération de données',
                '/osm/buildings - Récupération de bâtiments OSM'
            ]
        }, 404
    
    @app.errorhandler(500)
    def internal_error(error):
        """Gestionnaire pour les erreurs 500"""
        logger = logging.getLogger(__name__)
        logger.error(f"Erreur interne du serveur: {str(error)}")
        
        return {
            'success': False,
            'error': 'Erreur interne du serveur',
            'code': 500,
            'message': 'Une erreur inattendue s\'est produite. Consultez les logs pour plus de détails.'
        }, 500
    
    @app.errorhandler(400)
    def bad_request_error(error):
        """Gestionnaire pour les erreurs 400"""
        return {
            'success': False,
            'error': 'Requête malformée',
            'code': 400,
            'message': str(error.description) if hasattr(error, 'description') else 'Données de requête invalides'
        }, 400


def configure_template_filters(app):
    """
    Ajoute des filtres personnalisés pour les templates Jinja2.
    
    Args:
        app (Flask): Instance de l'application Flask
    """
    
    @app.template_filter('format_number')
    def format_number(value):
        """Formate un nombre avec des séparateurs de milliers"""
        try:
            return f"{int(value):,}".replace(",", " ")
        except (ValueError, TypeError):
            return value
    
    @app.template_filter('format_float')
    def format_float(value, precision=2):
        """Formate un nombre décimal avec une précision donnée"""
        try:
            return f"{float(value):.{precision}f}"
        except (ValueError, TypeError):
            return value
    
    @app.template_filter('file_size')
    def format_file_size(bytes_value):
        """Formate une taille de fichier en unités lisibles"""
        try:
            bytes_value = float(bytes_value)
            for unit in ['B', 'KB', 'MB', 'GB']:
                if bytes_value < 1024.0:
                    return f"{bytes_value:.1f} {unit}"
                bytes_value /= 1024.0
            return f"{bytes_value:.1f} TB"
        except (ValueError, TypeError):
            return bytes_value


def create_required_directories(app):
    """
    Crée les dossiers nécessaires au fonctionnement de l'application.
    
    Args:
        app (Flask): Instance de l'application Flask
    """
    logger = logging.getLogger(__name__)
    
    directories = [
        app.config['GENERATED_DATA_DIR'],
        app.config['CACHE_DIR'],
        app.config['LOG_DIR']
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.debug(f"📁 Dossier créé/vérifié: {directory}")
    
    logger.info("✅ Tous les dossiers requis sont prêts")


# Fonction utilitaire pour obtenir l'instance de l'application
def get_current_app():
    """
    Retourne l'instance courante de l'application Flask.
    Utile pour accéder aux services depuis les modules.
    
    Returns:
        Flask: Instance de l'application courante
    """
    from flask import current_app
    return current_app


# Configuration pour l'import direct
__all__ = ['create_app', 'get_current_app']
