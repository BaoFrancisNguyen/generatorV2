#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CONFIGURATION DE BASE - GÉNÉRATEUR MALAYSIA
Fichier: app/config/base.py

Classes de configuration pour différents environnements.
Gestion centralisée des paramètres de l'application.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Configuration modulaire
"""

import os
from datetime import timedelta
from pathlib import Path

# Répertoire racine du projet
BASE_DIR = Path(__file__).parent.parent.parent.absolute()

class Config:
    """
    Configuration de base commune à tous les environnements.
    """
    
    # === CONFIGURATION FLASK ===
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'malaysia-energy-generator-dev-key-2025'
    
    # Sécurité
    WTF_CSRF_ENABLED = True
    CSRF_TOKEN_DURATION = timedelta(hours=1)
    
    # JSON
    JSON_SORT_KEYS = False
    JSONIFY_PRETTYPRINT_REGULAR = True
    JSON_AS_ASCII = False
    
    # Upload
    MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB max
    
    # === CONFIGURATION APPLICATION ===
    
    # Répertoires
    GENERATED_DATA_DIR = BASE_DIR / 'data' / 'generated'
    CACHE_DIR = BASE_DIR / 'data' / 'cache'
    LOG_DIR = BASE_DIR / 'logs'
    SAMPLES_DIR = BASE_DIR / 'data' / 'samples'
    
    # === CONFIGURATION GENERATION ===
    
    # Limites de génération
    MAX_BUILDINGS = 10000
    MIN_BUILDINGS = 1
    MAX_PERIOD_DAYS = 1095  # 3 ans maximum
    MIN_PERIOD_DAYS = 1
    
    # Formats supportés
    SUPPORTED_FREQUENCIES = ['H', 'D', 'W', 'M', '30T', '15T']
    SUPPORTED_EXPORT_FORMATS = ['parquet', 'csv', 'json', 'excel']
    
    # Configurations par défaut
    DEFAULT_NUM_BUILDINGS = 100
    DEFAULT_FREQUENCY = 'D'
    DEFAULT_START_DATE = '2024-01-01'
    DEFAULT_END_DATE = '2024-01-31'
    
    # === CONFIGURATION OSM ===
    
    # API Overpass
    OVERPASS_API_URL = 'https://overpass-api.de/api/interpreter'
    OVERPASS_TIMEOUT = 60
    OVERPASS_MAX_SIZE = 1073741824  # 1GB
    
    # Cache OSM
    OSM_CACHE_ENABLED = True
    OSM_CACHE_DURATION = timedelta(hours=24)
    OSM_CACHE_MAX_SIZE_MB = 500
    
    # Limites OSM
    OSM_MAX_BUILDINGS_PER_REQUEST = 10000
    OSM_REQUEST_TIMEOUT = 120
    OSM_MAX_RETRIES = 3
    
    # === CONFIGURATION DONNÉES MALAYSIA ===
    
    # Fuseau horaire
    TIMEZONE = 'Asia/Kuala_Lumpur'
    
    # Devise et unités
    CURRENCY = 'MYR'
    ENERGY_UNIT = 'kWh'
    POWER_UNIT = 'kW'
    
    # Paramètres climatiques Malaysia
    MALAYSIA_CLIMATE = {
        'avg_temperature': 27,  # °C
        'avg_humidity': 80,     # %
        'monsoon_months': [11, 12, 1, 2],  # Novembre à Février
        'dry_months': [6, 7, 8],           # Juin à Août
        'peak_ac_months': [3, 4, 5, 9, 10] # Mars-Mai, Sept-Oct
    }
    
    # === CONFIGURATION VALIDATION ===
    
    # Seuils de validation
    VALIDATION_THRESHOLDS = {
        'min_consumption_kwh': 0.1,
        'max_consumption_kwh': 1000,
        'max_daily_variation': 0.5,  # 50% de variation max par jour
        'outlier_z_score': 3.0
    }
    
    # Règles de cohérence
    COHERENCE_RULES = {
        'residential_max_kwh': 50,
        'commercial_max_kwh': 500,
        'industrial_max_kwh': 1000,
        'public_max_kwh': 200
    }
    
    # === CONFIGURATION LOGGING ===
    
    LOG_LEVEL = 'INFO'
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    
    # Rotation des logs
    LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
    LOG_BACKUP_COUNT = 5
    
    # === CONFIGURATION PERFORMANCE ===
    
    # Cache
    CACHE_TYPE = 'simple'
    CACHE_DEFAULT_TIMEOUT = 300  # 5 minutes
    
    # Génération asynchrone
    ASYNC_GENERATION_ENABLED = False
    MAX_CONCURRENT_GENERATIONS = 2
    
    # Optimisations
    ENABLE_DATA_COMPRESSION = True
    CHUNK_SIZE_BUILDINGS = 1000
    CHUNK_SIZE_TIMESERIES = 10000


class DevelopmentConfig(Config):
    """
    Configuration pour l'environnement de développement.
    """
    
    DEBUG = True
    TESTING = False
    
    # Logging plus verbeux
    LOG_LEVEL = 'DEBUG'
    
    # Cache désactivé
    CACHE_TYPE = 'null'
    OSM_CACHE_ENABLED = False
    
    # Limites réduites pour le développement
    MAX_BUILDINGS = 1000
    MAX_PERIOD_DAYS = 90
    
    # Génération plus rapide
    CHUNK_SIZE_BUILDINGS = 100
    CHUNK_SIZE_TIMESERIES = 1000
    
    # Base de données SQLite pour le dev
    DATABASE_URL = f"sqlite:///{BASE_DIR}/dev.db"


class ProductionConfig(Config):
    """
    Configuration pour l'environnement de production.
    """
    
    DEBUG = False
    TESTING = False
    
    # Sécurité renforcée - Vérification reportée à l'initialisation
    @property
    def SECRET_KEY(self):
        secret_key = os.environ.get('SECRET_KEY')
        if not secret_key:
            raise ValueError("SECRET_KEY doit être définie en production")
        return secret_key
    
    # Performance optimisée
    CACHE_TYPE = 'redis'
    CACHE_REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
    
    # Génération asynchrone activée
    ASYNC_GENERATION_ENABLED = True
    MAX_CONCURRENT_GENERATIONS = 4
    
    # Base de données PostgreSQL pour la production
    DATABASE_URL = os.environ.get('DATABASE_URL')
    
    # Monitoring
    SENTRY_DSN = os.environ.get('SENTRY_DSN')
    
    # Limites de production
    MAX_BUILDINGS = 50000
    MAX_PERIOD_DAYS = 1095  # 3 ans
    
    # SSL et sécurité
    PREFERRED_URL_SCHEME = 'https'
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'


class TestingConfig(Config):
    """
    Configuration pour les tests unitaires.
    """
    
    TESTING = True
    DEBUG = True
    
    # Utiliser des dossiers temporaires pour les tests
    import tempfile
    temp_dir = Path(tempfile.gettempdir()) / 'malaysia_generator_tests'
    
    GENERATED_DATA_DIR = temp_dir / 'generated'
    CACHE_DIR = temp_dir / 'cache'
    LOG_DIR = temp_dir / 'logs'
    
    # Cache désactivé
    CACHE_TYPE = 'null'
    OSM_CACHE_ENABLED = False
    
    # Limites très réduites pour les tests
    MAX_BUILDINGS = 10
    MAX_PERIOD_DAYS = 7
    
    # Base de données en mémoire
    DATABASE_URL = 'sqlite:///:memory:'
    
    # Désactiver la validation CSRF pour les tests
    WTF_CSRF_ENABLED = False
    
    # Logging minimal
    LOG_LEVEL = 'WARNING'


# Dictionnaire de mappage des configurations
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}


def get_config(config_name=None):
    """
    Retourne la classe de configuration appropriée.
    
    Args:
        config_name (str): Nom de la configuration
        
    Returns:
        Config: Classe de configuration
    """
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV', 'development')
    
    return config.get(config_name, DevelopmentConfig)


# Fonction utilitaire pour valider la configuration
def validate_config(config_instance):
    """
    Valide les paramètres de configuration.
    
    Args:
        config_instance: Instance de configuration Flask (app.config)
        
    Raises:
        ValueError: Si la configuration est invalide
    """
    # ✅ CORRECTION: Utiliser des clés de dictionnaire au lieu d'attributs
    # car Flask transfère les attributs de classe vers un dictionnaire
    
    # Vérifier les répertoires obligatoires
    required_dirs = [
        config_instance.get('GENERATED_DATA_DIR'),
        config_instance.get('CACHE_DIR'), 
        config_instance.get('LOG_DIR')
    ]
    
    for directory in required_dirs:
        if directory is None:
            continue  # Ignorer si le répertoire n'est pas configuré
            
        try:
            os.makedirs(directory, exist_ok=True)
        except Exception as e:
            raise ValueError(f"Impossible de créer le répertoire {directory}: {e}")
    
    # Vérifier les limites logiques seulement si elles existent
    min_buildings = config_instance.get('MIN_BUILDINGS')
    max_buildings = config_instance.get('MAX_BUILDINGS')
    
    if min_buildings is not None and max_buildings is not None:
        if min_buildings >= max_buildings:
            raise ValueError("MIN_BUILDINGS doit être inférieur à MAX_BUILDINGS")
    
    min_period = config_instance.get('MIN_PERIOD_DAYS')
    max_period = config_instance.get('MAX_PERIOD_DAYS')
    
    if min_period is not None and max_period is not None:
        if min_period >= max_period:
            raise ValueError("MIN_PERIOD_DAYS doit être inférieur à MAX_PERIOD_DAYS")
    
    # Vérifier les fréquences supportées seulement si elles existent
    default_freq = config_instance.get('DEFAULT_FREQUENCY')
    supported_freq = config_instance.get('SUPPORTED_FREQUENCIES')
    
    if default_freq is not None and supported_freq is not None:
        if default_freq not in supported_freq:
            raise ValueError(f"DEFAULT_FREQUENCY doit être dans {supported_freq}")