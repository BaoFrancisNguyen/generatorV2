#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CONFIGURATION ULTRA-OPTIMISÉE - GÉNÉRATEUR MALAYSIA
Fichier: app/config/base.py

Configuration ultra-optimisée pour la récupération exhaustive de bâtiments OSM.

Auteur: Équipe Développement
Date: 2025
Version: 4.0 - Ultra-optimisé pour récupération complète
"""

import os
from datetime import timedelta
from pathlib import Path

# Répertoire racine du projet
BASE_DIR = Path(__file__).parent.parent.parent.absolute()


class Config:
    """Configuration de base ultra-optimisée."""
    
    # === CONFIGURATION FLASK ===
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'ultra-secret-key-for-malaysia-generator-v4'
    DEBUG = False
    TESTING = False
    
    # Sécurité
    WTF_CSRF_ENABLED = True
    CSRF_TOKEN_DURATION = timedelta(hours=1)
    JSON_SORT_KEYS = False
    JSONIFY_PRETTYPRINT_REGULAR = True
    JSON_AS_ASCII = False
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max
    
    # === CONFIGURATION OSM ULTRA-OPTIMISÉE ===
    
    # URLs Overpass (load balancing automatique)
    OVERPASS_API_URLS = [
        'https://overpass-api.de/api/interpreter',
        'https://lz4.overpass-api.de/api/interpreter', 
        'https://z.overpass-api.de/api/interpreter'
    ]
    OVERPASS_API_URL = OVERPASS_API_URLS[0]  # Compatibilité ancienne version
    
    # Timeouts optimisés pour récupération exhaustive
    OSM_REQUEST_TIMEOUT = int(os.environ.get('OSM_REQUEST_TIMEOUT', 600))  # 10 minutes
    OSM_MAX_RETRIES = int(os.environ.get('OSM_MAX_RETRIES', 5))
    OSM_RETRY_DELAY = float(os.environ.get('OSM_RETRY_DELAY', 2.0))  # Délai initial
    OVERPASS_TIMEOUT = OSM_REQUEST_TIMEOUT  # Compatibilité
    
    # Parallélisation ultra-optimisée
    OSM_MAX_CONCURRENT = int(os.environ.get('OSM_MAX_CONCURRENT', 15))  # 15 requêtes simultanées
    OSM_CHUNK_SIZE = float(os.environ.get('OSM_CHUNK_SIZE', 0.08))  # Subdivision en degrés
    OSM_BATCH_SIZE = int(os.environ.get('OSM_BATCH_SIZE', 1000))  # Bâtiments par batch
    
    # Cache ultra-optimisé
    OSM_CACHE_ENABLED = os.environ.get('OSM_CACHE_ENABLED', 'true').lower() == 'true'
    OSM_CACHE_DURATION_HOURS = int(os.environ.get('OSM_CACHE_DURATION_HOURS', 72))  # 3 jours
    OSM_CACHE_DURATION = timedelta(hours=OSM_CACHE_DURATION_HOURS)  # Compatibilité
    OSM_CACHE_COMPRESSION = os.environ.get('OSM_CACHE_COMPRESSION', 'true').lower() == 'true'
    OSM_CACHE_MAX_SIZE_GB = float(os.environ.get('OSM_CACHE_MAX_SIZE_GB', 10))  # 10 GB max
    OSM_CACHE_MAX_SIZE_MB = int(OSM_CACHE_MAX_SIZE_GB * 1024)  # Compatibilité
    
    # Limites OSM optimisées
    OSM_MAX_BUILDINGS_PER_REQUEST = int(os.environ.get('OSM_MAX_BUILDINGS_PER_REQUEST', 50000))
    OVERPASS_MAX_SIZE = 2 * 1024 * 1024 * 1024  # 2GB max
    
    # Optimisations spécifiques Malaysia
    OSM_MALAYSIA_PRIORITY_STATES = [
        'selangor', 'johor', 'sabah', 'sarawak', 'perak', 
        'kedah', 'pahang', 'kuala_lumpur', 'penang'
    ]
    OSM_ENABLE_ADAPTIVE_CHUNKING = os.environ.get('OSM_ADAPTIVE_CHUNKING', 'true').lower() == 'true'
    OSM_ENABLE_SMART_RETRY = os.environ.get('OSM_SMART_RETRY', 'true').lower() == 'true'
    
    # === CONFIGURATION GÉNÉRATION ÉNERGÉTIQUE ===
    
    # Limites optimisées pour génération exhaustive
    MAX_BUILDINGS = int(os.environ.get('MAX_BUILDINGS_PER_GENERATION', 1000000))  # 1M max
    MIN_BUILDINGS = 1
    MAX_PERIOD_DAYS = int(os.environ.get('MAX_PERIOD_DAYS', 1095))  # 3 ans maximum
    MIN_PERIOD_DAYS = 1
    MAX_TIMESERIES_RECORDS = int(os.environ.get('MAX_TIMESERIES_RECORDS', 365000000))  # 365M max
    
    # Génération par défaut
    DEFAULT_NUM_BUILDINGS = int(os.environ.get('DEFAULT_NUM_BUILDINGS', 1000))
    DEFAULT_START_DATE = os.environ.get('DEFAULT_START_DATE', '2024-01-01')
    DEFAULT_END_DATE = os.environ.get('DEFAULT_END_DATE', '2024-12-31')
    DEFAULT_FREQUENCY = os.environ.get('DEFAULT_FREQUENCY', 'D')  # Quotidien
    
    # Formats supportés
    SUPPORTED_FREQUENCIES = ['H', 'D', 'W', 'M', '30T', '15T']
    SUPPORTED_EXPORT_FORMATS = ['json', 'csv', 'parquet', 'excel', 'geojson']
    
    # Optimisations génération
    GENERATION_BATCH_SIZE = int(os.environ.get('GENERATION_BATCH_SIZE', 10000))
    GENERATION_PARALLEL_WORKERS = int(os.environ.get('GENERATION_PARALLEL_WORKERS', 8))
    GENERATION_MEMORY_LIMIT_GB = float(os.environ.get('GENERATION_MEMORY_LIMIT_GB', 16))
    CHUNK_SIZE_BUILDINGS = GENERATION_BATCH_SIZE  # Compatibilité
    CHUNK_SIZE_TIMESERIES = GENERATION_BATCH_SIZE * 10  # Compatibilité
    
    # === CONFIGURATION EXPORT ET TÉLÉCHARGEMENT ===
    
    # Export optimisé
    DEFAULT_EXPORT_FORMAT = os.environ.get('DEFAULT_EXPORT_FORMAT', 'parquet')
    ENABLE_EXPORT_COMPRESSION = os.environ.get('ENABLE_EXPORT_COMPRESSION', 'true').lower() == 'true'
    COMPRESSION_THRESHOLD_MB = float(os.environ.get('COMPRESSION_THRESHOLD_MB', 50))  # Compresser si > 50MB
    MAX_EXPORT_SIZE_GB = float(os.environ.get('MAX_EXPORT_SIZE_GB', 5))  # 5GB max
    EXPORT_CHUNK_SIZE_RECORDS = int(os.environ.get('EXPORT_CHUNK_SIZE_RECORDS', 100000))
    
    # === CONFIGURATION RÉPERTOIRES ===
    
    # Répertoires de base
    DATA_DIR = Path(os.environ.get('DATA_DIR', BASE_DIR / 'data'))
    GENERATED_DATA_DIR = DATA_DIR / 'generated'
    CACHE_DIR = Path(os.environ.get('CACHE_DIR', DATA_DIR / 'cache'))
    EXPORT_DIR = Path(os.environ.get('EXPORT_DIR', DATA_DIR / 'exports'))
    TEMP_DIR = Path(os.environ.get('TEMP_DIR', DATA_DIR / 'temp'))
    LOG_DIR = Path(os.environ.get('LOG_DIR', BASE_DIR / 'logs'))
    SAMPLES_DIR = DATA_DIR / 'samples'
    
    # === CONFIGURATION LOGGING ===
    
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    LOG_FILE = os.environ.get('LOG_FILE', str(LOG_DIR / 'app.log'))
    
    # Rotation des logs
    LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
    LOG_BACKUP_COUNT = 5
    
    # === CONFIGURATION PERFORMANCE ===
    
    # Cache application
    CACHE_TYPE = 'simple'
    CACHE_DEFAULT_TIMEOUT = 300  # 5 minutes
    
    # API et timeouts
    API_RATE_LIMIT = os.environ.get('API_RATE_LIMIT', '1000 per hour')
    API_TIMEOUT = int(os.environ.get('API_TIMEOUT', 3600))  # 1 heure
    
    # Génération asynchrone
    ASYNC_GENERATION_ENABLED = os.environ.get('ASYNC_GENERATION_ENABLED', 'false').lower() == 'true'
    MAX_CONCURRENT_GENERATIONS = int(os.environ.get('MAX_CONCURRENT_GENERATIONS', 2))
    
    # Optimisations générales
    ENABLE_DATA_COMPRESSION = True
    
    # === CONFIGURATION MONITORING ===
    
    # Métriques de performance
    ENABLE_PERFORMANCE_MONITORING = os.environ.get('ENABLE_PERFORMANCE_MONITORING', 'true').lower() == 'true'
    STATS_RETENTION_DAYS = int(os.environ.get('STATS_RETENTION_DAYS', 30))
    
    # Alertes
    ALERT_ON_HIGH_ERROR_RATE = float(os.environ.get('ALERT_ERROR_RATE_THRESHOLD', 0.1))  # 10%
    ALERT_ON_SLOW_REQUESTS = float(os.environ.get('ALERT_SLOW_REQUEST_THRESHOLD', 300))  # 5 minutes
    
    # === CONFIGURATION SÉCURITÉ ET VALIDATION ===
    
    # CORS
    CORS_ORIGINS = os.environ.get('CORS_ORIGINS', '*').split(',')
    
    # Validation
    VALIDATE_OSM_DATA = os.environ.get('VALIDATE_OSM_DATA', 'true').lower() == 'true'
    STRICT_VALIDATION = os.environ.get('STRICT_VALIDATION', 'false').lower() == 'true'
    
    # Limites de sécurité
    MAX_REQUEST_SIZE_MB = float(os.environ.get('MAX_REQUEST_SIZE_MB', 100))
    MAX_CONCURRENT_USERS = int(os.environ.get('MAX_CONCURRENT_USERS', 50))
    
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
    
    # === CONFIGURATION SPÉCIFIQUE MALAYSIA ===
    
    # Fuseau horaire et localisation
    TIMEZONE = 'Asia/Kuala_Lumpur'
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
    
    # === MÉTHODES UTILITAIRES ===
    
    @classmethod
    def create_directories(cls):
        """Crée tous les répertoires nécessaires."""
        directories = [
            cls.DATA_DIR, cls.GENERATED_DATA_DIR, cls.CACHE_DIR,
            cls.EXPORT_DIR, cls.TEMP_DIR, cls.LOG_DIR, cls.SAMPLES_DIR
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_osm_config(cls):
        """Retourne la configuration OSM optimisée."""
        return {
            'overpass_urls': cls.OVERPASS_API_URLS,
            'request_timeout': cls.OSM_REQUEST_TIMEOUT,
            'max_retries': cls.OSM_MAX_RETRIES,
            'retry_delay': cls.OSM_RETRY_DELAY,
            'max_concurrent': cls.OSM_MAX_CONCURRENT,
            'chunk_size': cls.OSM_CHUNK_SIZE,
            'batch_size': cls.OSM_BATCH_SIZE,
            'cache_enabled': cls.OSM_CACHE_ENABLED,
            'cache_duration_hours': cls.OSM_CACHE_DURATION_HOURS,
            'cache_compression': cls.OSM_CACHE_COMPRESSION,
            'cache_max_size_gb': cls.OSM_CACHE_MAX_SIZE_GB,
            'malaysia_priority_states': cls.OSM_MALAYSIA_PRIORITY_STATES,
            'adaptive_chunking': cls.OSM_ENABLE_ADAPTIVE_CHUNKING,
            'smart_retry': cls.OSM_ENABLE_SMART_RETRY,
            'cache_dir': cls.CACHE_DIR,
            'data_dir': cls.DATA_DIR
        }
    
    @classmethod
    def get_generation_config(cls):
        """Retourne la configuration de génération."""
        return {
            'default_num_buildings': cls.DEFAULT_NUM_BUILDINGS,
            'default_start_date': cls.DEFAULT_START_DATE,
            'default_end_date': cls.DEFAULT_END_DATE,
            'default_frequency': cls.DEFAULT_FREQUENCY,
            'max_buildings': cls.MAX_BUILDINGS,
            'max_timeseries_records': cls.MAX_TIMESERIES_RECORDS,
            'batch_size': cls.GENERATION_BATCH_SIZE,
            'parallel_workers': cls.GENERATION_PARALLEL_WORKERS,
            'memory_limit_gb': cls.GENERATION_MEMORY_LIMIT_GB
        }
    
    @classmethod
    def get_export_config(cls):
        """Retourne la configuration d'export."""
        return {
            'supported_formats': cls.SUPPORTED_EXPORT_FORMATS,
            'default_format': cls.DEFAULT_EXPORT_FORMAT,
            'enable_compression': cls.ENABLE_EXPORT_COMPRESSION,
            'compression_threshold_mb': cls.COMPRESSION_THRESHOLD_MB,
            'max_size_gb': cls.MAX_EXPORT_SIZE_GB,
            'chunk_size_records': cls.EXPORT_CHUNK_SIZE_RECORDS,
            'export_dir': cls.EXPORT_DIR,
            'temp_dir': cls.TEMP_DIR
        }


class DevelopmentConfig(Config):
    """Configuration pour développement."""
    DEBUG = True
    
    # OSM plus permissif en dev
    OSM_REQUEST_TIMEOUT = 300  # 5 minutes
    OSM_MAX_CONCURRENT = 5  # Moins de charge
    OSM_CACHE_DURATION_HOURS = 1  # Cache court pour tests
    
    # Limites réduites pour dev
    DEFAULT_NUM_BUILDINGS = 100
    MAX_BUILDINGS = 10000
    
    # Logs détaillés
    LOG_LEVEL = 'DEBUG'
    
    # Cache désactivé pour dev
    CACHE_TYPE = 'null'
    OSM_CACHE_ENABLED = False


class ProductionConfig(Config):
    """Configuration pour production."""
    DEBUG = False
    
    # OSM optimisé pour production
    OSM_REQUEST_TIMEOUT = 900  # 15 minutes
    OSM_MAX_CONCURRENT = 20  # Maximum de performance
    OSM_CACHE_DURATION_HOURS = 168  # 1 semaine
    
    # Limites production
    MAX_BUILDINGS = 2000000  # 2M bâtiments max
    MAX_TIMESERIES_RECORDS = 730000000  # 730M records max
    
    # Sécurité renforcée
    STRICT_VALIDATION = True
    MAX_CONCURRENT_USERS = 100
    
    # Monitoring activé
    ENABLE_PERFORMANCE_MONITORING = True
    
    # Performance optimisée
    CACHE_TYPE = 'redis'
    CACHE_REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
    
    # Génération asynchrone activée
    ASYNC_GENERATION_ENABLED = True
    MAX_CONCURRENT_GENERATIONS = 4
    
    # SSL et sécurité
    PREFERRED_URL_SCHEME = 'https'
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    
    @property
    def SECRET_KEY(self):
        """Vérification de la clé secrète en production."""
        secret_key = os.environ.get('SECRET_KEY')
        if not secret_key:
            raise ValueError("SECRET_KEY doit être définie en production")
        return secret_key


class TestingConfig(Config):
    """Configuration pour tests."""
    TESTING = True
    DEBUG = True
    
    # OSM minimal pour tests
    OSM_REQUEST_TIMEOUT = 30
    OSM_MAX_CONCURRENT = 2
    OSM_CACHE_ENABLED = False  # Pas de cache en tests
    
    # Limites minimales
    DEFAULT_NUM_BUILDINGS = 10
    MAX_BUILDINGS = 100
    
    # Répertoires de test
    import tempfile
    temp_dir = Path(tempfile.gettempdir()) / 'test_malaysia_generator'
    DATA_DIR = temp_dir
    CACHE_DIR = temp_dir / 'cache'
    EXPORT_DIR = temp_dir / 'exports'
    
    # Cache désactivé
    CACHE_TYPE = 'null'
    OSM_CACHE_ENABLED = False
    
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
    
    config_class = config.get(config_name, DevelopmentConfig)
    
    # Créer les répertoires nécessaires
    config_class.create_directories()
    
    print(f"🔧 Configuration chargée: {config_class.__name__}")
    print(f"   📡 OSM Timeout: {config_class.OSM_REQUEST_TIMEOUT}s")
    print(f"   🚀 OSM Concurrent: {config_class.OSM_MAX_CONCURRENT}")
    print(f"   💾 Cache: {'Activé' if config_class.OSM_CACHE_ENABLED else 'Désactivé'}")
    print(f"   🏢 Max buildings: {config_class.MAX_BUILDINGS:,}")
    
    return config_class


def validate_config(config_instance):
    """
    Valide les paramètres de configuration.
    
    Args:
        config_instance: Instance de configuration Flask (app.config)
        
    Raises:
        ValueError: Si la configuration est invalide
    """
    # Vérifier les répertoires obligatoires
    required_dirs = [
        config_instance.get('GENERATED_DATA_DIR'),
        config_instance.get('CACHE_DIR'), 
        config_instance.get('LOG_DIR')
    ]
    
    for directory in required_dirs:
        if directory is None:
            continue
            
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise ValueError(f"Impossible de créer le répertoire {directory}: {e}")
    
    # Vérifier les limites logiques
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
    
    # Vérifier les fréquences supportées
    default_freq = config_instance.get('DEFAULT_FREQUENCY')
    supported_freq = config_instance.get('SUPPORTED_FREQUENCIES')
    
    if default_freq is not None and supported_freq is not None:
        if default_freq not in supported_freq:
            raise ValueError(f"DEFAULT_FREQUENCY doit être dans {supported_freq}")


# Configuration globale
AppConfig = get_config()