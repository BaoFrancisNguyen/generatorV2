#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CONFIGURATION DU LOGGING - GÉNÉRATEUR MALAYSIA
Fichier: app/utils/logger.py

Configuration centralisée du système de logging avec rotation,
niveaux configurables et formatage adapté au contexte malaysien.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Logging modulaire
"""

import os
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path


def setup_logger(app=None):
    """
    Configure le système de logging pour l'application.
    
    Args:
        app: Instance Flask (optionnel)
    """
    
    # Déterminer la configuration de logging
    if app:
        log_level = app.config.get('LOG_LEVEL', 'INFO')
        log_dir = Path(app.config.get('LOG_DIR', 'logs'))
        log_format = app.config.get('LOG_FORMAT', 
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        date_format = app.config.get('LOG_DATE_FORMAT', '%Y-%m-%d %H:%M:%S')
        max_bytes = app.config.get('LOG_MAX_BYTES', 10 * 1024 * 1024)
        backup_count = app.config.get('LOG_BACKUP_COUNT', 5)
    else:
        # Configuration par défaut
        log_level = os.environ.get('LOG_LEVEL', 'INFO')
        log_dir = Path('logs')
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        date_format = '%Y-%m-%d %H:%M:%S'
        max_bytes = 10 * 1024 * 1024  # 10MB
        backup_count = 5
    
    # Créer le répertoire de logs
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Convertir le niveau de log string en constante
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Configuration du logger racine
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Supprimer les handlers existants pour éviter la duplication
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Formatter personnalisé avec timezone Malaysia
    class MalaysiaFormatter(logging.Formatter):
        """Formatter personnalisé avec timezone Malaysia."""
        
        def __init__(self, fmt=None, datefmt=None):
            super().__init__(fmt, datefmt)
            
        def formatTime(self, record, datefmt=None):
            """Formate le timestamp avec le timezone Malaysia."""
            import pytz
            
            # Convertir vers le timezone Malaysia
            malaysia_tz = pytz.timezone('Asia/Kuala_Lumpur')
            ct = datetime.fromtimestamp(record.created, tz=malaysia_tz)
            
            if datefmt:
                return ct.strftime(datefmt)
            else:
                return ct.strftime('%Y-%m-%d %H:%M:%S %Z')
        
        def format(self, record):
            """Formate le message de log avec des emojis selon le niveau."""
            
            # Ajouter des emojis selon le niveau de log
            level_emojis = {
                'DEBUG': '🔧',
                'INFO': 'ℹ️',
                'WARNING': '⚠️',
                'ERROR': '❌',
                'CRITICAL': '🚨'
            }
            
            emoji = level_emojis.get(record.levelname, '')
            
            # Formater le message original
            original_msg = super().format(record)
            
            # Ajouter l'emoji au début si on n'est pas en production
            if emoji and not os.environ.get('FLASK_ENV') == 'production':
                return f"{emoji} {original_msg}"
            
            return original_msg
    
    formatter = MalaysiaFormatter(log_format, date_format)
    
    # Handler pour fichier avec rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / 'malaysia_generator.log',
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # Handler pour la console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Configurer les loggers tiers pour réduire le bruit
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    # Logger spécial pour les erreurs critiques
    error_handler = logging.handlers.RotatingFileHandler(
        log_dir / 'errors.log',
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    # Créer un logger dédié aux erreurs
    error_logger = logging.getLogger('malaysia_generator.errors')
    error_logger.addHandler(error_handler)
    
    # Logger pour les requêtes API
    api_handler = logging.handlers.RotatingFileHandler(
        log_dir / 'api.log',
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    api_handler.setLevel(logging.INFO)
    api_handler.setFormatter(formatter)
    
    api_logger = logging.getLogger('malaysia_generator.api')
    api_logger.addHandler(api_handler)
    
    # Logger pour les performances
    perf_handler = logging.handlers.RotatingFileHandler(
        log_dir / 'performance.log',
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    perf_handler.setLevel(logging.INFO)
    perf_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(message)s',
        date_format
    )
    perf_handler.setFormatter(perf_formatter)
    
    perf_logger = logging.getLogger('malaysia_generator.performance')
    perf_logger.addHandler(perf_handler)
    
    # Logger pour les audits de sécurité
    if os.environ.get('FLASK_ENV') == 'production':
        audit_handler = logging.handlers.RotatingFileHandler(
            log_dir / 'audit.log',
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        audit_handler.setLevel(logging.INFO)
        audit_handler.setFormatter(formatter)
        
        audit_logger = logging.getLogger('malaysia_generator.audit')
        audit_logger.addHandler(audit_handler)
    
    # Message de confirmation
    logger = logging.getLogger(__name__)
    logger.info(f"📝 Système de logging configuré - Niveau: {log_level}")
    logger.info(f"📁 Logs sauvegardés dans: {log_dir}")


class LoggerMixin:
    """
    Mixin pour ajouter facilement le logging à une classe.
    """
    
    @property
    def logger(self):
        """Retourne un logger configuré pour cette classe."""
        return logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")


class PerformanceLogger:
    """
    Logger spécialisé pour mesurer les performances.
    """
    
    def __init__(self, operation_name: str):
        """
        Initialise le logger de performance.
        
        Args:
            operation_name: Nom de l'opération à mesurer
        """
        self.operation_name = operation_name
        self.logger = logging.getLogger('malaysia_generator.performance')
        self.start_time = None
    
    def __enter__(self):
        """Démarre la mesure de performance."""
        self.start_time = datetime.now()
        self.logger.info(f"🚀 DÉBUT {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Termine la mesure et log le résultat."""
        if self.start_time:
            duration = (datetime.now() - self.start_time).total_seconds()
            
            if exc_type:
                self.logger.error(f"❌ ÉCHEC {self.operation_name} - Durée: {duration:.3f}s - Erreur: {exc_val}")
            else:
                self.logger.info(f"✅ FIN {self.operation_name} - Durée: {duration:.3f}s")
    
    def log_metric(self, metric_name: str, value: any):
        """
        Log une métrique spécifique.
        
        Args:
            metric_name: Nom de la métrique
            value: Valeur de la métrique
        """
        self.logger.info(f"📊 {self.operation_name} - {metric_name}: {value}")


class APILogger:
    """
    Logger spécialisé pour les requêtes API.
    """
    
    def __init__(self):
        self.logger = logging.getLogger('malaysia_generator.api')
    
    def log_request(self, method: str, endpoint: str, ip: str, user_agent: str = None):
        """
        Log une requête API entrante.
        
        Args:
            method: Méthode HTTP
            endpoint: Endpoint appelé
            ip: Adresse IP du client
            user_agent: User agent du client
        """
        message = f"📥 {method} {endpoint} - IP: {ip}"
        if user_agent:
            message += f" - UA: {user_agent[:100]}"
        
        self.logger.info(message)
    
    def log_response(self, endpoint: str, status_code: int, duration: float, response_size: int = None):
        """
        Log une réponse API.
        
        Args:
            endpoint: Endpoint appelé
            status_code: Code de statut HTTP
            duration: Durée de traitement en secondes
            response_size: Taille de la réponse en bytes
        """
        message = f"📤 {endpoint} - Status: {status_code} - Durée: {duration:.3f}s"
        if response_size:
            message += f" - Taille: {response_size} bytes"
        
        if status_code >= 400:
            self.logger.warning(message)
        else:
            self.logger.info(message)
    
    def log_error(self, endpoint: str, error: str, stack_trace: str = None):
        """
        Log une erreur API.
        
        Args:
            endpoint: Endpoint où l'erreur s'est produite
            error: Message d'erreur
            stack_trace: Stack trace complète
        """
        message = f"💥 ERREUR {endpoint} - {error}"
        if stack_trace:
            message += f"\nStack trace:\n{stack_trace}"
        
        self.logger.error(message)


class SecurityLogger:
    """
    Logger spécialisé pour les événements de sécurité.
    """
    
    def __init__(self):
        self.logger = logging.getLogger('malaysia_generator.audit')
    
    def log_suspicious_activity(self, ip: str, activity: str, details: str = None):
        """
        Log une activité suspecte.
        
        Args:
            ip: Adresse IP suspecte
            activity: Type d'activité
            details: Détails supplémentaires
        """
        message = f"🔒 ACTIVITÉ SUSPECTE - IP: {ip} - {activity}"
        if details:
            message += f" - Détails: {details}"
        
        self.logger.warning(message)
    
    def log_access_denied(self, ip: str, endpoint: str, reason: str):
        """
        Log un accès refusé.
        
        Args:
            ip: Adresse IP
            endpoint: Endpoint accédé
            reason: Raison du refus
        """
        message = f"🚫 ACCÈS REFUSÉ - IP: {ip} - Endpoint: {endpoint} - Raison: {reason}"
        self.logger.warning(message)
    
    def log_rate_limit_exceeded(self, ip: str, endpoint: str, limit: int):
        """
        Log un dépassement de limite de taux.
        
        Args:
            ip: Adresse IP
            endpoint: Endpoint concerné
            limit: Limite dépassée
        """
        message = f"⚡ LIMITE DÉPASSÉE - IP: {ip} - Endpoint: {endpoint} - Limite: {limit}/min"
        self.logger.warning(message)


def get_logger(name: str = None):
    """
    Fonction utilitaire pour obtenir un logger configuré.
    
    Args:
        name: Nom du logger (optionnel)
        
    Returns:
        Logger configuré
    """
    if name:
        return logging.getLogger(f"malaysia_generator.{name}")
    else:
        return logging.getLogger("malaysia_generator")


def log_function_call(func):
    """
    Décorateur pour logger automatiquement les appels de fonction.
    
    Args:
        func: Fonction à décorer
        
    Returns:
        Fonction décorée
    """
    import functools
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(f"{func.__module__}.{func.__name__}")
        
        # Log l'appel
        logger.debug(f"🔧 Appel de {func.__name__} avec args={len(args)}, kwargs={list(kwargs.keys())}")
        
        try:
            # Mesurer le temps d'exécution
            start_time = datetime.now()
            result = func(*args, **kwargs)
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.debug(f"✅ {func.__name__} terminé en {duration:.3f}s")
            return result
            
        except Exception as e:
            logger.error(f"❌ Erreur dans {func.__name__}: {str(e)}")
            raise
    
    return wrapper


def log_generation_metrics(num_buildings: int, duration: float, file_size: int = None):
    """
    Log les métriques de génération de données.
    
    Args:
        num_buildings: Nombre de bâtiments générés
        duration: Durée de génération en secondes
        file_size: Taille du fichier généré en bytes
    """
    perf_logger = logging.getLogger('malaysia_generator.performance')
    
    metrics = [
        f"Bâtiments: {num_buildings}",
        f"Durée: {duration:.3f}s",
        f"Vitesse: {num_buildings/duration:.1f} bât/s"
    ]
    
    if file_size:
        metrics.append(f"Taille: {file_size} bytes")
        metrics.append(f"Bytes/bât: {file_size/num_buildings:.0f}")
    
    perf_logger.info(f"📊 GÉNÉRATION - {' - '.join(metrics)}")


# Export des classes et fonctions principales
__all__ = [
    'setup_logger',
    'LoggerMixin',
    'PerformanceLogger',
    'APILogger',
    'SecurityLogger',
    'get_logger',
    'log_function_call',
    'log_generation_metrics'
]