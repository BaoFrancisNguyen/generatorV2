
# ===== app/config/__init__.py =====
"""
Package de configuration pour le générateur Malaysia.
Gère les configurations par environnement (dev/prod/test).
"""

from .base import Config, DevelopmentConfig, ProductionConfig, TestingConfig, get_config

__all__ = ['Config', 'DevelopmentConfig', 'ProductionConfig', 'TestingConfig', 'get_config']
