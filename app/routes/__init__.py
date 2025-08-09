# ===== app/routes/__init__.py =====
"""
Package des routes Flask pour le générateur Malaysia.
Contient tous les blueprints organisés par domaine.
"""

from .main import main_bp
from .generation import generation_bp
from .api import api_bp
from .osm import osm_bp

__all__ = ['main_bp', 'generation_bp', 'api_bp', 'osm_bp']