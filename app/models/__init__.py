# ===== app/models/__init__.py =====
"""
Package des modèles de données pour le générateur Malaysia.
Définit les structures de données pour bâtiments, locations et séries temporelles.
"""

from .building import Building
from .location import Location
from .timeseries import TimeSeries, TimeSeriesCollection

__all__ = ['Building', 'Location', 'TimeSeries', 'TimeSeriesCollection']