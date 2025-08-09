#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MODÈLE LOCATION - GÉNÉRATEUR MALAYSIA
Fichier: app/models/location.py

Modèle de données pour représenter une localisation géographique
avec ses caractéristiques climatiques et administratives en Malaysia.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Modèles structurés
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple
from datetime import datetime
import math


@dataclass
class Location:
    """
    Modèle représentant une localisation géographique en Malaysia.
    
    Attributes:
        city: Nom de la ville
        latitude: Latitude (degrés décimaux)
        longitude: Longitude (degrés décimaux)
        state: État malaysien
        region: Région géographique
        population: Population de la ville
        timezone: Fuseau horaire
        altitude: Altitude en mètres
        climate_zone: Zone climatique
        postal_codes: Codes postaux associés
        administrative_info: Informations administratives
    """
    
    # Informations de base
    city: str
    latitude: float
    longitude: float
    
    # Informations administratives
    state: str = 'Unknown'
    region: str = 'Unknown'
    population: int = 0
    
    # Informations géographiques
    timezone: str = 'Asia/Kuala_Lumpur'
    altitude: Optional[float] = None
    climate_zone: str = 'tropical'
    
    # Informations supplémentaires
    postal_codes: List[str] = field(default_factory=list)
    administrative_info: Dict[str, any] = field(default_factory=dict)
    
    # Métadonnées
    location_id: str = field(default='', init=False)
    state_code: str = field(default='', init=False)
    created_at: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """Initialisation après création de l'instance."""
        self._validate_coordinates()
        self._generate_location_id()
        self._determine_state_code()
        self._determine_climate_zone()
        self._determine_region()
    
    def _validate_coordinates(self):
        """
        Valide les coordonnées géographiques.
        
        Raises:
            ValueError: Si les coordonnées sont invalides
        """
        if not (-90 <= self.latitude <= 90):
            raise ValueError(f"Latitude invalide: {self.latitude}. Doit être entre -90 et 90.")
        
        if not (-180 <= self.longitude <= 180):
            raise ValueError(f"Longitude invalide: {self.longitude}. Doit être entre -180 et 180.")
        
        # Vérification spécifique pour la Malaysia
        if not (0.5 <= self.latitude <= 7.5):
            # Warning mais pas d'erreur pour permettre les tests
            pass
        
        if not (99.0 <= self.longitude <= 120.0):
            # Warning mais pas d'erreur pour permettre les tests
            pass
    
    def _generate_location_id(self):
        """Génère un identifiant unique pour la localisation."""
        # Format: MY_<hash_des_coordonnées>
        coord_hash = abs(hash(f"{self.latitude:.4f},{self.longitude:.4f}")) % 100000
        self.location_id = f"MY_{coord_hash:05d}"
    
    def _determine_state_code(self):
        """Détermine le code de l'état malaysien selon les coordonnées."""
        
        # Mapping approximatif des états malaysiens par coordonnées
        state_mappings = {
            'Johor': {'lat_range': (1.2, 2.8), 'lon_range': (102.5, 104.8), 'code': 'JHR'},
            'Kedah': {'lat_range': (5.0, 6.8), 'lon_range': (99.6, 101.0), 'code': 'KDH'},
            'Kelantan': {'lat_range': (4.5, 6.3), 'lon_range': (101.8, 102.8), 'code': 'KTN'},
            'Malacca': {'lat_range': (2.0, 2.5), 'lon_range': (102.0, 102.6), 'code': 'MLK'},
            'Negeri Sembilan': {'lat_range': (2.4, 3.2), 'lon_range': (101.8, 102.8), 'code': 'NSN'},
            'Pahang': {'lat_range': (2.8, 4.8), 'lon_range': (101.8, 104.0), 'code': 'PHG'},
            'Penang': {'lat_range': (5.2, 5.6), 'lon_range': (100.1, 100.6), 'code': 'PNG'},
            'Perak': {'lat_range': (3.8, 5.8), 'lon_range': (100.5, 101.8), 'code': 'PRK'},
            'Perlis': {'lat_range': (6.3, 6.8), 'lon_range': (100.0, 100.6), 'code': 'PLS'},
            'Selangor': {'lat_range': (2.8, 3.8), 'lon_range': (101.0, 102.0), 'code': 'SGR'},
            'Terengganu': {'lat_range': (4.0, 6.0), 'lon_range': (102.8, 103.8), 'code': 'TRG'},
            'Federal Territory': {'lat_range': (3.0, 3.3), 'lon_range': (101.5, 101.8), 'code': 'WP'},
            'Sabah': {'lat_range': (4.0, 7.5), 'lon_range': (115.0, 119.5), 'code': 'SBH'},
            'Sarawak': {'lat_range': (0.8, 5.0), 'lon_range': (109.5, 115.5), 'code': 'SWK'}
        }
        
        # Déterminer l'état selon les coordonnées
        for state_name, info in state_mappings.items():
            lat_min, lat_max = info['lat_range']
            lon_min, lon_max = info['lon_range']
            
            if (lat_min <= self.latitude <= lat_max and 
                lon_min <= self.longitude <= lon_max):
                if self.state == 'Unknown':
                    self.state = state_name
                self.state_code = info['code']
                return
        
        # État par défaut si non trouvé
        self.state_code = 'UNK'
    
    def _determine_climate_zone(self):
        """Détermine la zone climatique selon la localisation."""
        # Malaysia a un climat tropical, mais avec variations
        if self.climate_zone == 'tropical':  # Valeur par défaut
            
            # Zones côtières vs intérieures
            coastal_distance = self._calculate_coastal_distance()
            
            if coastal_distance < 50:  # Moins de 50km de la côte
                if self.latitude > 5.0:  # Nord de la Malaysia
                    self.climate_zone = 'tropical_coastal_north'
                else:
                    self.climate_zone = 'tropical_coastal_south'
            else:
                if self.altitude and self.altitude > 500:
                    self.climate_zone = 'tropical_highland'
                else:
                    self.climate_zone = 'tropical_inland'
    
    def _determine_region(self):
        """Détermine la région géographique."""
        
        if self.region == 'Unknown':
            # Régions de la Malaysia péninsulaire
            if self.longitude < 109:  # Malaysia péninsulaire
                if self.latitude > 5.0:
                    self.region = 'Northern Peninsula'
                elif self.latitude > 3.5:
                    self.region = 'Central Peninsula'
                else:
                    self.region = 'Southern Peninsula'
            
            # Bornéo malaysien
            elif self.longitude > 109:
                if self.latitude > 4.0:
                    self.region = 'Sabah'
                else:
                    self.region = 'Sarawak'
    
    def _calculate_coastal_distance(self) -> float:
        """
        Calcule la distance approximative à la côte la plus proche.
        
        Returns:
            Distance en kilomètres
        """
        # Points côtiers de référence pour la Malaysia
        coastal_points = [
            (5.414, 100.333),  # Penang
            (3.139, 101.687),  # Port Klang
            (1.465, 103.747),  # Johor Bahru
            (2.189, 102.250),  # Malacca
            (5.979, 116.075),  # Kota Kinabalu
            (1.553, 110.359),  # Kuching
        ]
        
        min_distance = float('inf')
        
        for coastal_lat, coastal_lon in coastal_points:
            distance = self.calculate_distance_to(coastal_lat, coastal_lon)
            min_distance = min(min_distance, distance)
        
        return min_distance
    
    def calculate_distance_to(self, other_lat: float, other_lon: float) -> float:
        """
        Calcule la distance à un autre point géographique.
        
        Args:
            other_lat: Latitude du point de destination
            other_lon: Longitude du point de destination
            
        Returns:
            Distance en kilomètres
        """
        # Formule de Haversine
        R = 6371  # Rayon de la Terre en km
        
        lat1_rad = math.radians(self.latitude)
        lon1_rad = math.radians(self.longitude)
        lat2_rad = math.radians(other_lat)
        lon2_rad = math.radians(other_lon)
        
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = (math.sin(dlat/2)**2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        
        return R * c
    
    def is_in_malaysia(self) -> bool:
        """
        Vérifie si la localisation est en Malaysia.
        
        Returns:
            True si en Malaysia
        """
        # Limites approximatives de la Malaysia
        return (0.5 <= self.latitude <= 7.5 and 
                99.0 <= self.longitude <= 120.0)
    
    def is_urban_area(self) -> bool:
        """
        Détermine si la zone est urbaine selon la population.
        
        Returns:
            True si zone urbaine
        """
        return self.population >= 100000
    
    def is_metropolitan_area(self) -> bool:
        """
        Détermine si la zone est métropolitaine.
        
        Returns:
            True si zone métropolitaine
        """
        return self.population >= 500000
    
    def get_climate_characteristics(self) -> Dict[str, any]:
        """
        Retourne les caractéristiques climatiques de la zone.
        
        Returns:
            Dictionnaire des caractéristiques climatiques
        """
        base_climate = {
            'temperature_range': (24, 35),  # °C
            'humidity_range': (70, 90),     # %
            'rainfall_mm_year': 2500,
            'monsoon_season': 'November-February',
            'dry_season': 'June-August'
        }
        
        # Ajustements selon la zone climatique
        if self.climate_zone == 'tropical_coastal_north':
            base_climate['rainfall_mm_year'] = 2800
            base_climate['humidity_range'] = (75, 95)
        elif self.climate_zone == 'tropical_highland':
            base_climate['temperature_range'] = (18, 28)
            base_climate['humidity_range'] = (65, 85)
        elif self.climate_zone == 'tropical_inland':
            base_climate['temperature_range'] = (26, 38)
            base_climate['humidity_range'] = (60, 85)
        
        return base_climate
    
    def get_energy_context(self) -> Dict[str, any]:
        """
        Retourne le contexte énergétique de la localisation.
        
        Returns:
            Informations sur le contexte énergétique local
        """
        context = {
            'cooling_degree_days': 0,
            'heating_degree_days': 0,
            'solar_potential': 'high',
            'wind_potential': 'low',
            'grid_stability': 'high',
            'renewable_adoption': 'medium'
        }
        
        # Calcul approximatif des degrés-jours de refroidissement
        # Malaysia a toujours besoin de refroidissement
        climate = self.get_climate_characteristics()
        avg_temp = sum(climate['temperature_range']) / 2
        context['cooling_degree_days'] = max(0, avg_temp - 18) * 365
        
        # Ajustements selon la région
        if self.is_metropolitan_area():
            context['grid_stability'] = 'very_high'
            context['renewable_adoption'] = 'high'
        elif self.region in ['Sabah', 'Sarawak']:
            context['renewable_adoption'] = 'low'
            context['grid_stability'] = 'medium'
        
        return context
    
    def to_dict(self) -> Dict[str, any]:
        """
        Convertit la localisation en dictionnaire.
        
        Returns:
            Dictionnaire représentant la localisation
        """
        return {
            'location_id': self.location_id,
            'city': self.city,
            'state': self.state,
            'state_code': self.state_code,
            'region': self.region,
            'latitude': round(self.latitude, 6),
            'longitude': round(self.longitude, 6),
            'population': self.population,
            'timezone': self.timezone,
            'altitude': self.altitude,
            'climate_zone': self.climate_zone,
            'postal_codes': self.postal_codes,
            'administrative_info': self.administrative_info,
            'created_at': self.created_at.isoformat(),
            'climate_characteristics': self.get_climate_characteristics(),
            'energy_context': self.get_energy_context(),
            'is_urban': self.is_urban_area(),
            'is_metropolitan': self.is_metropolitan_area(),
            'is_in_malaysia': self.is_in_malaysia()
        }
    
    def __str__(self) -> str:
        """Représentation textuelle de la localisation."""
        return f"Location({self.city}, {self.state}, {self.latitude}, {self.longitude})"
    
    def __repr__(self) -> str:
        """Représentation pour le debug."""
        return (f"Location(city='{self.city}', state='{self.state}', "
                f"latitude={self.latitude}, longitude={self.longitude}, "
                f"location_id='{self.location_id}')")


# Fonctions utilitaires pour créer des locations

def create_location_from_coordinates(city: str, latitude: float, longitude: float, **kwargs) -> Location:
    """
    Crée une instance Location à partir de coordonnées.
    
    Args:
        city: Nom de la ville
        latitude: Latitude
        longitude: Longitude
        **kwargs: Paramètres additionnels
        
    Returns:
        Instance Location
    """
    return Location(
        city=city,
        latitude=latitude,
        longitude=longitude,
        **kwargs
    )


def create_location_from_dict(location_data: Dict) -> Location:
    """
    Crée une instance Location à partir d'un dictionnaire.
    
    Args:
        location_data: Dictionnaire contenant les données de localisation
        
    Returns:
        Instance Location
    """
    # Champs obligatoires
    required_fields = ['city', 'latitude', 'longitude']
    for field in required_fields:
        if field not in location_data:
            raise ValueError(f"Champ obligatoire manquant: {field}")
    
    # Créer l'instance avec tous les champs disponibles
    location_kwargs = {}
    for key, value in location_data.items():
        if hasattr(Location, key):
            location_kwargs[key] = value
    
    return Location(**location_kwargs)


# Export des classes et fonctions
__all__ = [
    'Location',
    'create_location_from_coordinates',
    'create_location_from_dict'
]