#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DONNÉES MALAYSIA - GÉNÉRATEUR MALAYSIA
Fichier: app/utils/malaysia_data.py

Données spécifiques à la Malaysia : villes, coordonnées, populations,
caractéristiques climatiques et énergétiques pour une génération réaliste.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Données structurées
"""

import logging
from typing import Dict, List, Tuple, Optional, Any
import random
import numpy as np


class MalaysiaLocationData:
    """
    Classe contenant toutes les données géographiques et démographiques de Malaysia.
    
    Fournit des méthodes pour sélectionner des localisations avec pondération
    réaliste selon la population et les caractéristiques urbaines.
    """
    
    def __init__(self):
        """Initialise les données Malaysia."""
        self.logger = logging.getLogger(__name__)
        self._malaysia_locations = self._load_malaysia_locations()
        self._building_type_distributions = self._load_building_distributions()
        self._climate_zones = self._load_climate_zones()
        
        self.logger.info(f"✅ Données Malaysia chargées: {len(self._malaysia_locations)} villes")
    
    def _load_malaysia_locations(self) -> Dict[str, Dict]:
        """Charge la base de données complète des localisations Malaysia."""
        
        return {
            # === PENINSULAR MALAYSIA ===
            
            # Federal Territory
            'Kuala Lumpur': {
                'latitude': 3.1390, 'longitude': 101.6869,
                'state': 'Federal Territory', 'state_code': 'WP',
                'region': 'Central Peninsula', 'population': 1800000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 65,
                'type': 'capital', 'urban_level': 'metropolitan',
                'climate_zone': 'tropical_urban', 'economic_level': 'very_high'
            },
            'Putrajaya': {
                'latitude': 2.9264, 'longitude': 101.6964,
                'state': 'Federal Territory', 'state_code': 'WP',
                'region': 'Central Peninsula', 'population': 109000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 38,
                'type': 'administrative', 'urban_level': 'urban',
                'climate_zone': 'tropical_urban', 'economic_level': 'high'
            },
            'Labuan': {
                'latitude': 5.2831, 'longitude': 115.2308,
                'state': 'Federal Territory', 'state_code': 'WP',
                'region': 'Sabah', 'population': 99000,
                'timezone': 'Asia/Kuching', 'altitude': 5,
                'type': 'port', 'urban_level': 'urban',
                'climate_zone': 'tropical_coastal', 'economic_level': 'medium'
            },
            
            # Selangor
            'Shah Alam': {
                'latitude': 3.0733, 'longitude': 101.5185,
                'state': 'Selangor', 'state_code': 'SGR',
                'region': 'Central Peninsula', 'population': 641000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 50,
                'type': 'state_capital', 'urban_level': 'metropolitan',
                'climate_zone': 'tropical_urban', 'economic_level': 'high'
            },
            'Petaling Jaya': {
                'latitude': 3.1073, 'longitude': 101.6059,
                'state': 'Selangor', 'state_code': 'SGR',
                'region': 'Central Peninsula', 'population': 613000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 75,
                'type': 'suburban', 'urban_level': 'metropolitan',
                'climate_zone': 'tropical_urban', 'economic_level': 'very_high'
            },
            'Subang Jaya': {
                'latitude': 3.0450, 'longitude': 101.5532,
                'state': 'Selangor', 'state_code': 'SGR',
                'region': 'Central Peninsula', 'population': 708000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 60,
                'type': 'suburban', 'urban_level': 'metropolitan',
                'climate_zone': 'tropical_urban', 'economic_level': 'high'
            },
            'Klang': {
                'latitude': 3.0333, 'longitude': 101.4500,
                'state': 'Selangor', 'state_code': 'SGR',
                'region': 'Central Peninsula', 'population': 879000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 10,
                'type': 'port', 'urban_level': 'metropolitan',
                'climate_zone': 'tropical_coastal', 'economic_level': 'medium'
            },
            
            # Johor
            'Johor Bahru': {
                'latitude': 1.4927, 'longitude': 103.7414,
                'state': 'Johor', 'state_code': 'JHR',
                'region': 'Southern Peninsula', 'population': 497000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 25,
                'type': 'state_capital', 'urban_level': 'metropolitan',
                'climate_zone': 'tropical_coastal', 'economic_level': 'high'
            },
            'Skudai': {
                'latitude': 1.5333, 'longitude': 103.6500,
                'state': 'Johor', 'state_code': 'JHR',
                'region': 'Southern Peninsula', 'population': 159000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 30,
                'type': 'suburban', 'urban_level': 'urban',
                'climate_zone': 'tropical_inland', 'economic_level': 'medium'
            },
            
            # Penang
            'George Town': {
                'latitude': 5.4164, 'longitude': 100.3327,
                'state': 'Penang', 'state_code': 'PNG',
                'region': 'Northern Peninsula', 'population': 708000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 10,
                'type': 'heritage_city', 'urban_level': 'metropolitan',
                'climate_zone': 'tropical_coastal', 'economic_level': 'high'
            },
            'Butterworth': {
                'latitude': 5.4164, 'longitude': 100.3633,
                'state': 'Penang', 'state_code': 'PNG',
                'region': 'Northern Peninsula', 'population': 107000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 5,
                'type': 'industrial', 'urban_level': 'urban',
                'climate_zone': 'tropical_coastal', 'economic_level': 'medium'
            },
            
            # Perak
            'Ipoh': {
                'latitude': 4.5975, 'longitude': 101.0901,
                'state': 'Perak', 'state_code': 'PRK',
                'region': 'Northern Peninsula', 'population': 657000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 200,
                'type': 'state_capital', 'urban_level': 'metropolitan',
                'climate_zone': 'tropical_inland', 'economic_level': 'medium'
            },
            'Taiping': {
                'latitude': 4.8500, 'longitude': 100.7333,
                'state': 'Perak', 'state_code': 'PRK',
                'region': 'Northern Peninsula', 'population': 245000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 75,
                'type': 'historic', 'urban_level': 'urban',
                'climate_zone': 'tropical_inland', 'economic_level': 'medium'
            },
            
            # Kedah
            'Alor Setar': {
                'latitude': 6.1184, 'longitude': 100.3685,
                'state': 'Kedah', 'state_code': 'KDH',
                'region': 'Northern Peninsula', 'population': 405000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 15,
                'type': 'state_capital', 'urban_level': 'urban',
                'climate_zone': 'tropical_coastal', 'economic_level': 'medium'
            },
            'Sungai Petani': {
                'latitude': 5.6667, 'longitude': 100.4833,
                'state': 'Kedah', 'state_code': 'KDH',
                'region': 'Northern Peninsula', 'population': 228000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 20,
                'type': 'agricultural', 'urban_level': 'urban',
                'climate_zone': 'tropical_inland', 'economic_level': 'medium'
            },
            
            # Negeri Sembilan
            'Seremban': {
                'latitude': 2.7297, 'longitude': 101.9381,
                'state': 'Negeri Sembilan', 'state_code': 'NSN',
                'region': 'Central Peninsula', 'population': 294000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 60,
                'type': 'state_capital', 'urban_level': 'urban',
                'climate_zone': 'tropical_inland', 'economic_level': 'medium'
            },
            
            # Malacca
            'Malacca': {
                'latitude': 2.1896, 'longitude': 102.2501,
                'state': 'Malacca', 'state_code': 'MLK',
                'region': 'Southern Peninsula', 'population': 180000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 15,
                'type': 'heritage_city', 'urban_level': 'urban',
                'climate_zone': 'tropical_coastal', 'economic_level': 'medium'
            },
            
            # Pahang
            'Kuantan': {
                'latitude': 3.8077, 'longitude': 103.3260,
                'state': 'Pahang', 'state_code': 'PHG',
                'region': 'Central Peninsula', 'population': 340000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 15,
                'type': 'state_capital', 'urban_level': 'urban',
                'climate_zone': 'tropical_coastal', 'economic_level': 'medium'
            },
            'Temerloh': {
                'latitude': 3.4500, 'longitude': 102.4167,
                'state': 'Pahang', 'state_code': 'PHG',
                'region': 'Central Peninsula', 'population': 190000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 50,
                'type': 'industrial', 'urban_level': 'urban',
                'climate_zone': 'tropical_inland', 'economic_level': 'medium'
            },
            
            # Terengganu
            'Kuala Terengganu': {
                'latitude': 5.3302, 'longitude': 103.1408,
                'state': 'Terengganu', 'state_code': 'TRG',
                'region': 'Central Peninsula', 'population': 285000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 10,
                'type': 'state_capital', 'urban_level': 'urban',
                'climate_zone': 'tropical_coastal', 'economic_level': 'medium'
            },
            
            # Kelantan
            'Kota Bharu': {
                'latitude': 6.1339, 'longitude': 102.2387,
                'state': 'Kelantan', 'state_code': 'KTN',
                'region': 'Northern Peninsula', 'population': 315000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 5,
                'type': 'state_capital', 'urban_level': 'urban',
                'climate_zone': 'tropical_coastal', 'economic_level': 'low'
            },
            
            # Perlis
            'Kangar': {
                'latitude': 6.4414, 'longitude': 100.1986,
                'state': 'Perlis', 'state_code': 'PLS',
                'region': 'Northern Peninsula', 'population': 48000,
                'timezone': 'Asia/Kuala_Lumpur', 'altitude': 25,
                'type': 'state_capital', 'urban_level': 'small_town',
                'climate_zone': 'tropical_inland', 'economic_level': 'low'
            },
            
            # === MALAYSIAN BORNEO ===
            
            # Sabah
            'Kota Kinabalu': {
                'latitude': 5.9788, 'longitude': 116.0753,
                'state': 'Sabah', 'state_code': 'SBH',
                'region': 'Sabah', 'population': 452000,
                'timezone': 'Asia/Kuching', 'altitude': 10,
                'type': 'state_capital', 'urban_level': 'metropolitan',
                'climate_zone': 'tropical_coastal', 'economic_level': 'medium'
            },
            'Sandakan': {
                'latitude': 5.8402, 'longitude': 118.1179,
                'state': 'Sabah', 'state_code': 'SBH',
                'region': 'Sabah', 'population': 157000,
                'timezone': 'Asia/Kuching', 'altitude': 15,
                'type': 'port', 'urban_level': 'urban',
                'climate_zone': 'tropical_coastal', 'economic_level': 'medium'
            },
            'Tawau': {
                'latitude': 4.2436, 'longitude': 117.8840,
                'state': 'Sabah', 'state_code': 'SBH',
                'region': 'Sabah', 'population': 113000,
                'timezone': 'Asia/Kuching', 'altitude': 20,
                'type': 'agricultural', 'urban_level': 'urban',
                'climate_zone': 'tropical_coastal', 'economic_level': 'medium'
            },
            
            # Sarawak
            'Kuching': {
                'latitude': 1.5533, 'longitude': 110.3592,
                'state': 'Sarawak', 'state_code': 'SWK',
                'region': 'Sarawak', 'population': 325000,
                'timezone': 'Asia/Kuching', 'altitude': 20,
                'type': 'state_capital', 'urban_level': 'metropolitan',
                'climate_zone': 'tropical_coastal', 'economic_level': 'medium'
            },
            'Miri': {
                'latitude': 4.3947, 'longitude': 113.9910,
                'state': 'Sarawak', 'state_code': 'SWK',
                'region': 'Sarawak', 'population': 234000,
                'timezone': 'Asia/Kuching', 'altitude': 25,
                'type': 'oil_gas', 'urban_level': 'urban',
                'climate_zone': 'tropical_coastal', 'economic_level': 'high'
            },
            'Sibu': {
                'latitude': 2.3000, 'longitude': 111.8167,
                'state': 'Sarawak', 'state_code': 'SWK',
                'region': 'Sarawak', 'population': 162000,
                'timezone': 'Asia/Kuching', 'altitude': 5,
                'type': 'river_port', 'urban_level': 'urban',
                'climate_zone': 'tropical_inland', 'economic_level': 'medium'
            }
        }
    
    def _load_building_distributions(self) -> Dict[str, Dict]:
        """Charge les distributions de types de bâtiments par type de ville."""
        
        return {
            'metropolitan': {
                'residential': 0.45,
                'commercial': 0.35,
                'industrial': 0.15,
                'public': 0.05
            },
            'urban': {
                'residential': 0.55,
                'commercial': 0.25,
                'industrial': 0.15,
                'public': 0.05
            },
            'small_town': {
                'residential': 0.65,
                'commercial': 0.20,
                'industrial': 0.10,
                'public': 0.05
            },
            'capital': {  # Kuala Lumpur
                'residential': 0.40,
                'commercial': 0.40,
                'industrial': 0.10,
                'public': 0.10
            },
            'heritage_city': {  # George Town, Malacca
                'residential': 0.50,
                'commercial': 0.35,
                'industrial': 0.10,
                'public': 0.05
            },
            'industrial': {  # Villes industrielles
                'residential': 0.45,
                'commercial': 0.20,
                'industrial': 0.30,
                'public': 0.05
            },
            'port': {  # Villes portuaires
                'residential': 0.40,
                'commercial': 0.30,
                'industrial': 0.25,
                'public': 0.05
            },
            'oil_gas': {  # Miri
                'residential': 0.35,
                'commercial': 0.25,
                'industrial': 0.35,
                'public': 0.05
            }
        }
    
    def _load_climate_zones(self) -> Dict[str, Dict]:
        """Charge les caractéristiques des zones climatiques Malaysia."""
        
        return {
            'tropical_urban': {
                'temperature_range': (26, 35),
                'humidity_range': (65, 90),
                'rainfall_mm_year': 2400,
                'cooling_factor': 1.2,  # Plus de besoin de climatisation
                'energy_baseline_multiplier': 1.15
            },
            'tropical_coastal': {
                'temperature_range': (24, 33),
                'humidity_range': (75, 95),
                'rainfall_mm_year': 2800,
                'cooling_factor': 1.1,
                'energy_baseline_multiplier': 1.05
            },
            'tropical_inland': {
                'temperature_range': (22, 36),
                'humidity_range': (60, 85),
                'rainfall_mm_year': 2000,
                'cooling_factor': 1.3,  # Plus chaud, plus de climatisation
                'energy_baseline_multiplier': 1.20
            },
            'tropical_highland': {
                'temperature_range': (18, 28),
                'humidity_range': (70, 90),
                'rainfall_mm_year': 2200,
                'cooling_factor': 0.7,  # Moins de climatisation
                'energy_baseline_multiplier': 0.85
            }
        }
    
    def get_available_locations(self) -> Dict[str, Dict]:
        """Retourne toutes les localisations disponibles."""
        return self._malaysia_locations.copy()
    
    def filter_locations(self, locations: Dict[str, Dict], 
                        location_filter: Dict) -> Dict[str, Dict]:
        """
        Filtre les localisations selon des critères.
        
        Args:
            locations: Dictionnaire des localisations
            location_filter: Critères de filtrage
            
        Returns:
            Localisations filtrées
        """
        filtered = locations.copy()
        
        # Filtrage par état
        if 'state' in location_filter:
            target_state = location_filter['state']
            filtered = {k: v for k, v in filtered.items() 
                       if v['state'] == target_state}
        
        # Filtrage par région
        if 'region' in location_filter:
            target_region = location_filter['region']
            filtered = {k: v for k, v in filtered.items() 
                       if v['region'] == target_region}
        
        # Filtrage par type de ville
        if 'type' in location_filter:
            target_type = location_filter['type']
            filtered = {k: v for k, v in filtered.items() 
                       if v['type'] == target_type}
        
        # Filtrage par niveau urbain
        if 'urban_level' in location_filter:
            target_level = location_filter['urban_level']
            filtered = {k: v for k, v in filtered.items() 
                       if v['urban_level'] == target_level}
        
        # Filtrage par population minimale
        if 'min_population' in location_filter:
            min_pop = location_filter['min_population']
            filtered = {k: v for k, v in filtered.items() 
                       if v['population'] >= min_pop}
        
        # Filtrage par ville spécifique
        if 'city' in location_filter:
            target_city = location_filter['city']
            if target_city in filtered:
                filtered = {target_city: filtered[target_city]}
            else:
                filtered = {}
        
        return filtered
    
    def select_weighted_location(self, available_locations: Dict[str, Dict]) -> Dict:
        """
        Sélectionne une localisation avec pondération selon la population.
        
        Args:
            available_locations: Localisations disponibles
            
        Returns:
            Données de la localisation sélectionnée
        """
        if not available_locations:
            # Fallback sur Kuala Lumpur
            return self._malaysia_locations['Kuala Lumpur']
        
        # Calculer les poids basés sur la population (log pour éviter trop de déséquilibre)
        cities = list(available_locations.keys())
        weights = [np.log(available_locations[city]['population'] + 1) 
                  for city in cities]
        
        # Normaliser les poids
        total_weight = sum(weights)
        if total_weight > 0:
            probabilities = [w / total_weight for w in weights]
        else:
            probabilities = [1.0 / len(cities)] * len(cities)
        
        # Sélection aléatoire pondérée
        selected_city = np.random.choice(cities, p=probabilities)
        
        # Retourner les données avec le nom de la ville ajouté
        location_data = available_locations[selected_city].copy()
        location_data['city'] = selected_city
        
        return location_data
    
    def get_building_type_distribution(self, location_data: Dict) -> Dict[str, float]:
        """
        Retourne la distribution des types de bâtiments pour une localisation.
        
        Args:
            location_data: Données de la localisation
            
        Returns:
            Distribution des types de bâtiments
        """
        city_type = location_data.get('type', 'urban')
        urban_level = location_data.get('urban_level', 'urban')
        
        # Prioriser le type de ville, puis le niveau urbain
        if city_type in self._building_type_distributions:
            return self._building_type_distributions[city_type]
        elif urban_level in self._building_type_distributions:
            return self._building_type_distributions[urban_level]
        else:
            # Distribution par défaut
            return self._building_type_distributions['urban']
    
    def get_building_types(self) -> List[str]:
        """Retourne la liste des types de bâtiments supportés."""
        return ['residential', 'commercial', 'industrial', 'public']
    
    def get_climate_characteristics(self, location_data: Dict) -> Dict:
        """
        Retourne les caractéristiques climatiques d'une localisation.
        
        Args:
            location_data: Données de la localisation
            
        Returns:
            Caractéristiques climatiques
        """
        climate_zone = location_data.get('climate_zone', 'tropical_coastal')
        
        if climate_zone in self._climate_zones:
            return self._climate_zones[climate_zone]
        else:
            # Zone climatique par défaut
            return self._climate_zones['tropical_coastal']
    
    def get_energy_baseline_multiplier(self, location_data: Dict) -> float:
        """
        Retourne le multiplicateur de consommation de base pour une localisation.
        
        Args:
            location_data: Données de la localisation
            
        Returns:
            Multiplicateur énergétique
        """
        climate_chars = self.get_climate_characteristics(location_data)
        base_multiplier = climate_chars.get('energy_baseline_multiplier', 1.0)
        
        # Ajustements selon le niveau économique
        economic_level = location_data.get('economic_level', 'medium')
        economic_multipliers = {
            'very_high': 1.3,
            'high': 1.15,
            'medium': 1.0,
            'low': 0.85
        }
        
        economic_multiplier = economic_multipliers.get(economic_level, 1.0)
        
        return base_multiplier * economic_multiplier
    
    def get_nearby_cities(self, target_city: str, radius_km: float = 100) -> List[Dict]:
        """
        Retourne les villes à proximité d'une ville donnée.
        
        Args:
            target_city: Ville de référence
            radius_km: Rayon de recherche en km
            
        Returns:
            Liste des villes proches
        """
        if target_city not in self._malaysia_locations:
            return []
        
        target_location = self._malaysia_locations[target_city]
        target_lat = target_location['latitude']
        target_lon = target_location['longitude']
        
        nearby_cities = []
        
        for city, location in self._malaysia_locations.items():
            if city != target_city:
                # Calcul de distance approximatif
                lat_diff = target_lat - location['latitude']
                lon_diff = target_lon - location['longitude']
                distance_approx = np.sqrt(lat_diff**2 + lon_diff**2) * 111  # Approximation en km
                
                if distance_approx <= radius_km:
                    city_info = location.copy()
                    city_info['city'] = city
                    city_info['distance_km'] = round(distance_approx, 1)
                    nearby_cities.append(city_info)
        
        # Trier par distance
        nearby_cities.sort(key=lambda x: x['distance_km'])
        
        return nearby_cities
    
    def get_statistics(self) -> Dict[str, Any]:
        """Retourne les statistiques des données Malaysia."""
        
        total_population = sum(loc['population'] for loc in self._malaysia_locations.values())
        
        # Statistiques par état
        states_stats = {}
        for city, data in self._malaysia_locations.items():
            state = data['state']
            if state not in states_stats:
                states_stats[state] = {'cities': 0, 'population': 0}
            states_stats[state]['cities'] += 1
            states_stats[state]['population'] += data['population']
        
        # Statistiques par niveau urbain
        urban_stats = {}
        for data in self._malaysia_locations.values():
            level = data['urban_level']
            if level not in urban_stats:
                urban_stats[level] = 0
            urban_stats[level] += 1
        
        return {
            'total_cities': len(self._malaysia_locations),
            'total_population': total_population,
            'states_count': len(states_stats),
            'states_statistics': states_stats,
            'urban_level_distribution': urban_stats,
            'climate_zones': list(self._climate_zones.keys()),
            'building_types': self.get_building_types(),
            'largest_city': max(self._malaysia_locations.items(), 
                              key=lambda x: x[1]['population'])[0],
            'geographic_coverage': {
                'min_latitude': min(loc['latitude'] for loc in self._malaysia_locations.values()),
                'max_latitude': max(loc['latitude'] for loc in self._malaysia_locations.values()),
                'min_longitude': min(loc['longitude'] for loc in self._malaysia_locations.values()),
                'max_longitude': max(loc['longitude'] for loc in self._malaysia_locations.values())
            }
        }


# Export de la classe principale
__all__ = ['MalaysiaLocationData']
