#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PATTERNS ÉNERGÉTIQUES - GÉNÉRATEUR MALAYSIA
Fichier: app/utils/energy_patterns.py

Générateur de patterns énergétiques réalistes adaptés au climat tropical
malaysien avec variations saisonnières, journalières et comportementales.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Patterns tropicaux
"""

import logging
import math
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import numpy as np
import pandas as pd


class EnergyPatternGenerator:
    """
    Générateur de patterns énergétiques adapté au contexte malaysien.
    
    Prend en compte:
    - Climat tropical avec saisons de mousson
    - Habitudes culturelles malaysiennes
    - Variations urbaines vs rurales
    - Types de bâtiments spécifiques
    """
    
    def __init__(self):
        """Initialise le générateur de patterns énergétiques."""
        self.logger = logging.getLogger(__name__)
        
        # Patterns de base par type de bâtiment
        self.base_patterns = self._load_base_patterns()
        
        # Variations saisonnières Malaysia
        self.seasonal_variations = self._load_seasonal_variations()
        
        # Patterns journaliers par type
        self.daily_patterns = self._load_daily_patterns()
        
        # Facteurs de randomisation
        self.noise_levels = {
            'residential': 0.15,  # 15% de variation aléatoire
            'commercial': 0.10,   # 10% de variation
            'industrial': 0.08,   # 8% de variation (plus prévisible)
            'public': 0.12        # 12% de variation
        }
        
        self.logger.info("✅ Générateur de patterns énergétiques initialisé")
    
    def _load_base_patterns(self) -> Dict[str, Dict]:
        """Charge les patterns énergétiques de base par type de bâtiment."""
        
        return {
            'residential': {
                'base_consumption_kwh': 25.0,
                'peak_hours': [7, 8, 19, 20, 21],  # Matin et soirée
                'low_hours': [1, 2, 3, 4, 5],      # Nuit profonde
                'ac_dependency': 0.6,               # 60% de la consommation pour AC
                'seasonal_sensitivity': 0.4,       # Sensible aux saisons
                'weekend_factor': 1.2,              # +20% le weekend
                'ramadan_factor': 0.85              # -15% pendant Ramadan
            },
            'commercial': {
                'base_consumption_kwh': 120.0,
                'peak_hours': [9, 10, 11, 14, 15, 16],  # Heures de bureau
                'low_hours': [22, 23, 0, 1, 2, 3, 4, 5, 6],  # Nuit
                'ac_dependency': 0.75,              # AC très important
                'seasonal_sensitivity': 0.5,       # Très sensible aux saisons
                'weekend_factor': 0.3,              # 70% de réduction le weekend
                'ramadan_factor': 0.7               # -30% pendant Ramadan (horaires réduits)
            },
            'industrial': {
                'base_consumption_kwh': 400.0,
                'peak_hours': [8, 9, 10, 11, 13, 14, 15, 16],  # Horaires de travail
                'low_hours': [22, 23, 0, 1, 2, 3, 4, 5],
                'ac_dependency': 0.25,              # AC moins important
                'seasonal_sensitivity': 0.2,       # Moins sensible aux saisons
                'weekend_factor': 0.8,              # Fonctionnement réduit le weekend
                'ramadan_factor': 0.9               # Légère réduction pendant Ramadan
            },
            'public': {
                'base_consumption_kwh': 80.0,
                'peak_hours': [8, 9, 10, 11, 13, 14, 15, 16, 17],  # Heures de service
                'low_hours': [20, 21, 22, 23, 0, 1, 2, 3, 4, 5],
                'ac_dependency': 0.65,              # AC important pour le confort public
                'seasonal_sensitivity': 0.35,      # Modérément sensible
                'weekend_factor': 0.6,              # Service réduit le weekend
                'ramadan_factor': 0.8               # Horaires réduits pendant Ramadan
            }
        }
    
    def _load_seasonal_variations(self) -> Dict[str, float]:
        """Charge les variations saisonnières pour Malaysia."""
        
        # Basé sur le climat tropical malaysien
        return {
            1: 0.85,   # Janvier - Mousson du nord-est (plus frais)
            2: 0.90,   # Février - Fin de mousson
            3: 1.15,   # Mars - Inter-mousson (chaud et humide)
            4: 1.25,   # Avril - Très chaud
            5: 1.20,   # Mai - Très chaud
            6: 1.00,   # Juin - Mousson du sud-ouest commence
            7: 0.95,   # Juillet - Mousson (plus frais)
            8: 0.95,   # Août - Mousson
            9: 1.10,   # Septembre - Inter-mousson
            10: 1.15,  # Octobre - Chaud et humide
            11: 0.95,  # Novembre - Début mousson nord-est
            12: 0.85   # Décembre - Mousson (plus frais)
        }
    
    def _load_daily_patterns(self) -> Dict[str, List[float]]:
        """Charge les patterns journaliers par type de bâtiment (24 heures)."""
        
        return {
            'residential': [
                # Profil résidentiel malaysien typique
                0.3, 0.2, 0.2, 0.2, 0.2, 0.3,  # 00-05: Nuit (AC en veille)
                0.6, 0.9, 0.7, 0.5, 0.4, 0.4,  # 06-11: Matin (préparation, départ)
                0.4, 0.4, 0.4, 0.4, 0.5, 0.6,  # 12-17: Journée (maison vide)
                0.8, 1.0, 1.0, 0.9, 0.7, 0.5   # 18-23: Soirée (retour, AC plein)
            ],
            'commercial': [
                # Profil commercial (bureaux, magasins)
                0.1, 0.1, 0.1, 0.1, 0.1, 0.2,  # 00-05: Fermé
                0.3, 0.4, 0.6, 1.0, 1.0, 0.9,  # 06-11: Ouverture, pic matinal
                0.8, 0.9, 1.0, 1.0, 0.9, 0.8,  # 12-17: Activité de jour
                0.6, 0.4, 0.3, 0.2, 0.1, 0.1   # 18-23: Fermeture progressive
            ],
            'industrial': [
                # Profil industriel (2-3 shifts)
                0.6, 0.6, 0.5, 0.5, 0.5, 0.6,  # 00-05: Shift de nuit
                0.7, 0.8, 1.0, 1.0, 1.0, 0.9,  # 06-11: Shift du matin
                0.8, 0.9, 1.0, 1.0, 0.9, 0.8,  # 12-17: Shift d'après-midi
                0.7, 0.7, 0.6, 0.6, 0.6, 0.6   # 18-23: Shift du soir
            ],
            'public': [
                # Profil service public (hôpitaux, écoles, bureaux gov)
                0.2, 0.2, 0.2, 0.2, 0.2, 0.3,  # 00-05: Service minimal
                0.4, 0.6, 0.8, 1.0, 1.0, 0.9,  # 06-11: Ouverture service
                0.8, 0.9, 1.0, 1.0, 0.9, 0.7,  # 12-17: Pleine activité
                0.5, 0.4, 0.3, 0.3, 0.2, 0.2   # 18-23: Fermeture progressive
            ]
        }
    
    def generate_consumption_pattern(self, building_type: str, location: str,
                                   date_range: pd.DatetimeIndex, frequency: str,
                                   characteristics: Dict = None) -> np.ndarray:
        """
        Génère un pattern de consommation réaliste pour un bâtiment.
        
        Args:
            building_type: Type de bâtiment
            location: Nom de la localisation
            date_range: Plage temporelle
            frequency: Fréquence des données
            characteristics: Caractéristiques du bâtiment
            
        Returns:
            Array des valeurs de consommation
        """
        self.logger.debug(f"🔋 Génération pattern {building_type} à {location}")
        
        if building_type not in self.base_patterns:
            building_type = 'residential'  # Fallback
        
        base_pattern = self.base_patterns[building_type]
        daily_pattern = self.daily_patterns[building_type]
        
        # Caractéristiques par défaut
        if characteristics is None:
            characteristics = {}
        
        consumption_values = []
        
        for timestamp in date_range:
            # Consommation de base
            base_consumption = base_pattern['base_consumption_kwh']
            
            # Facteur journalier (selon l'heure)
            hour = timestamp.hour
            daily_factor = daily_pattern[hour]
            
            # Facteur saisonnier (selon le mois)
            month = timestamp.month
            seasonal_factor = self.seasonal_variations[month]
            
            # Facteur weekend
            is_weekend = timestamp.weekday() >= 5
            weekend_factor = base_pattern['weekend_factor'] if is_weekend else 1.0
            
            # Facteur climatique (température et humidité estimées)
            climate_factor = self._calculate_climate_factor(timestamp, location, base_pattern)
            
            # Facteurs spéciaux (Ramadan, jours fériés, etc.)
            special_factor = self._calculate_special_factors(timestamp, building_type)
            
            # Facteurs spécifiques au bâtiment
            building_factor = self._calculate_building_factors(characteristics, building_type)
            
            # Calcul de la consommation
            consumption = (base_consumption * 
                          daily_factor * 
                          seasonal_factor * 
                          weekend_factor * 
                          climate_factor * 
                          special_factor * 
                          building_factor)
            
            # Ajouter du bruit réaliste
            noise_level = self.noise_levels.get(building_type, 0.1)
            noise = np.random.normal(1.0, noise_level)
            consumption *= max(0.1, noise)  # Éviter les valeurs négatives
            
            consumption_values.append(max(0.0, consumption))
        
        return np.array(consumption_values)
    
    def _calculate_climate_factor(self, timestamp: datetime, location: str, 
                                base_pattern: Dict) -> float:
        """
        Calcule le facteur climatique basé sur la température estimée.
        
        Args:
            timestamp: Moment de l'observation
            location: Localisation
            base_pattern: Pattern de base du bâtiment
            
        Returns:
            Facteur climatique (multiplicateur)
        """
        # Température estimée pour Malaysia (modèle simplifié)
        month = timestamp.month
        hour = timestamp.hour
        
        # Température de base selon le mois (°C)
        base_temps = {
            1: 26, 2: 27, 3: 29, 4: 30, 5: 30, 6: 29,
            7: 28, 8: 28, 9: 29, 10: 29, 11: 28, 12: 26
        }
        
        base_temp = base_temps[month]
        
        # Variation journalière (-3°C la nuit, +3°C l'après-midi)
        if 6 <= hour <= 8:
            temp_variation = -1  # Matin frais
        elif 14 <= hour <= 16:
            temp_variation = 3   # Après-midi chaud
        elif 22 <= hour or hour <= 5:
            temp_variation = -3  # Nuit fraîche
        else:
            temp_variation = 0
        
        estimated_temp = base_temp + temp_variation
        
        # Facteur de climatisation selon la température
        ac_dependency = base_pattern['ac_dependency']
        
        if estimated_temp >= 30:
            # Très chaud - AC à fond
            climate_factor = 1.0 + (estimated_temp - 30) * ac_dependency * 0.05
        elif estimated_temp >= 27:
            # Chaud - AC normal
            climate_factor = 1.0 + (estimated_temp - 27) * ac_dependency * 0.03
        elif estimated_temp >= 24:
            # Tempéré - AC modéré
            climate_factor = 1.0
        else:
            # Frais - AC réduit
            climate_factor = 1.0 - (24 - estimated_temp) * ac_dependency * 0.02
        
        return max(0.5, climate_factor)  # Minimum 50% de la consommation
    
    def _calculate_special_factors(self, timestamp: datetime, building_type: str) -> float:
        """
        Calcule les facteurs spéciaux (Ramadan, jours fériés, etc.).
        
        Args:
            timestamp: Moment de l'observation
            building_type: Type de bâtiment
            
        Returns:
            Facteur spécial (multiplicateur)
        """
        base_pattern = self.base_patterns[building_type]
        special_factor = 1.0
        
        # Approximation Ramadan (se déplace chaque année)
        # Pour simplicité, on utilise une période fixe approximative
        if self._is_approximate_ramadan(timestamp):
            special_factor *= base_pattern['ramadan_factor']
        
        # Jours fériés malaysiens approximatifs
        if self._is_public_holiday(timestamp):
            if building_type == 'commercial':
                special_factor *= 0.2  # Magasins fermés
            elif building_type == 'public':
                special_factor *= 0.3  # Services réduits
            elif building_type == 'residential':
                special_factor *= 1.1  # Plus à la maison
        
        return special_factor
    
    def _calculate_building_factors(self, characteristics: Dict, building_type: str) -> float:
        """
        Calcule les facteurs spécifiques au bâtiment.
        
        Args:
            characteristics: Caractéristiques du bâtiment
            building_type: Type de bâtiment
            
        Returns:
            Facteur du bâtiment (multiplicateur)
        """
        factor = 1.0
        
        # Facteur selon la surface
        if 'floor_area_sqm' in characteristics:
            area = characteristics['floor_area_sqm']
            if building_type == 'commercial':
                # Plus grand magasin = plus de consommation
                if area > 1000:
                    factor *= 1.3
                elif area < 100:
                    factor *= 0.7
            elif building_type == 'residential':
                # Plus grande maison = plus de consommation
                if area > 200:
                    factor *= 1.2
                elif area < 50:
                    factor *= 0.8
        
        # Facteur selon l'âge du bâtiment
        if 'building_age' in characteristics:
            age = characteristics['building_age']
            if age > 20:
                factor *= 1.15  # Bâtiments anciens moins efficaces
            elif age < 5:
                factor *= 0.9   # Bâtiments récents plus efficaces
        
        # Facteur selon l'efficacité énergétique
        if 'energy_efficiency' in characteristics:
            efficiency = characteristics['energy_efficiency']
            # efficiency entre 0.5 (inefficace) et 1.5 (très efficace)
            factor *= (2.0 - efficiency)
        
        # Facteur selon le nombre d'occupants (résidentiel)
        if building_type == 'residential' and 'avg_occupancy' in characteristics:
            occupancy = characteristics['avg_occupancy']
            # Plus d'occupants = plus de consommation
            factor *= (0.7 + occupancy * 0.1)
        
        # Facteur selon l'activité (commercial/industriel)
        if 'operating_hours' in characteristics:
            hours = characteristics['operating_hours']
            if hours > 12:
                factor *= 1.2  # Longues heures d'opération
            elif hours < 8:
                factor *= 0.8  # Heures réduites
        
        return max(0.3, factor)  # Minimum 30% de la consommation de base
    
    def _is_approximate_ramadan(self, timestamp: datetime) -> bool:
        """Vérifie si la date est approximativement pendant Ramadan."""
        # Approximation très simplifiée du Ramadan
        # En réalité, Ramadan se décale de ~11 jours chaque année
        year = timestamp.year
        
        # Dates approximatives de Ramadan pour différentes années
        ramadan_periods = {
            2024: (datetime(2024, 3, 10), datetime(2024, 4, 9)),
            2025: (datetime(2025, 2, 28), datetime(2025, 3, 30)),
            2026: (datetime(2026, 2, 17), datetime(2026, 3, 19)),
        }
        
        if year in ramadan_periods:
            start, end = ramadan_periods[year]
            return start <= timestamp <= end
        
        return False
    
    def _is_public_holiday(self, timestamp: datetime) -> bool:
        """Vérifie si c'est un jour férié malaysien approximatif."""
        month = timestamp.month
        day = timestamp.day
        
        # Jours fériés fixes approximatifs
        fixed_holidays = [
            (1, 1),   # Nouvel An
            (2, 1),   # Fête Fédérale
            (5, 1),   # Fête du Travail
            (6, 5),   # Anniversaire du Roi (approximatif)
            (8, 31),  # Fête Nationale
            (9, 16),  # Jour de la Malaysia
            (12, 25), # Noël
        ]
        
        return (month, day) in fixed_holidays
    
    def generate_weather_context(self, timestamp: datetime, location: str) -> Dict[str, Any]:
        """
        Génère un contexte météorologique approximatif.
        
        Args:
            timestamp: Moment de l'observation
            location: Localisation
            
        Returns:
            Contexte météorologique
        """
        month = timestamp.month
        hour = timestamp.hour
        
        # Température de base selon le mois
        base_temps = {
            1: 26, 2: 27, 3: 29, 4: 30, 5: 30, 6: 29,
            7: 28, 8: 28, 9: 29, 10: 29, 11: 28, 12: 26
        }
        
        base_temp = base_temps[month]
        
        # Variation journalière
        if 6 <= hour <= 8:
            temp_variation = -1
        elif 14 <= hour <= 16:
            temp_variation = 3
        elif 22 <= hour or hour <= 5:
            temp_variation = -3
        else:
            temp_variation = 0
        
        temperature = base_temp + temp_variation + np.random.normal(0, 1)
        
        # Humidité (toujours élevée en Malaysia)
        base_humidity = 80
        humidity_variation = np.random.normal(0, 5)
        humidity = max(60, min(95, base_humidity + humidity_variation))
        
        # Probabilité de pluie selon la saison
        rain_probabilities = {
            1: 0.4, 2: 0.3, 3: 0.2, 4: 0.3, 5: 0.3, 6: 0.5,
            7: 0.5, 8: 0.5, 9: 0.4, 10: 0.4, 11: 0.6, 12: 0.5
        }
        
        is_raining = np.random.random() < rain_probabilities[month]
        
        return {
            'temperature': round(temperature, 1),
            'humidity': round(humidity, 1),
            'is_raining': is_raining,
            'season': self._get_season(month),
            'heat_index': round(temperature + (humidity - 40) * 0.1, 1)
        }
    
    def _get_season(self, month: int) -> str:
        """Retourne la saison malaysienne selon le mois."""
        if month in [11, 12, 1, 2]:
            return 'northeast_monsoon'
        elif month in [6, 7, 8]:
            return 'southwest_monsoon'
        else:
            return 'inter_monsoon'
    
    def get_pattern_statistics(self, building_type: str) -> Dict[str, Any]:
        """
        Retourne les statistiques des patterns pour un type de bâtiment.
        
        Args:
            building_type: Type de bâtiment
            
        Returns:
            Statistiques des patterns
        """
        if building_type not in self.base_patterns:
            return {}
        
        base_pattern = self.base_patterns[building_type]
        daily_pattern = self.daily_patterns[building_type]
        
        return {
            'base_consumption_kwh': base_pattern['base_consumption_kwh'],
            'peak_hours': base_pattern['peak_hours'],
            'low_hours': base_pattern['low_hours'],
            'daily_pattern_peak': max(daily_pattern),
            'daily_pattern_min': min(daily_pattern),
            'daily_pattern_avg': np.mean(daily_pattern),
            'seasonal_variation_range': {
                'min': min(self.seasonal_variations.values()),
                'max': max(self.seasonal_variations.values())
            },
            'ac_dependency': base_pattern['ac_dependency'],
            'noise_level': self.noise_levels.get(building_type, 0.1)
        }


# Export de la classe principale
__all__ = ['EnergyPatternGenerator']