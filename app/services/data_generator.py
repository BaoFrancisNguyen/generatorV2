#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SERVICE GÉNÉRATEUR DE DONNÉES ÉNERGÉTIQUES - MALAYSIA
Fichier: app/services/data_generator.py

Service principal pour la génération de données synthétiques de consommation électrique
spécialement adaptées au contexte malaysien. Gère les patterns climatiques tropicaux,
les spécificités culturelles et les coordonnées géographiques précises.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Service modulaire
"""

import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import numpy as np
import pandas as pd
from faker import Faker

from app.utils.malaysia_data import MalaysiaLocationData
from app.utils.energy_patterns import EnergyPatternGenerator
from app.models.building import Building
from app.models.location import Location
from app.models.timeseries import TimeSeries


class ElectricityDataGenerator:
    """
    Générateur principal de données énergétiques pour la Malaysia.
    
    Ce service génère des données synthétiques réalistes en tenant compte:
    - Des patterns climatiques tropicaux
    - Des habitudes de consommation malaysiennes
    - De la distribution géographique des villes
    - Des types de bâtiments et leur usage
    """
    
    def __init__(self, config=None):
        """
        Initialise le générateur avec la configuration fournie.
        
        Args:
            config: Configuration de l'application (optionnel)
        """
        self.logger = logging.getLogger(__name__)
        self.config = config
        
        # ✅ CORRECTION: Utiliser des locales supportés par Faker
        # en_US pour l'anglais et id_ID pour l'indonésien (proche du malaysien)
        try:
            self.faker = Faker(['en_US', 'id_ID'])  # Locales supportés
        except AttributeError:
            # Fallback si id_ID n'est pas disponible
            self.faker = Faker(['en_US'])
            self.logger.warning("⚠️ Locale id_ID non disponible, utilisation de en_US uniquement")
        
        # Initialiser les données de base
        self.malaysia_data = MalaysiaLocationData()
        self.pattern_generator = EnergyPatternGenerator()
        
        # Cache pour optimiser les performances
        self._location_cache = {}
        self._building_cache = {}
        
        self.logger.info("✅ Générateur de données énergétiques Malaysia initialisé")
    
    def generate_complete_dataset(self, 
                                num_buildings: int = 100,
                                start_date: str = '2024-01-01',
                                end_date: str = '2024-01-31',
                                frequency: str = 'D',
                                location_filter: Optional[Dict] = None,
                                building_types: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        Génère un dataset complet avec métadonnées des bâtiments et séries temporelles.
        
        Args:
            num_buildings: Nombre de bâtiments à générer
            start_date: Date de début (format YYYY-MM-DD)
            end_date: Date de fin (format YYYY-MM-DD)
            frequency: Fréquence des données ('H', 'D', '30T', etc.)
            location_filter: Filtre pour les localisations (ville, état, région)
            building_types: Types de bâtiments à inclure
            
        Returns:
            Dict contenant les DataFrames 'buildings' et 'timeseries'
        """
        self.logger.info(f"🏗️ Génération de {num_buildings} bâtiments du {start_date} au {end_date}")
        
        try:
            # Validation des paramètres
            self._validate_generation_parameters(num_buildings, start_date, end_date, frequency)
            
            # Génération des métadonnées des bâtiments
            buildings_df = self.generate_building_metadata(
                num_buildings=num_buildings,
                location_filter=location_filter,
                building_types=building_types
            )
            
            # Génération des séries temporelles
            timeseries_df = self.generate_timeseries_data(
                buildings_df=buildings_df,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency
            )
            
            self.logger.info(f"✅ Dataset généré: {len(buildings_df)} bâtiments, {len(timeseries_df)} observations")
            
            return {
                'buildings': buildings_df,
                'timeseries': timeseries_df,
                'metadata': self._generate_dataset_metadata(buildings_df, timeseries_df, start_date, end_date, frequency)
            }
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la génération: {str(e)}")
            raise
    
    def generate_building_metadata(self, 
                                 num_buildings: int,
                                 location_filter: Optional[Dict] = None,
                                 building_types: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Génère les métadonnées des bâtiments (localisation, type, caractéristiques).
        
        Args:
            num_buildings: Nombre de bâtiments à générer
            location_filter: Filtre pour les localisations
            building_types: Types de bâtiments autorisés
            
        Returns:
            DataFrame avec les métadonnées des bâtiments
        """
        self.logger.debug(f"📋 Génération des métadonnées pour {num_buildings} bâtiments")
        
        buildings_data = []
        
        for i in range(num_buildings):
            # Sélectionner une localisation
            location = self._select_location(location_filter)
            
            # Sélectionner un type de bâtiment
            building_type = self._select_building_type(location, building_types)
            
            # Générer les caractéristiques du bâtiment
            characteristics = self._generate_building_characteristics(building_type, location)
            
            # Créer l'instance Building
            building = Building(
                unique_id=self._generate_unique_id(),
                building_id=self._generate_building_id(location, i),
                location=location,
                building_type=building_type,
                characteristics=characteristics,
                osm_source=False,
                validation_status='valid'
            )
            
            # Convertir en dictionnaire pour le DataFrame
            buildings_data.append(building.to_dict())
        
        buildings_df = pd.DataFrame(buildings_data)
        self.logger.debug(f"✅ Métadonnées générées: {len(buildings_df)} bâtiments")
        
        return buildings_df
    
    def generate_timeseries_data(self,
                               buildings_df: pd.DataFrame,
                               start_date: str,
                               end_date: str,
                               frequency: str = 'D') -> pd.DataFrame:
        """
        Génère les séries temporelles de consommation électrique.
        
        Args:
            buildings_df: DataFrame des bâtiments
            start_date: Date de début
            end_date: Date de fin
            frequency: Fréquence des données
            
        Returns:
            DataFrame des séries temporelles
        """
        self.logger.debug(f"⏰ Génération des séries temporelles en fréquence {frequency}")
        
        # Créer la plage temporelle
        date_range = pd.date_range(start=start_date, end=end_date, freq=frequency)
        
        timeseries_data = []
        
        for _, building in buildings_df.iterrows():
            # Générer les patterns de consommation pour ce bâtiment
            building_timeseries = self.pattern_generator.generate_consumption_pattern(
                building_type=building['building_class'],
                date_range=date_range,
                location_data=building,
                characteristics=building.get('characteristics', {})
            )
            
            # Convertir en observations TimeSeries
            for timestamp, consumption in building_timeseries.items():
                observation = TimeSeries(
                    unique_id=building['unique_id'],
                    timestamp=timestamp,
                    consumption_kwh=consumption,
                    quality_score=np.random.uniform(95, 100),  # Haute qualité pour données synthétiques
                    anomaly_flag=False,
                    validation_status='valid'
                )
                
                timeseries_data.append(observation.to_dict())
        
        timeseries_df = pd.DataFrame(timeseries_data)
        self.logger.debug(f"✅ Séries temporelles générées: {len(timeseries_df)} observations")
        
        return timeseries_df
    
    def _validate_generation_parameters(self, num_buildings: int, start_date: str, 
                                      end_date: str, frequency: str):
        """
        Valide les paramètres de génération.
        
        Args:
            num_buildings: Nombre de bâtiments
            start_date: Date de début
            end_date: Date de fin
            frequency: Fréquence
            
        Raises:
            ValueError: Si les paramètres sont invalides
        """
        # Validation du nombre de bâtiments
        if num_buildings <= 0:
            raise ValueError("Le nombre de bâtiments doit être positif")
        
        if self.config and num_buildings > self.config.MAX_BUILDINGS:
            raise ValueError(f"Nombre maximum de bâtiments: {self.config.MAX_BUILDINGS}")
        
        # Validation des dates
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Format de date invalide. Utilisez YYYY-MM-DD")
        
        if start_dt >= end_dt:
            raise ValueError("La date de fin doit être après la date de début")
        
        # Valider la période
        period_days = (end_dt - start_dt).days
        if self.config and hasattr(self.config, 'MAX_PERIOD_DAYS') and period_days > self.config.MAX_PERIOD_DAYS:
            raise ValueError(f"Période maximum: {self.config.MAX_PERIOD_DAYS} jours")
        
        # Valider la fréquence
        if self.config and hasattr(self.config, 'SUPPORTED_FREQUENCIES') and frequency not in self.config.SUPPORTED_FREQUENCIES:
            raise ValueError(f"Fréquences supportées: {self.config.SUPPORTED_FREQUENCIES}")
    
    def _select_location(self, location_filter: Optional[Dict] = None) -> Location:
        """
        Sélectionne une localisation selon les filtres donnés.
        
        Args:
            location_filter: Critères de filtrage
            
        Returns:
            Instance Location
        """
        # Obtenir les localisations disponibles
        available_locations = self.malaysia_data.get_available_locations()
        
        # Appliquer les filtres si fournis
        if location_filter:
            available_locations = self.malaysia_data.filter_locations(
                available_locations, 
                location_filter
            )
        
        # Sélectionner aléatoirement avec pondération par population
        location_data = self.malaysia_data.select_weighted_location(available_locations)
        
        # Créer une instance Location
        from app.models.location import create_location_from_dict
        return create_location_from_dict(location_data)
    
    def _select_building_type(self, location: Location, building_types: Optional[List[str]] = None) -> str:
        """
        Sélectionne un type de bâtiment approprié pour la localisation.
        
        Args:
            location: Localisation du bâtiment
            building_types: Types autorisés (optionnel)
            
        Returns:
            Type de bâtiment sélectionné
        """
        # Obtenir la distribution des types selon la localisation
        location_dict = location.to_dict() if hasattr(location, 'to_dict') else location.__dict__
        type_distribution = self.malaysia_data.get_building_type_distribution(location_dict)
        
        # Filtrer selon les types autorisés
        if building_types:
            type_distribution = {
                k: v for k, v in type_distribution.items() 
                if k in building_types
            }
        
        # Sélection pondérée
        if not type_distribution:
            return 'residential'  # Fallback
            
        types = list(type_distribution.keys())
        weights = list(type_distribution.values())
        
        return np.random.choice(types, p=weights)
    
    def _generate_building_characteristics(self, building_type: str, location: Location) -> Dict:
        """
        Génère les caractéristiques spécifiques d'un bâtiment.
        
        Args:
            building_type: Type du bâtiment
            location: Localisation
            
        Returns:
            Dictionnaire des caractéristiques
        """
        characteristics = {
            'building_type': building_type,
            'construction_year': np.random.randint(1980, 2024),
            'floor_area_sqm': self._generate_floor_area(building_type),
            'floors': self._generate_floor_count(building_type),
            'ac_installed': np.random.choice([True, False], p=[0.8, 0.2]),  # 80% ont la climatisation en Malaysia
            'energy_efficiency': np.random.choice(['A', 'B', 'C', 'D'], p=[0.2, 0.3, 0.4, 0.1])
        }
        
        # Ajustements selon le type de bâtiment
        if building_type == 'residential':
            characteristics.update({
                'occupants': np.random.randint(1, 6),
                'apartment_type': np.random.choice(['condo', 'terrace', 'semi-d', 'bungalow'], 
                                                 p=[0.4, 0.3, 0.2, 0.1])
            })
        elif building_type == 'commercial':
            characteristics.update({
                'business_type': np.random.choice(['office', 'retail', 'restaurant', 'hotel'], 
                                                p=[0.4, 0.3, 0.2, 0.1]),
                'employees': np.random.randint(5, 200)
            })
        elif building_type == 'industrial':
            characteristics.update({
                'industry_type': np.random.choice(['manufacturing', 'warehouse', 'processing'], 
                                                p=[0.5, 0.3, 0.2]),
                'machinery_count': np.random.randint(10, 100)
            })
        
        return characteristics
    
    def _generate_floor_area(self, building_type: str) -> float:
        """Génère une superficie réaliste selon le type de bâtiment."""
        area_ranges = {
            'residential': (50, 300),
            'commercial': (100, 2000),
            'industrial': (500, 5000),
            'public': (200, 1500)
        }
        
        min_area, max_area = area_ranges.get(building_type, (100, 500))
        return round(np.random.uniform(min_area, max_area), 1)
    
    def _generate_floor_count(self, building_type: str) -> int:
        """Génère un nombre d'étages réaliste selon le type de bâtiment."""
        floor_ranges = {
            'residential': (1, 3),
            'commercial': (1, 20),
            'industrial': (1, 4),
            'public': (1, 10)
        }
        
        min_floors, max_floors = floor_ranges.get(building_type, (1, 3))
        return np.random.randint(min_floors, max_floors + 1)
    
    def _generate_unique_id(self) -> str:
        """
        Génère un identifiant unique de 16 caractères.
        
        Returns:
            Identifiant unique
        """
        return uuid.uuid4().hex[:16]
    
    def _generate_building_id(self, location: Location, index: int) -> str:
        """
        Génère un ID de bâtiment avec code géographique.
        
        Args:
            location: Localisation
            index: Index du bâtiment
            
        Returns:
            ID formaté (ex: MY_KUL_000123)
        """
        state_code = getattr(location, 'state_code', 'UNK')
        return f"MY_{state_code}_{index:06d}"
    
    def _generate_dataset_metadata(self, 
                                 buildings_df: pd.DataFrame,
                                 timeseries_df: pd.DataFrame,
                                 start_date: str,
                                 end_date: str,
                                 frequency: str) -> Dict:
        """
        Génère les métadonnées du dataset généré.
        
        Returns:
            Dictionnaire des métadonnées
        """
        return {
            'dataset_name': 'malaysia_electricity_v3',
            'generation_timestamp': datetime.now().isoformat(),
            'generator_version': '3.0',
            'total_buildings': len(buildings_df),
            'total_observations': len(timeseries_df),
            'period': {
                'start_date': start_date,
                'end_date': end_date,
                'frequency': frequency,
                'total_days': (datetime.strptime(end_date, '%Y-%m-%d') - 
                              datetime.strptime(start_date, '%Y-%m-%d')).days
            },
            'geographic_coverage': {
                'country': 'Malaysia',
                'cities_included': buildings_df['location'].unique().tolist() if 'location' in buildings_df.columns else [],
                'states_included': buildings_df['state'].unique().tolist() if 'state' in buildings_df.columns else []
            },
            'building_distribution': buildings_df['building_class'].value_counts().to_dict() if 'building_class' in buildings_df.columns else {},
            'data_quality': {
                'completeness': 100.0,
                'null_values': 0,
                'outliers_detected': 0
            }
        }
    
    def get_generation_statistics(self) -> Dict:
        """
        Retourne les statistiques du générateur.
        
        Returns:
            Dictionnaire des statistiques
        """
        return {
            'total_locations': len(self.malaysia_data.get_available_locations()),
            'supported_building_types': self.malaysia_data.get_building_types(),
            'cache_size': {
                'locations': len(self._location_cache),
                'buildings': len(self._building_cache)
            },
            'configuration': {
                'timezone': 'Asia/Kuala_Lumpur',
                'currency': 'MYR',
                'energy_unit': 'kWh'
            },
            'faker_locales': ['en_US', 'id_ID']  # Locales utilisés
        }


# Export de la classe principale
__all__ = ['ElectricityDataGenerator']