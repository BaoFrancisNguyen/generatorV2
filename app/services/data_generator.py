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
        self.faker = Faker(['en_MY', 'ms_MY'])  # Locale Malaysia
        
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
            location_filter: Filtre géographique
            building_types: Types de bâtiments souhaités
            
        Returns:
            DataFrame avec les métadonnées des bâtiments
        """
        self.logger.info(f"📊 Génération des métadonnées pour {num_buildings} bâtiments")
        
        buildings = []
        
        for i in range(num_buildings):
            # Sélectionner une localisation
            location = self._select_location(location_filter)
            
            # Sélectionner un type de bâtiment
            building_type = self._select_building_type(location, building_types)
            
            # Générer un identifiant unique
            unique_id = self._generate_unique_id()
            building_id = self._generate_building_id(location, i)
            
            # Créer le bâtiment
            building = Building(
                unique_id=unique_id,
                building_id=building_id,
                location=location,
                building_type=building_type,
                characteristics=self._generate_building_characteristics(building_type, location)
            )
            
            buildings.append(building.to_dict())
        
        df = pd.DataFrame(buildings)
        self.logger.info(f"✅ {len(df)} bâtiments générés avec métadonnées complètes")
        
        return df
    
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
            DataFrame avec les séries temporelles
        """
        self.logger.info(f"⏰ Génération des séries temporelles {frequency} du {start_date} au {end_date}")
        
        # Créer l'index temporel
        date_range = pd.date_range(
            start=start_date,
            end=end_date,
            freq=frequency,
            tz='Asia/Kuala_Lumpur'
        )
        
        all_timeseries = []
        
        for _, building in buildings_df.iterrows():
            # Générer la série temporelle pour ce bâtiment
            consumption_data = self._generate_building_consumption(
                building=building,
                date_range=date_range,
                frequency=frequency
            )
            
            # Créer les enregistrements de série temporelle
            for timestamp, consumption in zip(date_range, consumption_data):
                timeseries_record = TimeSeries(
                    unique_id=building['unique_id'],
                    timestamp=timestamp,
                    consumption_kwh=consumption
                )
                all_timeseries.append(timeseries_record.to_dict())
        
        df = pd.DataFrame(all_timeseries)
        self.logger.info(f"✅ {len(df)} observations de consommation générées")
        
        return df
    
    def _validate_generation_parameters(self, num_buildings, start_date, end_date, frequency):
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
        # Valider le nombre de bâtiments
        if not isinstance(num_buildings, int) or num_buildings < 1:
            raise ValueError("Le nombre de bâtiments doit être un entier positif")
        
        if self.config:
            if num_buildings > self.config.MAX_BUILDINGS:
                raise ValueError(f"Nombre maximum de bâtiments: {self.config.MAX_BUILDINGS}")
        
        # Valider les dates
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Format de date invalide. Utilisez YYYY-MM-DD")
        
        if start_dt >= end_dt:
            raise ValueError("La date de fin doit être après la date de début")
        
        # Valider la période
        period_days = (end_dt - start_dt).days
        if self.config and period_days > self.config.MAX_PERIOD_DAYS:
            raise ValueError(f"Période maximum: {self.config.MAX_PERIOD_DAYS} jours")
        
        # Valider la fréquence
        if self.config and frequency not in self.config.SUPPORTED_FREQUENCIES:
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
        
        return Location.from_dict(location_data)
    
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
        type_distribution = self.malaysia_data.get_building_type_distribution(location)
        
        # Filtrer selon les types autorisés
        if building_types:
            type_distribution = {
                k: v for k, v in type_distribution.items() 
                if k in building_types
            }
        
        # Sélection pondérée
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
        base_characteristics = {
            'building_type': building_type,
            'location_type': location.type,
            'climate_zone': location.climate_zone
        }
        
        # Caractéristiques selon le type de bâtiment
        if building_type == 'residential':
            base_characteristics.update({
                'num_units': np.random.randint(1, 20),
                'avg_occupancy': np.random.uniform(2.5, 4.5),
                'has_ac': np.random.choice([True, False], p=[0.8, 0.2]),
                'building_age': np.random.randint(1, 50)
            })
        
        elif building_type == 'commercial':
            base_characteristics.update({
                'floor_area_sqm': np.random.randint(100, 2000),
                'business_type': np.random.choice(['retail', 'office', 'restaurant', 'mall']),
                'operating_hours': np.random.randint(8, 16),
                'cooling_intensity': np.random.uniform(0.7, 1.0)
            })
        
        elif building_type == 'industrial':
            base_characteristics.update({
                'industry_type': np.random.choice(['manufacturing', 'processing', 'warehouse']),
                'shift_pattern': np.random.choice(['1-shift', '2-shift', '3-shift']),
                'energy_intensity': np.random.uniform(0.8, 1.2),
                'process_load': np.random.uniform(0.3, 0.8)
            })
        
        return base_characteristics
    
    def _generate_building_consumption(self, 
                                     building: pd.Series,
                                     date_range: pd.DatetimeIndex,
                                     frequency: str) -> np.ndarray:
        """
        Génère la série de consommation pour un bâtiment spécifique.
        
        Args:
            building: Données du bâtiment
            date_range: Plage temporelle
            frequency: Fréquence des données
            
        Returns:
            Array des valeurs de consommation
        """
        # Utiliser le générateur de patterns énergétiques
        return self.pattern_generator.generate_consumption_pattern(
            building_type=building['building_class'],
            location=building['location'],
            date_range=date_range,
            frequency=frequency,
            characteristics=building.get('characteristics', {})
        )
    
    def _generate_unique_id(self) -> str:
        """Génère un identifiant unique de 16 caractères."""
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
        state_code = location.state_code if hasattr(location, 'state_code') else 'UNK'
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
                'cities_included': buildings_df['location'].unique().tolist(),
                'states_included': buildings_df['state'].unique().tolist() if 'state' in buildings_df.columns else []
            },
            'building_distribution': buildings_df['building_class'].value_counts().to_dict(),
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
            }
        }


# Export de la classe principale
__all__ = ['ElectricityDataGenerator']