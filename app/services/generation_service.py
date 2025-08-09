#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SERVICE DE GÉNÉRATION SIMPLE - GÉNÉRATEUR MALAYSIA
Fichier: app/services/generation_service.py

Service simple de génération de données énergétiques compatible.

Auteur: Équipe Développement
Date: 2025
Version: 4.0 - Version simple
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any


class GenerationService:
    """Service simple de génération de données énergétiques."""
    
    def __init__(self, config=None):
        """Initialise le service de génération."""
        self.logger = logging.getLogger(__name__)
        self.config = config or {}
        
        self.logger.info("✅ Service Génération simple initialisé")
    
    def generate_from_buildings(self, generation_config: Dict) -> Dict:
        """
        Génère des données énergétiques basiques à partir de bâtiments OSM.
        
        Args:
            generation_config: Configuration de génération
            
        Returns:
            Dict contenant buildings et timeseries
        """
        buildings = generation_config.get('buildings', [])
        start_date = generation_config.get('start_date', '2024-01-01')
        end_date = generation_config.get('end_date', '2024-01-31')
        frequency = generation_config.get('frequency', 'D')
        
        self.logger.info(f"🔄 Génération pour {len(buildings)} bâtiments")
        
        # Créer DataFrame des bâtiments
        buildings_df = pd.DataFrame(buildings) if buildings else pd.DataFrame()
        
        # Créer données temporelles simples
        date_range = pd.date_range(start=start_date, end=end_date, freq=frequency)
        timeseries_data = []
        
        for building in buildings[:100]:  # Limiter pour test
            building_id = building.get('osm_id', f"building_{len(timeseries_data)}")
            building_type = building.get('building_type', 'residential')
            
            # Génération simple de consommation
            base_consumption = self._get_base_consumption(building_type)
            
            for date in date_range:
                # Variation simple selon le type et la date
                consumption = base_consumption * (0.8 + 0.4 * (date.dayofyear % 100) / 100)
                
                timeseries_data.append({
                    'building_id': building_id,
                    'date': date,
                    'consumption_kwh': round(consumption, 2),
                    'building_type': building_type
                })
        
        timeseries_df = pd.DataFrame(timeseries_data)
        
        self.logger.info(f"✅ Génération terminée: {len(timeseries_df)} records")
        
        return {
            'buildings': buildings_df,
            'timeseries': timeseries_df
        }
    
    def _get_base_consumption(self, building_type: str) -> float:
        """Retourne une consommation de base selon le type de bâtiment."""
        consumption_mapping = {
            'residential': 15.0,
            'commercial': 45.0,
            'industrial': 120.0,
            'public': 30.0,
            'office': 25.0,
            'other': 10.0
        }
        return consumption_mapping.get(building_type, 10.0)