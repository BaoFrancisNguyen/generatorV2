#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MODÈLE TIMESERIES - GÉNÉRATEUR MALAYSIA
Fichier: app/models/timeseries.py

Modèle de données pour représenter une observation de série temporelle
de consommation électrique avec validation et métadonnées.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Modèles structurés
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, Any, List
from datetime import datetime, timezone
import pytz


@dataclass
class TimeSeries:
    """
    Modèle représentant une observation de série temporelle énergétique.
    
    Attributes:
        unique_id: Identifiant du bâtiment associé
        timestamp: Moment de l'observation
        consumption_kwh: Consommation électrique en kWh
        quality_score: Score de qualité de la mesure (0-100)
        anomaly_flag: Indicateur d'anomalie détectée
        weather_context: Contexte météorologique
        validation_status: Statut de validation de l'observation
        metadata: Métadonnées additionnelles
    """
    
    # Données principales
    unique_id: str
    timestamp: datetime
    consumption_kwh: float
    
    # Qualité et validation
    quality_score: float = 100.0
    anomaly_flag: bool = False
    validation_status: str = 'valid'  # valid, invalid, suspect, interpolated
    
    # Contexte
    weather_context: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Métadonnées calculées
    _consumption_category: Optional[str] = field(default=None, init=False)
    _time_features: Optional[Dict[str, Any]] = field(default=None, init=False)
    
    def __post_init__(self):
        """Initialisation après création de l'instance."""
        self._ensure_timezone()
        self._validate_data()
        self._calculate_features()
        self._categorize_consumption()
    
    def _ensure_timezone(self):
        """S'assure que le timestamp a un timezone (Malaysia par défaut)."""
        if self.timestamp.tzinfo is None:
            # Assumer le timezone de Malaysia si non spécifié
            malaysia_tz = pytz.timezone('Asia/Kuala_Lumpur')
            self.timestamp = malaysia_tz.localize(self.timestamp)
        elif self.timestamp.tzinfo != pytz.timezone('Asia/Kuala_Lumpur'):
            # Convertir vers le timezone Malaysia
            malaysia_tz = pytz.timezone('Asia/Kuala_Lumpur')
            self.timestamp = self.timestamp.astimezone(malaysia_tz)
    
    def _validate_data(self):
        """
        Valide les données de l'observation.
        
        Raises:
            ValueError: Si les données sont invalides
        """
        # Validation de l'unique_id
        if not self.unique_id or len(self.unique_id) != 16:
            raise ValueError("unique_id doit être une chaîne de 16 caractères")
        
        # Validation du timestamp
        if not isinstance(self.timestamp, datetime):
            raise ValueError("timestamp doit être un objet datetime")
        
        # Validation de la consommation
        if not isinstance(self.consumption_kwh, (int, float)):
            raise ValueError("consumption_kwh doit être un nombre")
        
        if self.consumption_kwh < 0:
            raise ValueError("consumption_kwh ne peut pas être négatif")
        
        # Validation du quality_score
        if not (0 <= self.quality_score <= 100):
            raise ValueError("quality_score doit être entre 0 et 100")
        
        # Validation du validation_status
        valid_statuses = ['valid', 'invalid', 'suspect', 'interpolated']
        if self.validation_status not in valid_statuses:
            raise ValueError(f"validation_status doit être dans {valid_statuses}")
    
    def _calculate_features(self):
        """Calcule les caractéristiques temporelles."""
        self._time_features = {
            'year': self.timestamp.year,
            'month': self.timestamp.month,
            'day': self.timestamp.day,
            'hour': self.timestamp.hour,
            'minute': self.timestamp.minute,
            'day_of_week': self.timestamp.weekday(),  # 0=Lundi, 6=Dimanche
            'day_of_year': self.timestamp.timetuple().tm_yday,
            'week_of_year': self.timestamp.isocalendar()[1],
            'quarter': (self.timestamp.month - 1) // 3 + 1,
            'is_weekend': self.timestamp.weekday() >= 5,
            'is_business_hour': 8 <= self.timestamp.hour <= 17,
            'is_peak_hour': self.timestamp.hour in [7, 8, 18, 19, 20],
            'is_night': self.timestamp.hour >= 22 or self.timestamp.hour <= 6
        }
        
        # Saisons en Malaysia (approximatives)
        month = self.timestamp.month
        if month in [11, 12, 1, 2]:
            season = 'monsoon'  # Saison des pluies
        elif month in [6, 7, 8]:
            season = 'dry'      # Saison sèche
        else:
            season = 'inter_monsoon'  # Inter-mousson
        
        self._time_features['season'] = season
        
        # Période de la journée
        hour = self.timestamp.hour
        if 6 <= hour < 12:
            time_period = 'morning'
        elif 12 <= hour < 18:
            time_period = 'afternoon'
        elif 18 <= hour < 22:
            time_period = 'evening'
        else:
            time_period = 'night'
        
        self._time_features['time_period'] = time_period
    
    def _categorize_consumption(self):
        """Catégorise la consommation selon des seuils."""
        consumption = self.consumption_kwh
        
        if consumption == 0:
            self._consumption_category = 'zero'
        elif consumption < 5:
            self._consumption_category = 'very_low'
        elif consumption < 20:
            self._consumption_category = 'low'
        elif consumption < 50:
            self._consumption_category = 'medium'
        elif consumption < 100:
            self._consumption_category = 'high'
        elif consumption < 200:
            self._consumption_category = 'very_high'
        else:
            self._consumption_category = 'extreme'
    
    @property
    def time_features(self) -> Dict[str, Any]:
        """Retourne les caractéristiques temporelles."""
        return self._time_features or {}
    
    @property
    def consumption_category(self) -> str:
        """Retourne la catégorie de consommation."""
        return self._consumption_category or 'unknown'
    
    @property
    def is_anomaly(self) -> bool:
        """Indique si l'observation est considérée comme une anomalie."""
        return (self.anomaly_flag or 
                self.quality_score < 50 or 
                self.validation_status in ['invalid', 'suspect'])
    
    @property
    def timestamp_local(self) -> datetime:
        """Retourne le timestamp en heure locale Malaysia."""
        return self.timestamp
    
    @property
    def timestamp_utc(self) -> datetime:
        """Retourne le timestamp en UTC."""
        return self.timestamp.astimezone(pytz.UTC)
    
    def get_consumption_per_hour(self) -> float:
        """
        Calcule la consommation normalisée par heure.
        
        Returns:
            Consommation en kWh normalisée pour une heure
        """
        # Si les données sont à une fréquence différente, normaliser
        # Pour l'instant, on assume des données horaires
        return self.consumption_kwh
    
    def is_in_time_range(self, start_hour: int, end_hour: int) -> bool:
        """
        Vérifie si l'observation est dans une plage horaire.
        
        Args:
            start_hour: Heure de début (0-23)
            end_hour: Heure de fin (0-23)
            
        Returns:
            True si dans la plage
        """
        hour = self.timestamp.hour
        
        if start_hour <= end_hour:
            return start_hour <= hour <= end_hour
        else:
            # Plage qui traverse minuit
            return hour >= start_hour or hour <= end_hour
    
    def get_weather_adjusted_consumption(self) -> float:
        """
        Retourne la consommation ajustée selon le contexte météo.
        
        Returns:
            Consommation ajustée
        """
        if not self.weather_context:
            return self.consumption_kwh
        
        adjusted_consumption = self.consumption_kwh
        
        # Ajustement selon la température
        if 'temperature' in self.weather_context:
            temp = self.weather_context['temperature']
            # Plus il fait chaud, plus la climatisation est utilisée
            if temp > 30:
                temp_factor = 1.0 + (temp - 30) * 0.02  # +2% par degré au-dessus de 30°C
                adjusted_consumption *= temp_factor
            elif temp < 24:
                temp_factor = 1.0 - (24 - temp) * 0.01  # -1% par degré en-dessous de 24°C
                adjusted_consumption *= max(0.8, temp_factor)
        
        # Ajustement selon l'humidité
        if 'humidity' in self.weather_context:
            humidity = self.weather_context['humidity']
            if humidity > 80:
                humidity_factor = 1.0 + (humidity - 80) * 0.001  # +0.1% par point au-dessus de 80%
                adjusted_consumption *= humidity_factor
        
        return round(adjusted_consumption, 3)
    
    def calculate_anomaly_score(self, baseline_consumption: float = None) -> float:
        """
        Calcule un score d'anomalie pour cette observation.
        
        Args:
            baseline_consumption: Consommation de référence
            
        Returns:
            Score d'anomalie (0-100, 100 = très anormal)
        """
        anomaly_score = 0.0
        
        # Score basé sur la qualité
        anomaly_score += (100 - self.quality_score) * 0.3
        
        # Score basé sur la validation
        status_scores = {
            'valid': 0,
            'interpolated': 20,
            'suspect': 60,
            'invalid': 100
        }
        anomaly_score += status_scores.get(self.validation_status, 50) * 0.2
        
        # Score basé sur la deviation par rapport au baseline
        if baseline_consumption and baseline_consumption > 0:
            deviation_ratio = abs(self.consumption_kwh - baseline_consumption) / baseline_consumption
            
            if deviation_ratio > 2.0:  # Plus de 200% de déviation
                anomaly_score += 50
            elif deviation_ratio > 1.0:  # Plus de 100% de déviation
                anomaly_score += 30
            elif deviation_ratio > 0.5:  # Plus de 50% de déviation
                anomaly_score += 15
        
        # Score basé sur des valeurs extrêmes
        if self.consumption_kwh > 1000:  # Très haute consommation
            anomaly_score += 40
        elif self.consumption_kwh == 0 and not self.is_in_time_range(2, 5):  # Zéro en dehors des heures creuses
            anomaly_score += 25
        
        return min(100.0, round(anomaly_score, 1))
    
    def interpolate_missing_value(self, previous_value: float, 
                                next_value: float, position_ratio: float) -> 'TimeSeries':
        """
        Crée une nouvelle observation interpolée.
        
        Args:
            previous_value: Valeur précédente
            next_value: Valeur suivante
            position_ratio: Position dans l'intervalle (0-1)
            
        Returns:
            Nouvelle observation TimeSeries interpolée
        """
        # Interpolation linéaire simple
        interpolated_value = previous_value + (next_value - previous_value) * position_ratio
        
        # Créer une nouvelle observation
        interpolated_obs = TimeSeries(
            unique_id=self.unique_id,
            timestamp=self.timestamp,
            consumption_kwh=round(interpolated_value, 3),
            quality_score=max(50.0, self.quality_score - 20),  # Réduire la qualité
            validation_status='interpolated',
            weather_context=self.weather_context,
            metadata={**self.metadata, 'interpolation_method': 'linear'}
        )
        
        return interpolated_obs
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convertit l'observation en dictionnaire.
        
        Returns:
            Dictionnaire représentant l'observation
        """
        return {
            'unique_id': self.unique_id,
            'timestamp': self.timestamp.isoformat(),
            'y': self.consumption_kwh,  # Nom de colonne standard pour compatibilité
            