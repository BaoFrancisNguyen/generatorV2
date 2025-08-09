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
        if self.consumption_kwh < 0:
            self.validation_status = 'invalid'
            self.quality_score = 0.0
        
        if not (0 <= self.quality_score <= 100):
            raise ValueError(f"Quality score invalide: {self.quality_score}")
        
        valid_statuses = ['valid', 'invalid', 'suspect', 'interpolated']
        if self.validation_status not in valid_statuses:
            raise ValueError(f"Status de validation invalide: {self.validation_status}")
    
    def _calculate_features(self):
        """Calcule les caractéristiques temporelles de l'observation."""
        self._time_features = {
            'hour': self.timestamp.hour,
            'day_of_week': self.timestamp.weekday(),
            'day_of_month': self.timestamp.day,
            'month': self.timestamp.month,
            'quarter': (self.timestamp.month - 1) // 3 + 1,
            'is_weekend': self.timestamp.weekday() >= 5,
            'is_business_hours': 8 <= self.timestamp.hour <= 18,
            'is_peak_hours': self.timestamp.hour in [9, 10, 11, 14, 15, 16, 17, 18, 19, 20],
            'season': self._get_season()
        }
    
    def _get_season(self) -> str:
        """Détermine la saison en Malaysia (basée sur les moussons)."""
        month = self.timestamp.month
        if month in [11, 12, 1, 2]:
            return 'northeast_monsoon'
        elif month in [6, 7, 8]:
            return 'southwest_monsoon'
        else:
            return 'inter_monsoon'
    
    def _categorize_consumption(self):
        """Catégorise le niveau de consommation."""
        if self.consumption_kwh == 0:
            self._consumption_category = 'zero'
        elif self.consumption_kwh < 5:
            self._consumption_category = 'very_low'
        elif self.consumption_kwh < 15:
            self._consumption_category = 'low'
        elif self.consumption_kwh < 30:
            self._consumption_category = 'medium'
        elif self.consumption_kwh < 60:
            self._consumption_category = 'high'
        else:
            self._consumption_category = 'very_high'
    
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
            'y': self.consumption_kwh,  # ✅ CORRECTION: Supprimé l'apostrophe mal placée
            'consumption_kwh': self.consumption_kwh,
            'quality_score': self.quality_score,
            'anomaly_flag': self.anomaly_flag,
            'validation_status': self.validation_status,
            'weather_context': self.weather_context,
            'consumption_category': self.consumption_category,
            'time_features': self.time_features,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimeSeries':
        """
        Crée une instance TimeSeries depuis un dictionnaire.
        
        Args:
            data: Dictionnaire avec les données de l'observation
            
        Returns:
            Instance TimeSeries
        """
        # Gérer différents noms de colonnes pour la consommation
        consumption_kwh = data.get('y') or data.get('consumption_kwh') or data.get('value', 0.0)
        
        # Parser le timestamp si c'est une string
        timestamp = data.get('timestamp')
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        return cls(
            unique_id=data.get('unique_id'),
            timestamp=timestamp,
            consumption_kwh=float(consumption_kwh),
            quality_score=data.get('quality_score', 100.0),
            anomaly_flag=data.get('anomaly_flag', False),
            validation_status=data.get('validation_status', 'valid'),
            weather_context=data.get('weather_context'),
            metadata=data.get('metadata', {})
        )
    
    def __str__(self) -> str:
        """Représentation string de l'observation."""
        return (f"TimeSeries({self.unique_id}, {self.timestamp.strftime('%Y-%m-%d %H:%M')}, "
                f"{self.consumption_kwh}kWh, {self.validation_status})")
    
    def __repr__(self) -> str:
        """Représentation détaillée de l'observation."""
        return (f"TimeSeries(unique_id='{self.unique_id}', "
                f"timestamp='{self.timestamp.isoformat()}', "
                f"consumption_kwh={self.consumption_kwh}, "
                f"quality_score={self.quality_score})")


@dataclass 
class TimeSeriesCollection:
    """
    Collection de séries temporelles avec méthodes d'analyse et manipulation.
    
    Attributes:
        observations: Liste des observations TimeSeries
        building_id: Identifiant du bâtiment associé
        metadata: Métadonnées de la collection
    """
    
    observations: List[TimeSeries]
    building_id: str = ''
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialisation après création."""
        self.observations.sort(key=lambda x: x.timestamp)
        self._calculate_collection_metadata()
    
    def _calculate_collection_metadata(self):
        """Calcule les métadonnées de la collection."""
        if not self.observations:
            return
        
        consumptions = [obs.consumption_kwh for obs in self.observations]
        quality_scores = [obs.quality_score for obs in self.observations]
        
        self.metadata.update({
            'total_observations': len(self.observations),
            'date_range': {
                'start': self.observations[0].timestamp.isoformat(),
                'end': self.observations[-1].timestamp.isoformat()
            },
            'consumption_stats': {
                'total_kwh': sum(consumptions),
                'avg_kwh': sum(consumptions) / len(consumptions),
                'max_kwh': max(consumptions),
                'min_kwh': min(consumptions)
            },
            'quality_stats': {
                'avg_quality_score': sum(quality_scores) / len(quality_scores),
                'min_quality_score': min(quality_scores)
            },
            'validation_summary': self._get_validation_summary()
        })
    
    def _get_validation_summary(self) -> Dict[str, int]:
        """Résumé des statuts de validation."""
        summary = {
            'valid': 0,
            'invalid': 0,
            'suspect': 0,
            'interpolated': 0
        }
        
        for obs in self.observations:
            if obs.validation_status in summary:
                summary[obs.validation_status] += 1
        
        return summary
    
    def add_observation(self, observation: TimeSeries):
        """Ajoute une observation à la collection."""
        self.observations.append(observation)
        self.observations.sort(key=lambda x: x.timestamp)
        self._calculate_collection_metadata()
    
    def get_observations_in_range(self, start_time: datetime, end_time: datetime) -> List[TimeSeries]:
        """Retourne les observations dans une plage temporelle."""
        return [obs for obs in self.observations 
                if start_time <= obs.timestamp <= end_time]
    
    def get_daily_consumption(self) -> Dict[str, float]:
        """Calcule la consommation quotidienne."""
        daily_consumption = {}
        
        for obs in self.observations:
            date_key = obs.timestamp.date().isoformat()
            if date_key not in daily_consumption:
                daily_consumption[date_key] = 0.0
            daily_consumption[date_key] += obs.consumption_kwh
        
        return daily_consumption
    
    def detect_anomalies(self, threshold: float = 70.0) -> List[TimeSeries]:
        """
        Détecte les anomalies dans la collection.
        
        Args:
            threshold: Seuil de score d'anomalie
            
        Returns:
            Liste des observations anormales
        """
        # Calculer la consommation moyenne comme baseline
        consumptions = [obs.consumption_kwh for obs in self.observations if obs.validation_status == 'valid']
        avg_consumption = sum(consumptions) / len(consumptions) if consumptions else 0
        
        anomalies = []
        for obs in self.observations:
            anomaly_score = obs.calculate_anomaly_score(avg_consumption)
            if anomaly_score >= threshold:
                anomalies.append(obs)
        
        return anomalies
    
    def to_dataframe(self):
        """Convertit la collection en DataFrame pandas."""
        try:
            import pandas as pd
            data = [obs.to_dict() for obs in self.observations]
            return pd.DataFrame(data)
        except ImportError:
            raise ImportError("pandas requis pour to_dataframe()")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertit la collection en dictionnaire."""
        return {
            'building_id': self.building_id,
            'metadata': self.metadata,
            'observations': [obs.to_dict() for obs in self.observations]
        }


# Fonctions utilitaires

def create_timeseries_from_dataframe(df, building_id: str = '') -> TimeSeriesCollection:
    """
    Crée une TimeSeriesCollection à partir d'un DataFrame.
    
    Args:
        df: DataFrame avec les colonnes requises
        building_id: Identifiant du bâtiment
        
    Returns:
        TimeSeriesCollection
    """
    observations = []
    
    for _, row in df.iterrows():
        obs = TimeSeries.from_dict(row.to_dict())
        observations.append(obs)
    
    return TimeSeriesCollection(observations=observations, building_id=building_id)


def interpolate_missing_observations(collection: TimeSeriesCollection, 
                                   frequency: str = 'H') -> TimeSeriesCollection:
    """
    Interpole les observations manquantes dans une collection.
    
    Args:
        collection: Collection de séries temporelles
        frequency: Fréquence attendue ('H', 'D', etc.)
        
    Returns:
        Nouvelle collection avec observations interpolées
    """
    # Implementation basique - peut être étendue selon les besoins
    return collection


# Export des classes principales
__all__ = [
    'TimeSeries',
    'TimeSeriesCollection',
    'create_timeseries_from_dataframe',
    'interpolate_missing_observations'
]