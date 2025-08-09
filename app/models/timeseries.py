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
            'y': self.'consumption_kwh': self.consumption_kwh,
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
        if self.observations:
            # Trier par timestamp
            self.observations.sort(key=lambda x: x.timestamp)
            
            # Vérifier la cohérence des unique_id
            unique_ids = set(obs.unique_id for obs in self.observations)
            if len(unique_ids) > 1:
                raise ValueError(f"Observations avec des unique_id différents: {unique_ids}")
            
            if not self.building_id and self.observations:
                self.building_id = self.observations[0].unique_id
    
    @property
    def start_date(self) -> Optional[datetime]:
        """Date de début de la série."""
        return self.observations[0].timestamp if self.observations else None
    
    @property
    def end_date(self) -> Optional[datetime]:
        """Date de fin de la série."""
        return self.observations[-1].timestamp if self.observations else None
    
    @property
    def duration_days(self) -> int:
        """Durée de la série en jours."""
        if self.start_date and self.end_date:
            return (self.end_date - self.start_date).days
        return 0
    
    @property
    def total_observations(self) -> int:
        """Nombre total d'observations."""
        return len(self.observations)
    
    @property
    def total_consumption(self) -> float:
        """Consommation totale en kWh."""
        return sum(obs.consumption_kwh for obs in self.observations)
    
    @property
    def average_consumption(self) -> float:
        """Consommation moyenne en kWh."""
        if self.observations:
            return self.total_consumption / len(self.observations)
        return 0.0
    
    @property
    def peak_consumption(self) -> float:
        """Consommation maximale en kWh."""
        if self.observations:
            return max(obs.consumption_kwh for obs in self.observations)
        return 0.0
    
    @property
    def valid_observations_count(self) -> int:
        """Nombre d'observations valides."""
        return len([obs for obs in self.observations if obs.validation_status == 'valid'])
    
    @property
    def anomalies_count(self) -> int:
        """Nombre d'anomalies détectées."""
        return len([obs for obs in self.observations if obs.is_anomaly])
    
    def add_observation(self, observation: TimeSeries):
        """
        Ajoute une observation à la collection.
        
        Args:
            observation: Observation à ajouter
        """
        if self.observations and observation.unique_id != self.building_id:
            raise ValueError(f"unique_id {observation.unique_id} ne correspond pas au building_id {self.building_id}")
        
        self.observations.append(observation)
        self.observations.sort(key=lambda x: x.timestamp)
        
        if not self.building_id:
            self.building_id = observation.unique_id
    
    def get_observations_by_period(self, start_date: datetime, end_date: datetime) -> List[TimeSeries]:
        """
        Récupère les observations dans une période donnée.
        
        Args:
            start_date: Date de début
            end_date: Date de fin
            
        Returns:
            Liste des observations dans la période
        """
        return [
            obs for obs in self.observations
            if start_date <= obs.timestamp <= end_date
        ]
    
    def get_observations_by_hours(self, hours: List[int]) -> List[TimeSeries]:
        """
        Récupère les observations pour des heures spécifiques.
        
        Args:
            hours: Liste des heures (0-23)
            
        Returns:
            Liste des observations
        """
        return [
            obs for obs in self.observations
            if obs.timestamp.hour in hours
        ]
    
    def get_peak_hours_consumption(self) -> List[TimeSeries]:
        """Récupère les observations aux heures de pointe."""
        peak_hours = [7, 8, 18, 19, 20]  # Heures de pointe génériques
        return self.get_observations_by_hours(peak_hours)
    
    def get_off_peak_consumption(self) -> List[TimeSeries]:
        """Récupère les observations aux heures creuses."""
        off_peak_hours = [1, 2, 3, 4, 5, 6]  # Heures creuses
        return self.get_observations_by_hours(off_peak_hours)
    
    def get_weekend_consumption(self) -> List[TimeSeries]:
        """Récupère les observations de weekend."""
        return [
            obs for obs in self.observations
            if obs.time_features.get('is_weekend', False)
        ]
    
    def get_weekday_consumption(self) -> List[TimeSeries]:
        """Récupère les observations de semaine."""
        return [
            obs for obs in self.observations
            if not obs.time_features.get('is_weekend', False)
        ]
    
    def calculate_daily_totals(self) -> Dict[str, float]:
        """
        Calcule les totaux journaliers de consommation.
        
        Returns:
            Dictionnaire {date: consommation_totale}
        """
        daily_totals = {}
        
        for obs in self.observations:
            date_key = obs.timestamp.date().isoformat()
            if date_key not in daily_totals:
                daily_totals[date_key] = 0.0
            daily_totals[date_key] += obs.consumption_kwh
        
        return daily_totals
    
    def calculate_hourly_averages(self) -> Dict[int, float]:
        """
        Calcule les moyennes horaires de consommation.
        
        Returns:
            Dictionnaire {heure: consommation_moyenne}
        """
        hourly_data = {}
        
        for obs in self.observations:
            hour = obs.timestamp.hour
            if hour not in hourly_data:
                hourly_data[hour] = []
            hourly_data[hour].append(obs.consumption_kwh)
        
        return {
            hour: sum(values) / len(values)
            for hour, values in hourly_data.items()
        }
    
    def detect_anomalies(self, threshold_factor: float = 2.0) -> List[TimeSeries]:
        """
        Détecte les anomalies statistiques dans la série.
        
        Args:
            threshold_factor: Facteur de seuil (écarts-types)
            
        Returns:
            Liste des observations anormales
        """
        if len(self.observations) < 10:
            return []  # Pas assez de données pour détecter des anomalies
        
        # Calculer moyenne et écart-type
        consumptions = [obs.consumption_kwh for obs in self.observations]
        mean_consumption = sum(consumptions) / len(consumptions)
        variance = sum((x - mean_consumption) ** 2 for x in consumptions) / len(consumptions)
        std_deviation = variance ** 0.5
        
        # Détecter les valeurs aberrantes
        threshold = threshold_factor * std_deviation
        anomalies = []
        
        for obs in self.observations:
            if abs(obs.consumption_kwh - mean_consumption) > threshold:
                anomalies.append(obs)
        
        return anomalies
    
    def interpolate_missing_values(self, method: str = 'linear') -> 'TimeSeriesCollection':
        """
        Interpole les valeurs manquantes dans la série.
        
        Args:
            method: Méthode d'interpolation ('linear', 'previous', 'next')
            
        Returns:
            Nouvelle collection avec valeurs interpolées
        """
        if not self.observations:
            return TimeSeriesCollection([], self.building_id, self.metadata.copy())
        
        interpolated_observations = []
        
        for i, obs in enumerate(self.observations):
            interpolated_observations.append(obs)
            
            # Vérifier s'il y a un gap avec l'observation suivante
            if i < len(self.observations) - 1:
                next_obs = self.observations[i + 1]
                time_diff = next_obs.timestamp - obs.timestamp
                
                # Si gap > 2 heures, interpol
                if time_diff.total_seconds() > 7200:  # 2 heures
                    if method == 'linear':
                        # Interpolation linéaire simple
                        hours_gap = int(time_diff.total_seconds() / 3600)
                        for h in range(1, hours_gap):
                            ratio = h / hours_gap
                            interpolated_timestamp = obs.timestamp + timedelta(hours=h)
                            interpolated_consumption = (
                                obs.consumption_kwh + 
                                (next_obs.consumption_kwh - obs.consumption_kwh) * ratio
                            )
                            
                            interpolated_obs = TimeSeries(
                                unique_id=obs.unique_id,
                                timestamp=interpolated_timestamp,
                                consumption_kwh=round(interpolated_consumption, 3),
                                quality_score=70.0,  # Score réduit pour interpolation
                                validation_status='interpolated',
                                metadata={'interpolation_method': method}
                            )
                            interpolated_observations.append(interpolated_obs)
        
        return TimeSeriesCollection(
            interpolated_observations, 
            self.building_id, 
            {**self.metadata, 'interpolated': True}
        )
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Calcule les statistiques complètes de la série.
        
        Returns:
            Dictionnaire des statistiques
        """
        if not self.observations:
            return {}
        
        consumptions = [obs.consumption_kwh for obs in self.observations]
        consumptions.sort()
        
        n = len(consumptions)
        mean = sum(consumptions) / n
        variance = sum((x - mean) ** 2 for x in consumptions) / n
        std_dev = variance ** 0.5
        
        return {
            'total_observations': n,
            'period_days': self.duration_days,
            'total_consumption_kwh': round(self.total_consumption, 2),
            'average_consumption_kwh': round(mean, 2),
            'median_consumption_kwh': round(consumptions[n//2], 2),
            'min_consumption_kwh': round(min(consumptions), 2),
            'max_consumption_kwh': round(max(consumptions), 2),
            'std_deviation': round(std_dev, 2),
            'coefficient_variation': round(std_dev / mean if mean > 0 else 0, 3),
            'valid_observations': self.valid_observations_count,
            'anomalies_count': self.anomalies_count,
            'quality_percentage': round(self.valid_observations_count / n * 100, 1),
            'start_date': self.start_date.isoformat() if self.start_date else None,
            'end_date': self.end_date.isoformat() if self.end_date else None
        }
    
    def to_dataframe(self):
        """
        Convertit la collection en DataFrame pandas.
        
        Returns:
            DataFrame pandas
        """
        import pandas as pd
        
        data = [obs.to_dict() for obs in self.observations]
        return pd.DataFrame(data)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convertit la collection en dictionnaire.
        
        Returns:
            Dictionnaire représentant la collection
        """
        return {
            'building_id': self.building_id,
            'total_observations': self.total_observations,
            'statistics': self.get_statistics(),
            'metadata': self.metadata,
            'observations': [obs.to_dict() for obs in self.observations]
        }
    
    @classmethod
    def from_dataframe(cls, df, building_id: str = '') -> 'TimeSeriesCollection':
        """
        Crée une collection depuis un DataFrame pandas.
        
        Args:
            df: DataFrame pandas
            building_id: Identifiant du bâtiment
            
        Returns:
            Instance TimeSeriesCollection
        """
        observations = []
        
        for _, row in df.iterrows():
            obs = TimeSeries.from_dict(row.to_dict())
            observations.append(obs)
        
        return cls(observations, building_id)
    
    def __len__(self) -> int:
        """Nombre d'observations dans la collection."""
        return len(self.observations)
    
    def __iter__(self):
        """Itérateur sur les observations."""
        return iter(self.observations)
    
    def __getitem__(self, index) -> TimeSeries:
        """Accès par index."""
        return self.observations[index]


# Export des classes principales
__all__ = ['TimeSeries', 'TimeSeriesCollection']
            