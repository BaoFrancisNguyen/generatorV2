#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MODÈLE BUILDING - GÉNÉRATEUR MALAYSIA
Fichier: app/models/building.py

Modèle de données pour représenter un bâtiment avec ses caractéristiques
énergétiques et géographiques dans le contexte malaysien.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Modèles structurés
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, Any
from datetime import datetime
import uuid


@dataclass
class Building:
    """
    Modèle représentant un bâtiment avec ses caractéristiques énergétiques.
    
    Attributes:
        unique_id: Identifiant unique du bâtiment (16 caractères)
        building_id: Identifiant formaté avec code géographique (ex: MY_KUL_000123)
        location: Instance Location avec coordonnées et informations géographiques
        building_type: Type de bâtiment (residential, commercial, industrial, public)
        characteristics: Dictionnaire des caractéristiques spécifiques
        created_at: Timestamp de création
        osm_source: Indique si les données proviennent d'OSM
        validation_status: Statut de validation du bâtiment
    """
    
    # Identifiants
    unique_id: str
    building_id: str
    
    # Localisation (sera une instance Location)
    location: Any  # Type Any pour éviter import circulaire
    
    # Type et classification
    building_type: str
    
    # Caractéristiques détaillées
    characteristics: Dict[str, Any] = field(default_factory=dict)
    
    # Métadonnées
    created_at: datetime = field(default_factory=datetime.now)
    osm_source: bool = False
    validation_status: str = 'pending'  # pending, valid, invalid, warning
    
    # Propriétés calculées ou dérivées
    _energy_profile: Optional[Dict] = field(default=None, init=False)
    _consumption_baseline: Optional[float] = field(default=None, init=False)
    
    def __post_init__(self):
        """Validation et initialisation après création de l'instance."""
        self._validate_building_data()
        self._initialize_energy_profile()
    
    def _validate_building_data(self):
        """
        Valide les données du bâtiment selon les règles métier.
        
        Raises:
            ValueError: Si les données sont invalides
        """
        # Validation de l'unique_id
        if not self.unique_id or len(self.unique_id) != 16:
            raise ValueError("unique_id doit être une chaîne de 16 caractères")
        
        # Validation du type de bâtiment
        valid_types = ['residential', 'commercial', 'industrial', 'public']
        if self.building_type not in valid_types:
            raise ValueError(f"building_type doit être dans {valid_types}")
        
        # Validation du building_id
        if not self.building_id or not self.building_id.startswith('MY_'):
            raise ValueError("building_id doit commencer par 'MY_'")
        
        # Validation du statut
        valid_statuses = ['pending', 'valid', 'invalid', 'warning']
        if self.validation_status not in valid_statuses:
            raise ValueError(f"validation_status doit être dans {valid_statuses}")
    
    def _initialize_energy_profile(self):
        """Initialise le profil énergétique basé sur le type de bâtiment."""
        
        # Profils énergétiques par défaut selon le type
        energy_profiles = {
            'residential': {
                'base_consumption_kwh': 25.0,
                'peak_hours': [7, 8, 18, 19, 20],
                'seasonal_variation': 0.3,
                'ac_dependency': 0.6
            },
            'commercial': {
                'base_consumption_kwh': 150.0,
                'peak_hours': [9, 10, 11, 14, 15, 16],
                'seasonal_variation': 0.4,
                'ac_dependency': 0.8
            },
            'industrial': {
                'base_consumption_kwh': 400.0,
                'peak_hours': [8, 9, 10, 11, 13, 14, 15, 16],
                'seasonal_variation': 0.2,
                'ac_dependency': 0.3
            },
            'public': {
                'base_consumption_kwh': 80.0,
                'peak_hours': [8, 9, 10, 11, 13, 14, 15, 16, 17],
                'seasonal_variation': 0.25,
                'ac_dependency': 0.7
            }
        }
        
        base_profile = energy_profiles.get(self.building_type, energy_profiles['residential'])
        
        # Ajuster selon les caractéristiques spécifiques
        self._energy_profile = self._customize_energy_profile(base_profile)
        
        # Calculer la consommation de base
        self._consumption_baseline = self._calculate_baseline_consumption()
    
    def _customize_energy_profile(self, base_profile: Dict) -> Dict:
        """
        Personnalise le profil énergétique selon les caractéristiques du bâtiment.
        
        Args:
            base_profile: Profil énergétique de base
            
        Returns:
            Profil énergétique personnalisé
        """
        profile = base_profile.copy()
        
        # Ajustements selon les caractéristiques
        if 'floor_area_sqm' in self.characteristics:
            area = self.characteristics['floor_area_sqm']
            # Ajuster la consommation selon la surface
            if area > 1000:
                profile['base_consumption_kwh'] *= 1.5
            elif area < 100:
                profile['base_consumption_kwh'] *= 0.7
        
        if 'building_age' in self.characteristics:
            age = self.characteristics['building_age']
            # Bâtiments plus anciens consomment plus
            if age > 20:
                profile['base_consumption_kwh'] *= 1.2
            elif age < 5:
                profile['base_consumption_kwh'] *= 0.9
        
        if 'has_ac' in self.characteristics:
            has_ac = self.characteristics['has_ac']
            if not has_ac:
                profile['ac_dependency'] = 0.1
                profile['seasonal_variation'] *= 0.5
        
        if 'energy_efficiency' in self.characteristics:
            efficiency = self.characteristics['energy_efficiency']
            # efficiency entre 0.5 (inefficace) et 1.5 (très efficace)
            profile['base_consumption_kwh'] *= (2.0 - efficiency)
        
        return profile
    
    def _calculate_baseline_consumption(self) -> float:
        """
        Calcule la consommation de base du bâtiment.
        
        Returns:
            Consommation de base en kWh
        """
        if not self._energy_profile:
            return 0.0
        
        baseline = self._energy_profile['base_consumption_kwh']
        
        # Ajustements selon la localisation
        if hasattr(self.location, 'population') and self.location.population:
            # Grandes villes = consommation légèrement plus élevée
            if self.location.population > 1000000:
                baseline *= 1.1
            elif self.location.population < 100000:
                baseline *= 0.95
        
        if hasattr(self.location, 'climate_zone'):
            # Zones climatiques plus chaudes = plus de climatisation
            if self.location.climate_zone == 'tropical_hot':
                baseline *= 1.15
            elif self.location.climate_zone == 'tropical_moderate':
                baseline *= 1.05
        
        return round(baseline, 2)
    
    @property
    def energy_profile(self) -> Dict:
        """Retourne le profil énergétique du bâtiment."""
        return self._energy_profile or {}
    
    @property
    def consumption_baseline(self) -> float:
        """Retourne la consommation de base du bâtiment."""
        return self._consumption_baseline or 0.0
    
    @property
    def coordinates(self) -> tuple:
        """Retourne les coordonnées (latitude, longitude) du bâtiment."""
        if hasattr(self.location, 'latitude') and hasattr(self.location, 'longitude'):
            return (self.location.latitude, self.location.longitude)
        return (0.0, 0.0)
    
    @property
    def location_name(self) -> str:
        """Retourne le nom de la localisation."""
        if hasattr(self.location, 'city'):
            return self.location.city
        elif hasattr(self.location, 'name'):
            return self.location.name
        return 'Unknown'
    
    def get_consumption_multiplier(self, hour: int, month: int, 
                                 is_weekend: bool = False) -> float:
        """
        Calcule le multiplicateur de consommation pour un moment donné.
        
        Args:
            hour: Heure de la journée (0-23)
            month: Mois de l'année (1-12)
            is_weekend: Indique si c'est un weekend
            
        Returns:
            Multiplicateur à appliquer à la consommation de base
        """
        if not self._energy_profile:
            return 1.0
        
        multiplier = 1.0
        
        # Variation horaire
        peak_hours = self._energy_profile.get('peak_hours', [])
        if hour in peak_hours:
            multiplier *= 1.3
        elif hour in [22, 23, 0, 1, 2, 3, 4, 5]:  # Heures creuses
            multiplier *= 0.6
        else:
            multiplier *= 0.9
        
        # Variation saisonnière (Malaysia = plus chaud Mar-Mai, Sept-Oct)
        seasonal_variation = self._energy_profile.get('seasonal_variation', 0.0)
        if month in [3, 4, 5, 9, 10]:  # Mois les plus chauds
            multiplier *= (1.0 + seasonal_variation)
        elif month in [11, 12, 1, 2]:  # Saison des pluies (plus frais)
            multiplier *= (1.0 - seasonal_variation * 0.5)
        
        # Variation weekend (sauf industriel qui fonctionne 24/7)
        if is_weekend and self.building_type != 'industrial':
            if self.building_type == 'commercial':
                multiplier *= 0.7  # Commercial réduit le weekend
            elif self.building_type == 'public':
                multiplier *= 0.8
            else:  # Résidentiel
                multiplier *= 1.1  # Plus présent à la maison
        
        return round(multiplier, 3)
    
    def estimate_daily_consumption(self, month: int = 6, 
                                 is_weekend: bool = False) -> Dict[str, float]:
        """
        Estime la consommation journalière heure par heure.
        
        Args:
            month: Mois pour les variations saisonnières
            is_weekend: Type de jour
            
        Returns:
            Dictionnaire {heure: consommation_kwh}
        """
        daily_consumption = {}
        
        for hour in range(24):
            multiplier = self.get_consumption_multiplier(hour, month, is_weekend)
            consumption = self.consumption_baseline * multiplier
            daily_consumption[str(hour)] = round(consumption, 2)
        
        return daily_consumption
    
    def get_building_summary(self) -> Dict[str, Any]:
        """
        Retourne un résumé des informations du bâtiment.
        
        Returns:
            Dictionnaire avec les informations principales
        """
        summary = {
            'unique_id': self.unique_id,
            'building_id': self.building_id,
            'type': self.building_type,
            'location': self.location_name,
            'coordinates': self.coordinates,
            'baseline_consumption_kwh': self.consumption_baseline,
            'validation_status': self.validation_status,
            'osm_source': self.osm_source,
            'created_at': self.created_at.isoformat()
        }
        
        # Ajouter les caractéristiques importantes
        important_chars = ['floor_area_sqm', 'building_age', 'has_ac', 'levels', 'energy_efficiency']
        for char in important_chars:
            if char in self.characteristics:
                summary[char] = self.characteristics[char]
        
        return summary
    
    def update_characteristics(self, new_characteristics: Dict[str, Any]):
        """
        Met à jour les caractéristiques du bâtiment et recalcule le profil énergétique.
        
        Args:
            new_characteristics: Nouvelles caractéristiques à ajouter/modifier
        """
        self.characteristics.update(new_characteristics)
        
        # Recalculer le profil énergétique
        self._initialize_energy_profile()
        
        # Marquer comme nécessitant une revalidation
        if self.validation_status == 'valid':
            self.validation_status = 'pending'
    
    def validate(self) -> Dict[str, Any]:
        """
        Valide le bâtiment selon les règles métier.
        
        Returns:
            Résultat de validation avec statut et messages
        """
        validation_result = {
            'status': 'valid',
            'warnings': [],
            'errors': [],
            'score': 100.0
        }
        
        # Vérifications de base
        try:
            self._validate_building_data()
        except ValueError as e:
            validation_result['errors'].append(str(e))
            validation_result['status'] = 'invalid'
            validation_result['score'] = 0.0
            return validation_result
        
        # Vérification des coordonnées
        lat, lon = self.coordinates
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            validation_result['errors'].append("Coordonnées invalides")
            validation_result['score'] -= 30
        
        # Vérification spécifique Malaysia
        if not (0.5 <= lat <= 7.5 and 99.0 <= lon <= 120.0):
            validation_result['warnings'].append("Coordonnées hors Malaysia")
            validation_result['score'] -= 10
        
        # Vérification de la consommation de base
        if self.consumption_baseline <= 0:
            validation_result['errors'].append("Consommation de base invalide")
            validation_result['score'] -= 20
        elif self.consumption_baseline > 1000:
            validation_result['warnings'].append("Consommation de base très élevée")
            validation_result['score'] -= 5
        
        # Vérification des caractéristiques
        if 'floor_area_sqm' in self.characteristics:
            area = self.characteristics['floor_area_sqm']
            if area <= 0:
                validation_result['errors'].append("Surface négative ou nulle")
                validation_result['score'] -= 15
            elif area > 10000:
                validation_result['warnings'].append("Surface très importante")
                validation_result['score'] -= 5
        
        # Déterminer le statut final
        if validation_result['errors']:
            validation_result['status'] = 'invalid'
        elif validation_result['warnings']:
            validation_result['status'] = 'warning'
        
        # Mettre à jour le statut du bâtiment
        self.validation_status = validation_result['status']
        
        return validation_result
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convertit le bâtiment en dictionnaire pour sérialisation.
        
        Returns:
            Dictionnaire représentant le bâtiment
        """
        # Gérer la sérialisation de l'objet location
        location_data = {}
        if hasattr(self.location, 'to_dict'):
            location_data = self.location.to_dict()
        elif hasattr(self.location, '__dict__'):
            location_data = self.location.__dict__
        else:
            # Fallback si location est déjà un dict
            location_data = self.location if isinstance(self.location, dict) else {}
        
        return {
            'unique_id': self.unique_id,
            'building_id': self.building_id,
            'latitude': self.coordinates[0],
            'longitude': self.coordinates[1],
            'location': self.location_name,
            'state': location_data.get('state', 'Unknown'),
            'region': location_data.get('region', 'Unknown'),
            'population': location_data.get('population', 0),
            'timezone': location_data.get('timezone', 'Asia/Kuala_Lumpur'),
            'building_class': self.building_type,
            'cluster_size': self.characteristics.get('cluster_size', 1),
            'freq': 'H',  # Valeur par défaut
            'dataset': 'malaysia_electricity_v3',
            'location_id': location_data.get('location_id', f"MY_{self.unique_id[:5]}"),
            'osm_source': self.osm_source,
            'validation_status': self.validation_status,
            'baseline_consumption_kwh': self.consumption_baseline,
            'characteristics': self.characteristics.copy(),
            'created_at': self.created_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Building':
        """
        Crée une instance Building depuis un dictionnaire.
        
        Args:
            data: Dictionnaire avec les données du bâtiment
            
        Returns:
            Instance Building
        """
        # Import local pour éviter import circulaire
        from app.models.location import Location
        
        # Créer l'objet location
        location_data = {
            'city': data.get('location', 'Unknown'),
            'state': data.get('state', 'Unknown'),
            'region': data.get('region', 'Unknown'),
            'latitude': data.get('latitude', 0.0),
            'longitude': data.get('longitude', 0.0),
            'population': data.get('population', 0),
            'timezone': data.get('timezone', 'Asia/Kuala_Lumpur')
        }
        location = Location.from_dict(location_data)
        
        # Extraire les caractéristiques
        characteristics = data.get('characteristics', {})
        
        # Ajouter d'autres champs dans characteristics s'ils ne sont pas déjà présents
        for key in ['cluster_size', 'floor_area_sqm', 'building_age', 'levels']:
            if key in data and key not in characteristics:
                characteristics[key] = data[key]
        
        # Créer l'instance
        building = cls(
            unique_id=data.get('unique_id', str(uuid.uuid4()).replace('-', '')[:16]),
            building_id=data.get('building_id', f"MY_UNK_{data.get('unique_id', '000000')[:6]}"),
            location=location,
            building_type=data.get('building_class', data.get('building_type', 'residential')),
            characteristics=characteristics,
            osm_source=data.get('osm_source', False),
            validation_status=data.get('validation_status', 'pending')
        )
        
        # Restaurer la date de création si fournie
        if 'created_at' in data:
            try:
                building.created_at = datetime.fromisoformat(data['created_at'].replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                pass  # Garder la date actuelle
        
        return building
    
    @classmethod
    def from_osm_data(cls, osm_data: Dict[str, Any], city: str = None) -> 'Building':
        """
        Crée une instance Building depuis des données OSM.
        
        Args:
            osm_data: Données du bâtiment OSM
            city: Nom de la ville (optionnel)
            
        Returns:
            Instance Building
        """
        # Import local pour éviter import circulaire
        from app.models.location import Location
        
        # Créer la location depuis les données OSM
        location = Location(
            city=city or osm_data.get('city', 'Unknown'),
            latitude=osm_data['latitude'],
            longitude=osm_data['longitude'],
            state='Unknown',  # À déterminer depuis les coordonnées
            region='Unknown'
        )
        
        # Mapper le type OSM vers notre type
        building_type = osm_data.get('building_type', 'residential')
        
        # Extraire les caractéristiques depuis les tags OSM
        characteristics = {
            'osm_id': osm_data.get('osm_id'),
            'osm_type': osm_data.get('osm_type', 'way'),
            'osm_tags': osm_data.get('tags', {}),
            'area_sqm': osm_data.get('area_sqm'),
            'levels': osm_data.get('levels'),
            'height': osm_data.get('height'),
            'floor_area_sqm': osm_data.get('area_sqm'),  # Approximation
        }
        
        # Nettoyer les valeurs None
        characteristics = {k: v for k, v in characteristics.items() if v is not None}
        
        # Générer les IDs
        unique_id = str(uuid.uuid4()).replace('-', '')[:16]
        building_id = f"OSM_{osm_data.get('osm_id', unique_id)}"
        
        return cls(
            unique_id=unique_id,
            building_id=building_id,
            location=location,
            building_type=building_type,
            characteristics=characteristics,
            osm_source=True,
            validation_status='pending'
        )
    
    def __str__(self) -> str:
        """Représentation string du bâtiment."""
        return (f"Building({self.building_id}, {self.building_type}, "
                f"{self.location_name}, {self.consumption_baseline}kWh)")
    
    def __repr__(self) -> str:
        """Représentation détaillée du bâtiment."""
        return (f"Building(unique_id='{self.unique_id}', "
                f"building_type='{self.building_type}', "
                f"location='{self.location_name}', "
                f"osm_source={self.osm_source})")


# Export de la classe principale
__all__ = ['Building']
