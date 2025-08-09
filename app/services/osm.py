#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SERVICE OPENSTREETMAP - GÉNÉRATEUR MALAYSIA
Fichier: app/services/osm_service.py

Service pour l'intégration avec OpenStreetMap et l'API Overpass.
Récupère les données réelles de bâtiments pour générer des datasets basés
sur la géométrie et les caractéristiques réelles des bâtiments malaysiens.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Service modulaire
"""

import logging
import json
import time
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import requests
import pandas as pd
from pathlib import Path

from app.models.building import Building
from app.models.location import Location
from app.utils.validators import validate_coordinates, validate_osm_data


class OSMService:
    """
    Service pour l'intégration avec OpenStreetMap.
    
    Fonctionnalités:
    - Récupération de bâtiments via l'API Overpass
    - Cache intelligent des requêtes OSM
    - Traitement et nettoyage des données géographiques
    - Conversion en format compatible avec le générateur
    """
    
    def __init__(self, config=None):
        """
        Initialise le service OSM.
        
        Args:
            config: Configuration de l'application
        """
        self.logger = logging.getLogger(__name__)
        self.config = config
        
        # Configuration OSM
        self.overpass_url = self._get_config_value('OVERPASS_API_URL', 'https://overpass-api.de/api/interpreter')
        self.timeout = self._get_config_value('OSM_REQUEST_TIMEOUT', 120)
        self.max_retries = self._get_config_value('OSM_MAX_RETRIES', 3)
        self.cache_enabled = self._get_config_value('OSM_CACHE_ENABLED', True)
        self.cache_duration = timedelta(hours=self._get_config_value('OSM_CACHE_DURATION_HOURS', 24))
        
        # Initialiser le cache
        self.cache_dir = self._get_config_value('CACHE_DIR', Path('data/cache'))
        self.cache_dir = Path(self.cache_dir) / 'osm'
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Statistiques de requêtes
        self.request_stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'failed_requests': 0
        }
        
        self.logger.info("✅ Service OSM initialisé")
    
    def get_buildings_for_city(self, city_name: str, limit: Optional[int] = None) -> List[Dict]:
        """
        Récupère tous les bâtiments d'une ville malaysienne.
        
        Args:
            city_name: Nom de la ville
            limit: Limite du nombre de bâtiments (optionnel)
            
        Returns:
            Liste des bâtiments avec leurs propriétés
        """
        self.logger.info(f"🏙️ Récupération des bâtiments pour {city_name}")
        
        try:
            # Obtenir les coordonnées de la ville
            city_coords = self._get_city_coordinates(city_name)
            if not city_coords:
                raise ValueError(f"Ville '{city_name}' non trouvée dans la base de données Malaysia")
            
            # Construire la requête Overpass
            query = self._build_city_overpass_query(city_name, city_coords)
            
            # Exécuter la requête
            osm_data = self._execute_overpass_query(query, f"city_{city_name}")
            
            # Traiter les données
            buildings = self._process_osm_buildings(osm_data, city_name)
            
            # Appliquer la limite si spécifiée
            if limit and len(buildings) > limit:
                buildings = buildings[:limit]
                self.logger.info(f"✂️ Limitation appliquée: {limit} bâtiments retenus")
            
            self.logger.info(f"✅ {len(buildings)} bâtiments récupérés pour {city_name}")
            return buildings
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la récupération des bâtiments pour {city_name}: {e}")
            raise
    
    def get_buildings_in_bbox(self, bbox: Tuple[float, float, float, float], 
                            building_types: Optional[List[str]] = None) -> List[Dict]:
        """
        Récupère les bâtiments dans une zone géographique définie.
        
        Args:
            bbox: Boîte englobante (sud, ouest, nord, est)
            building_types: Types de bâtiments à récupérer
            
        Returns:
            Liste des bâtiments
        """
        south, west, north, east = bbox
        self.logger.info(f"📦 Récupération des bâtiments dans bbox: {bbox}")
        
        try:
            # Valider les coordonnées
            validate_coordinates(south, west)
            validate_coordinates(north, east)
            
            # Construire la requête
            query = self._build_bbox_overpass_query(bbox, building_types)
            
            # Exécuter la requête
            cache_key = f"bbox_{south}_{west}_{north}_{east}"
            osm_data = self._execute_overpass_query(query, cache_key)
            
            # Traiter les données
            buildings = self._process_osm_buildings(osm_data)
            
            self.logger.info(f"✅ {len(buildings)} bâtiments récupérés dans la zone")
            return buildings
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la récupération des bâtiments bbox: {e}")
            raise
    
    def get_buildings_around_point(self, lat: float, lon: float, 
                                 radius: int = 1000, 
                                 building_types: Optional[List[str]] = None) -> List[Dict]:
        """
        Récupère les bâtiments autour d'un point géographique.
        
        Args:
            lat: Latitude du centre
            lon: Longitude du centre
            radius: Rayon en mètres
            building_types: Types de bâtiments à récupérer
            
        Returns:
            Liste des bâtiments
        """
        self.logger.info(f"📍 Récupération des bâtiments autour de ({lat}, {lon}) - rayon {radius}m")
        
        try:
            # Valider les coordonnées
            validate_coordinates(lat, lon)
            
            # Construire la requête
            query = self._build_around_overpass_query(lat, lon, radius, building_types)
            
            # Exécuter la requête
            cache_key = f"around_{lat}_{lon}_{radius}"
            osm_data = self._execute_overpass_query(query, cache_key)
            
            # Traiter les données
            buildings = self._process_osm_buildings(osm_data)
            
            self.logger.info(f"✅ {len(buildings)} bâtiments récupérés autour du point")
            return buildings
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la récupération des bâtiments autour du point: {e}")
            raise
    
    def _build_city_overpass_query(self, city_name: str, coords: Dict) -> str:
        """
        Construit une requête Overpass pour une ville spécifique.
        
        Args:
            city_name: Nom de la ville
            coords: Coordonnées de la ville
            
        Returns:
            Requête Overpass formatée
        """
        # Calculer une bbox approximative autour de la ville (±0.05 degrés = ~5-6km)
        buffer = 0.05
        south = coords['lat'] - buffer
        north = coords['lat'] + buffer
        west = coords['lon'] - buffer
        east = coords['lon'] + buffer
        
        query = f"""
        [out:json][timeout:{self.timeout}][maxsize:1073741824];
        (
            way["building"]({south},{west},{north},{east});
            relation["building"]({south},{west},{north},{east});
        );
        out geom;
        """
        
        return query.strip()
    
    def _build_bbox_overpass_query(self, bbox: Tuple[float, float, float, float], 
                                 building_types: Optional[List[str]] = None) -> str:
        """
        Construit une requête Overpass pour une bbox.
        
        Args:
            bbox: Boîte englobante
            building_types: Types de bâtiments
            
        Returns:
            Requête Overpass formatée
        """
        south, west, north, east = bbox
        
        # Filtres par type de bâtiment
        building_filter = ""
        if building_types:
            type_conditions = '|'.join([f'"{bt}"' for bt in building_types])
            building_filter = f'["building"~"^({type_conditions})$"]'
        else:
            building_filter = '["building"]'
        
        query = f"""
        [out:json][timeout:{self.timeout}][maxsize:1073741824];
        (
            way{building_filter}({south},{west},{north},{east});
            relation{building_filter}({south},{west},{north},{east});
        );
        out geom;
        """
        
        return query.strip()
    
    def _build_around_overpass_query(self, lat: float, lon: float, radius: int, 
                                   building_types: Optional[List[str]] = None) -> str:
        """
        Construit une requête Overpass autour d'un point.
        
        Args:
            lat: Latitude
            lon: Longitude
            radius: Rayon en mètres
            building_types: Types de bâtiments
            
        Returns:
            Requête Overpass formatée
        """
        # Filtres par type de bâtiment
        building_filter = ""
        if building_types:
            type_conditions = '|'.join([f'"{bt}"' for bt in building_types])
            building_filter = f'["building"~"^({type_conditions})$"]'
        else:
            building_filter = '["building"]'
        
        query = f"""
        [out:json][timeout:{self.timeout}][maxsize:1073741824];
        (
            way{building_filter}(around:{radius},{lat},{lon});
            relation{building_filter}(around:{radius},{lat},{lon});
        );
        out geom;
        """
        
        return query.strip()
    
    def _execute_overpass_query(self, query: str, cache_key: str) -> Dict:
        """
        Exécute une requête Overpass avec gestion du cache et des erreurs.
        
        Args:
            query: Requête Overpass
            cache_key: Clé pour le cache
            
        Returns:
            Données OSM brutes
        """
        self.request_stats['total_requests'] += 1
        
        # Vérifier le cache
        if self.cache_enabled:
            cached_data = self._get_cached_data(cache_key)
            if cached_data:
                self.request_stats['cache_hits'] += 1
                self.logger.debug(f"📋 Cache hit pour {cache_key}")
                return cached_data
        
        self.request_stats['cache_misses'] += 1
        
        # Exécuter la requête avec retry
        for attempt in range(self.max_retries):
            try:
                self.logger.debug(f"🔄 Tentative {attempt + 1}/{self.max_retries} pour la requête Overpass")
                
                response = requests.post(
                    self.overpass_url,
                    data=query,
                    headers={'Content-Type': 'text/plain; charset=utf-8'},
                    timeout=self.timeout
                )
                
                response.raise_for_status()
                data = response.json()
                
                # Valider les données
                validate_osm_data(data)
                
                # Sauvegarder en cache
                if self.cache_enabled:
                    self._save_to_cache(cache_key, data)
                
                self.logger.debug(f"✅ Requête Overpass réussie: {len(data.get('elements', []))} éléments")
                return data
                
            except requests.exceptions.Timeout:
                self.logger.warning(f"⏰ Timeout lors de la tentative {attempt + 1}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Backoff exponentiel
                    
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"🌐 Erreur réseau lors de la tentative {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    
            except json.JSONDecodeError as e:
                self.logger.error(f"📄 Erreur de parsing JSON: {e}")
                break
                
            except Exception as e:
                self.logger.error(f"❌ Erreur inattendue: {e}")
                break
        
        self.request_stats['failed_requests'] += 1
        raise Exception(f"Échec de la requête Overpass après {self.max_retries} tentatives")
    
    def _process_osm_buildings(self, osm_data: Dict, city_name: Optional[str] = None) -> List[Dict]:
        """
        Traite les données brutes OSM pour extraire les informations des bâtiments.
        
        Args:
            osm_data: Données brutes de l'API Overpass
            city_name: Nom de la ville (optionnel)
            
        Returns:
            Liste des bâtiments traités
        """
        buildings = []
        elements = osm_data.get('elements', [])
        
        self.logger.debug(f"🔍 Traitement de {len(elements)} éléments OSM")
        
        for element in elements:
            try:
                building_data = self._extract_building_data(element, city_name)
                if building_data:
                    buildings.append(building_data)
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Erreur lors du traitement de l'élément {element.get('id', 'unknown')}: {e}")
                continue
        
        # Dédoublonner par coordonnées
        buildings = self._deduplicate_buildings(buildings)
        
        self.logger.debug(f"✅ {len(buildings)} bâtiments valides extraits")
        return buildings
    
    def _extract_building_data(self, element: Dict, city_name: Optional[str] = None) -> Optional[Dict]:
        """
        Extrait les données d'un bâtiment depuis un élément OSM.
        
        Args:
            element: Élément OSM (way ou relation)
            city_name: Nom de la ville
            
        Returns:
            Données du bâtiment ou None si invalide
        """
        # Vérifier que l'élément a une géométrie
        if 'geometry' not in element or not element['geometry']:
            return None
        
        # Extraire les coordonnées du centroïde
        geometry = element['geometry']
        if not geometry:
            return None
        
        # Calculer le centroïde
        lat, lon = self._calculate_centroid(geometry)
        if not lat or not lon:
            return None
        
        # Extraire les tags
        tags = element.get('tags', {})
        building_type = tags.get('building', 'residential')
        
        # Mapper le type OSM vers nos types standards
        mapped_type = self._map_osm_building_type(building_type)
        
        # Extraire les informations additionnelles
        building_data = {
            'osm_id': element.get('id'),
            'osm_type': element.get('type', 'way'),
            'latitude': lat,
            'longitude': lon,
            'building_type': mapped_type,
            'osm_building_tag': building_type,
            'city': city_name,
            'tags': tags,
            'geometry': geometry,
            'area_sqm': self._calculate_area(geometry),
            'levels': self._extract_levels(tags),
            'height': self._extract_height(tags),
            'name': tags.get('name'),
            'addr_street': tags.get('addr:street'),
            'addr_housenumber': tags.get('addr:housenumber'),
            'addr_postcode': tags.get('addr:postcode')
        }
        
        return building_data
    
    def _calculate_centroid(self, geometry: List[Dict]) -> Tuple[Optional[float], Optional[float]]:
        """
        Calcule le centroïde d'une géométrie OSM.
        
        Args:
            geometry: Liste des points de la géométrie
            
        Returns:
            Tuple (latitude, longitude) ou (None, None)
        """
        try:
            if not geometry or len(geometry) < 3:
                return None, None
            
            # Extraire les coordonnées
            lats = [point.get('lat') for point in geometry if point.get('lat')]
            lons = [point.get('lon') for point in geometry if point.get('lon')]
            
            if not lats or not lons:
                return None, None
            
            # Calculer la moyenne (centroïde simple)
            centroid_lat = sum(lats) / len(lats)
            centroid_lon = sum(lons) / len(lons)
            
            return centroid_lat, centroid_lon
            
        except Exception:
            return None, None
    
    def _calculate_area(self, geometry: List[Dict]) -> Optional[float]:
        """
        Calcule l'aire approximative d'un bâtiment en m².
        
        Args:
            geometry: Géométrie du bâtiment
            
        Returns:
            Aire en m² ou None
        """
        try:
            if not geometry or len(geometry) < 4:
                return None
            
            # Formule de Shoelace pour calculer l'aire d'un polygone
            # Approximation en considérant les coordonnées comme planes
            coords = [(p.get('lat', 0), p.get('lon', 0)) for p in geometry]
            
            if len(coords) < 3:
                return None
            
            # Convertir en mètres approximativement (1 degré ≈ 111km)
            area = 0.0
            for i in range(len(coords)):
                j = (i + 1) % len(coords)
                area += coords[i][0] * coords[j][1]
                area -= coords[j][0] * coords[i][1]
            
            area = abs(area) / 2.0
            # Convertir de degrés² en m² (approximation)
            area_m2 = area * (111000 ** 2)
            
            return area_m2
            
        except Exception:
            return None
    
    def _map_osm_building_type(self, osm_type: str) -> str:
        """
        Mappe les types de bâtiments OSM vers nos types standards.
        
        Args:
            osm_type: Type OSM original
            
        Returns:
            Type standardisé
        """
        mapping = {
            # Résidentiel
            'house': 'residential',
            'residential': 'residential',
            'apartments': 'residential',
            'detached': 'residential',
            'terrace': 'residential',
            'bungalow': 'residential',
            
            # Commercial
            'retail': 'commercial',
            'commercial': 'commercial',
            'office': 'commercial',
            'shop': 'commercial',
            'mall': 'commercial',
            'supermarket': 'commercial',
            'restaurant': 'commercial',
            'hotel': 'commercial',
            
            # Industriel
            'industrial': 'industrial',
            'warehouse': 'industrial',
            'factory': 'industrial',
            'manufacture': 'industrial',
            
            # Public
            'public': 'public',
            'hospital': 'public',
            'school': 'public',
            'university': 'public',
            'government': 'public',
            'civic': 'public'
        }
        
        return mapping.get(osm_type.lower(), 'residential')
    
    def _extract_levels(self, tags: Dict) -> Optional[int]:
        """Extrait le nombre d'étages depuis les tags OSM."""
        levels_tag = tags.get('building:levels') or tags.get('levels')
        if levels_tag:
            try:
                return int(float(levels_tag))
            except (ValueError, TypeError):
                pass
        return None
    
    def _extract_height(self, tags: Dict) -> Optional[float]:
        """Extrait la hauteur depuis les tags OSM."""
        height_tag = tags.get('height') or tags.get('building:height')
        if height_tag:
            try:
                # Enlever les unités si présentes
                height_str = str(height_tag).replace('m', '').replace('ft', '').strip()
                return float(height_str)
            except (ValueError, TypeError):
                pass
        return None
    
    def _deduplicate_buildings(self, buildings: List[Dict]) -> List[Dict]:
        """
        Supprime les bâtiments dupliqués basés sur les coordonnées.
        
        Args:
            buildings: Liste des bâtiments
            
        Returns:
            Liste dédoublonnée
        """
        seen_coords = set()
        unique_buildings = []
        
        for building in buildings:
            # Arrondir les coordonnées pour éviter les micro-différences
            lat = round(building['latitude'], 6)
            lon = round(building['longitude'], 6)
            coord_key = (lat, lon)
            
            if coord_key not in seen_coords:
                seen_coords.add(coord_key)
                unique_buildings.append(building)
        
        removed_count = len(buildings) - len(unique_buildings)
        if removed_count > 0:
            self.logger.debug(f"🔄 {removed_count} bâtiments dupliqués supprimés")
        
        return unique_buildings
    
    def _get_city_coordinates(self, city_name: str) -> Optional[Dict]:
        """
        Récupère les coordonnées d'une ville malaysienne.
        
        Args:
            city_name: Nom de la ville
            
        Returns:
            Dictionnaire avec lat/lon ou None
        """
        # Base de données simplifiée des villes malaysiennes
        malaysia_cities = {
            'Kuala Lumpur': {'lat': 3.1390, 'lon': 101.6869},
            'George Town': {'lat': 5.4164, 'lon': 100.3327},
            'Ipoh': {'lat': 4.5975, 'lon': 101.0901},
            'Shah Alam': {'lat': 3.0733, 'lon': 101.5185},
            'Petaling Jaya': {'lat': 3.1073, 'lon': 101.6059},
            'Johor Bahru': {'lat': 1.4927, 'lon': 103.7414},
            'Kota Kinabalu': {'lat': 5.9788, 'lon': 116.0753},
            'Kuching': {'lat': 1.5533, 'lon': 110.3592},
            'Malacca': {'lat': 2.1896, 'lon': 102.2501},
            'Alor Setar': {'lat': 6.1184, 'lon': 100.3685}
        }
        
        return malaysia_cities.get(city_name)
    
    def _get_cached_data(self, cache_key: str) -> Optional[Dict]:
        """
        Récupère les données depuis le cache.
        
        Args:
            cache_key: Clé de cache
            
        Returns:
            Données cachées ou None
        """
        try:
            cache_file = self.cache_dir / f"{self._hash_key(cache_key)}.json"
            
            if not cache_file.exists():
                return None
            
            # Vérifier l'âge du cache
            file_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            if file_age > self.cache_duration:
                cache_file.unlink()  # Supprimer le cache expiré
                return None
            
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
                
        except Exception as e:
            self.logger.warning(f"⚠️ Erreur lors de la lecture du cache: {e}")
            return None
    
    def _save_to_cache(self, cache_key: str, data: Dict):
        """
        Sauvegarde les données dans le cache.
        
        Args:
            cache_key: Clé de cache
            data: Données à sauvegarder
        """
        try:
            cache_file = self.cache_dir / f"{self._hash_key(cache_key)}.json"
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            self.logger.warning(f"⚠️ Erreur lors de la sauvegarde du cache: {e}")
    
    def _hash_key(self, key: str) -> str:
        """Génère un hash MD5 pour une clé de cache."""
        return hashlib.md5(key.encode('utf-8')).hexdigest()
    
    def _get_config_value(self, key: str, default: Any) -> Any:
        """Récupère une valeur de configuration avec fallback."""
        if self.config and hasattr(self.config, key):
            return getattr(self.config, key)
        return default
    
    def get_statistics(self) -> Dict:
        """
        Retourne les statistiques du service OSM.
        
        Returns:
            Dictionnaire des statistiques
        """
        return {
            'request_stats': self.request_stats.copy(),
            'cache_info': {
                'enabled': self.cache_enabled,
                'directory': str(self.cache_dir),
                'duration_hours': self.cache_duration.total_seconds() / 3600,
                'files_count': len(list(self.cache_dir.glob('*.json'))) if self.cache_dir.exists() else 0
            },
            'configuration': {
                'overpass_url': self.overpass_url,
                'timeout': self.timeout,
                'max_retries': self.max_retries
            }
        }
    
    def clear_cache(self):
        """Vide le cache OSM."""
        try:
            if self.cache_dir.exists():
                for cache_file in self.cache_dir.glob('*.json'):
                    cache_file.unlink()
                self.logger.info("🗑️ Cache OSM vidé")
            else:
                self.logger.info("📁 Aucun cache à vider")
        except Exception as e:
            self.logger.error(f"❌ Erreur lors du vidage du cache: {e}")


# Export de la classe principale
__all__ = ['OSMService']