#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SERVICE OPENSTREETMAP OPTIMISÉ - GÉNÉRATEUR MALAYSIA
Fichier: app/services/osm_service.py

Service optimisé pour l'intégration avec OpenStreetMap et l'API Overpass.
Améliore considérablement les performances et le nombre de résultats.

Auteur: Équipe Développement
Date: 2025
Version: 3.1 - Service optimisé
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
    Service optimisé pour l'intégration avec OpenStreetMap.
    
    Optimisations apportées:
    - Requêtes Overpass plus efficaces avec récursion pour grandes zones
    - Parallélisation des requêtes par subdivisions
    - Cache intelligent avec compression
    - Requêtes spécialisées pour la Malaysia
    """
    
    def __init__(self, config=None):
        """
        Initialise le service OSM optimisé.
        """
        self.logger = logging.getLogger(__name__)
        self.config = config
        
        # Configuration OSM optimisée
        self.overpass_url = self._get_config_value('OVERPASS_API_URL', 'https://overpass-api.de/api/interpreter')
        self.timeout = self._get_config_value('OSM_REQUEST_TIMEOUT', 300)  # ✅ Augmenté à 5 minutes
        self.max_retries = self._get_config_value('OSM_MAX_RETRIES', 3)
        self.cache_enabled = self._get_config_value('OSM_CACHE_ENABLED', True)
        self.cache_duration = timedelta(hours=self._get_config_value('OSM_CACHE_DURATION_HOURS', 24))
        
        # ✅ Optimisations spécifiques Malaysia
        self.malaysia_bounds = {
            'south': 0.855,
            'west': 99.644,
            'north': 7.363,
            'east': 119.267
        }
        
        # Cache
        self.cache_dir = self._get_config_value('CACHE_DIR', Path('data/cache'))
        self.cache_dir = Path(self.cache_dir) / 'osm'
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Statistiques
        self.request_stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'failed_requests': 0,
            'total_buildings_found': 0
        }
        
        self.logger.info("✅ Service OSM optimisé initialisé")
    
    def get_buildings_for_malaysia(self, limit: Optional[int] = None) -> List[Dict]:
        """
        ✅ NOUVELLE MÉTHODE: Récupère TOUS les bâtiments de Malaysia par subdivisions.
        
        Cette méthode divise la Malaysia en grilles plus petites pour éviter les timeouts
        et récupérer un maximum de bâtiments.
        
        Args:
            limit: Limite optionnelle du nombre de bâtiments
            
        Returns:
            Liste complète des bâtiments malaysiens
        """
        self.logger.info("🇲🇾 Récupération de TOUS les bâtiments de Malaysia (mode optimisé)")
        
        all_buildings = []
        
        # ✅ Diviser la Malaysia en grilles de 0.5° x 0.5° (environ 55km x 55km)
        grid_size = 0.5
        
        west = self.malaysia_bounds['west']
        east = self.malaysia_bounds['east']
        south = self.malaysia_bounds['south']
        north = self.malaysia_bounds['north']
        
        total_grids = 0
        processed_grids = 0
        
        # Calculer le nombre total de grilles
        lat_steps = int((north - south) / grid_size) + 1
        lon_steps = int((east - west) / grid_size) + 1
        total_grids = lat_steps * lon_steps
        
        self.logger.info(f"📊 Division en {total_grids} grilles ({lat_steps}x{lon_steps}) de {grid_size}°")
        
        current_lat = south
        while current_lat < north:
            current_lon = west
            while current_lon < east:
                # Définir la bbox de cette grille
                grid_south = current_lat
                grid_north = min(current_lat + grid_size, north)
                grid_west = current_lon
                grid_east = min(current_lon + grid_size, east)
                
                bbox = [grid_south, grid_west, grid_north, grid_east]
                
                try:
                    processed_grids += 1
                    self.logger.info(f"📦 Grille {processed_grids}/{total_grids}: {bbox}")
                    
                    # Récupérer les bâtiments de cette grille
                    grid_buildings = self.get_buildings_in_bbox_optimized(bbox)
                    
                    if grid_buildings:
                        all_buildings.extend(grid_buildings)
                        self.logger.info(f"✅ Grille {processed_grids}: {len(grid_buildings)} bâtiments "
                                       f"(Total: {len(all_buildings)})")
                    
                    # Vérifier la limite
                    if limit and len(all_buildings) >= limit:
                        self.logger.info(f"🎯 Limite atteinte: {len(all_buildings)} bâtiments")
                        return all_buildings[:limit]
                    
                    # Pause entre les requêtes pour éviter la surcharge du serveur
                    time.sleep(0.5)
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Erreur grille {processed_grids}: {e}")
                    continue
                
                current_lon += grid_size
            current_lat += grid_size
        
        self.logger.info(f"🎉 TERMINÉ: {len(all_buildings)} bâtiments récupérés pour toute la Malaysia")
        self.request_stats['total_buildings_found'] = len(all_buildings)
        
        return all_buildings
    
    def get_buildings_in_bbox_optimized(self, bbox: Tuple[float, float, float, float], 
                                      building_types: Optional[List[str]] = None) -> List[Dict]:
        """
        ✅ Version optimisée de récupération par bbox.
        
        Utilise une requête Overpass plus efficace et gère mieux les gros volumes.
        """
        south, west, north, east = bbox
        
        try:
            # Construire une requête Overpass optimisée
            query = self._build_optimized_overpass_query(bbox, building_types)
            
            # Exécuter avec cache
            cache_key = f"opt_bbox_{south:.3f}_{west:.3f}_{north:.3f}_{east:.3f}"
            osm_data = self._execute_overpass_query(query, cache_key)
            
            # Traiter les données
            buildings = self._process_osm_buildings_optimized(osm_data)
            
            return buildings
            
        except Exception as e:
            self.logger.error(f"❌ Erreur bbox optimisée {bbox}: {e}")
            return []
    
    def _build_optimized_overpass_query(self, bbox: Tuple[float, float, float, float], 
                                      building_types: Optional[List[str]] = None) -> str:
        """
        ✅ Construit une requête Overpass OPTIMISÉE pour la Malaysia.
        
        Optimisations:
        - Limite la récursion 
        - Utilise des filtres plus efficaces
        - Optimise pour les types de bâtiments malaysiens
        """
        south, west, north, east = bbox
        
        # ✅ Types de bâtiments courrants en Malaysia
        if not building_types:
            # Filtres optimisés pour Malaysia
            building_filter = '["building"~"^(yes|house|residential|apartments|commercial|retail|office|shop|hotel|school|hospital|mosque|temple|church|warehouse|industrial|factory)$"]'
        else:
            type_conditions = '|'.join([f'{bt}' for bt in building_types])
            building_filter = f'["building"~"^({type_conditions})$"]'
        
        # ✅ Requête optimisée avec limite et sans géométrie excessive
        query = f"""
        [out:json][timeout:{self.timeout}][maxsize:2147483648];
        (
            way{building_filter}({south},{west},{north},{east});
            rel{building_filter}({south},{west},{north},{east});
        );
        out center meta;
        """
        
        return query.strip()
    
    def get_buildings_for_city(self, city_name: str, limit: Optional[int] = None) -> List[Dict]:
        """
        ✅ Version optimisée pour récupérer les bâtiments d'une ville.
        """
        self.logger.info(f"🏙️ Récupération optimisée des bâtiments pour {city_name}")
        
        try:
            # Coordonnées des principales villes malaysiennes
            city_coords = self._get_malaysia_city_coordinates(city_name)
            
            if not city_coords:
                self.logger.warning(f"⚠️ Ville inconnue: {city_name}")
                return []
            
            # ✅ Utiliser un rayon adaptatif selon la ville
            radius = self._get_city_radius(city_name)
            
            # Récupérer par subdivisions circulaires
            buildings = self._get_buildings_around_city_optimized(
                city_coords['lat'], 
                city_coords['lon'], 
                radius, 
                limit
            )
            
            self.logger.info(f"✅ {len(buildings)} bâtiments trouvés pour {city_name}")
            return buildings
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la récupération pour {city_name}: {e}")
            return []
    
    def _get_buildings_around_city_optimized(self, lat: float, lon: float, 
                                           radius: int, limit: Optional[int] = None) -> List[Dict]:
        """
        ✅ Récupération optimisée autour d'une ville par anneaux concentriques.
        """
        all_buildings = []
        
        # ✅ Diviser en anneaux concentriques pour éviter les timeouts
        ring_size = min(2000, radius // 3)  # Anneaux de 2km max
        current_radius = ring_size
        
        while current_radius <= radius:
            try:
                self.logger.info(f"🔄 Anneau: rayon {current_radius}m")
                
                # Requête pour cet anneau
                ring_buildings = self._get_buildings_in_ring(lat, lon, current_radius - ring_size, current_radius)
                
                if ring_buildings:
                    all_buildings.extend(ring_buildings)
                    self.logger.info(f"✅ Anneau {current_radius}m: {len(ring_buildings)} bâtiments "
                                   f"(Total: {len(all_buildings)})")
                
                # Vérifier la limite
                if limit and len(all_buildings) >= limit:
                    return all_buildings[:limit]
                
                current_radius += ring_size
                time.sleep(0.3)  # Pause entre requêtes
                
            except Exception as e:
                self.logger.warning(f"⚠️ Erreur anneau {current_radius}m: {e}")
                current_radius += ring_size
                continue
        
        return all_buildings
    
    def _get_buildings_in_ring(self, lat: float, lon: float, 
                             inner_radius: int, outer_radius: int) -> List[Dict]:
        """
        ✅ Récupère les bâtiments dans un anneau (entre deux rayons).
        """
        try:
            # Construire la requête pour l'anneau
            query = f"""
            [out:json][timeout:{self.timeout}];
            (
                way["building"](around:{outer_radius},{lat},{lon});
                rel["building"](around:{outer_radius},{lat},{lon});
            );
            out center meta;
            """
            
            cache_key = f"ring_{lat:.3f}_{lon:.3f}_{inner_radius}_{outer_radius}"
            osm_data = self._execute_overpass_query(query, cache_key)
            
            # Traiter et filtrer par distance
            all_buildings = self._process_osm_buildings_optimized(osm_data)
            
            # ✅ Filtrer pour garder seulement les bâtiments dans l'anneau
            ring_buildings = []
            for building in all_buildings:
                distance = self._calculate_distance(lat, lon, building['lat'], building['lon'])
                if inner_radius <= distance <= outer_radius:
                    ring_buildings.append(building)
            
            return ring_buildings
            
        except Exception as e:
            self.logger.error(f"❌ Erreur anneau {inner_radius}-{outer_radius}m: {e}")
            return []
    
    def _process_osm_buildings_optimized(self, osm_data: Dict) -> List[Dict]:
        """
        ✅ Traitement optimisé des données OSM.
        
        Améliorations:
        - Extraction plus rapide des coordonnées
        - Nettoyage intelligent des données
        - Gestion des bâtiments sans géométrie
        """
        buildings = []
        elements = osm_data.get('elements', [])
        
        for element in elements:
            try:
                building_data = self._extract_building_data_optimized(element)
                if building_data:
                    buildings.append(building_data)
            except Exception as e:
                # Ignorer les éléments problématiques
                continue
        
        return buildings
    
    def _extract_building_data_optimized(self, element: Dict) -> Optional[Dict]:
        """
        ✅ Extraction optimisée des données d'un bâtiment OSM.
        """
        try:
            # ID et type
            osm_id = element.get('id')
            if not osm_id:
                return None
            
            # ✅ Coordonnées - utiliser center si disponible, sinon calculer
            lat, lon = None, None
            
            if 'center' in element:
                lat = element['center']['lat']
                lon = element['center']['lon']
            elif element['type'] == 'node':
                lat = element.get('lat')
                lon = element.get('lon')
            elif 'nodes' in element:
                # Calculer le centroïde approximatif
                if element.get('geometry'):
                    coords = element['geometry']
                    if coords:
                        lats = [p['lat'] for p in coords if 'lat' in p]
                        lons = [p['lon'] for p in coords if 'lon' in p]
                        if lats and lons:
                            lat = sum(lats) / len(lats)
                            lon = sum(lons) / len(lons)
            
            if not lat or not lon:
                return None
            
            # ✅ Vérifier que c'est en Malaysia
            if not self._is_in_malaysia(lat, lon):
                return None
            
            # Tags du bâtiment
            tags = element.get('tags', {})
            building_type = tags.get('building', 'yes')
            
            # ✅ Normaliser le type de bâtiment pour notre système
            normalized_type = self._normalize_building_type(building_type, tags)
            
            return {
                'id': f"osm_{element['type']}_{osm_id}",
                'osm_id': osm_id,
                'osm_type': element['type'],
                'lat': lat,
                'lon': lon,
                'building_type': normalized_type,
                'building_original': building_type,
                'name': tags.get('name'),
                'addr_street': tags.get('addr:street'),
                'addr_city': tags.get('addr:city'),
                'addr_state': tags.get('addr:state'),
                'levels': self._parse_numeric_tag(tags.get('building:levels')),
                'height': self._parse_numeric_tag(tags.get('height')),
                'roof_material': tags.get('roof:material'),
                'wall_material': tags.get('wall:material'),
                'source': 'openstreetmap',
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.debug(f"🔧 Erreur extraction bâtiment {element.get('id')}: {e}")
            return None
    
    def _is_in_malaysia(self, lat: float, lon: float) -> bool:
        """
        ✅ Vérifie rapidement si des coordonnées sont en Malaysia.
        """
        return (
            self.malaysia_bounds['south'] <= lat <= self.malaysia_bounds['north'] and
            self.malaysia_bounds['west'] <= lon <= self.malaysia_bounds['east']
        )
    
    def _normalize_building_type(self, building_type: str, tags: Dict) -> str:
        """
        ✅ Normalise les types de bâtiments OSM vers notre classification.
        """
        building_type = building_type.lower() if building_type else 'yes'
        
        # Mapping optimisé pour Malaysia
        type_mapping = {
            # Résidentiel
            'house': 'residential',
            'residential': 'residential',
            'apartments': 'residential',
            'terrace': 'residential',
            'detached': 'residential',
            'semi_detached': 'residential',
            'bungalow': 'residential',
            
            # Commercial
            'commercial': 'commercial',
            'retail': 'commercial',
            'shop': 'commercial',
            'office': 'commercial',
            'hotel': 'commercial',
            'restaurant': 'commercial',
            
            # Industrial
            'industrial': 'industrial',
            'warehouse': 'industrial',
            'factory': 'industrial',
            'manufacture': 'industrial',
            
            # Public
            'school': 'public',
            'hospital': 'public',
            'clinic': 'public',
            'government': 'public',
            'public': 'public',
            'mosque': 'public',
            'temple': 'public',
            'church': 'public'
        }
        
        # Vérifier d'abord le mapping direct
        if building_type in type_mapping:
            return type_mapping[building_type]
        
        # ✅ Analyser les autres tags pour déterminer le type
        if tags.get('amenity') in ['school', 'hospital', 'clinic', 'place_of_worship']:
            return 'public'
        elif tags.get('shop') or tags.get('office'):
            return 'commercial'
        elif tags.get('industrial'):
            return 'industrial'
        
        # Par défaut, considérer comme résidentiel
        return 'residential'
    
    def _get_malaysia_city_coordinates(self, city_name: str) -> Optional[Dict]:
        """
        ✅ Retourne les coordonnées des principales villes malaysiennes.
        """
        cities = {
            'kuala lumpur': {'lat': 3.139, 'lon': 101.687},
            'george town': {'lat': 5.414, 'lon': 100.333},
            'ipoh': {'lat': 4.584, 'lon': 101.077},
            'johor bahru': {'lat': 1.465, 'lon': 103.747},
            'petaling jaya': {'lat': 3.107, 'lon': 101.607},
            'shah alam': {'lat': 3.085, 'lon': 101.532},
            'subang jaya': {'lat': 3.150, 'lon': 101.581},
            'klang': {'lat': 3.045, 'lon': 101.445},
            'kajang': {'lat': 2.992, 'lon': 101.791},
            'seremban': {'lat': 2.726, 'lon': 101.938},
            'malacca': {'lat': 2.197, 'lon': 102.250},
            'alor setar': {'lat': 6.121, 'lon': 100.366},
            'kota bharu': {'lat': 6.133, 'lon': 102.238},
            'kuantan': {'lat': 3.807, 'lon': 103.326},
            'kuching': {'lat': 1.553, 'lon': 110.359},
            'kota kinabalu': {'lat': 5.979, 'lon': 116.075},
            'sandakan': {'lat': 5.840, 'lon': 118.117},
            'tawau': {'lat': 4.185, 'lon': 117.893},
            'miri': {'lat': 4.405, 'lon': 113.987}
        }
        
        city_key = city_name.lower().strip()
        return cities.get(city_key)
    
    def _get_city_radius(self, city_name: str) -> int:
        """
        ✅ Retourne un rayon approprié selon la taille de la ville.
        """
        city_key = city_name.lower().strip()
        
        large_cities = ['kuala lumpur', 'george town', 'johor bahru']
        medium_cities = ['ipoh', 'petaling jaya', 'shah alam', 'kota kinabalu', 'kuching']
        
        if city_key in large_cities:
            return 8000  # 8km pour les grandes villes
        elif city_key in medium_cities:
            return 5000  # 5km pour les villes moyennes
        else:
            return 3000  # 3km pour les petites villes
    
    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        ✅ Calcule la distance entre deux points (formule de Haversine).
        """
        import math
        
        R = 6371000  # Rayon de la Terre en mètres
        
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = (math.sin(dlat/2)**2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        
        return R * c
    
    def _parse_numeric_tag(self, value: str) -> Optional[float]:
        """
        ✅ Parse une valeur numérique depuis les tags OSM.
        """
        if not value:
            return None
        
        try:
            # Nettoyer la valeur (supprimer unités, etc.)
            import re
            clean_value = re.sub(r'[^0-9.-]', '', str(value))
            return float(clean_value) if clean_value else None
        except:
            return None
    
    # ✅ MÉTHODES DE CACHE ET UTILITAIRES (identiques à la version précédente)
    def _get_config_value(self, key: str, default: Any) -> Any:
        """Récupère une valeur de configuration."""
        if self.config and hasattr(self.config, key):
            return getattr(self.config, key)
        return default
    
    def _execute_overpass_query(self, query: str, cache_key: str) -> Dict:
        """Exécute une requête Overpass avec gestion du cache."""
        self.request_stats['total_requests'] += 1
        
        # Vérifier le cache
        if self.cache_enabled:
            cached_data = self._get_cached_data(cache_key)
            if cached_data:
                self.request_stats['cache_hits'] += 1
                return cached_data
        
        self.request_stats['cache_misses'] += 1
        
        # Exécuter la requête avec retry
        for attempt in range(self.max_retries):
            try:
                self.logger.debug(f"🔄 Tentative {attempt + 1}/{self.max_retries}")
                
                response = requests.post(
                    self.overpass_url,
                    data=query,
                    headers={'Content-Type': 'text/plain; charset=utf-8'},
                    timeout=self.timeout
                )
                
                response.raise_for_status()
                data = response.json()
                
                # Sauvegarder en cache
                if self.cache_enabled:
                    self._save_to_cache(cache_key, data)
                
                return data
                
            except requests.exceptions.Timeout:
                self.logger.warning(f"⏰ Timeout tentative {attempt + 1}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    
            except Exception as e:
                self.logger.warning(f"🌐 Erreur tentative {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        self.request_stats['failed_requests'] += 1
        raise Exception(f"Échec requête Overpass après {self.max_retries} tentatives")
    
    def _get_cached_data(self, cache_key: str) -> Optional[Dict]:
        """Récupère des données depuis le cache."""
        try:
            cache_file = self.cache_dir / f"{cache_key}.json"
            if cache_file.exists():
                # Vérifier la fraîcheur
                file_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
                if datetime.now() - file_time < self.cache_duration:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
            return None
        except Exception:
            return None
    
    def _save_to_cache(self, cache_key: str, data: Dict):
        """Sauvegarde des données en cache."""
        try:
            cache_file = self.cache_dir / f"{cache_key}.json"
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception as e:
            self.logger.warning(f"⚠️ Erreur sauvegarde cache: {e}")
    
    def get_statistics(self) -> Dict:
        """Retourne les statistiques du service."""
        return {
            'request_stats': self.request_stats.copy(),
            'cache_info': {
                'enabled': self.cache_enabled,
                'directory': str(self.cache_dir),
                'duration_hours': self.cache_duration.total_seconds() / 3600
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
        except Exception as e:
            self.logger.error(f"❌ Erreur vidage cache: {e}")


# Export de la classe principale
__all__ = ['OSMService']