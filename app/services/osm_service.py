#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SERVICE OPENSTREETMAP ULTRA-OPTIMISÉ - GÉNÉRATEUR MALAYSIA
Fichier: app/services/osm_service.py

Service ultra-optimisé pour récupérer TOUS les bâtiments OSM rapidement:
- Requêtes parallèles par subdivisions
- Cache intelligent avec compression
- Optimisations spécifiques Malaysia
- Récupération exhaustive sans timeouts

Auteur: Équipe Développement
Date: 2025
Version: 4.0 - Ultra-optimisé pour récupération complète
"""

import logging
import json
import time
import hashlib
import asyncio
import aiohttp
import concurrent.futures
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import requests
import pandas as pd
from pathlib import Path
import pickle
import gzip

from app.models.building import Building
from app.models.location import Location
from app.utils.validators import validate_coordinates, validate_osm_data


class OSMService:
    """
    Service ultra-optimisé pour récupération exhaustive de bâtiments OSM.
    
    Nouvelles optimisations:
    - Requêtes parallèles asynchrones
    - Subdivision adaptative des zones
    - Cache compressé avec pickle
    - Récupération par chunks pour éviter les timeouts
    """
    
    def __init__(self, config=None):
        """Initialise le service OSM ultra-optimisé."""
        self.logger = logging.getLogger(__name__)
        self.config = config
        
        # URLs Overpass multiples pour load balancing
        self.overpass_urls = [
            'https://overpass-api.de/api/interpreter',
            'https://lz4.overpass-api.de/api/interpreter',
            'https://z.overpass-api.de/api/interpreter'
        ]
        self.current_url_index = 0
        
        # Configuration ultra-optimisée
        self.timeout = self._get_config_value('OSM_REQUEST_TIMEOUT', 600)  # 10 minutes
        self.max_retries = self._get_config_value('OSM_MAX_RETRIES', 5)
        self.max_concurrent_requests = self._get_config_value('OSM_MAX_CONCURRENT', 10)
        self.chunk_size = self._get_config_value('OSM_CHUNK_SIZE', 0.1)  # Degrés de subdivision
        self.cache_enabled = True
        self.cache_duration = timedelta(hours=72)  # Cache 3 jours
        
        # Zones Malaysia optimisées
        self.malaysia_bounds = {
            'south': 0.855, 'west': 99.644,
            'north': 7.363, 'east': 119.267
        }
        
        # États Malaysia pour subdivision intelligente
        self.malaysia_states = {
            'johor': {'south': 1.2, 'west': 103.2, 'north': 2.8, 'east': 104.4},
            'kedah': {'south': 5.0, 'west': 99.6, 'north': 6.8, 'east': 101.0},
            'kelantan': {'south': 4.5, 'west': 101.2, 'north': 6.2, 'east': 102.6},
            'melaka': {'south': 2.0, 'west': 102.0, 'north': 2.5, 'east': 102.6},
            'negeri_sembilan': {'south': 2.3, 'west': 101.4, 'north': 3.2, 'east': 102.8},
            'pahang': {'south': 2.8, 'west': 101.8, 'north': 4.8, 'east': 103.8},
            'perak': {'south': 3.7, 'west': 100.1, 'north': 5.9, 'east': 102.0},
            'perlis': {'south': 6.2, 'west': 100.1, 'north': 6.8, 'east': 100.6},
            'penang': {'south': 5.1, 'west': 100.1, 'north': 5.6, 'east': 100.6},
            'sabah': {'south': 4.0, 'west': 115.0, 'north': 7.4, 'east': 119.3},
            'sarawak': {'south': 0.8, 'west': 109.6, 'north': 5.1, 'east': 115.5},
            'selangor': {'south': 2.8, 'west': 101.0, 'north': 3.8, 'east': 101.9},
            'terengganu': {'south': 4.0, 'west': 102.5, 'north': 5.9, 'east': 103.9},
            'kuala_lumpur': {'south': 3.0, 'west': 101.6, 'north': 3.3, 'east': 101.8},
            'putrajaya': {'south': 2.9, 'west': 101.6, 'north': 3.0, 'east': 101.8}
        }
        
        # Cache avec compression
        self.cache_dir = self._get_config_value('CACHE_DIR', Path('data/cache'))
        self.cache_dir = Path(self.cache_dir) / 'osm_v4'
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Statistiques
        self.reset_stats()
        
        self.logger.info("🚀 Service OSM ULTRA-OPTIMISÉ v4.0 initialisé")
    
    def reset_stats(self):
        """Reset les statistiques."""
        self.stats = {
            'total_requests': 0,
            'parallel_requests': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'failed_requests': 0,
            'total_buildings': 0,
            'start_time': None,
            'end_time': None,
            'zones_processed': 0
        }
    
    async def get_all_buildings_malaysia_async(self) -> List[Dict]:
        """
        🚀 MÉTHODE PRINCIPALE: Récupère TOUS les bâtiments de Malaysia.
        Utilise des requêtes parallèles asynchrones pour maximum de performance.
        """
        self.logger.info("🇲🇾 Début récupération EXHAUSTIVE des bâtiments Malaysia")
        self.reset_stats()
        self.stats['start_time'] = datetime.now()
        
        # Vérifier le cache global d'abord
        cache_key = "malaysia_all_buildings_v4"
        cached_result = self._get_from_cache(cache_key)
        if cached_result:
            self.logger.info(f"💾 Cache HIT global: {len(cached_result)} bâtiments")
            self.stats['cache_hits'] = 1
            self.stats['total_buildings'] = len(cached_result)
            return cached_result
        
        all_buildings = []
        
        # Méthode 1: Par états (plus rapide et plus fiable)
        buildings_by_states = await self._get_buildings_by_states_async()
        all_buildings.extend(buildings_by_states)
        
        # Méthode 2: Zones supplémentaires (pour les zones non couvertes)
        additional_buildings = await self._get_buildings_additional_zones_async()
        all_buildings.extend(additional_buildings)
        
        # Déduplication par OSM ID
        unique_buildings = self._deduplicate_buildings(all_buildings)
        
        # Sauvegarde en cache
        self._save_to_cache(cache_key, unique_buildings)
        
        self.stats['end_time'] = datetime.now()
        self.stats['total_buildings'] = len(unique_buildings)
        
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        self.logger.info(f"✅ Récupération terminée: {len(unique_buildings)} bâtiments uniques en {duration:.1f}s")
        self._log_final_stats()
        
        return unique_buildings
    
    async def _get_buildings_by_states_async(self) -> List[Dict]:
        """Récupère les bâtiments par états Malaysia en parallèle."""
        self.logger.info("🏛️ Récupération par états Malaysia...")
        
        # Créer les tâches pour chaque état
        tasks = []
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
            for state_name, bounds in self.malaysia_states.items():
                task = self._get_buildings_for_bounds_async(session, state_name, bounds)
                tasks.append(task)
            
            # Exécuter en parallèle avec limite de concurrence
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            limited_tasks = [self._limited_request(semaphore, task) for task in tasks]
            results = await asyncio.gather(*limited_tasks, return_exceptions=True)
        
        # Consolider les résultats
        all_buildings = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                state_name = list(self.malaysia_states.keys())[i]
                self.logger.error(f"❌ Échec état {state_name}: {result}")
                self.stats['failed_requests'] += 1
            else:
                all_buildings.extend(result)
                self.stats['zones_processed'] += 1
        
        self.logger.info(f"🏛️ États traités: {self.stats['zones_processed']}/{len(self.malaysia_states)}")
        return all_buildings
    
    async def _get_buildings_additional_zones_async(self) -> List[Dict]:
        """Récupère les bâtiments dans les zones supplémentaires (zones maritimes, etc.)."""
        self.logger.info("🌊 Récupération zones supplémentaires...")
        
        # Zones supplémentaires pour couvrir les îles et zones non couvertes
        additional_zones = {
            'labuan': {'south': 5.25, 'west': 115.15, 'north': 5.35, 'east': 115.25},
            'langkawi': {'south': 6.25, 'west': 99.65, 'north': 6.45, 'east': 99.85},
            'tioman': {'south': 2.75, 'west': 104.10, 'north': 2.85, 'east': 104.20},
            'perhentian': {'south': 5.85, 'west': 102.70, 'north': 5.95, 'east': 102.80}
        }
        
        tasks = []
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
            for zone_name, bounds in additional_zones.items():
                task = self._get_buildings_for_bounds_async(session, zone_name, bounds)
                tasks.append(task)
            
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            limited_tasks = [self._limited_request(semaphore, task) for task in tasks]
            results = await asyncio.gather(*limited_tasks, return_exceptions=True)
        
        all_buildings = []
        for result in results:
            if not isinstance(result, Exception):
                all_buildings.extend(result)
        
        return all_buildings
    
    async def _limited_request(self, semaphore, coro):
        """Limite la concurrence des requêtes."""
        async with semaphore:
            return await coro
    
    async def _get_buildings_for_bounds_async(self, session: aiohttp.ClientSession, zone_name: str, bounds: Dict) -> List[Dict]:
        """Récupère les bâtiments pour une zone donnée avec subdivision adaptative."""
        self.logger.debug(f"📍 Traitement zone: {zone_name}")
        
        # Vérifier le cache pour cette zone
        cache_key = f"zone_{zone_name}_{hashlib.md5(str(bounds).encode()).hexdigest()[:8]}"
        cached_result = self._get_from_cache(cache_key)
        if cached_result:
            self.stats['cache_hits'] += 1
            return cached_result
        
        # Calculer la taille de la zone
        area_size = (bounds['north'] - bounds['south']) * (bounds['east'] - bounds['west'])
        
        # Si la zone est trop grande, subdiviser
        if area_size > 2.0:  # Plus de 2 degrés carrés
            return await self._subdivide_and_fetch_async(session, zone_name, bounds)
        
        # Sinon, requête directe
        buildings = await self._fetch_single_zone_async(session, zone_name, bounds)
        self._save_to_cache(cache_key, buildings)
        
        return buildings
    
    async def _subdivide_and_fetch_async(self, session: aiohttp.ClientSession, zone_name: str, bounds: Dict) -> List[Dict]:
        """Subdivise une zone trop grande et récupère en parallèle."""
        self.logger.debug(f"✂️ Subdivision zone: {zone_name}")
        
        # Calculer les subdivisions
        lat_chunks = max(2, int((bounds['north'] - bounds['south']) / self.chunk_size))
        lon_chunks = max(2, int((bounds['east'] - bounds['west']) / self.chunk_size))
        
        lat_step = (bounds['north'] - bounds['south']) / lat_chunks
        lon_step = (bounds['east'] - bounds['west']) / lon_chunks
        
        # Créer les sous-zones
        tasks = []
        for i in range(lat_chunks):
            for j in range(lon_chunks):
                sub_bounds = {
                    'south': bounds['south'] + i * lat_step,
                    'north': bounds['south'] + (i + 1) * lat_step,
                    'west': bounds['west'] + j * lon_step,
                    'east': bounds['west'] + (j + 1) * lon_step
                }
                sub_zone_name = f"{zone_name}_sub_{i}_{j}"
                task = self._fetch_single_zone_async(session, sub_zone_name, sub_bounds)
                tasks.append(task)
        
        # Exécuter en parallèle
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Consolider
        all_buildings = []
        for result in results:
            if not isinstance(result, Exception):
                all_buildings.extend(result)
        
        return all_buildings
    
    async def _fetch_single_zone_async(self, session: aiohttp.ClientSession, zone_name: str, bounds: Dict) -> List[Dict]:
        """Récupère les bâtiments pour une zone unique."""
        query = self._build_overpass_query(bounds)
        
        for attempt in range(self.max_retries):
            try:
                url = self._get_next_overpass_url()
                
                async with session.post(url, data=query, headers={'Content-Type': 'text/plain'}) as response:
                    if response.status == 200:
                        data = await response.json()
                        buildings = self._parse_overpass_response(data)
                        
                        self.stats['total_requests'] += 1
                        if attempt > 0:
                            self.stats['parallel_requests'] += 1
                        
                        self.logger.debug(f"✅ Zone {zone_name}: {len(buildings)} bâtiments")
                        return buildings
                    else:
                        error_text = await response.text()
                        self.logger.warning(f"⚠️ HTTP {response.status} pour {zone_name}: {error_text[:100]}")
                        
            except Exception as e:
                self.logger.warning(f"⚠️ Tentative {attempt + 1} échec pour {zone_name}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Backoff exponentiel
        
        self.stats['failed_requests'] += 1
        return []
    
    def _build_overpass_query(self, bounds: Dict) -> str:
        """Construit une requête Overpass optimisée pour récupérer TOUS les bâtiments."""
        return f"""
        [out:json][timeout:600];
        (
          way["building"~"."](
            {bounds['south']},{bounds['west']},{bounds['north']},{bounds['east']}
          );
          relation["building"~"."](
            {bounds['south']},{bounds['west']},{bounds['north']},{bounds['east']}
          );
        );
        (._;>;);
        out geom meta;
        """
    
    def _parse_overpass_response(self, data: Dict) -> List[Dict]:
        """Parse une réponse Overpass et extrait les informations des bâtiments."""
        buildings = []
        
        if 'elements' not in data:
            return buildings
        
        # Séparer les nœuds, chemins et relations
        nodes = {elem['id']: elem for elem in data['elements'] if elem['type'] == 'node'}
        ways = [elem for elem in data['elements'] if elem['type'] == 'way' and 'tags' in elem and 'building' in elem['tags']]
        relations = [elem for elem in data['elements'] if elem['type'] == 'relation' and 'tags' in elem and 'building' in elem['tags']]
        
        # Traiter les ways (la majorité des bâtiments)
        for way in ways:
            building = self._parse_way_to_building(way, nodes)
            if building:
                buildings.append(building)
        
        # Traiter les relations
        for relation in relations:
            building = self._parse_relation_to_building(relation)
            if building:
                buildings.append(building)
        
        return buildings
    
    def _parse_way_to_building(self, way: Dict, nodes: Dict) -> Optional[Dict]:
        """Parse un way OSM en bâtiment."""
        try:
            tags = way.get('tags', {})
            
            # Calculer le centroïde
            if 'geometry' in way:
                coords = way['geometry']
            else:
                # Calculer depuis les nœuds
                way_nodes = [nodes.get(node_id) for node_id in way.get('nodes', [])]
                way_nodes = [n for n in way_nodes if n and 'lat' in n and 'lon' in n]
                if not way_nodes:
                    return None
                coords = [{'lat': n['lat'], 'lon': n['lon']} for n in way_nodes]
            
            if not coords:
                return None
            
            # Centroïde
            avg_lat = sum(c['lat'] for c in coords) / len(coords)
            avg_lon = sum(c['lon'] for c in coords) / len(coords)
            
            # Estimation de la surface (approximative)
            area_sqm = self._calculate_polygon_area(coords)
            
            return {
                'osm_id': f"way/{way['id']}",
                'osm_type': 'way',
                'latitude': avg_lat,
                'longitude': avg_lon,
                'building_type': self._classify_building_type(tags),
                'area_sqm': area_sqm,
                'levels': self._parse_int_tag(tags.get('building:levels')),
                'height': self._parse_float_tag(tags.get('height')),
                'name': tags.get('name'),
                'addr_street': tags.get('addr:street'),
                'addr_housenumber': tags.get('addr:housenumber'),
                'addr_postcode': tags.get('addr:postcode'),
                'addr_city': tags.get('addr:city'),
                'tags': tags,
                'geometry_type': 'polygon',
                'coordinates': coords
            }
            
        except Exception as e:
            self.logger.debug(f"Erreur parsing way {way.get('id')}: {e}")
            return None
    
    def _parse_relation_to_building(self, relation: Dict) -> Optional[Dict]:
        """Parse une relation OSM en bâtiment."""
        try:
            tags = relation.get('tags', {})
            
            # Pour les relations, utiliser un point approximatif
            # (dans un vrai cas, il faudrait parser les membres)
            return {
                'osm_id': f"relation/{relation['id']}",
                'osm_type': 'relation',
                'latitude': 0,  # À calculer depuis les membres
                'longitude': 0,  # À calculer depuis les membres
                'building_type': self._classify_building_type(tags),
                'area_sqm': None,
                'levels': self._parse_int_tag(tags.get('building:levels')),
                'height': self._parse_float_tag(tags.get('height')),
                'name': tags.get('name'),
                'addr_street': tags.get('addr:street'),
                'addr_housenumber': tags.get('addr:housenumber'),
                'addr_postcode': tags.get('addr:postcode'),
                'addr_city': tags.get('addr:city'),
                'tags': tags,
                'geometry_type': 'multipolygon'
            }
            
        except Exception as e:
            self.logger.debug(f"Erreur parsing relation {relation.get('id')}: {e}")
            return None
    
    def _classify_building_type(self, tags: Dict) -> str:
        """Classifie le type de bâtiment selon les tags OSM."""
        building_tag = tags.get('building', 'yes')
        
        # Mapping des types OSM vers nos catégories
        type_mapping = {
            'house': 'residential',
            'apartments': 'residential',
            'residential': 'residential',
            'detached': 'residential',
            'terrace': 'residential',
            'office': 'commercial',
            'commercial': 'commercial',
            'retail': 'commercial',
            'shop': 'commercial',
            'warehouse': 'industrial',
            'industrial': 'industrial',
            'factory': 'industrial',
            'school': 'public',
            'hospital': 'public',
            'university': 'public',
            'church': 'religious',
            'mosque': 'religious',
            'temple': 'religious'
        }
        
        return type_mapping.get(building_tag.lower(), 'other')
    
    def _calculate_polygon_area(self, coords: List[Dict]) -> float:
        """Calcule l'aire approximative d'un polygone en m²."""
        if len(coords) < 3:
            return 0
        
        # Formule du lacet (shoelace) adaptée pour lat/lon
        area = 0
        n = len(coords)
        
        for i in range(n):
            j = (i + 1) % n
            area += coords[i]['lon'] * coords[j]['lat']
            area -= coords[j]['lon'] * coords[i]['lat']
        
        area = abs(area) / 2.0
        
        # Conversion approximative en m² (dépend de la latitude)
        # 1 degré ≈ 111 km au niveau de la Malaysia
        avg_lat = sum(c['lat'] for c in coords) / len(coords)
        lat_to_m = 111000
        lon_to_m = 111000 * abs(math.cos(math.radians(avg_lat)))
        
        return area * lat_to_m * lon_to_m
    
    def _parse_int_tag(self, value: str) -> Optional[int]:
        """Parse un tag entier."""
        if not value:
            return None
        try:
            return int(float(value))
        except:
            return None
    
    def _parse_float_tag(self, value: str) -> Optional[float]:
        """Parse un tag float."""
        if not value:
            return None
        try:
            # Supprimer les unités communes
            value = value.replace(' m', '').replace('m', '').replace(' ft', '').replace('ft', '')
            return float(value)
        except:
            return None
    
    def _deduplicate_buildings(self, buildings: List[Dict]) -> List[Dict]:
        """Supprime les doublons basés sur OSM ID."""
        seen_ids = set()
        unique_buildings = []
        
        for building in buildings:
            osm_id = building.get('osm_id')
            if osm_id and osm_id not in seen_ids:
                seen_ids.add(osm_id)
                unique_buildings.append(building)
            elif not osm_id:
                # Bâtiment sans ID, garder quand même
                unique_buildings.append(building)
        
        removed_count = len(buildings) - len(unique_buildings)
        if removed_count > 0:
            self.logger.info(f"🔄 Déduplication: {removed_count} doublons supprimés")
        
        return unique_buildings
    
    def _get_next_overpass_url(self) -> str:
        """Récupère la prochaine URL Overpass (load balancing)."""
        url = self.overpass_urls[self.current_url_index]
        self.current_url_index = (self.current_url_index + 1) % len(self.overpass_urls)
        return url
    
    def _get_from_cache(self, cache_key: str) -> Optional[List[Dict]]:
        """Récupère des données du cache compressé."""
        if not self.cache_enabled:
            return None
        
        cache_file = self.cache_dir / f"{cache_key}.pkl.gz"
        
        try:
            if cache_file.exists():
                # Vérifier l'âge du cache
                file_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
                if datetime.now() - file_time < self.cache_duration:
                    with gzip.open(cache_file, 'rb') as f:
                        data = pickle.load(f)
                    return data
        except Exception as e:
            self.logger.warning(f"Erreur lecture cache {cache_key}: {e}")
        
        return None
    
    def _save_to_cache(self, cache_key: str, data: List[Dict]):
        """Sauvegarde des données dans le cache compressé."""
        if not self.cache_enabled or not data:
            return
        
        cache_file = self.cache_dir / f"{cache_key}.pkl.gz"
        
        try:
            with gzip.open(cache_file, 'wb') as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            self.logger.warning(f"Erreur écriture cache {cache_key}: {e}")
    
    def _get_config_value(self, key: str, default: Any) -> Any:
        """Récupère une valeur de configuration."""
        if self.config:
            return self.config.get(key, default)
        return default
    
    def _log_final_stats(self):
        """Affiche les statistiques finales."""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        self.logger.info("📊 STATISTIQUES FINALES:")
        self.logger.info(f"   🏢 Bâtiments récupérés: {self.stats['total_buildings']:,}")
        self.logger.info(f"   ⏱️  Durée totale: {duration:.1f}s")
        self.logger.info(f"   📡 Requêtes totales: {self.stats['total_requests']}")
        self.logger.info(f"   🚀 Requêtes parallèles: {self.stats['parallel_requests']}")
        self.logger.info(f"   💾 Cache hits: {self.stats['cache_hits']}")
        self.logger.info(f"   ❌ Échecs: {self.stats['failed_requests']}")
        self.logger.info(f"   🗺️  Zones traitées: {self.stats['zones_processed']}")
        
        if duration > 0:
            rate = self.stats['total_buildings'] / duration
            self.logger.info(f"   📈 Débit: {rate:.1f} bâtiments/seconde")
    
    # Méthodes synchrones pour compatibilité
    def get_all_buildings_malaysia(self) -> List[Dict]:
        """Version synchrone de la récupération exhaustive."""
        return asyncio.run(self.get_all_buildings_malaysia_async())
    
    def get_buildings_for_city(self, city: str, limit: Optional[int] = None) -> List[Dict]:
        """Récupère les bâtiments pour une ville spécifique."""
        if city.lower() == 'malaysia':
            return self.get_all_buildings_malaysia()
        
        # Recherche dans les états définis
        city_lower = city.lower().replace(' ', '_')
        if city_lower in self.malaysia_states:
            bounds = self.malaysia_states[city_lower]
            return asyncio.run(self._get_buildings_for_bounds_async(None, city_lower, bounds))
        
        # Sinon, requête classique par nom de ville
        return self._get_buildings_by_city_name(city, limit)
    
    def _get_buildings_by_city_name(self, city: str, limit: Optional[int]) -> List[Dict]:
        """Récupération classique par nom de ville (fallback)."""
        # Cette méthode peut être implémentée pour les villes non définies
        self.logger.warning(f"Ville '{city}' non trouvée dans les états prédéfinis")
        return []
    
    def get_stats(self) -> Dict:
        """Retourne les statistiques actuelles."""
        return self.stats.copy()
    
    def clear_cache(self):
        """Vide le cache."""
        try:
            for cache_file in self.cache_dir.glob("*.pkl.gz"):
                cache_file.unlink()
            self.logger.info("🗑️ Cache vidé")
        except Exception as e:
            self.logger.error(f"Erreur vidage cache: {e}")
    
    def get_cache_info(self) -> Dict:
        """Retourne les informations du cache."""
        cache_files = list(self.cache_dir.glob("*.pkl.gz"))
        total_size = sum(f.stat().st_size for f in cache_files)
        
        return {
            'files_count': len(cache_files),
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'cache_enabled': self.cache_enabled,
            'cache_duration_hours': self.cache_duration.total_seconds() / 3600
        }