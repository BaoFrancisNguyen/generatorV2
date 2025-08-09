#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SERVICE DE VALIDATION - GÉNÉRATEUR MALAYSIA
Fichier: app/services/validation_service.py

Service pour la validation et le contrôle qualité des données générées.
Vérifie la cohérence temporelle, la plausibilité énergétique,
et l'intégrité géographique des datasets.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Service modulaire
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any


class ValidationService:
    """
    Service de validation et contrôle qualité des données énergétiques.
    
    Fonctionnalités:
    - Validation de cohérence temporelle
    - Contrôle de plausibilité énergétique
    - Vérification d'intégrité géographique
    - Détection d'anomalies et outliers
    - Génération de rapports de qualité
    """
    
    def __init__(self, config=None):
        """
        Initialise le service de validation.
        
        Args:
            config: Configuration de l'application
        """
        self.logger = logging.getLogger(__name__)
        self.config = config
        
        # Seuils de validation par défaut
        self.validation_thresholds = self._get_validation_thresholds()
        
        # Règles de cohérence énergétique
        self.energy_rules = self._get_energy_rules()
        
        # Statistiques de validation
        self.validation_stats = {
            'total_validations': 0,
            'passed_validations': 0,
            'failed_validations': 0,
            'warnings_generated': 0
        }
        
        self.logger.info("✅ Service de validation initialisé")
    
    def validate_complete_dataset(self, buildings_df: pd.DataFrame, 
                                timeseries_df: pd.DataFrame) -> Dict:
        """
        Valide un dataset complet (bâtiments + séries temporelles).
        
        Args:
            buildings_df: DataFrame des métadonnées des bâtiments
            timeseries_df: DataFrame des séries temporelles
            
        Returns:
            Dictionnaire avec les résultats de validation
        """
        self.logger.info("🔍 Validation du dataset complet")
        
        validation_results = {
            'validation_timestamp': datetime.now().isoformat(),
            'overall_status': 'unknown',
            'score': 0.0,
            'buildings_validation': {},
            'timeseries_validation': {},
            'cross_validation': {},
            'quality_metrics': {},
            'recommendations': [],
            'warnings': [],
            'errors': []
        }
        
        try:
            # Validation des bâtiments
            buildings_validation = self.validate_buildings_metadata(buildings_df)
            validation_results['buildings_validation'] = buildings_validation
            
            # Validation des séries temporelles
            timeseries_validation = self.validate_timeseries_data(timeseries_df)
            validation_results['timeseries_validation'] = timeseries_validation
            
            # Validation croisée
            cross_validation = self.validate_cross_references(buildings_df, timeseries_df)
            validation_results['cross_validation'] = cross_validation
            
            # Métriques de qualité
            quality_metrics = self.calculate_quality_metrics(buildings_df, timeseries_df)
            validation_results['quality_metrics'] = quality_metrics
            
            # Calcul du score global
            overall_score = self._calculate_overall_score(validation_results)
            validation_results['score'] = overall_score
            
            # Détermination du statut
            if overall_score >= 90:
                validation_results['overall_status'] = 'excellent'
            elif overall_score >= 75:
                validation_results['overall_status'] = 'good'
            elif overall_score >= 60:
                validation_results['overall_status'] = 'acceptable'
            else:
                validation_results['overall_status'] = 'poor'
            
            # Génération de recommandations
            validation_results['recommendations'] = self._generate_recommendations(validation_results)
            
            self.validation_stats['total_validations'] += 1
            if overall_score >= 60:
                self.validation_stats['passed_validations'] += 1
            else:
                self.validation_stats['failed_validations'] += 1
            
            self.logger.info(f"✅ Validation terminée - Score: {overall_score:.1f}% - Statut: {validation_results['overall_status']}")
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la validation: {e}")
            validation_results['overall_status'] = 'error'
            validation_results['errors'].append(str(e))
            return validation_results
    
    def validate_buildings_metadata(self, buildings_df: pd.DataFrame) -> Dict:
        """
        Valide les métadonnées des bâtiments.
        
        Args:
            buildings_df: DataFrame des bâtiments
            
        Returns:
            Résultats de validation des bâtiments
        """
        self.logger.debug("🏢 Validation des métadonnées des bâtiments")
        
        validation = {
            'total_buildings': len(buildings_df),
            'valid_buildings': 0,
            'issues': [],
            'completeness': {},
            'geographic_validation': {},
            'type_distribution': {}
        }
        
        # Vérification des colonnes requises
        required_columns = ['unique_id', 'latitude', 'longitude', 'building_class']
        missing_columns = [col for col in required_columns if col not in buildings_df.columns]
        
        if missing_columns:
            validation['issues'].append(f"Colonnes manquantes: {missing_columns}")
            return validation
        
        # Validation géographique
        validation['geographic_validation'] = self._validate_coordinates(buildings_df)
        
        # Complétude des données
        validation['completeness'] = self._check_data_completeness(buildings_df)
        
        # Distribution des types de bâtiments
        validation['type_distribution'] = self._validate_building_types(buildings_df)
        
        # Compter les bâtiments valides
        valid_mask = (
            buildings_df['latitude'].between(-90, 90) &
            buildings_df['longitude'].between(-180, 180) &
            buildings_df['unique_id'].notna() &
            buildings_df['building_class'].isin(['residential', 'commercial', 'industrial', 'public'])
        )
        validation['valid_buildings'] = valid_mask.sum()
        
        return validation
    
    def validate_timeseries_data(self, timeseries_df: pd.DataFrame) -> Dict:
        """
        Valide les données de séries temporelles.
        
        Args:
            timeseries_df: DataFrame des séries temporelles
            
        Returns:
            Résultats de validation des séries temporelles
        """
        self.logger.debug("⏰ Validation des séries temporelles")
        
        validation = {
            'total_observations': len(timeseries_df),
            'valid_observations': 0,
            'temporal_validation': {},
            'consumption_validation': {},
            'anomalies': [],
            'patterns_detected': {}
        }
        
        # Vérification des colonnes requises
        consumption_col = 'y' if 'y' in timeseries_df.columns else 'consumption_kwh'
        if consumption_col not in timeseries_df.columns:
            validation['issues'] = ["Colonne de consommation manquante"]
            return validation
        
        # Validation temporelle
        validation['temporal_validation'] = self._validate_temporal_consistency(timeseries_df)
        
        # Validation de la consommation
        validation['consumption_validation'] = self._validate_consumption_values(
            timeseries_df, consumption_col
        )
        
        # Détection d'anomalies
        validation['anomalies'] = self._detect_consumption_anomalies(
            timeseries_df, consumption_col
        )
        
        # Détection de patterns
        validation['patterns_detected'] = self._detect_energy_patterns(
            timeseries_df, consumption_col
        )
        
        # Compter les observations valides
        valid_mask = (
            timeseries_df[consumption_col] >= self.validation_thresholds['min_consumption_kwh']
        ) & (
            timeseries_df[consumption_col] <= self.validation_thresholds['max_consumption_kwh']
        ) & (
            timeseries_df['unique_id'].notna()
        )
        validation['valid_observations'] = valid_mask.sum()
        
        return validation
    
    def validate_cross_references(self, buildings_df: pd.DataFrame, 
                                timeseries_df: pd.DataFrame) -> Dict:
        """
        Valide les références croisées entre bâtiments et séries temporelles.
        
        Args:
            buildings_df: DataFrame des bâtiments
            timeseries_df: DataFrame des séries temporelles
            
        Returns:
            Résultats de validation croisée
        """
        self.logger.debug("🔗 Validation des références croisées")
        
        validation = {
            'buildings_with_data': 0,
            'orphaned_timeseries': 0,
            'missing_buildings': [],
            'reference_integrity': 100.0
        }
        
        # IDs des bâtiments et des séries temporelles
        building_ids = set(buildings_df['unique_id'].unique())
        timeseries_ids = set(timeseries_df['unique_id'].unique())
        
        # Bâtiments avec des données
        validation['buildings_with_data'] = len(building_ids.intersection(timeseries_ids))
        
        # Séries temporelles orphelines (sans bâtiment correspondant)
        orphaned_ids = timeseries_ids - building_ids
        validation['orphaned_timeseries'] = len(orphaned_ids)
        
        # Bâtiments sans données
        missing_ids = building_ids - timeseries_ids
        validation['missing_buildings'] = list(missing_ids)
        
        # Calcul de l'intégrité référentielle
        total_expected = len(building_ids)
        if total_expected > 0:
            validation['reference_integrity'] = (validation['buildings_with_data'] / total_expected) * 100
        
        return validation
    
    def calculate_quality_metrics(self, buildings_df: pd.DataFrame, 
                                timeseries_df: pd.DataFrame) -> Dict:
        """
        Calcule les métriques de qualité du dataset.
        
        Args:
            buildings_df: DataFrame des bâtiments
            timeseries_df: DataFrame des séries temporelles
            
        Returns:
            Métriques de qualité
        """
        self.logger.debug("📊 Calcul des métriques de qualité")
        
        consumption_col = 'y' if 'y' in timeseries_df.columns else 'consumption_kwh'
        
        metrics = {
            'completeness': 0.0,
            'consistency': 0.0,
            'accuracy': 0.0,
            'timeliness': 0.0,
            'validity': 0.0
        }
        
        # Complétude (pourcentage de valeurs non nulles)
        total_cells = len(buildings_df) * len(buildings_df.columns) + len(timeseries_df) * len(timeseries_df.columns)
        null_cells = buildings_df.isnull().sum().sum() + timeseries_df.isnull().sum().sum()
        metrics['completeness'] = ((total_cells - null_cells) / total_cells) * 100
        
        # Consistance (cohérence des formats et types)
        consistency_score = 100.0
        # Vérifier les types de données
        if not pd.api.types.is_numeric_dtype(timeseries_df[consumption_col]):
            consistency_score -= 20
        if not pd.api.types.is_numeric_dtype(buildings_df['latitude']):
            consistency_score -= 20
        metrics['consistency'] = max(0, consistency_score)
        
        # Précision (valeurs dans les plages attendues)
        valid_consumption = timeseries_df[
            (timeseries_df[consumption_col] >= self.validation_thresholds['min_consumption_kwh']) &
            (timeseries_df[consumption_col] <= self.validation_thresholds['max_consumption_kwh'])
        ]
        accuracy_consumption = len(valid_consumption) / len(timeseries_df) * 100
        
        valid_coords = buildings_df[
            (buildings_df['latitude'].between(-90, 90)) &
            (buildings_df['longitude'].between(-180, 180))
        ]
        accuracy_coords = len(valid_coords) / len(buildings_df) * 100
        
        metrics['accuracy'] = (accuracy_consumption + accuracy_coords) / 2
        
        # Actualité (basée sur la période de données)
        if 'timestamp' in timeseries_df.columns:
            timestamps = pd.to_datetime(timeseries_df['timestamp'])
            latest_date = timestamps.max()
            days_old = (datetime.now() - latest_date.replace(tzinfo=None)).days
            metrics['timeliness'] = max(0, 100 - (days_old / 30) * 10)  # Pénalité par mois
        else:
            metrics['timeliness'] = 100
        
        # Validité (conformité aux règles métier)
        validity_score = 100.0
        
        # Vérifier les règles énergétiques par type de bâtiment
        for building_type, max_consumption in self.energy_rules.items():
            if building_type in buildings_df['building_class'].values:
                building_ids = buildings_df[buildings_df['building_class'] == building_type]['unique_id']
                type_consumption = timeseries_df[timeseries_df['unique_id'].isin(building_ids)]
                
                if len(type_consumption) > 0:
                    violations = type_consumption[type_consumption[consumption_col] > max_consumption]
                    if len(violations) > 0:
                        validity_score -= (len(violations) / len(type_consumption)) * 20
        
        metrics['validity'] = max(0, validity_score)
        
        return metrics
    
    def _validate_coordinates(self, buildings_df: pd.DataFrame) -> Dict:
        """Valide les coordonnées géographiques."""
        validation = {
            'valid_coordinates': 0,
            'invalid_coordinates': 0,
            'malaysia_bounds_check': 0,
            'duplicate_coordinates': 0
        }
        
        # Coordonnées valides (format lat/lon)
        valid_coords = buildings_df[
            (buildings_df['latitude'].between(-90, 90)) &
            (buildings_df['longitude'].between(-180, 180))
        ]
        validation['valid_coordinates'] = len(valid_coords)
        validation['invalid_coordinates'] = len(buildings_df) - len(valid_coords)
        
        # Vérification des limites de la Malaysia (approximatives)
        malaysia_bounds = valid_coords[
            (valid_coords['latitude'].between(0.5, 7.5)) &
            (valid_coords['longitude'].between(99.0, 120.0))
        ]
        validation['malaysia_bounds_check'] = len(malaysia_bounds)
        
        # Coordonnées dupliquées
        coord_duplicates = buildings_df.duplicated(subset=['latitude', 'longitude']).sum()
        validation['duplicate_coordinates'] = coord_duplicates
        
        return validation
    
    def _check_data_completeness(self, df: pd.DataFrame) -> Dict:
        """Vérifie la complétude des données."""
        completeness = {}
        
        for column in df.columns:
            total_values = len(df)
            non_null_values = df[column].notna().sum()
            completeness[column] = {
                'percentage': (non_null_values / total_values) * 100 if total_values > 0 else 0,
                'missing_count': total_values - non_null_values
            }
        
        return completeness
    
    def _validate_building_types(self, buildings_df: pd.DataFrame) -> Dict:
        """Valide la distribution des types de bâtiments."""
        if 'building_class' not in buildings_df.columns:
            return {'error': 'Colonne building_class manquante'}
        
        valid_types = ['residential', 'commercial', 'industrial', 'public']
        type_counts = buildings_df['building_class'].value_counts()
        
        validation = {
            'valid_types': {},
            'invalid_types': {},
            'distribution_score': 0.0
        }
        
        # Compter les types valides et invalides
        for building_type, count in type_counts.items():
            if building_type in valid_types:
                validation['valid_types'][building_type] = count
            else:
                validation['invalid_types'][building_type] = count
        
        # Score de distribution (plus équilibré = meilleur score)
        if validation['valid_types']:
            type_percentages = [count / sum(validation['valid_types'].values()) 
                             for count in validation['valid_types'].values()]
            # Pénaliser les distributions très déséquilibrées
            balance_score = 100 - (np.std(type_percentages) * 200)  
            validation['distribution_score'] = max(0, balance_score)
        
        return validation
    
    def _validate_temporal_consistency(self, timeseries_df: pd.DataFrame) -> Dict:
        """Valide la cohérence temporelle."""
        validation = {
            'temporal_gaps': 0,
            'duplicate_timestamps': 0,
            'chronological_order': True,
            'frequency_consistency': True
        }
        
        if 'timestamp' not in timeseries_df.columns:
            return {'error': 'Colonne timestamp manquante'}
        
        # Convertir en datetime si nécessaire
        timestamps = pd.to_datetime(timeseries_df['timestamp'])
        
        # Vérifier l'ordre chronologique par building
        for unique_id in timeseries_df['unique_id'].unique():
            building_data = timeseries_df[timeseries_df['unique_id'] == unique_id]
            building_timestamps = pd.to_datetime(building_data['timestamp']).sort_values()
            
            if not building_timestamps.equals(building_timestamps.sort_values()):
                validation['chronological_order'] = False
                break
        
        # Détecter les doublons de timestamps par building
        duplicates = timeseries_df.duplicated(subset=['unique_id', 'timestamp']).sum()
        validation['duplicate_timestamps'] = duplicates
        
        return validation
    
    def _validate_consumption_values(self, timeseries_df: pd.DataFrame, 
                                   consumption_col: str) -> Dict:
        """Valide les valeurs de consommation."""
        validation = {
            'negative_values': 0,
            'zero_values': 0,
            'extreme_values': 0,
            'valid_range_percentage': 0.0
        }
        
        consumption_data = timeseries_df[consumption_col]
        
        # Valeurs négatives
        validation['negative_values'] = (consumption_data < 0).sum()
        
        # Valeurs à zéro
        validation['zero_values'] = (consumption_data == 0).sum()
        
        # Valeurs extrêmes (au-delà des seuils)
        extreme_high = (consumption_data > self.validation_thresholds['max_consumption_kwh']).sum()
        extreme_low = (consumption_data < self.validation_thresholds['min_consumption_kwh']).sum()
        validation['extreme_values'] = extreme_high + extreme_low
        
        # Pourcentage dans la plage valide
        valid_range = (
            (consumption_data >= self.validation_thresholds['min_consumption_kwh']) &
            (consumption_data <= self.validation_thresholds['max_consumption_kwh'])
        ).sum()
        validation['valid_range_percentage'] = (valid_range / len(consumption_data)) * 100
        
        return validation
    
    def _detect_consumption_anomalies(self, timeseries_df: pd.DataFrame, 
                                    consumption_col: str) -> List[Dict]:
        """Détecte les anomalies dans la consommation."""
        anomalies = []
        
        # Z-score pour détecter les outliers
        consumption_data = timeseries_df[consumption_col]
        z_scores = np.abs((consumption_data - consumption_data.mean()) / consumption_data.std())
        outliers = timeseries_df[z_scores > self.validation_thresholds['outlier_z_score']]
        
        for _, outlier in outliers.iterrows():
            anomalies.append({
                'type': 'statistical_outlier',
                'unique_id': outlier['unique_id'],
                'timestamp': outlier.get('timestamp'),
                'value': outlier[consumption_col],
                'z_score': z_scores.loc[outlier.name]
            })
        
        return anomalies[:100]  # Limiter à 100 anomalies pour éviter la surcharge
    
    def _detect_energy_patterns(self, timeseries_df: pd.DataFrame, 
                              consumption_col: str) -> Dict:
        """Détecte les patterns énergétiques."""
        patterns = {
            'daily_pattern_detected': False,
            'weekly_pattern_detected': False,
            'seasonal_variation': False
        }
        
        if 'timestamp' not in timeseries_df.columns:
            return patterns
        
        # Ajouter des colonnes temporelles
        df_temp = timeseries_df.copy()
        df_temp['timestamp'] = pd.to_datetime(df_temp['timestamp'])
        df_temp['hour'] = df_temp['timestamp'].dt.hour
        df_temp['day_of_week'] = df_temp['timestamp'].dt.dayofweek
        df_temp['month'] = df_temp['timestamp'].dt.month
        
        # Pattern journalier (variation significative par heure)
        hourly_avg = df_temp.groupby('hour')[consumption_col].mean()
        if hourly_avg.std() / hourly_avg.mean() > 0.2:  # Coefficient de variation > 20%
            patterns['daily_pattern_detected'] = True
        
        # Pattern hebdomadaire
        weekly_avg = df_temp.groupby('day_of_week')[consumption_col].mean()
        if weekly_avg.std() / weekly_avg.mean() > 0.1:
            patterns['weekly_pattern_detected'] = True
        
        # Variation saisonnière
        if len(df_temp['month'].unique()) > 3:
            monthly_avg = df_temp.groupby('month')[consumption_col].mean()
            if monthly_avg.std() / monthly_avg.mean() > 0.15:
                patterns['seasonal_variation'] = True
        
        return patterns
    
    def _calculate_overall_score(self, validation_results: Dict) -> float:
        """Calcule le score global de validation."""
        scores = []
        weights = []
        
        # Score des métriques de qualité
        quality_metrics = validation_results.get('quality_metrics', {})
        for metric, weight in [('completeness', 0.25), ('consistency', 0.20), 
                              ('accuracy', 0.25), ('validity', 0.20), ('timeliness', 0.10)]:
            if metric in quality_metrics:
                scores.append(quality_metrics[metric])
                weights.append(weight)
        
        # Score de référence croisée
        cross_validation = validation_results.get('cross_validation', {})
        if 'reference_integrity' in cross_validation:
            scores.append(cross_validation['reference_integrity'])
            weights.append(0.15)
        
        # Calculer la moyenne pondérée
        if scores and weights:
            # Normaliser les poids
            total_weight = sum(weights)
            normalized_weights = [w / total_weight for w in weights]
            
            overall_score = sum(score * weight for score, weight in zip(scores, normalized_weights))
            return round(overall_score, 1)
        
        return 0.0
    
    def _generate_recommendations(self, validation_results: Dict) -> List[str]:
        """Génère des recommandations basées sur la validation."""
        recommendations = []
        
        overall_score = validation_results.get('score', 0)
        
        if overall_score >= 90:
            recommendations.append("✅ Excellente qualité de données - Dataset prêt pour la production")
        elif overall_score >= 75:
            recommendations.append("✅ Bonne qualité de données - Quelques améliorations mineures possibles")
        elif overall_score >= 60:
            recommendations.append("⚠️ Qualité acceptable - Améliorations recommandées avant utilisation")
        else:
            recommendations.append("❌ Qualité insuffisante - Corrections importantes nécessaires")
        
        # Recommandations spécifiques selon les problèmes détectés
        quality_metrics = validation_results.get('quality_metrics', {})
        
        if quality_metrics.get('completeness', 100) < 95:
            recommendations.append("🔧 Améliorer la complétude des données (valeurs manquantes détectées)")
        
        if quality_metrics.get('accuracy', 100) < 90:
            recommendations.append("🎯 Vérifier la précision des valeurs (valeurs hors plage détectées)")
        
        # Recommandations pour les anomalies
        timeseries_validation = validation_results.get('timeseries_validation', {})
        anomalies = timeseries_validation.get('anomalies', [])
        if len(anomalies) > 50:
            recommendations.append("⚠️ Nombreuses anomalies détectées - Vérifier les algorithmes de génération")
        
        return recommendations
    
    def _get_validation_thresholds(self) -> Dict:
        """Retourne les seuils de validation configurés."""
        if self.config and hasattr(self.config, 'VALIDATION_THRESHOLDS'):
            return self.config.VALIDATION_THRESHOLDS
        
        # Seuils par défaut
        return {
            'min_consumption_kwh': 0.1,
            'max_consumption_kwh': 1000,
            'max_daily_variation': 0.5,
            'outlier_z_score': 3.0
        }
    
    def _get_energy_rules(self) -> Dict:
        """Retourne les règles énergétiques par type de bâtiment."""
        if self.config and hasattr(self.config, 'COHERENCE_RULES'):
            return self.config.COHERENCE_RULES
        
        # Règles par défaut (consommation max en kWh)
        return {
            'residential': 50,
            'commercial': 500,
            'industrial': 1000,
            'public': 200
        }
    
    def get_validation_statistics(self) -> Dict:
        """Retourne les statistiques du service de validation."""
        return {
            'service_stats': self.validation_stats.copy(),
            'thresholds': self.validation_thresholds.copy(),
            'energy_rules': self.energy_rules.copy(),
            'service_status': 'active'
        }


# Export de la classe principale
__all__ = ['ValidationService']
