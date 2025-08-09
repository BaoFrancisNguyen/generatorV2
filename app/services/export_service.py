#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SERVICE D'EXPORT - GÉNÉRATEUR MALAYSIA
Fichier: app/services/export_service.py

Service pour l'export des données générées dans différents formats.
Gère la création de fichiers optimisés avec métadonnées et compression.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Service modulaire
"""

import logging
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np


class ExportService:
    """
    Service d'export multi-formats pour les données énergétiques.
    
    Formats supportés:
    - Parquet (recommandé pour performance)
    - CSV (compatibilité universelle)
    - JSON (intégrations API)
    - Excel (reporting et analyse)
    """
    
    def __init__(self, output_dir: Optional[str] = None, config=None):
        """
        Initialise le service d'export.
        
        Args:
            output_dir: Répertoire de sortie des fichiers
            config: Configuration de l'application
        """
        self.logger = logging.getLogger(__name__)
        self.config = config
        
        # Configuration du répertoire de sortie
        if output_dir:
            self.output_dir = Path(output_dir)
        elif config and hasattr(config, 'GENERATED_DATA_DIR'):
            self.output_dir = Path(config.GENERATED_DATA_DIR)
        else:
            self.output_dir = Path('data/generated')
        
        # Créer le répertoire s'il n'existe pas
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Formats supportés et leurs configurations
        self.supported_formats = {
            'parquet': {
                'extension': '.parquet',
                'compression': 'snappy',
                'engine': 'pyarrow'
            },
            'csv': {
                'extension': '.csv',
                'encoding': 'utf-8',
                'separator': ','
            },
            'json': {  # ✅ CORRECTION: Accolade fermante ajoutée
                'extension': '.json',
                'encoding': 'utf-8',
                'indent': 2,
                'ensure_ascii': False
            },
            'excel': {
                'extension': '.xlsx',
                'engine': 'openpyxl',
                'index': False
            }
        }
        
        # Statistiques d'export
        self.export_statistics = {
            'total_exports': 0,
            'successful_exports': 0,
            'failed_exports': 0,
            'formats_used': {},
            'total_files_created': 0,
            'total_size_bytes': 0
        }
        
        self.logger.info(f"🚀 ExportService initialisé - Répertoire: {self.output_dir}")
    
    def export_dataset(self, dataset: Dict[str, pd.DataFrame], 
                      export_format: str = 'parquet',
                      include_metadata: bool = True,
                      filename_prefix: str = 'malaysia_energy',
                      compression: bool = True) -> Dict[str, Any]:
        """
        Exporte un dataset complet dans le format spécifié.
        
        Args:
            dataset: Dataset avec 'buildings' et 'timeseries' DataFrames
            export_format: Format d'export ('parquet', 'csv', 'json', 'excel')
            include_metadata: Inclure les métadonnées
            filename_prefix: Préfixe pour les noms de fichiers
            compression: Activer la compression si supportée
            
        Returns:
            Informations sur les fichiers créés
        """
        self.logger.info(f"📤 Export du dataset en format {export_format}")
        
        try:
            # Validation du format
            if export_format not in self.supported_formats:
                raise ValueError(f"Format non supporté: {export_format}")
            
            # Validation du dataset
            if not isinstance(dataset, dict):
                raise ValueError("Dataset doit être un dictionnaire")
            
            if 'buildings' not in dataset or 'timeseries' not in dataset:
                raise ValueError("Dataset doit contenir 'buildings' et 'timeseries'")
            
            # Générer timestamp pour les noms de fichiers
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            export_result = {
                'format': export_format,
                'timestamp': timestamp,
                'files': [],
                'total_size': 0,
                'success': True
            }
            
            # Export selon le format
            if export_format == 'parquet':
                export_result = self._export_parquet(dataset, filename_prefix, timestamp, compression)
            elif export_format == 'csv':
                export_result = self._export_csv(dataset, filename_prefix, timestamp)
            elif export_format == 'json':
                export_result = self._export_json(dataset, filename_prefix, timestamp, include_metadata)
            elif export_format == 'excel':
                export_result = self._export_excel(dataset, filename_prefix, timestamp)
            
            # Mettre à jour les statistiques
            self._update_export_statistics(export_format, export_result)
            
            self.logger.info(f"✅ Export réussi: {len(export_result['files'])} fichiers créés")
            return export_result
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de l'export: {e}")
            self.export_statistics['failed_exports'] += 1
            raise
    
    def _export_parquet(self, dataset: Dict[str, pd.DataFrame], 
                       prefix: str, timestamp: str, compression: bool) -> Dict[str, Any]:
        """Exporte en format Parquet."""
        files_created = []
        total_size = 0
        
        format_config = self.supported_formats['parquet']
        compression_type = format_config['compression'] if compression else None
        
        for table_name, df in dataset.items():
            if not isinstance(df, pd.DataFrame):
                continue
                
            filename = f"{prefix}_{table_name}_{timestamp}.parquet"
            filepath = self.output_dir / filename
            
            # Optimisations pour Parquet
            df_optimized = self._optimize_dataframe_for_parquet(df)
            
            # Sauvegarde
            df_optimized.to_parquet(
                filepath,
                engine=format_config['engine'],
                compression=compression_type,
                index=False
            )
            
            file_size = filepath.stat().st_size
            files_created.append({
                'name': filename,
                'path': str(filepath),
                'size_bytes': file_size,
                'rows': len(df),
                'columns': len(df.columns)
            })
            total_size += file_size
        
        return {
            'format': 'parquet',
            'files': files_created,
            'total_size': total_size,
            'compression': compression_type,
            'success': True
        }
    
    def _export_csv(self, dataset: Dict[str, pd.DataFrame], 
                   prefix: str, timestamp: str) -> Dict[str, Any]:
        """Exporte en format CSV."""
        files_created = []
        total_size = 0
        
        format_config = self.supported_formats['csv']
        
        for table_name, df in dataset.items():
            if not isinstance(df, pd.DataFrame):
                continue
                
            filename = f"{prefix}_{table_name}_{timestamp}.csv"
            filepath = self.output_dir / filename
            
            # Sauvegarde CSV
            df.to_csv(
                filepath,
                index=False,
                encoding=format_config['encoding'],
                sep=format_config['separator']
            )
            
            file_size = filepath.stat().st_size
            files_created.append({
                'name': filename,
                'path': str(filepath),
                'size_bytes': file_size,
                'rows': len(df),
                'columns': len(df.columns)
            })
            total_size += file_size
        
        return {
            'format': 'csv',
            'files': files_created,
            'total_size': total_size,
            'success': True
        }
    
    def _export_json(self, dataset: Dict[str, pd.DataFrame], 
                    prefix: str, timestamp: str, include_metadata: bool) -> Dict[str, Any]:
        """Exporte en format JSON."""
        files_created = []
        total_size = 0
        
        format_config = self.supported_formats['json']
        
        # Export des tables individuelles
        for table_name, df in dataset.items():
            if not isinstance(df, pd.DataFrame):
                continue
                
            filename = f"{prefix}_{table_name}_{timestamp}.json"
            filepath = self.output_dir / filename
            
            # Convertir DataFrame en JSON
            data_dict = {
                'table_name': table_name,
                'metadata': {
                    'rows': len(df),
                    'columns': len(df.columns),
                    'column_names': df.columns.tolist(),
                    'export_timestamp': datetime.now().isoformat()
                } if include_metadata else {},
                'data': df.to_dict('records')
            }
            
            # Sauvegarde JSON
            with open(filepath, 'w', encoding=format_config['encoding']) as f:
                json.dump(data_dict, f, 
                         indent=format_config['indent'],
                         ensure_ascii=format_config['ensure_ascii'],
                         default=str)  # Pour gérer les dates
            
            file_size = filepath.stat().st_size
            files_created.append({
                'name': filename,
                'path': str(filepath),
                'size_bytes': file_size,
                'rows': len(df),
                'columns': len(df.columns)
            })
            total_size += file_size
        
        # Export combiné optionnel
        if len(dataset) > 1:
            combined_filename = f"{prefix}_combined_{timestamp}.json"
            combined_filepath = self.output_dir / combined_filename
            
            combined_data = {
                'dataset_metadata': {
                    'tables': list(dataset.keys()),
                    'total_rows': sum(len(df) for df in dataset.values() if isinstance(df, pd.DataFrame)),
                    'export_timestamp': datetime.now().isoformat()
                } if include_metadata else {},
                'tables': {}
            }
            
            for table_name, df in dataset.items():
                if isinstance(df, pd.DataFrame):
                    combined_data['tables'][table_name] = df.to_dict('records')
            
            with open(combined_filepath, 'w', encoding=format_config['encoding']) as f:
                json.dump(combined_data, f,
                         indent=format_config['indent'],
                         ensure_ascii=format_config['ensure_ascii'],
                         default=str)
            
            file_size = combined_filepath.stat().st_size
            files_created.append({
                'name': combined_filename,
                'path': str(combined_filepath),
                'size_bytes': file_size,
                'type': 'combined'
            })
            total_size += file_size
        
        return {
            'format': 'json',
            'files': files_created,
            'total_size': total_size,
            'success': True
        }
    
    def _export_excel(self, dataset: Dict[str, pd.DataFrame], 
                     prefix: str, timestamp: str) -> Dict[str, Any]:
        """Exporte en format Excel avec feuilles multiples."""
        filename = f"{prefix}_{timestamp}.xlsx"
        filepath = self.output_dir / filename
        
        format_config = self.supported_formats['excel']
        
        # Créer le fichier Excel avec feuilles multiples
        with pd.ExcelWriter(filepath, engine=format_config['engine']) as writer:
            total_rows = 0
            
            for table_name, df in dataset.items():
                if not isinstance(df, pd.DataFrame):
                    continue
                
                # Limiter à 1M lignes par feuille Excel
                if len(df) > 1000000:
                    self.logger.warning(f"⚠️ Table {table_name} tronquée à 1M lignes pour Excel")
                    df_to_save = df.head(1000000)
                else:
                    df_to_save = df
                
                # Nettoyer le nom de feuille (limite Excel: 31 caractères)
                sheet_name = table_name[:31]
                
                df_to_save.to_excel(writer, sheet_name=sheet_name, index=format_config['index'])
                total_rows += len(df_to_save)
        
        file_size = filepath.stat().st_size
        
        return {
            'format': 'excel',
            'files': [{
                'name': filename,
                'path': str(filepath),
                'size_bytes': file_size,
                'total_rows': total_rows,
                'sheets': list(dataset.keys())
            }],
            'total_size': file_size,
            'success': True
        }
    
    def _optimize_dataframe_for_parquet(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimise un DataFrame pour l'export Parquet."""
        df_optimized = df.copy()
        
        # Convertir les colonnes d'objets en catégories si approprié
        for col in df_optimized.select_dtypes(include=['object']).columns:
            if df_optimized[col].nunique() / len(df_optimized) < 0.5:  # Moins de 50% de valeurs uniques
                df_optimized[col] = df_optimized[col].astype('category')
        
        # Optimiser les types numériques
        for col in df_optimized.select_dtypes(include=['int64']).columns:
            col_min = df_optimized[col].min()
            col_max = df_optimized[col].max()
            
            if col_min >= 0:
                if col_max < 255:
                    df_optimized[col] = df_optimized[col].astype('uint8')
                elif col_max < 65535:
                    df_optimized[col] = df_optimized[col].astype('uint16')
                elif col_max < 4294967295:
                    df_optimized[col] = df_optimized[col].astype('uint32')
            else:
                if col_min >= -128 and col_max <= 127:
                    df_optimized[col] = df_optimized[col].astype('int8')
                elif col_min >= -32768 and col_max <= 32767:
                    df_optimized[col] = df_optimized[col].astype('int16')
                elif col_min >= -2147483648 and col_max <= 2147483647:
                    df_optimized[col] = df_optimized[col].astype('int32')
        
        return df_optimized
    
    def _update_export_statistics(self, export_format: str, export_result: Dict[str, Any]):
        """Met à jour les statistiques d'export."""
        self.export_statistics['total_exports'] += 1
        
        if export_result.get('success', False):
            self.export_statistics['successful_exports'] += 1
            self.export_statistics['total_files_created'] += len(export_result.get('files', []))
            self.export_statistics['total_size_bytes'] += export_result.get('total_size', 0)
            
            # Statistiques par format
            if export_format not in self.export_statistics['formats_used']:
                self.export_statistics['formats_used'][export_format] = 0
            self.export_statistics['formats_used'][export_format] += 1
        else:
            self.export_statistics['failed_exports'] += 1
    
    def get_export_statistics(self) -> Dict[str, Any]:
        """Retourne les statistiques d'export."""
        stats = self.export_statistics.copy()
        stats['success_rate'] = (
            (stats['successful_exports'] / stats['total_exports'] * 100) 
            if stats['total_exports'] > 0 else 0
        )
        stats['average_file_size_mb'] = (
            (stats['total_size_bytes'] / stats['total_files_created'] / 1024 / 1024)
            if stats['total_files_created'] > 0 else 0
        )
        return stats
    
    def cleanup_old_files(self, days_old: int = 7) -> Dict[str, Any]:
        """
        Nettoie les anciens fichiers d'export.
        
        Args:
            days_old: Âge des fichiers à supprimer (en jours)
            
        Returns:
            Informations sur le nettoyage
        """
        self.logger.info(f"🧹 Nettoyage des fichiers de plus de {days_old} jours")
        
        cleanup_result = {
            'files_deleted': 0,
            'size_freed_bytes': 0,
            'success': True
        }
        
        try:
            cutoff_time = datetime.now().timestamp() - (days_old * 24 * 60 * 60)
            
            for file_path in self.output_dir.iterdir():
                if file_path.is_file() and file_path.stat().st_mtime < cutoff_time:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    
                    cleanup_result['files_deleted'] += 1
                    cleanup_result['size_freed_bytes'] += file_size
            
            self.logger.info(f"✅ Nettoyage terminé: {cleanup_result['files_deleted']} fichiers supprimés")
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors du nettoyage: {e}")
            cleanup_result['success'] = False
            cleanup_result['error'] = str(e)
        
        return cleanup_result
    
    def list_export_files(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Liste les fichiers d'export récents.
        
        Args:
            limit: Nombre maximum de fichiers à retourner
            
        Returns:
            Liste des fichiers avec métadonnées
        """
        files_info = []
        
        try:
            # Récupérer tous les fichiers et les trier par date de modification
            all_files = [f for f in self.output_dir.iterdir() if f.is_file()]
            all_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            for file_path in all_files[:limit]:
                stat = file_path.stat()
                files_info.append({
                    'name': file_path.name,
                    'path': str(file_path),
                    'size_bytes': stat.st_size,
                    'size_mb': round(stat.st_size / 1024 / 1024, 2),
                    'created': datetime.fromtimestamp(stat.st_ctime).isoformat(),
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    'extension': file_path.suffix
                })
                
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la liste des fichiers: {e}")
        
        return files_info


# Export de la classe principale
__all__ = ['ExportService']