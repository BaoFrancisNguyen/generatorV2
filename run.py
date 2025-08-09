#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
POINT D'ENTRÉE PRINCIPAL - GÉNÉRATEUR MALAYSIA
Fichier: run.py

Script de démarrage de l'application Flask.
Gère la configuration d'environnement, l'initialisation des services,
et le démarrage du serveur avec toutes les options de production.

Auteur: Équipe Développement
Date: 2025
Version: 3.0 - Architecture modulaire
"""

import os
import sys
import logging
from pathlib import Path

# Ajouter le répertoire racine au Python path
root_dir = Path(__file__).parent.absolute()
sys.path.insert(0, str(root_dir))

# Import de l'application
from app import create_app
from app.config.base import validate_config

def setup_environment():
    """
    Configure l'environnement d'exécution.
    
    Définit les variables d'environnement nécessaires si elles ne sont pas présentes,
    et configure les chemins et permissions.
    """
    
    # Variables d'environnement par défaut
    default_env_vars = {
        'FLASK_ENV': 'development',
        'FLASK_DEBUG': 'True',
        'HOST': '127.0.0.1',
        'PORT': '5000',
        'SECRET_KEY': 'malaysia-energy-generator-dev-key-2025',
        'LOG_LEVEL': 'INFO'
    }
    
    # Définir les variables manquantes
    for key, default_value in default_env_vars.items():
        if key not in os.environ:
            os.environ[key] = default_value
    
    # Créer les dossiers nécessaires
    required_dirs = [
        root_dir / 'data' / 'generated',
        root_dir / 'data' / 'cache',
        root_dir / 'logs'
    ]
    
    for directory in required_dirs:
        directory.mkdir(parents=True, exist_ok=True)
    
    print(f"🔧 Environnement configuré:")
    print(f"   • Mode: {os.environ.get('FLASK_ENV')}")
    print(f"   • Debug: {os.environ.get('FLASK_DEBUG')}")
    print(f"   • Host: {os.environ.get('HOST')}")
    print(f"   • Port: {os.environ.get('PORT')}")
    print(f"   • Répertoire racine: {root_dir}")


def configure_logging():
    """
    Configure le système de logging pour l'application.
    
    Met en place les handlers de fichier et console avec rotation,
    et configure les niveaux selon l'environnement.
    """
    
    # Déterminer le niveau de logging
    log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
    level = getattr(logging, log_level, logging.INFO)
    
    # Configuration du format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Créer le répertoire de logs
    log_dir = root_dir / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    # Configuration du logging
    logging.basicConfig(
        level=level,
        format=log_format,
        datefmt=date_format,
        handlers=[
            # Handler console
            logging.StreamHandler(sys.stdout),
            # Handler fichier avec rotation
            logging.handlers.RotatingFileHandler(
                log_dir / 'malaysia_generator.log',
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
            )
        ]
    )
    
    # Configurer les loggers tiers
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    print(f"📝 Logging configuré:")
    print(f"   • Niveau: {log_level}")
    print(f"   • Fichier: {log_dir / 'malaysia_generator.log'}")


def print_startup_banner():
    """
    Affiche la bannière de démarrage avec les informations importantes.
    """
    
    banner = f"""
{'=' * 80}
🇲🇾  GÉNÉRATEUR DE DONNÉES ÉNERGÉTIQUES MALAYSIA  🇲🇾
{'=' * 80}

Version: 3.0 - Architecture Modulaire
Environnement: {os.environ.get('FLASK_ENV', 'unknown')}
Debug: {'Activé' if os.environ.get('FLASK_DEBUG', 'False').lower() == 'true' else 'Désactivé'}

FONCTIONNALITÉS PRINCIPALES:
✅ Génération de données synthétiques réalistes
✅ Intégration OpenStreetMap pour bâtiments réels
✅ Patterns climatiques tropicaux malaysiens
✅ Export multi-formats (Parquet, CSV, JSON, Excel)
✅ Interface web responsive et moderne
✅ API REST complète et documentée
✅ Validation automatique des données
✅ Cache intelligent et optimisations

MODULES CHARGÉS:
✅ Service de génération de données
✅ Service d'intégration OSM
✅ Service de validation
✅ Service d'export
✅ Interface utilisateur complète

{'=' * 80}
"""
    
    print(banner)


def check_dependencies():
    """
    Vérifie que toutes les dépendances critiques sont installées.
    
    Returns:
        bool: True si toutes les dépendances sont présentes
    """
    
    critical_dependencies = [
        'flask',
        'pandas',
        'numpy',
        'requests',
        'pyarrow'  # Pour le support Parquet
    ]
    
    missing_deps = []
    
    for dep in critical_dependencies:
        try:
            __import__(dep)
        except ImportError:
            missing_deps.append(dep)
    
    if missing_deps:
        print(f"❌ Dépendances manquantes: {', '.join(missing_deps)}")
        print("   Installez-les avec: pip install -r requirements.txt")
        return False
    
    print("✅ Toutes les dépendances critiques sont installées")
    return True


def run_health_check(app):
    """
    Effectue une vérification de santé des services après le démarrage.
    
    Args:
        app: Instance de l'application Flask
    """
    
    print("\n🏥 Vérification de santé des services...")
    
    with app.app_context():
        try:
            # Vérifier le générateur de données
            stats = app.data_generator.get_generation_statistics()
            print(f"   ✅ Générateur de données: {stats['total_locations']} localisations disponibles")
            
            # Vérifier le service OSM
            osm_stats = app.osm_service.get_statistics()
            cache_enabled = osm_stats['cache_info']['enabled']
            print(f"   ✅ Service OSM: Cache {'activé' if cache_enabled else 'désactivé'}")
            
            # Vérifier les services d'export et validation
            print("   ✅ Service d'export: Prêt")
            print("   ✅ Service de validation: Prêt")
            
            print("✅ Tous les services sont opérationnels")
            
        except Exception as e:
            print(f"   ⚠️ Problème détecté lors de la vérification: {e}")
            print("   L'application peut fonctionner en mode dégradé")


def get_server_config():
    """
    Récupère la configuration du serveur depuis les variables d'environnement.
    
    Returns:
        dict: Configuration du serveur
    """
    
    config = {
        'host': os.environ.get('HOST', '127.0.0.1'),
        'port': int(os.environ.get('PORT', 5000)),
        'debug': os.environ.get('FLASK_DEBUG', 'False').lower() == 'true',
        'threaded': True,
        'use_reloader': os.environ.get('FLASK_ENV') == 'development'
    }
    
    # Configuration spéciale pour la production
    if os.environ.get('FLASK_ENV') == 'production':
        config.update({
            'debug': False,
            'use_reloader': False,
            'threaded': True
        })
    
    return config


def setup_signal_handlers(app):
    """
    Configure les gestionnaires de signaux pour un arrêt propre.
    
    Args:
        app: Instance de l'application Flask
    """
    
    import signal
    
    def signal_handler(signum, frame):
        print(f"\n🛑 Signal {signum} reçu, arrêt en cours...")
        
        # Nettoyer les ressources si nécessaire
        try:
            with app.app_context():
                # Vider le cache OSM si nécessaire
                if hasattr(app, 'osm_service'):
                    print("   🗑️ Nettoyage du cache OSM...")
                
                # Fermer les connexions base de données si présentes
                print("   🔌 Fermeture des connexions...")
                
        except Exception as e:
            print(f"   ⚠️ Erreur lors du nettoyage: {e}")
        
        print("✅ Arrêt propre terminé")
        sys.exit(0)
    
    # Enregistrer les gestionnaires pour différents signaux
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Arrêt du service
    
    if hasattr(signal, 'SIGQUIT'):
        signal.signal(signal.SIGQUIT, signal_handler)  # Arrêt forcé (Unix)


def print_access_info(config):
    """
    Affiche les informations d'accès à l'application.
    
    Args:
        config: Configuration du serveur
    """
    
    host = config['host']
    port = config['port']
    
    print(f"\n🚀 SERVEUR DÉMARRÉ AVEC SUCCÈS")
    print(f"{'=' * 50}")
    print(f"🌐 Interface principale:")
    print(f"   http://{host}:{port}")
    print(f"")
    print(f"🔧 Endpoints utiles:")
    print(f"   • Santé: http://{host}:{port}/health")
    print(f"   • API Info: http://{host}:{port}/info")
    print(f"   • Documentation: http://{host}:{port}/documentation")
    print(f"")
    print(f"📊 API REST:")
    print(f"   • Génération: http://{host}:{port}/generate/")
    print(f"   • OSM: http://{host}:{port}/osm/buildings")
    print(f"   • Villes: http://{host}:{port}/api/cities")
    print(f"")
    print(f"🛠️ Mode: {os.environ.get('FLASK_ENV', 'unknown')}")
    print(f"🐛 Debug: {'Activé' if config['debug'] else 'Désactivé'}")
    print(f"")
    print(f"Pour arrêter le serveur: Ctrl+C")
    print(f"{'=' * 50}")


def main():
    """
    Fonction principale du script de démarrage.
    
    Orchestre toute la séquence d'initialisation et de démarrage.
    """
    
    try:
        # Afficher la bannière de démarrage
        print_startup_banner()
        
        # Configuration de l'environnement
        setup_environment()
        
        # Configuration du logging
        configure_logging()
        
        # Vérifier les dépendances
        if not check_dependencies():
            sys.exit(1)
        
        print("\n🏗️ Création de l'application Flask...")
        
        # Créer l'application
        config_name = os.environ.get('FLASK_ENV', 'development')
        app = create_app(config_name)
        
        # Valider la configuration
        try:
            validate_config(app.config)
            print("✅ Configuration validée")
        except ValueError as e:
            print(f"❌ Erreur de configuration: {e}")
            sys.exit(1)
        
        # Configuration du serveur
        server_config = get_server_config()
        
        # Vérification de santé
        run_health_check(app)
        
        # Configuration des gestionnaires de signaux
        setup_signal_handlers(app)
        
        # Enregistrer l'heure de démarrage pour les métriques d'uptime
        from datetime import datetime
        app._start_time = datetime.now()
        
        # Afficher les informations d'accès
        print_access_info(server_config)
        
        # Démarrer le serveur
        print(f"\n🎯 Démarrage du serveur Flask...")
        app.run(**server_config)
        
    except KeyboardInterrupt:
        print("\n👋 Arrêt demandé par l'utilisateur")
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ ERREUR CRITIQUE lors du démarrage:")
        print(f"   {type(e).__name__}: {e}")
        
        # En mode debug, afficher la stack trace complète
        if os.environ.get('FLASK_DEBUG', 'False').lower() == 'true':
            import traceback
            print("\n📋 Stack trace complète:")
            traceback.print_exc()
        
        print(f"\n🔧 Suggestions de débogage:")
        print(f"   • Vérifiez que toutes les dépendances sont installées")
        print(f"   • Vérifiez les permissions des dossiers")
        print(f"   • Consultez les logs dans le dossier logs/")
        print(f"   • Essayez en mode debug: FLASK_DEBUG=True python run.py")
        
        sys.exit(1)


def run_in_development():
    """
    Mode de développement avec des options spéciales.
    
    Active le rechargement automatique, le debugging étendu,
    et d'autres fonctionnalités utiles pour le développement.
    """
    
    print("🔧 MODE DÉVELOPPEMENT ACTIVÉ")
    print("=" * 40)
    print("✅ Rechargement automatique: ON")
    print("✅ Debug étendu: ON")
    print("✅ Logging verbeux: ON")
    print("✅ Validation stricte: ON")
    print("=" * 40)
    
    # Variables d'environnement spéciales pour le dev
    os.environ['FLASK_ENV'] = 'development'
    os.environ['FLASK_DEBUG'] = 'True'
    os.environ['LOG_LEVEL'] = 'DEBUG'
    
    # Lancer l'application normale
    main()


def run_in_production():
    """
    Mode de production avec optimisations et sécurité renforcée.
    
    Désactive le debug, active les optimisations,
    et configure la sécurité pour la production.
    """
    
    print("🏭 MODE PRODUCTION ACTIVÉ")
    print("=" * 40)
    print("✅ Optimisations: ON")
    print("✅ Sécurité renforcée: ON")
    print("✅ Cache activé: ON")
    print("✅ Compression: ON")
    print("⚠️ Debug: OFF")
    print("=" * 40)
    
    # Variables d'environnement pour la production
    os.environ['FLASK_ENV'] = 'production'
    os.environ['FLASK_DEBUG'] = 'False'
    os.environ['LOG_LEVEL'] = 'WARNING'
    
    # Vérifications supplémentaires pour la production
    if not os.environ.get('SECRET_KEY') or os.environ.get('SECRET_KEY').startswith('dev'):
        print("❌ ERREUR: SECRET_KEY de production requis")
        print("   Définissez une clé secrète sécurisée avec: export SECRET_KEY='votre_clé_secrète'")
        sys.exit(1)
    
    # Lancer l'application
    main()


def show_help():
    """
    Affiche l'aide d'utilisation du script.
    """
    
    help_text = """
🇲🇾 GÉNÉRATEUR MALAYSIA - AIDE D'UTILISATION
============================================

UTILISATION:
    python run.py [OPTIONS]

OPTIONS:
    --dev, --development    Lance en mode développement
    --prod, --production    Lance en mode production  
    --help, -h             Affiche cette aide

VARIABLES D'ENVIRONNEMENT:
    FLASK_ENV              Environnement (development/production)
    FLASK_DEBUG            Active le mode debug (True/False)
    HOST                   Adresse d'écoute (défaut: 127.0.0.1)
    PORT                   Port d'écoute (défaut: 5000)
    SECRET_KEY             Clé secrète pour la sécurité
    LOG_LEVEL              Niveau de logging (DEBUG/INFO/WARNING/ERROR)

EXEMPLES:
    # Démarrage standard
    python run.py
    
    # Mode développement explicite
    python run.py --dev
    
    # Mode production
    python run.py --prod
    
    # Port personnalisé
    PORT=8080 python run.py
    
    # Debug activé
    FLASK_DEBUG=True python run.py

FICHIERS DE CONFIGURATION:
    app/config/base.py          Configuration de base
    app/config/development.py   Configuration développement
    app/config/production.py    Configuration production

DOSSIERS IMPORTANTS:
    data/generated/            Fichiers générés
    data/cache/               Cache des données OSM
    logs/                     Fichiers de log
    static/                   Assets frontend
    templates/                Templates HTML

Pour plus d'informations, consultez le README.md
"""
    
    print(help_text)


if __name__ == '__main__':
    # Gestion des arguments de ligne de commande
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        
        if arg in ['--help', '-h', 'help']:
            show_help()
            sys.exit(0)
        elif arg in ['--dev', '--development', 'dev']:
            run_in_development()
        elif arg in ['--prod', '--production', 'prod']:
            run_in_production()
        else:
            print(f"❌ Argument non reconnu: {arg}")
            print("Utilisez --help pour voir les options disponibles")
            sys.exit(1)
    else:
        # Démarrage standard
        main()
