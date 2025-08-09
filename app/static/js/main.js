/**
 * SCRIPT PRINCIPAL - GÉNÉRATEUR DE DONNÉES MALAYSIA
 * Fichier: static/js/main.js
 * 
 * Point d'entrée principal pour l'interface utilisateur.
 * Gère l'initialisation des composants, la coordination entre modules,
 * et les interactions utilisateur de haut niveau.
 * 
 * Auteur: Équipe Développement
 * Date: 2025
 * Version: 3.0 - Architecture modulaire
 */

// ==================== CONFIGURATION GLOBALE ====================

/**
 * Configuration principale de l'application
 */
const APP_CONFIG = {
    name: 'Malaysia Energy Generator',
    version: '3.0',
    api: {
        baseUrl: '',
        timeout: 300000, // 5 minutes
        retryAttempts: 3
    },
    ui: {
        loadingDelay: 300,
        animationDuration: 250,
        debounceDelay: 500
    },
    validation: {
        realTime: true,
        showWarnings: true
    },
    debug: {
        enabled: false,
        logLevel: 'info'
    }
};

/**
 * État global de l'application
 */
const APP_STATE = {
    initialized: false,
    loading: false,
    currentGeneration: null,
    lastGenerationTime: null,
    errors: [],
    warnings: [],
    currentView: 'form',
    modules: {}
};

// ==================== SYSTÈME DE LOGGING ====================

/**
 * Système de logging centralisé
 */
class Logger {
    constructor(level = 'info') {
        this.levels = { debug: 0, info: 1, warn: 2, error: 3 };
        this.currentLevel = this.levels[level] || 1;
    }

    debug(message, ...args) {
        if (this.currentLevel <= this.levels.debug) {
            console.debug(`🔧 [DEBUG] ${message}`, ...args);
        }
    }

    info(message, ...args) {
        if (this.currentLevel <= this.levels.info) {
            console.info(`ℹ️ [INFO] ${message}`, ...args);
        }
    }

    warn(message, ...args) {
        if (this.currentLevel <= this.levels.warn) {
            console.warn(`⚠️ [WARN] ${message}`, ...args);
        }
    }

    error(message, ...args) {
        if (this.currentLevel <= this.levels.error) {
            console.error(`❌ [ERROR] ${message}`, ...args);
        }
    }
}

const logger = new Logger(APP_CONFIG.debug.logLevel);

// ==================== GESTIONNAIRE D'ÉVÉNEMENTS ====================

/**
 * Gestionnaire centralisé des événements de l'application
 */
class EventManager {
    constructor() {
        this.listeners = new Map();
    }

    /**
     * Enregistre un écouteur d'événement
     */
    on(eventType, callback, context = null) {
        if (!this.listeners.has(eventType)) {
            this.listeners.set(eventType, []);
        }
        
        this.listeners.get(eventType).push({
            callback,
            context
        });
        
        logger.debug(`Écouteur enregistré pour '${eventType}'`);
    }

    /**
     * Émet un événement
     */
    emit(eventType, data = null) {
        logger.debug(`Émission de l'événement '${eventType}'`, data);
        
        if (this.listeners.has(eventType)) {
            this.listeners.get(eventType).forEach(listener => {
                try {
                    if (listener.context) {
                        listener.callback.call(listener.context, data);
                    } else {
                        listener.callback(data);
                    }
                } catch (error) {
                    logger.error(`Erreur dans l'écouteur pour '${eventType}':`, error);
                }
            });
        }
    }

    /**
     * Supprime un écouteur d'événement
     */
    off(eventType, callback) {
        if (this.listeners.has(eventType)) {
            const listeners = this.listeners.get(eventType);
            const index = listeners.findIndex(l => l.callback === callback);
            if (index > -1) {
                listeners.splice(index, 1);
                logger.debug(`Écouteur supprimé pour '${eventType}'`);
            }
        }
    }
}

const eventManager = new EventManager();

// ==================== GESTIONNAIRE D'ÉTAT ====================

/**
 * Gestionnaire centralisé de l'état de l'application
 */
class StateManager {
    constructor(initialState = {}) {
        this.state = { ...initialState };
        this.subscribers = new Set();
    }

    /**
     * Met à jour l'état
     */
    setState(updates) {
        const previousState = { ...this.state };
        this.state = { ...this.state, ...updates };
        
        logger.debug('État mis à jour:', updates);
        
        // Notifier les abonnés
        this.subscribers.forEach(subscriber => {
            try {
                subscriber(this.state, previousState);
            } catch (error) {
                logger.error('Erreur dans un abonné d\'état:', error);
            }
        });
        
        // Émettre un événement global
        eventManager.emit('stateChanged', {
            newState: this.state,
            previousState,
            updates
        });
    }

    /**
     * Récupère l'état actuel
     */
    getState() {
        return { ...this.state };
    }

    /**
     * Abonne une fonction aux changements d'état
     */
    subscribe(callback) {
        this.subscribers.add(callback);
        return () => this.subscribers.delete(callback);
    }
}

const stateManager = new StateManager(APP_STATE);

// ==================== GESTIONNAIRE DE CHARGEMENT ====================

/**
 * Gestionnaire centralisé des états de chargement
 */
class LoadingManager {
    constructor() {
        this.activeLoads = new Set();
        this.overlay = null;
        this.progressBar = null;
        this.statusElement = null;
    }

    /**
     * Initialise les éléments de l'interface de chargement
     */
    initialize() {
        this.overlay = document.getElementById('loadingOverlay');
        this.progressBar = document.getElementById('loadingProgressBar');
        this.statusElement = document.getElementById('loadingStatus');
        
        if (!this.overlay) {
            this.createLoadingElements();
        }
        
        logger.debug('Gestionnaire de chargement initialisé');
    }

    /**
     * Crée les éléments de chargement s'ils n'existent pas
     */
    createLoadingElements() {
        // Créer l'overlay de chargement
        this.overlay = document.createElement('div');
        this.overlay.id = 'loadingOverlay';
        this.overlay.className = 'loading-overlay';
        this.overlay.innerHTML = `
            <div class="loading-content">
                <div class="loading-spinner"></div>
                <div class="loading-message">Chargement en cours...</div>
                <div class="loading-progress">
                    <div class="progress-bar">
                        <div id="loadingProgressBar" class="progress-fill"></div>
                    </div>
                    <div id="loadingStatus" class="loading-status"></div>
                </div>
            </div>
        `;
        
        document.body.appendChild(this.overlay);
        
        this.progressBar = document.getElementById('loadingProgressBar');
        this.statusElement = document.getElementById('loadingStatus');
    }

    /**
     * Démarre un chargement avec un identifiant unique
     */
    startLoading(loadId, message = 'Chargement en cours...', showProgress = false) {
        this.activeLoads.add(loadId);
        
        if (this.overlay) {
            this.overlay.style.display = 'flex';
            this.overlay.querySelector('.loading-message').textContent = message;
            
            if (showProgress && this.progressBar) {
                this.progressBar.style.display = 'block';
                this.progressBar.style.width = '0%';
            }
        }
        
        // Mettre à jour l'état global
        stateManager.setState({ loading: true });
        
        logger.debug(`Chargement démarré: ${loadId}`);
        eventManager.emit('loadingStarted', { loadId, message });
    }

    /**
     * Met à jour le progrès d'un chargement
     */
    updateProgress(loadId, progress, status = '') {
        if (!this.activeLoads.has(loadId)) return;
        
        if (this.progressBar) {
            this.progressBar.style.width = `${Math.min(100, Math.max(0, progress))}%`;
        }
        
        if (this.statusElement && status) {
            this.statusElement.textContent = status;
        }
        
        eventManager.emit('loadingProgress', { loadId, progress, status });
    }

    /**
     * Termine un chargement
     */
    stopLoading(loadId) {
        this.activeLoads.delete(loadId);
        
        // Si plus aucun chargement actif, cacher l'overlay
        if (this.activeLoads.size === 0) {
            if (this.overlay) {
                this.overlay.style.display = 'none';
            }
            
            stateManager.setState({ loading: false });
        }
        
        logger.debug(`Chargement terminé: ${loadId}`);
        eventManager.emit('loadingStopped', { loadId });
    }

    /**
     * Force l'arrêt de tous les chargements
     */
    stopAllLoading() {
        this.activeLoads.clear();
        
        if (this.overlay) {
            this.overlay.style.display = 'none';
        }
        
        stateManager.setState({ loading: false });
        
        logger.debug('Tous les chargements arrêtés');
        eventManager.emit('allLoadingStopped');
    }
}

const loadingManager = new LoadingManager();

// ==================== GESTIONNAIRE D'API ====================

/**
 * Gestionnaire centralisé des appels API
 */
class ApiManager {
    constructor() {
        this.baseUrl = APP_CONFIG.api.baseUrl;
        this.timeout = APP_CONFIG.api.timeout;
        this.retryAttempts = APP_CONFIG.api.retryAttempts;
    }

    /**
     * Effectue une requête HTTP avec retry automatique
     */
    async request(endpoint, options = {}) {
        const url = `${this.baseUrl}${endpoint}`;
        const requestId = `api_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        
        const defaultOptions = {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            timeout: this.timeout
        };
        
        const finalOptions = { ...defaultOptions, ...options };
        
        logger.debug(`Requête API: ${finalOptions.method} ${url}`);
        
        for (let attempt = 1; attempt <= this.retryAttempts; attempt++) {
            try {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), this.timeout);
                
                const response = await fetch(url, {
                    ...finalOptions,
                    signal: controller.signal
                });
                
                clearTimeout(timeoutId);
                
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                
                const data = await response.json();
                
                logger.debug(`Requête API réussie: ${url}`);
                eventManager.emit('apiSuccess', { endpoint, data, requestId });
                
                return data;
                
            } catch (error) {
                logger.warn(`Tentative ${attempt}/${this.retryAttempts} échouée pour ${url}:`, error.message);
                
                if (attempt === this.retryAttempts) {
                    logger.error(`Échec final de la requête API: ${url}`);
                    eventManager.emit('apiError', { endpoint, error, requestId });
                    throw error;
                }
                
                // Délai avant retry (backoff exponentiel)
                await this.delay(Math.pow(2, attempt - 1) * 1000);
            }
        }
    }

    /**
     * Requête GET
     */
    async get(endpoint, params = {}) {
        const queryString = new URLSearchParams(params).toString();
        const url = queryString ? `${endpoint}?${queryString}` : endpoint;
        
        return this.request(url);
    }

    /**
     * Requête POST
     */
    async post(endpoint, data = {}) {
        return this.request(endpoint, {
            method: 'POST',
            body: JSON.stringify(data)
        });
    }

    /**
     * Délai d'attente
     */
    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

const apiManager = new ApiManager();

// ==================== GESTIONNAIRE DE NOTIFICATIONS ====================

/**
 * Système de notifications utilisateur
 */
class NotificationManager {
    constructor() {
        this.container = null;
        this.notifications = new Map();
    }

    /**
     * Initialise le conteneur de notifications
     */
    initialize() {
        this.container = document.getElementById('notificationContainer');
        
        if (!this.container) {
            this.createContainer();
        }
        
        logger.debug('Gestionnaire de notifications initialisé');
    }

    /**
     * Crée le conteneur de notifications
     */
    createContainer() {
        this.container = document.createElement('div');
        this.container.id = 'notificationContainer';
        this.container.className = 'notification-container';
        document.body.appendChild(this.container);
    }

    /**
     * Affiche une notification
     */
    show(message, type = 'info', duration = 5000) {
        const notificationId = `notif_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.innerHTML = `
            <div class="notification-content">
                <div class="notification-icon">${this.getIcon(type)}</div>
                <div class="notification-message">${message}</div>
                <button class="notification-close" onclick="notificationManager.close('${notificationId}')">×</button>
            </div>
        `;
        
        this.container.appendChild(notification);
        this.notifications.set(notificationId, notification);
        
        // Animation d'entrée
        setTimeout(() => notification.classList.add('notification-show'), 10);
        
        // Auto-suppression
        if (duration > 0) {
            setTimeout(() => this.close(notificationId), duration);
        }
        
        logger.debug(`Notification affichée: ${type} - ${message}`);
        eventManager.emit('notificationShown', { id: notificationId, message, type });
        
        return notificationId;
    }

    /**
     * Ferme une notification
     */
    close(notificationId) {
        const notification = this.notifications.get(notificationId);
        if (notification) {
            notification.classList.add('notification-hide');
            
            setTimeout(() => {
                if (notification.parentNode) {
                    notification.parentNode.removeChild(notification);
                }
                this.notifications.delete(notificationId);
            }, APP_CONFIG.ui.animationDuration);
            
            logger.debug(`Notification fermée: ${notificationId}`);
            eventManager.emit('notificationClosed', { id: notificationId });
        }
    }

    /**
     * Méthodes de commodité pour différents types
     */
    success(message, duration = 5000) {
        return this.show(message, 'success', duration);
    }

    error(message, duration = 8000) {
        return this.show(message, 'error', duration);
    }

    warning(message, duration = 6000) {
        return this.show(message, 'warning', duration);
    }

    info(message, duration = 5000) {
        return this.show(message, 'info', duration);
    }

    /**
     * Retourne l'icône pour un type de notification
     */
    getIcon(type) {
        const icons = {
            success: '✅',
            error: '❌',
            warning: '⚠️',
            info: 'ℹ️'
        };
        return icons[type] || icons.info;
    }
}

const notificationManager = new NotificationManager();

// ==================== INITIALISATION DE L'APPLICATION ====================

/**
 * Classe principale de l'application
 */
class MalaysiaGeneratorApp {
    constructor() {
        this.modules = new Map();
        this.initialized = false;
    }

    /**
     * Initialise l'application complète
     */
    async initialize() {
        logger.info('🚀 Initialisation de l\'application Malaysia Generator');
        
        try {
            // Initialiser les gestionnaires de base
            loadingManager.initialize();
            notificationManager.initialize();
            
            // Charger les modules
            await this.loadModules();
            
            // Configurer les écouteurs d'événements globaux
            this.setupGlobalEventListeners();
            
            // Initialiser l'interface utilisateur
            this.initializeUI();
            
            // Marquer comme initialisé
            this.initialized = true;
            stateManager.setState({ initialized: true });
            
            logger.info('✅ Application initialisée avec succès');
            notificationManager.success('Application chargée avec succès');
            
            eventManager.emit('appInitialized');
            
        } catch (error) {
            logger.error('❌ Échec de l\'initialisation de l\'application:', error);
            notificationManager.error('Erreur lors du chargement de l\'application');
            throw error;
        }
    }

    /**
     * Charge tous les modules nécessaires
     */
    async loadModules() {
        logger.info('📦 Chargement des modules...');
        
        const moduleLoadingId = 'modules_loading';
        loadingManager.startLoading(moduleLoadingId, 'Chargement des modules...', true);
        
        try {
            // Liste des modules à charger
            const modulesToLoad = [
                { name: 'formValidator', path: '/static/js/components/form-validator.js' },
                { name: 'mapController', path: '/static/js/components/map-controller.js' },
                { name: 'dataVisualizer', path: '/static/js/components/data-visualizer.js' },
                { name: 'osmIntegration', path: '/static/js/modules/osm-integration.js' },
                { name: 'exportManager', path: '/static/js/modules/export-manager.js' }
            ];
            
            for (let i = 0; i < modulesToLoad.length; i++) {
                const module = modulesToLoad[i];
                const progress = ((i + 1) / modulesToLoad.length) * 100;
                
                loadingManager.updateProgress(moduleLoadingId, progress, `Chargement de ${module.name}...`);
                
                try {
                    // Charger le module dynamiquement
                    await this.loadModule(module.name, module.path);
                    logger.debug(`Module chargé: ${module.name}`);
                } catch (error) {
                    logger.warn(`Impossible de charger le module ${module.name}:`, error);
                    // Continuer sans ce module
                }
            }
            
            logger.info(`✅ ${this.modules.size} modules chargés`);
            
        } finally {
            loadingManager.stopLoading(moduleLoadingId);
        }
    }

    /**
     * Charge un module spécifique
     */
    async loadModule(name, path) {
        try {
            // Créer un script pour charger le module
            const script = document.createElement('script');
            script.src = path;
            script.async = true;
            
            return new Promise((resolve, reject) => {
                script.onload = () => {
                    // Le module devrait s'enregistrer dans window[name]
                    if (window[name]) {
                        this.modules.set(name, window[name]);
                        resolve(window[name]);
                    } else {
                        reject(new Error(`Module ${name} non trouvé après chargement`));
                    }
                };
                
                script.onerror = () => {
                    reject(new Error(`Erreur lors du chargement de ${path}`));
                };
                
                document.head.appendChild(script);
            });
            
        } catch (error) {
            logger.error(`Erreur lors du chargement du module ${name}:`, error);
            throw error;
        }
    }

    /**
     * Configure les écouteurs d'événements globaux
     */
    setupGlobalEventListeners() {
        // Gestion des erreurs globales
        window.addEventListener('error', (event) => {
            logger.error('Erreur JavaScript globale:', event.error);
            notificationManager.error('Une erreur inattendue s\'est produite');
        });

        // Gestion des erreurs de promesses non capturées
        window.addEventListener('unhandledrejection', (event) => {
            logger.error('Promesse rejetée non gérée:', event.reason);
            notificationManager.error('Erreur lors d\'une opération asynchrone');
        });

        // Raccourcis clavier globaux
        document.addEventListener('keydown', (event) => {
            this.handleGlobalKeyboard(event);
        });

        // Gestion de la visibilité de la page
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                logger.debug('Application mise en arrière-plan');
            } else {
                logger.debug('Application remise au premier plan');
            }
        });

        logger.debug('Écouteurs d\'événements globaux configurés');
    }

    /**
     * Gère les raccourcis clavier globaux
     */
    handleGlobalKeyboard(event) {
        // Ctrl/Cmd + D : Activer/désactiver le mode debug
        if ((event.ctrlKey || event.metaKey) && event.key === 'd') {
            event.preventDefault();
            this.toggleDebugMode();
        }

        // Échap : Fermer les overlays/modals
        if (event.key === 'Escape') {
            eventManager.emit('escapePressed');
        }

        // F5 / Ctrl+R : Empêcher le rechargement accidentel pendant les générations
        if ((event.key === 'F5' || (event.ctrlKey && event.key === 'r')) && stateManager.getState().loading) {
            event.preventDefault();
            notificationManager.warning('Génération en cours - rechargement bloqué');
        }
    }

    /**
     * Initialise l'interface utilisateur de base
     */
    initializeUI() {
        logger.info('🎨 Initialisation de l\'interface utilisateur');

        // Configurer les formulaires
        this.setupForms();

        // Configurer les boutons principaux
        this.setupMainButtons();

        // Initialiser les tooltips et l'aide contextuelle
        this.setupTooltips();

        // Configurer la responsivité
        this.setupResponsive();

        logger.debug('Interface utilisateur initialisée');
    }

    /**
     * Configure les formulaires principaux
     */
    setupForms() {
        const mainForm = document.getElementById('generationForm');
        if (mainForm) {
            mainForm.addEventListener('submit', (event) => {
                event.preventDefault();
                this.handleFormSubmit(event);
            });
        }

        // Validation en temps réel si activée
        if (APP_CONFIG.validation.realTime) {
            const inputs = mainForm?.querySelectorAll('input, select, textarea');
            inputs?.forEach(input => {
                input.addEventListener('blur', () => {
                    this.validateField(input);
                });
            });
        }
    }

    /**
     * Configure les boutons principaux
     */
    setupMainButtons() {
        const buttons = {
            'generateBtn': () => this.startGeneration(),
            'previewBtn': () => this.showPreview(),
            'sampleBtn': () => this.generateSample(),
            'resetBtn': () => this.resetForm()
        };

        Object.entries(buttons).forEach(([buttonId, handler]) => {
            const button = document.getElementById(buttonId);
            if (button) {
                button.addEventListener('click', handler.bind(this));
            }
        });
    }

    /**
     * Configure les tooltips
     */
    setupTooltips() {
        const tooltipElements = document.querySelectorAll('[data-tooltip]');
        tooltipElements.forEach(element => {
            element.addEventListener('mouseenter', (event) => {
                this.showTooltip(event.target);
            });
            
            element.addEventListener('mouseleave', () => {
                this.hideTooltip();
            });
        });
    }

    /**
     * Configure la responsivité
     */
    setupResponsive() {
        const mediaQuery = window.matchMedia('(max-width: 768px)');
        
        const handleResponsive = (e) => {
            stateManager.setState({ isMobile: e.matches });
            eventManager.emit('viewportChanged', { isMobile: e.matches });
        };
        
        mediaQuery.addListener(handleResponsive);
        handleResponsive(mediaQuery); // Exécuter immédiatement
    }

    /**
     * Démarre une génération
     */
    async startGeneration() {
        logger.info('🏗️ Démarrage de la génération');
        
        try {
            // Valider le formulaire
            const formData = this.getFormData();
            const validation = this.validateForm(formData);
            
            if (!validation.valid) {
                notificationManager.error('Veuillez corriger les erreurs du formulaire');
                return;
            }

            // Démarrer le chargement
            const generationId = 'main_generation';
            loadingManager.startLoading(generationId, 'Génération en cours...', true);

            // Appel API
            const result = await apiManager.post('/generate/', formData);

            if (result.success) {
                notificationManager.success('Génération terminée avec succès');
                stateManager.setState({ currentGeneration: result });
                eventManager.emit('generationCompleted', result);
            } else {
                throw new Error(result.error || 'Erreur de génération');
            }

        } catch (error) {
            logger.error('Erreur lors de la génération:', error);
            notificationManager.error(`Erreur: ${error.message}`);
        } finally {
            loadingManager.stopLoading('main_generation');
        }
    }

    /**
     * Bascule le mode debug
     */
    toggleDebugMode() {
        APP_CONFIG.debug.enabled = !APP_CONFIG.debug.enabled;
        
        if (APP_CONFIG.debug.enabled) {
            logger.currentLevel = logger.levels.debug;
            notificationManager.info('Mode debug activé');
            document.body.classList.add('debug-mode');
        } else {
            logger.currentLevel = logger.levels.info;
            notificationManager.info('Mode debug désactivé');
            document.body.classList.remove('debug-mode');
        }
        
        logger.info(`Mode debug: ${APP_CONFIG.debug.enabled ? 'ON' : 'OFF'}`);
    }

    /**
     * Retourne un module chargé
     */
    getModule(name) {
        return this.modules.get(name);
    }

    /**
     * Vérifie si l'application est initialisée
     */
    isInitialized() {
        return this.initialized;
    }
}

// ==================== INITIALISATION ET EXPORT ====================

// Instance globale de l'application
const app = new MalaysiaGeneratorApp();

// Initialisation au chargement du DOM
document.addEventListener('DOMContentLoaded', async () => {
    try {
        await app.initialize();
    } catch (error) {
        console.error('❌ Échec de l\'initialisation:', error);
    }
});

// Export global pour l'utilisation dans d'autres scripts
window.MalaysiaGenerator = {
    app,
    eventManager,
    stateManager,
    loadingManager,
    notificationManager,
    apiManager,
    logger,
    APP_CONFIG
};

// Messages de bienvenue dans la console
console.log(`
🇲🇾 GÉNÉRATEUR MALAYSIA - VERSION ${APP_CONFIG.version}
=============================================
✅ Framework JavaScript chargé
✅ Gestionnaires initialisés
✅ Modules en cours de chargement
✅ Interface utilisateur prête

COMMANDES UTILES:
• MalaysiaGenerator.app.toggleDebugMode() - Active/désactive le debug
• MalaysiaGenerator.logger.level = 'debug' - Change le niveau de log
• MalaysiaGenerator.stateManager.getState() - Affiche l'état actuel

RACCOURCIS:
• Ctrl+D - Bascule le mode debug
• Échap - Ferme les overlays

Pour plus d'aide, consultez la documentation.
`);
