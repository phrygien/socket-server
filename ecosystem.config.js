module.exports = {
  apps: [
    {
      name: "socket-server",
      script: "./server.js",

      // Fork mode obligatoire — Socket.IO sans Redis adapter
      instances: 1,
      exec_mode: "fork",

      // Redémarrage automatique si fuite mémoire
      max_memory_restart: "200M",

      // Délai entre redémarrages (évite les restart loops)
      restart_delay: 2000,
      exp_backoff_restart_delay: 100,

      // Variables d'environnement
      env: {
        NODE_ENV: "production",
      },

      // Logs
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      error_file: "./logs/socket-error.log",
      out_file: "./logs/socket-out.log",
      merge_logs: true,

      // Ne pas redémarrer si arrêt volontaire (SIGINT/SIGTERM)
      stop_exit_codes: [0],

      // Surveillance des fichiers désactivée en prod
      watch: false,
    },
  ],
};
