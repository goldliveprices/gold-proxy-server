// ecosystem.config.js — PM2 process manager config
// 
// Usage:
//   pm2 start ecosystem.config.js
//   pm2 save
//   pm2 startup

module.exports = {
  apps: [{
    name:         'rr-jewellers',
    script:       'server.js',          // rename server_v17.js → server.js first
    instances:    1,                    // Single instance (WS state is in-memory)
    exec_mode:    'fork',

    // Environment variables — set in Render dashboard or .env file
    env: {
      NODE_ENV:          'production',
      PORT:              3000,
    },

    // Restart policy
    autorestart:  true,
    max_restarts: 10,
    restart_delay: 3000,                // 3s between restarts
    min_uptime:   '10s',               // Must stay up 10s to count as healthy

    // Memory limits
    max_memory_restart: '200M',         // Restart if RSS > 200MB (GC escape hatch)

    // Logging
    log_file:     './logs/combined.log',
    error_file:   './logs/error.log',
    out_file:     './logs/out.log',
    log_date_format: 'YYYY-MM-DD HH:mm:ss',
    merge_logs:   true,

    // Node.js optimization flags
    node_args: [
      '--max-old-space-size=150',       // Limit heap to 150MB
      '--optimize-for-size',            // Smaller footprint
    ],

    // Watch (disable in production)
    watch:        false,
  }]
};
