const express = require('express');
const cors = require('cors');
const { WebSocketServer } = require('ws');
const http = require('http');
const path = require('path');
const mysql = require('mysql2');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

// ===============================
// CONFIGURATION
// ===============================

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || 'CHANGE_ME_SECURE_KEY';
const MAX_HISTORY = 288;

// ===============================
// MIDDLEWARES
// ===============================

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ===============================
// CONNEXION MYSQL
// ===============================

const db = mysql.createPool({
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'stationmeteoconnectee',
  port: process.env.DB_PORT || 3306,
  waitForConnections: true,
  connectionLimit: 10
});

// ===============================
// DONNÉES TEMPS RÉEL
// ===============================

let lastData = {
  temperature: null,
  humidite: null,
  pression_atmospherique: null,
  vitesse_vent: null,
  pluviometrie: null,
  luminosite: null,
  timestamp: null
};

let history = [];

// ===============================
// AUTHENTIFICATION ESP32
// ===============================

function authESP32(req, res, next) {
  const key = req.headers['x-api-key'] || req.query.key;

  if (key !== API_KEY) {
    return res.status(401).json({
      error: 'Clé API invalide'
    });
  }

  next();
}

// ===============================
// PAGE D'ACCUEIL
// ===============================

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ===============================
// ROUTE ENVOI DONNÉES ESP32
// ===============================

app.post('/api/data', authESP32, (req, res) => {

  const {
    temperature,
    humidite,
    pression_atmospherique,
    vitesse_vent,
    pluviometrie,
    luminosite
  } = req.body;

  // ===============================
  // VALIDATION
  // ===============================

  if (temperature === undefined || humidite === undefined) {
    return res.status(400).json({
      error: 'Température et humidité requises'
    });
  }

  if (isNaN(temperature) || isNaN(humidite)) {
    return res.status(400).json({
      error: 'Valeurs invalides'
    });
  }

  // ===============================
  // NETTOYAGE DONNÉES
  // ===============================

  const entry = {
    temperature: parseFloat(temperature),
    humidite: parseFloat(humidite),

    pression_atmospherique: isNaN(parseFloat(pression_atmospherique))
      ? 1013
      : parseFloat(pression_atmospherique),

    vitesse_vent: isNaN(parseFloat(vitesse_vent))
      ? 0
      : parseFloat(vitesse_vent),

    pluviometrie: isNaN(parseFloat(pluviometrie))
      ? 0
      : parseFloat(pluviometrie),

    luminosite: isNaN(parseFloat(luminosite))
      ? 0
      : parseFloat(luminosite),

    timestamp: new Date().toISOString()
  };

  // ===============================
  // STOCKAGE RAM
  // ===============================

  lastData = entry;

  history.push(entry);

  if (history.length > MAX_HISTORY) {
    history.shift();
  }

  // ===============================
  // ENREGISTREMENT MYSQL
  // ===============================

  const sql = `
    INSERT INTO mesures
    (
      temperature,
      humidite,
      pression_atmospherique,
      vitesse_vent,
      luminosite,
      pluviometrie
    )
    VALUES (?, ?, ?, ?, ?, ?)
  `;
  ;

  db.query(
    sql,
    [
      entry.temperature,
      entry.humidite,
      entry.pression_atmospherique,
      entry.vitesse_vent,
      entry.luminosite,
      entry.pluviometrie
    ],
    (err) => {
      if (err) {
        console.log('❌ Erreur SQL :', err);
      }
    }
  );

  // ===============================
  // ALERTES
  // ===============================

  const alerts = checkAlerts(entry);

  // ===============================
  // WEBSOCKET TEMPS RÉEL
  // ===============================

  const payload = JSON.stringify({
    type: 'data',
    data: entry,
    alerts
  });

  wss.clients.forEach(client => {
    if (client.readyState === 1) {
      client.send(payload);
    }
  });

  // ===============================
  // LOG CONSOLE
  // ===============================

  console.log(
    `[${entry.timestamp}] Temp: ${entry.temperature}°C | Humidité: ${entry.humidite}%`
  );

  res.json({
    status: 'ok',
    alerts
  });
});

// ===============================
// DERNIÈRE DONNÉE
// ===============================

app.get('/api/data', (req, res) => {

  if (!lastData.timestamp) {
    return res.status(503).json({
      error: 'Aucune donnée disponible'
    });
  }

  res.json(lastData);
});

// ===============================
// HISTORIQUE MYSQL
// ===============================

app.get('/api/history', (req, res) => {

  const limit = parseInt(req.query.limit) || 100;

  db.query(
    'SELECT * FROM mesures ORDER BY id DESC LIMIT ?',
    [limit],
    (err, result) => {

      if (err) {
        return res.status(500).json({
          error: 'Erreur SQL'
        });
      }

      res.json(result);
    }
  );
});

// ===============================
// STATISTIQUES
// ===============================

app.get('/api/stats', (req, res) => {

  if (history.length === 0) {
    return res.json({});
  }

  const fields = [
    'temperature',
    'humidite',
    'pression_atmospherique',
    'vitesse_vent',
    'pluviometrie',
    'luminosite'
  ];

  const stats = {};

  fields.forEach(field => {

    const values = history
      .map(h => h[field])
      .filter(v => !isNaN(v));

    if (!values.length) return;

    stats[field] = {
      min: +Math.min(...values).toFixed(2),
      max: +Math.max(...values).toFixed(2),
      avg: +(values.reduce((a, b) => a + b, 0) / values.length).toFixed(2),
      last: values[values.length - 1]
    };
  });

  res.json(stats);
});
// ===============================
// PREVISIONS METEO (basées sur pression 24h)
// ===============================

app.get('/api/previsions', (req, res) => {

  const sql = `
    SELECT pression, date
    FROM mesures
    WHERE date >= NOW() - INTERVAL 24 HOUR
    ORDER BY date ASC
  `;

  db.query(sql, (err, result) => {

    if (err) {
  console.log('Erreur previsions:', err.message);
  return res.status(500).json({ error: err.message });
  }

    if (!result || result.length < 2) {
      return res.json({
        icone: '🌤️',
        message: 'Données insuffisantes pour une prévision',
        confiance: 'faible'
      });
    }

    const milieu = Math.floor(result.length / 2);
    const premiereMoitie = result.slice(0, milieu);
    const deuxiemeMoitie = result.slice(milieu);

    const moyennePremiere =
      premiereMoitie.reduce((sum, r) => sum + r.pression, 0) / premiereMoitie.length;

    const moyenneDeuxieme =
      deuxiemeMoitie.reduce((sum, r) => sum + r.pression, 0) / deuxiemeMoitie.length;

    const delta = moyenneDeuxieme - moyennePremiere;

    let prevision = {
      icone: '🌤️',
      message: 'Temps stable',
      confiance: 'moyenne',
      delta: delta.toFixed(2)
    };

    if (delta < -3) {
      prevision = {
        icone: '🌧️',
        message: 'Pluie probable dans les prochaines heures',
        confiance: 'élevée',
        delta: delta.toFixed(2)
      };
    } else if (delta > 3) {
      prevision = {
        icone: '☀️',
        message: 'Beau temps attendu',
        confiance: 'élevée',
        delta: delta.toFixed(2)
      };
    }

    res.json(prevision);
  });
});
// ===============================
// HEALTH CHECK
// ===============================

app.get('/api/health', (req, res) => {

  res.json({
    status: 'ok',
    uptime: process.uptime()
  });
});

// ===============================
// WEBSOCKET
// ===============================

wss.on('connection', (ws, req) => {

  console.log(
    `[WS] Client connecté : ${req.socket.remoteAddress}`
  );

  // Dernière donnée
  if (lastData.timestamp) {

    ws.send(JSON.stringify({
      type: 'data',
      data: lastData,
      alerts: checkAlerts(lastData)
    }));
  }

  // Historique récent
  if (history.length > 0) {

    ws.send(JSON.stringify({
      type: 'history',
      data: history.slice(-48)
    }));
  }

  ws.on('close', () => {
    console.log('[WS] Client déconnecté');
  });
});

// ===============================
// SYSTÈME D'ALERTES
// ===============================

function checkAlerts(data) {

  const alerts = [];

  if (data.temperature > 35) {
    alerts.push({
      level: 'danger',
      msg: `Température critique : ${data.temperature}°C`
    });
  }

  if (data.temperature < 0) {
    alerts.push({
      level: 'danger',
      msg: `Gel détecté : ${data.temperature}°C`
    });
  }

  if (data.humidite > 90) {
    alerts.push({
      level: 'warn',
      msg: `Humidité élevée : ${data.humidite}%`
    });
  }

  if (data.vitesse_vent > 50) {
    alerts.push({
      level: 'warn',
      msg: `Vent fort : ${data.vitesse_vent} km/h`
    });
  }

  if (data.pression_atmospherique < 990) {
    alerts.push({
      level: 'warn',
      msg: `Dépression atmosphérique : ${data.pression_atmospherique} hPa`
    });
  }

  return alerts;
}

// ===============================
// DÉMARRAGE SERVEUR
// ===============================

server.listen(PORT, () => {

  console.log('\n🌦️ ===============================');
  console.log(`🚀 Serveur lancé sur :`);
  console.log(`http://localhost:${PORT}`);
  console.log('🌦️ ===============================\n');
});