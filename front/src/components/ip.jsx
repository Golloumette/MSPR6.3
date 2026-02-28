// ip.jsx
// Ce module exporte l'adresse IP de l'API, récupérée via
// une variable d'environnement (REACT_APP_API_IP). Cela évite
// de maintenir un fichier avec des adresses codées en dur.
// Le fichier est désormais déprécié ; il est préférable
// d'accéder directement à process.env.REACT_APP_API_IP.

const ip = process.env.REACT_APP_API_IP || "127.0.0.1"; // fallback locale

export default ip;

