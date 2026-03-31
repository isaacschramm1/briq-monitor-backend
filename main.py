"""
Briq Monitor Backend
Scraper + Alert service para mercadosecundario.briq.mx
Deploy en Railway o Fly.io
"""

import os
import json
import time
import logging
import tempfile
import schedule
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, messaging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)

# ── Firebase init ──────────────────────────────────────────────────────────────
def init_firebase():
    # Opción 1: variable de entorno (Railway)
    cred_json = os.environ.get("FIREBASE_CREDENTIALS_JSON")
    if cred_json:
        try:
            cred_dict = json.loads(cred_json)
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
            log.info("Firebase inicializado desde variable de entorno ✓")
            return True
        except Exception as e:
            log.error(f"Error inicializando Firebase desde env var: {e}")

    # Opción 2: archivo local (desarrollo)
    cred_path = "firebase-credentials.json"
    if os.path.exists(cred_path):
        try:
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred)
            log.info("Firebase inicializado desde archivo local ✓")
            return True
        except Exception as e:
            log.error(f"Error inicializando Firebase desde archivo: {e}")

    log.warning("Firebase NO inicializado — notificaciones desactivadas")
    return False

FIREBASE_OK = init_firebase()

# ── Configuración persistente (reglas por proyecto) ───────────────────────────
RULES_FILE = "rules.json"

DEFAULT_RULES = {
    "narvarte":     {"name": "Edificio Local Narvarte",       "threshold": 29.0,  "enabled": True},
    "sanmiguel":    {"name": "Hotel San Miguel de Allende",   "threshold": 22.0,  "enabled": True},
    "sanah":        {"name": "Sanâh Tulum",                   "threshold": 20.0,  "enabled": True},
    "ubika_ref":    {"name": "UBIKA El Refugio",              "threshold": 20.0,  "enabled": True},
    "ubika_mar":    {"name": "UBIKA Mariano Otero",           "threshold": 18.0,  "enabled": True},
    "wesley":       {"name": "The Wesley",                    "threshold": 20.0,  "enabled": True},
    "jacksonville": {"name": "Jacksonville Hotel",            "threshold": 15.0,  "enabled": True},
    "multitex":     {"name": "Multi-Tex Platform",            "threshold": 12.0,  "enabled": True},
    "alarcon":      {"name": "Alarcón PerSe",                 "threshold": 1.0,   "enabled": True},
    "alamos":       {"name": "Álamos Lifestyle Center",       "threshold": 10.0,  "enabled": True},
    "clinton":      {"name": "14 Clinton Street",             "threshold": 5.0,   "enabled": True},
    "nextipark":    {"name": "Nextipark",                     "threshold": 2.0,   "enabled": True},
    "salara":       {"name": "Salara Hotel",                  "threshold": 8.0,   "enabled": True},
    "agge":         {"name": "Fondo AGGE",                    "threshold": -0.5,  "enabled": True},
}

def load_rules():
    if os.path.exists(RULES_FILE):
        with open(RULES_FILE) as f:
            return json.load(f)
    return DEFAULT_RULES.copy()

def save_rules(rules):
    with open(RULES_FILE, "w") as f:
        json.dump(rules, f, ensure_ascii=False, indent=2)

# ── FCM token del dispositivo ─────────────────────────────────────────────────
TOKEN_FILE = "device_token.txt"

def get_device_token():
    if os.path.exists(TOKEN_FILE):
        return open(TOKEN_FILE).read().strip()
    return os.environ.get("DEVICE_FCM_TOKEN", "")

def save_device_token(token):
    with open(TOKEN_FILE, "w") as f:
        f.write(token)

# ── Ofertas ya notificadas (evita spam) ───────────────────────────────────────
NOTIFIED_FILE = "notified.json"

def load_notified():
    if os.path.exists(NOTIFIED_FILE):
        with open(NOTIFIED_FILE) as f:
            return set(json.load(f))
    return set()

def save_notified(ids: set):
    with open(NOTIFIED_FILE, "w") as f:
        json.dump(list(ids), f)

# ── Mapeo nombre de proyecto → clave de regla ─────────────────────────────────
PROJECT_KEY_MAP = {
    "Edificio Local Narvarte":       "narvarte",
    "Hotel San Miguel de Allende":   "sanmiguel",
    "Sanâh Tulum":                   "sanah",
    "UBIKA El Refugio":              "ubika_ref",
    "UBIKA Mariano Otero":           "ubika_mar",
    "The Wesley":                    "wesley",
    "Jacksonville Hotel":            "jacksonville",
    "Multi-Tex Platform":            "multitex",
    "Alarcón PerSe":                 "alarcon",
    "Álamos Lifestyle Center":       "alamos",
    "14 Clinton Street":             "clinton",
    "Nextipark":                     "nextipark",
    "Salara Hotel":                  "salara",
    "Fondo AGGE":                    "agge",
}

# ── Scraper ───────────────────────────────────────────────────────────────────
def scrape_offers():
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/120 Mobile Safari/537.36"
    }
    try:
        resp = requests.get("https://mercadosecundario.briq.mx", headers=headers, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"Error al descargar la página: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    offers = []

    for h2 in soup.find_all("h2"):
        project_name = h2.get_text(strip=True)
        table = h2.find_next("table")
        if not table:
            continue

        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) < 6:
                continue
            offer_id   = cols[0].get_text(strip=True)
            diferencia = cols[4].get_text(strip=True)
            precio_txt = cols[5].get_text(strip=True)
            link_tag   = cols[5].find("a", href=True)
            link       = link_tag["href"] if link_tag else ""

            discount = parse_discount(diferencia)
            price    = parse_price(precio_txt)

            if discount is None:
                continue

            offers.append({
                "id":       offer_id,
                "project":  project_name,
                "discount": discount,
                "price":    price,
                "link":     link,
            })

    log.info(f"Ofertas encontradas: {len(offers)}")
    return offers

def parse_discount(texto: str):
    import re
    m = re.search(r"([\d.]+)%\s*(abajo|arriba)", texto, re.IGNORECASE)
    if not m:
        return None
    val = float(m.group(1))
    if "arriba" in m.group(2).lower():
        val = -val
    return val

def parse_price(texto: str):
    import re
    m = re.search(r"\$([\d,]+\.?\d*)", texto)
    if m:
        return float(m.group(1).replace(",", ""))
    return 0.0

# ── Enviar notificación push ──────────────────────────────────────────────────
def send_push(offer: dict, threshold: float):
    if not FIREBASE_OK:
        log.warning("Firebase no disponible, no se envió push")
        return

    token = get_device_token()
    if not token:
        log.warning("No hay token FCM registrado aún")
        return

    disc_str  = f"{offer['discount']:.1f}% abajo"
    price_str = f"${offer['price']:,.0f}"

    message = messaging.Message(
        notification=messaging.Notification(
            title=f"🔔 {offer['project']}",
            body=f"{offer['id']} — {disc_str} · {price_str}",
        ),
        data={
            "offer_id":  offer["id"],
            "project":   offer["project"],
            "discount":  str(offer["discount"]),
            "price":     str(offer["price"]),
            "link":      offer["link"],
            "threshold": str(threshold),
        },
        android=messaging.AndroidConfig(
            priority="high",
            notification=messaging.AndroidNotification(
                sound="default",
                channel_id="briq_alerts",
            ),
        ),
        token=token,
    )

    try:
        resp = messaging.send(message)
        log.info(f"Push enviado: {offer['id']} ({offer['project']}) → {resp}")
    except Exception as e:
        log.error(f"Error enviando push: {e}")

# ── Ciclo principal de verificación ──────────────────────────────────────────
def check_offers():
    log.info("── Verificando ofertas ──")
    rules    = load_rules()
    notified = load_notified()
    offers   = scrape_offers()

    new_alerts = 0
    for offer in offers:
        key = PROJECT_KEY_MAP.get(offer["project"])
        if not key:
            continue
        rule = rules.get(key)
        if not rule or not rule.get("enabled", True):
            continue

        threshold = rule.get("threshold", 20.0)
        if offer["discount"] >= threshold:
            uid = f"{offer['id']}-{offer['discount']}"
            if uid not in notified:
                log.info(f"ALERTA: {offer['project']} {offer['id']} {offer['discount']}% (umbral {threshold}%)")
                send_push(offer, threshold)
                notified.add(uid)
                new_alerts += 1

    save_notified(notified)
    log.info(f"Verificación completa. Nuevas alertas: {new_alerts}")

# ── API REST ──────────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status":   "ok",
        "firebase": FIREBASE_OK,
        "token":    bool(get_device_token()),
        "time":     datetime.utcnow().isoformat()
    })

@app.route("/register-token", methods=["POST"])
def register_token():
    data  = request.get_json()
    token = data.get("token", "")
    if not token:
        return jsonify({"error": "token requerido"}), 400
    save_device_token(token)
    log.info(f"Token FCM registrado: {token[:20]}...")
    return jsonify({"ok": True})

@app.route("/rules", methods=["GET"])
def get_rules():
    return jsonify(load_rules())

@app.route("/rules", methods=["POST"])
def update_rules():
    data  = request.get_json()
    rules = load_rules()
    for key, val in data.items():
        if key in rules:
            rules[key].update(val)
    save_rules(rules)
    return jsonify({"ok": True})

@app.route("/offers", methods=["GET"])
def get_offers():
    return jsonify(scrape_offers())

@app.route("/check", methods=["POST"])
def force_check():
    check_offers()
    return jsonify({"ok": True})

# ── Scheduler (cada 2 minutos) ────────────────────────────────────────────────
def run_scheduler():
    import threading
    schedule.every(2).minutes.do(check_offers)
    def loop():
        while True:
            schedule.run_pending()
            time.sleep(10)
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    log.info("Scheduler iniciado: verificación cada 2 minutos ✓")

if __name__ == "__main__":
    run_scheduler()
    check_offers()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

