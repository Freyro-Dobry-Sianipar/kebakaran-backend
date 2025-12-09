# ==========================================================
#  app.py — FINAL RAILWAY VERSION (No Pump, Stable)
# ==========================================================
import os
from datetime import datetime
from collections import deque
from flask import Flask, request, jsonify
import joblib
import numpy as np
import csv
from flask_cors import CORS

# ==========================================================
# CONFIG
# ==========================================================
MODEL_FILE = "model_random_forest.pkl"
ENCODER_FILE = "label_encoder.pkl"

# Railway has read-only file system EXCEPT /tmp
LOG_CSV = "/tmp/fire_data.csv"

MAX_HISTORY = 240

app = Flask(__name__)
CORS(app, origins="*")

# Load ML model
model = joblib.load(MODEL_FILE)
label_encoder = joblib.load(ENCODER_FILE)

# Memory buffer
history = deque(maxlen=MAX_HISTORY)

# Buzzer state
buzzer_state = {"mode": "OFF"}  # OFF / WARN / DANGER

# Create CSV in /tmp if not exist
if not os.path.exists(LOG_CSV):
    with open(LOG_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "temp", "hum", "gas", "flame", "status"])


def append_csv(entry):
    """Append log into /tmp/fire_data.csv"""
    try:
        with open(LOG_CSV, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                entry["timestamp"],
                entry["temp"],
                entry["hum"],
                entry["gas"],
                entry["flame"],
                entry["status"]
            ])
    except Exception as e:
        print("CSV Write Error:", e)


# ==========================================================
# ROUTES
# ==========================================================
@app.route("/", methods=["GET"])
def home():
    return jsonify({"message": "Backend Running OK on Railway"})


@app.route("/api/predict", methods=["POST"])
def api_predict():
    data = request.get_json(silent=True) or {}

    try:
        temp = float(data["temp"])
        hum = float(data["hum"])
        gas = float(data["gas"])
        flame = float(data["flame"])
    except:
        return jsonify({"error": "Invalid input"}), 400

    sample = np.array([[temp, hum, gas, flame]])
    pred = model.predict(sample)[0]
    status = label_encoder.inverse_transform([pred])[0].upper()

    entry = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "temp": temp,
        "hum": hum,
        "gas": gas,
        "flame": flame,
        "status": status
    }

    history.append(entry)
    append_csv(entry)

    return jsonify({"status": status, "entry": entry})


@app.route("/api/save-data", methods=["POST"])
def api_save():
    data = request.form.to_dict()

    try:
        temp = float(data["temperature"])
        hum = float(data["humidity"])
        gas = float(data["gas"])
        flame = float(data["flame"])
        status = data.get("status", "").upper()
    except:
        return jsonify({"error": "Invalid input"}), 400

    entry = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "temp": temp,
        "hum": hum,
        "gas": gas,
        "flame": flame,
        "status": status
    }

    history.append(entry)
    append_csv(entry)

    return jsonify({"saved": True})


@app.route("/latest", methods=["GET"])
def latest():
    return jsonify({
        "last": history[-1] if history else {},
        "history": list(history)
    })


# ==========================================================
# BUZZER CONTROL
# ==========================================================
@app.route("/buzzer/<mode>", methods=["POST"])
def buzzer_set(mode):
    mode = mode.upper()
    if mode not in ("OFF", "WARN", "DANGER"):
        return jsonify({"error": "Invalid mode"}), 400

    buzzer_state["mode"] = mode
    return jsonify({"buzzer": buzzer_state})


@app.route("/device/commands", methods=["GET"])
def get_commands():
    return jsonify({
        "buzzer": buzzer_state["mode"]
    })


# ==========================================================
# RUN
# ==========================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
