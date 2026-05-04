import json
import os
from flask import Flask, render_template, jsonify

app = Flask(__name__)

BASE = os.path.dirname(__file__)
DATA = os.path.join(BASE, "data")

def load_json(filename):
    path = os.path.join(DATA, filename)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/data")
def get_data():
    spark = load_json("spark_results.json")
    live  = load_json("live_api.json")
    news  = load_json("live_rss.json")
    return jsonify({"spark": spark, "live": live, "news": news})

@app.route("/api/forecast")
def get_forecast():
    forecast = load_json("forecast.json")
    return jsonify(forecast)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
