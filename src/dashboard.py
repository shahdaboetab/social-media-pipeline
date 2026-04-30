from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
import json

app = Flask(__name__)

consumer = KafkaConsumer(
    "wordcount-stream",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest"
)

latest_counts = {}

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/data")
def data():
    return jsonify(latest_counts)

def listen_kafka():
    global latest_counts
    for msg in consumer:
        row = msg.value
        latest_counts[row["word"]] = row["count"]

import threading
threading.Thread(target=listen_kafka, daemon=True).start()

if __name__ == "__main__":
    app.run(port=5000)