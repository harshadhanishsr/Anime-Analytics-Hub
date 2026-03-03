"""
app/app.py
----------
Flask dashboard — reads from PostgreSQL via queries.py and renders HTML.
Start with: python app/app.py → http://localhost:5000
"""

import os
import sys
from app import queries
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from flask import Flask, render_template


app = Flask(__name__)


@app.route("/")
def dashboard():
    return render_template("index.html",
        stats          = queries.summary_stats(),
        top_rated      = queries.top_rated(10),
        most_popular   = queries.most_popular(10),
        type_breakdown = queries.type_breakdown(),
        health         = queries.pipeline_health(),
    )


@app.route("/top")
def top():
    return render_template("top.html", anime=queries.top_rated(50))


@app.route("/popular")
def popular():
    return render_template("popular.html", anime=queries.most_popular(50))


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
