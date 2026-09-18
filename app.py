import logging
import os

from flask import Flask

from api.routes import bp
from models.models import init_db


def create_app():
    app = Flask(__name__)
    app.register_blueprint(bp, url_prefix="/api")
    init_db()

    @app.get("/")
    def index():
        return {
            "service": "hubspot_deals",
            "version": "1.0.0",
            "documentation": "/docs/",
            "health": "/api/health",
        }

    @app.get("/docs/")
    def docs():
        return """<!doctype html>
<html>
<head>
    <title>HubSpot Deals ETL API</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
        window.onload = () => SwaggerUIBundle({
            url: "/openapi.json",
            dom_id: "#swagger-ui"
        });
    </script>
</body>
</html>"""

    @app.get("/openapi.json")
    def openapi():
        with open("docs/openapi.json") as f:
            return app.response_class(f.read(), mimetype="application/json")

    return app


app = create_app()


if __name__ == "__main__":
    app.run(
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "5200")),
        debug=False,
    )
