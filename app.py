import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from api.routes import router
from config import Config
from services.be2_scan_service import BE2ScanService
from services.pipeline.be2_controller import ScanConflict


def create_app(service=None, hmac_secret=None):
    secret = hmac_secret if hmac_secret is not None else Config.COORDINATOR_HMAC_SECRET

    @asynccontextmanager
    async def lifespan(app):
        if len(secret) < 32:
            raise RuntimeError(
                "COORDINATOR_HMAC_SECRET must contain at least 32 characters"
            )
        if Config.MAX_CONCURRENT_SCANS < 1 or Config.RESOURCE_WORKERS < 1:
            raise RuntimeError("Worker counts must be positive")
        app.state.scans = service or BE2ScanService()
        app.state.hmac_secret = secret
        app.state.scans.start()
        try:
            yield
        finally:
            app.state.scans.close()

    app = FastAPI(title=Config.APP_TITLE, version=Config.APP_VERSION, lifespan=lifespan)
    app.include_router(router)

    @app.exception_handler(ScanConflict)
    async def conflict(request: Request, exc: ScanConflict):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(KeyError)
    async def missing(request: Request, exc: KeyError):
        return JSONResponse(status_code=404, content={"detail": "Scan not found"})

    @app.exception_handler(RequestValidationError)
    async def validation(request: Request, exc: RequestValidationError):
        # Pydantic's default error response includes input values (including tokens).
        errors = [
            {"loc": e["loc"], "type": e["type"], "msg": e["msg"]} for e in exc.errors()
        ]
        return JSONResponse(status_code=422, content={"detail": errors})

    @app.get("/")
    def index():
        return {
            "service": "hubspot_be2",
            "documentation": "/docs",
            "health": "/api/health",
        }

    @app.get("/api/health")
    def health(request: Request):
        with request.app.state.scans.controller.connect() as db:
            db.execute("SELECT 1")
        return {"status": "healthy", "state": "healthy"}

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "5200"))
    )
