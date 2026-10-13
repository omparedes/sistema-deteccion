"""API base. No contiene aún ingesta de eventos ni conexión a DB."""
from fastapi import FastAPI

app = FastAPI(title="API de detección de anomalías", version="0.1.0")


@app.get("/health", tags=["system"])
def health() -> dict[str, str]:
    return {"status": "ok", "phase": "scaffold"}
