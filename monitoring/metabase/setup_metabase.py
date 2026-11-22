import json
import os
import time
import urllib.error
import urllib.request

METABASE_URL = os.getenv("METABASE_URL", "http://metabase:3000")
ADMIN_EMAIL = os.getenv("MB_ADMIN_EMAIL", "admin@nyc-taxi.local")
ADMIN_PASSWORD = os.getenv("MB_ADMIN_PASSWORD", "NycTaxi2026!")
WAREHOUSE_FILE = "/data/warehouse/nyc_taxi.duckdb"
DB_DISPLAY_NAME = "NYC Taxi Data Warehouse"


def http_json(
    method: str,
    path: str,
    payload: dict | None = None,
    session_token: str | None = None,
) -> dict:
    url = f"{METABASE_URL}{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers: dict[str, str] = {}
    if data is not None:
        headers["Content-Type"] = "application/json"
    if session_token:
        headers["X-Metabase-Session"] = session_token

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} en {method} {path}: {err_body}") from exc


def wait_for_metabase(max_wait_sec: int = 300) -> str | None:
    start = time.time()
    deadline = start + max_wait_sec
    last_log = 0.0

    while time.time() < deadline:
        try:
            health = http_json("GET", "/api/health")
            if health.get("status") == "ok":
                props = http_json("GET", "/api/session/properties")
                return props.get("setup-token")
        except Exception:
            pass

        elapsed = time.time() - start
        if elapsed - last_log >= 15:
            print(
                f"[METABASE SETUP] Esperando inicialización de Metabase ({int(elapsed)}s)..."
            )
            last_log = elapsed
        time.sleep(3)

    raise TimeoutError("Metabase no respondió dentro del tiempo esperado (300s).")


def ensure_admin_user(setup_token: str | None) -> None:
    if not setup_token:
        print("[METABASE SETUP] Usuario administrador ya existente.")
        return

    payload = {
        "token": setup_token,
        "user": {
            "first_name": "Admin",
            "last_name": "TLC",
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
        },
        "database": None,
        "prefs": {
            "site_name": "NYC Taxi BI",
            "allow_tracking": False,
        },
    }
    http_json("POST", "/api/setup", payload)
    print(f"[METABASE SETUP] Usuario administrador '{ADMIN_EMAIL}' creado.")


def ensure_duckdb_connected() -> None:
    if not os.path.exists(WAREHOUSE_FILE):
        print(
            f"[METABASE SETUP] '{WAREHOUSE_FILE}' aún no existe. "
            "Ejecuta 'make all' o 'make trigger' y vuelve a correr 'docker compose up -d metabase-setup'."
        )
        return

    session = http_json(
        "POST",
        "/api/session",
        {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    session_token = session["id"]

    dbs_resp = http_json("GET", "/api/database", session_token=session_token)
    existing_dbs = (
        dbs_resp.get("data", dbs_resp) if isinstance(dbs_resp, dict) else dbs_resp
    )

    for db in existing_dbs:
        if db.get("name") == DB_DISPLAY_NAME or db.get("engine") == "duckdb":
            print(
                f"[METABASE SETUP] Base '{db.get('name')}' ya conectada (id={db.get('id')})."
            )
            http_json(
                "POST",
                f"/api/database/{db['id']}/sync_schema",
                session_token=session_token,
            )
            return

    created = http_json(
        "POST",
        "/api/database",
        {
            "name": DB_DISPLAY_NAME,
            "engine": "duckdb",
            "details": {
                "database_file": WAREHOUSE_FILE,
                "read_only": True,
            },
            "auto_run_queries": True,
            "is_full_sync": True,
        },
        session_token=session_token,
    )
    db_id = created.get("id")
    print(
        f"[METABASE SETUP] Base '{DB_DISPLAY_NAME}' conectada en modo read-only (id={db_id})."
    )
    if db_id:
        http_json(
            "POST", f"/api/database/{db_id}/sync_schema", session_token=session_token
        )


def main() -> None:
    setup_token = wait_for_metabase()
    ensure_admin_user(setup_token)
    ensure_duckdb_connected()


if __name__ == "__main__":
    main()
