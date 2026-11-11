# NYC Yellow Taxi Batch Pipeline (TLC)

Pipeline batch reproducible de ingeniería de datos para la ingesta, validación de contrato, compuerta de calidad con cuarentena, almacenamiento columnar particionado y modelado dimensional estrella sobre los viajes de taxis amarillos de Nueva York (TLC).

---

## Integrantes del grupo
- Brahim, Camila Ayelén - 56066
- Breslauer, León - 56340

## Instrucciones de Instalación y Ejecución

### Requisitos del Sistema
* Python 3.10 o superior (compatible con Python 3.11/3.12).
* Entorno virtual recomendado.
* Hardware mínimo: 8 GB de RAM (el pipeline opera con pico < 1.5 GB de RAM).

### Preparación del Entorno
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
