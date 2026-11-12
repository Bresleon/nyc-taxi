"""
Test de reconciliación de conteos (consigna: filas leídas == aceptadas + cuarentena).
"""

import pytest

from src.step3_quality import reconcile_records


def test_reconciliation_invariant_success():
    """Si la suma coincide, la reconciliación pasa sin errores."""
    # 5.000 leídas = 4.750 aceptadas + 250 en cuarentena
    reconcile_records(
        period_label="2025-03",
        rows_read=5000,
        rows_accepted=4750,
        rows_quarantined=250,
    )


def test_reconciliation_invariant_failure_raises_exception():
    """Si se pierde una fila, el pipeline se detiene con RuntimeError."""
    # 5.000 leídas != 4.750 aceptadas + 249 en cuarentena (falta 1 fila)
    with pytest.raises(RuntimeError) as exc_info:
        reconcile_records(
            period_label="2025-03",
            rows_read=5000,
            rows_accepted=4750,
            rows_quarantined=249,
        )

    assert "ERROR DE RECONCILIACIÓN" in str(exc_info.value)
    assert "Diferencia: 1" in str(exc_info.value)
