"""
generate_synthetic_dim_unit_snapshots.py

Gera snapshots periódicos completos da dimensão de unidade (equivalente
sintético ao GLD_DIM_FILIAL real), simulando extrações completas de um
sistema de origem (ex: Teknisa) em datas distintas, com mudanças de
atributo propositais entre snapshots para exercitar SCD Type 2 na Gold.

Padrão replicado: dimensão é reconstruída a cada snapshot; a camada Gold
é responsável por comparar contra o estado corrente e decidir o que
gerou uma nova versão (SCD2), exatamente como o pipeline real faz via
MERGE contra GLD_DIM_FILIAL.
"""

import csv
import os
from datetime import datetime

OUTPUT_DIR = "/mnt/user-data/outputs/uber_synthetic_data_dim_unit"

# Estado inicial (snapshot 1 - dia 2026-04-01)
UNITS_V1 = [
    {"unit_code": "01001", "unit_name": "Unidade Caxias do Sul - Centro", "region": "Serra Gaucha", "cost_center": "CC-100-RH"},
    {"unit_code": "01002", "unit_name": "Unidade Porto Alegre - Matriz", "region": "Metropolitana", "cost_center": "CC-200-FIN"},
    {"unit_code": "01003", "unit_name": "Unidade Curitiba - Batel", "region": "Sul", "cost_center": "CC-300-COM"},
    {"unit_code": "02001", "unit_name": "Unidade Sao Paulo - Paulista", "region": "Sudeste", "cost_center": "CC-400-TI"},
    {"unit_code": "02002", "unit_name": "Unidade Uberaba - Centro", "region": "Triangulo Mineiro", "cost_center": "CC-500-OPS"},
]

SNAPSHOT_DATES = [
    datetime(2026, 4, 1),
    datetime(2026, 5, 1),
    datetime(2026, 5, 31),
]

FIELDNAMES = ["unit_code", "unit_name", "region", "cost_center", "snapshot_date"]


def apply_change(units, unit_code, **changes):
    """Retorna uma nova lista de units com o registro alterado (imutabilidade proposital)."""
    new_units = []
    for u in units:
        if u["unit_code"] == unit_code:
            updated = u.copy()
            updated.update(changes)
            new_units.append(updated)
        else:
            new_units.append(u.copy())
    return new_units


def write_snapshot(units, snapshot_date, output_dir):
    filename = f"dim_unit_snapshot-{snapshot_date.strftime('%Y_%m_%d')}.csv"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for u in units:
            row = u.copy()
            row["snapshot_date"] = snapshot_date.strftime("%Y-%m-%d")
            writer.writerow(row)
    return filename


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Snapshot 1: estado inicial (2026-04-01)
    snapshot_1 = UNITS_V1
    f1 = write_snapshot(snapshot_1, SNAPSHOT_DATES[0], OUTPUT_DIR)

    # Mudança 1: unidade 01001 é transferida de RH (CC-100-RH) para TI (CC-400-TI)
    # -> simula transferência de centro de custo responsável, mantendo nome/região
    snapshot_2 = apply_change(
        snapshot_1, "01001", cost_center="CC-400-TI"
    )
    f2 = write_snapshot(snapshot_2, SNAPSHOT_DATES[1], OUTPUT_DIR)

    # Mudança 2: unidade 02002 é renomeada e sua região é reclassificada
    # -> simula reorganização administrativa (nome + região mudam, unit_code mantém)
    snapshot_3 = apply_change(
        snapshot_2, "02002",
        unit_name="Unidade Uberaba - Zona Sul",
        region="Sudeste",
    )
    f3 = write_snapshot(snapshot_3, SNAPSHOT_DATES[2], OUTPUT_DIR)

    # Gabarito para validação posterior do SCD2 na Gold
    gabarito_path = os.path.join(OUTPUT_DIR, "DIM_UNIT_ANOMALIES.md")
    with open(gabarito_path, "w", encoding="utf-8") as f:
        f.write("# Gabarito de mudancas historicas - dim_unit\n\n")
        f.write("Uso: validar se o MERGE de SCD2 na Gold versionou corretamente.\n\n")
        f.write(f"## Snapshots gerados\n- {f1}\n- {f2}\n- {f3}\n\n")
        f.write("## Mudanca 1 (efetiva a partir de 2026-05-01)\n")
        f.write("- unit_code: 01001\n")
        f.write("- campo alterado: cost_center\n")
        f.write("- de: CC-100-RH -> para: CC-400-TI\n")
        f.write("- expectativa SCD2: 2 versoes para 01001, a antiga com valid_to = 2026-04-30 "
                 "e is_current = false, a nova com valid_from = 2026-05-01 e is_current = true\n\n")
        f.write("## Mudanca 2 (efetiva a partir de 2026-05-31)\n")
        f.write("- unit_code: 02002\n")
        f.write("- campos alterados: unit_name, region\n")
        f.write("- unit_name: 'Unidade Uberaba - Centro' -> 'Unidade Uberaba - Zona Sul'\n")
        f.write("- region: 'Triangulo Mineiro' -> 'Sudeste'\n")
        f.write("- expectativa SCD2: 2 versoes para 02002, versionadas na mesma data\n\n")
        f.write("## Unidades sem mudanca (controle negativo)\n")
        f.write("- 01002, 01003, 02001 -> devem ter apenas 1 versao (is_current = true) "
                 "em todos os snapshots\n")

    print(f"Snapshots gerados: {f1}, {f2}, {f3}")
    print(f"Diretorio de saida: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
