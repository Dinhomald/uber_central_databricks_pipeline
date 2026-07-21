"""
generate_synthetic_uber_data.py

Gera dataset sintético replicando a estrutura de arquivos do pipeline
Uber Central (daily_trips-YYYY_MM_DD.csv), com anomalias injetadas de
propósito para exercitar tratamento de dados no Databricks (Auto Loader,
Delta Lake, MERGE / SCD2).

Nenhum dado real da empresa é utilizado. Todos os valores (rotas, custos,
centros de custo, filiais) são gerados aleatoriamente com seed fixa para
reprodutibilidade.

Anomalias injetadas (documentadas em ANOMALIES.md ao final da execução):
  1. Arquivo faltante   -> dia inteiro sem arquivo gerado (simula falha de carga/SFTP)
  2. Duplicidade        -> subconjunto de registros duplicados dentro do arquivo do dia
  3. Corte de fuso/dia   -> corridas entre 23:45 e 23:59 gravadas no arquivo do dia
                            seguinte (replica o bug real de day-boundary cutoff)
"""

import csv
import os
import random
import uuid
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
random.seed(42)

OUTPUT_DIR = "/mnt/user-data/outputs/uber_synthetic_data"
NUM_DAYS = 90
START_DATE = datetime(2026, 4, 1)

MIN_TRIPS_PER_DAY = 40
MAX_TRIPS_PER_DAY = 150

COST_CENTERS = ["CC-100-RH", "CC-200-FIN", "CC-300-COM", "CC-400-TI", "CC-500-OPS"]
# Replica o padrão IDENT = EMPRESA||UNIDADE do DW real, mas com valores fictícios
UNIT_CODES = ["01001", "01002", "01003", "02001", "02002"]
CITIES = ["Caxias do Sul/RS", "Porto Alegre/RS", "São Paulo/SP", "Curitiba/PR", "Uberaba/MG"]
VEHICLE_TYPES = ["UberX", "Comfort", "Black"]
PAYMENT_STATUS = ["Aprovado", "Pendente", "Cancelado"]

FIELDNAMES = [
    "trip_uuid", "request_date", "pickup_datetime", "dropoff_datetime",
    "employee_id", "cost_center", "unit_code", "pickup_city", "dropoff_city",
    "distance_km", "duration_min", "fare_value", "currency",
    "payment_status", "vehicle_type",
]

# Percentual/quantidade de dias afetados por cada anomalia
PCT_MISSING_DAYS = 0.03      # ~3 dias em 90
PCT_DUPLICATE_DAYS = 0.06    # ~5 dias em 90
PCT_BOUNDARY_DAYS = 0.06     # ~5 dias em 90


def generate_trip(request_date: datetime, force_late_night: bool = False) -> dict:
    """Gera uma corrida sintética para uma data de referência."""
    if force_late_night:
        # Corrida entre 23:45:00 e 23:59:59 -> candidata a bug de cutoff
        pickup_dt = request_date.replace(
            hour=23, minute=random.randint(45, 59), second=random.randint(0, 59)
        )
    else:
        pickup_dt = request_date.replace(
            hour=random.randint(0, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        )

    duration_min = random.randint(5, 60)
    dropoff_dt = pickup_dt + timedelta(minutes=duration_min)

    return {
        "trip_uuid": str(uuid.uuid4()),
        "request_date": request_date.strftime("%Y-%m-%d"),
        "pickup_datetime": pickup_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "dropoff_datetime": dropoff_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "employee_id": f"EMP{random.randint(1000, 1300)}",
        "cost_center": random.choice(COST_CENTERS),
        "unit_code": random.choice(UNIT_CODES),
        "pickup_city": random.choice(CITIES),
        "dropoff_city": random.choice(CITIES),
        "distance_km": round(random.uniform(1.5, 45.0), 2),
        "duration_min": duration_min,
        "fare_value": round(random.uniform(8.0, 120.0), 2),
        "currency": "BRL",
        "payment_status": random.choices(
            PAYMENT_STATUS, weights=[0.85, 0.10, 0.05]
        )[0],
        "vehicle_type": random.choice(VEHICLE_TYPES),
    }


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_days = [START_DATE + timedelta(days=i) for i in range(NUM_DAYS)]

    num_missing = max(1, int(NUM_DAYS * PCT_MISSING_DAYS))
    num_duplicate = max(1, int(NUM_DAYS * PCT_DUPLICATE_DAYS))
    num_boundary = max(1, int(NUM_DAYS * PCT_BOUNDARY_DAYS))

    # Sorteia dias afetados por cada anomalia, sem sobreposição com "missing"
    candidate_days = all_days.copy()
    random.shuffle(candidate_days)

    missing_days = set(candidate_days[:num_missing])
    remaining = candidate_days[num_missing:]
    duplicate_days = set(remaining[:num_duplicate])
    remaining = remaining[num_duplicate:]
    boundary_days = set(remaining[:num_boundary])

    # Estrutura em memória: day -> lista de registros (permite mover registros
    # de "corte de fuso" para o arquivo do dia seguinte antes de gravar)
    day_records = {day: [] for day in all_days}

    for day in all_days:
        n_trips = random.randint(MIN_TRIPS_PER_DAY, MAX_TRIPS_PER_DAY)
        # Uma fração pequena das corridas do dia nasce perto da meia-noite
        n_late_night = max(1, int(n_trips * 0.05)) if day in boundary_days else 0

        for _ in range(n_trips - n_late_night):
            day_records[day].append(generate_trip(day))

        for _ in range(n_late_night):
            trip = generate_trip(day, force_late_night=True)
            if day in boundary_days:
                # BUG replicado: corrida das 23:45-23:59 é gravada no arquivo
                # do dia seguinte em vez do dia correto (corte de fuso/dia)
                next_day = day + timedelta(days=1)
                if next_day in day_records:
                    day_records[next_day].append(trip)
                else:
                    day_records[day].append(trip)
            else:
                day_records[day].append(trip)

    # Injeta duplicidade (após o corte de fuso, para não duplicar já duplicado)
    for day in duplicate_days:
        records = day_records[day]
        if not records:
            continue
        n_dupes = max(1, int(len(records) * 0.08))
        dupes = random.sample(records, min(n_dupes, len(records)))
        day_records[day].extend(dupes)

    # Grava os arquivos, pulando os dias marcados como "arquivo faltante"
    written_files = []
    for day in all_days:
        if day in missing_days:
            continue
        filename = f"daily_trips-{day.strftime('%Y_%m_%d')}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(day_records[day])
        written_files.append(filename)

    # Documenta o gabarito das anomalias para validação posterior do pipeline
    anomalies_path = os.path.join(OUTPUT_DIR, "ANOMALIES.md")
    with open(anomalies_path, "w", encoding="utf-8") as f:
        f.write("# Gabarito de anomalias injetadas\n\n")
        f.write("Uso: validar se o pipeline Bronze/Silver detecta e trata corretamente cada caso.\n\n")
        f.write("## Arquivos faltantes (dia sem CSV gerado)\n")
        for d in sorted(missing_days):
            f.write(f"- {d.strftime('%Y-%m-%d')}\n")
        f.write("\n## Duplicidade injetada (~8% dos registros do dia)\n")
        for d in sorted(duplicate_days):
            f.write(f"- {d.strftime('%Y-%m-%d')}\n")
        f.write("\n## Corte de fuso/dia (corridas 23:45-23:59 gravadas no dia seguinte)\n")
        for d in sorted(boundary_days):
            f.write(f"- {d.strftime('%Y-%m-%d')} -> registros aparecem em {(d + timedelta(days=1)).strftime('%Y-%m-%d')}\n")

    print(f"Arquivos gerados: {len(written_files)} de {NUM_DAYS} dias esperados")
    print(f"Dias com arquivo faltante: {len(missing_days)}")
    print(f"Dias com duplicidade: {len(duplicate_days)}")
    print(f"Dias com corte de fuso: {len(boundary_days)}")
    print(f"Diretório de saída: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
