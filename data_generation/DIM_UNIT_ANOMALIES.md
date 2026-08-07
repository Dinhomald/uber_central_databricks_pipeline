# Gabarito de mudancas historicas - dim_unit

Uso: validar se o MERGE de SCD2 na Gold versionou corretamente.

## Snapshots gerados
- dim_unit_snapshot-2026_04_01.csv
- dim_unit_snapshot-2026_05_01.csv
- dim_unit_snapshot-2026_05_31.csv

## Mudanca 1 (efetiva a partir de 2026-05-01)
- unit_code: 01001
- campo alterado: cost_center
- de: CC-100-RH -> para: CC-400-TI
- expectativa SCD2: 2 versoes para 01001, a antiga com valid_to = 2026-04-30 e is_current = false, a nova com valid_from = 2026-05-01 e is_current = true

## Mudanca 2 (efetiva a partir de 2026-05-31)
- unit_code: 02002
- campos alterados: unit_name, region
- unit_name: 'Unidade Uberaba - Centro' -> 'Unidade Uberaba - Zona Sul'
- region: 'Triangulo Mineiro' -> 'Sudeste'
- expectativa SCD2: 2 versoes para 02002, versionadas na mesma data

## Unidades sem mudanca (controle negativo)
- 01002, 01003, 02001 -> devem ter apenas 1 versao (is_current = true) em todos os snapshots
