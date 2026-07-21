# Gabarito de anomalias injetadas

Uso: validar se o pipeline Bronze/Silver detecta e trata corretamente cada caso.

## Arquivos faltantes (dia sem CSV gerado)
- 2026-04-11
- 2026-05-29

## Duplicidade injetada (~8% dos registros do dia)
- 2026-04-02
- 2026-04-16
- 2026-05-13
- 2026-06-06
- 2026-06-12

## Corte de fuso/dia (corridas 23:45-23:59 gravadas no dia seguinte)
- 2026-04-27 -> registros aparecem em 2026-04-28
- 2026-05-03 -> registros aparecem em 2026-05-04
- 2026-05-11 -> registros aparecem em 2026-05-12
- 2026-05-16 -> registros aparecem em 2026-05-17
- 2026-05-18 -> registros aparecem em 2026-05-19
