# Uber Central - Pipeline Medalhão em Databricks

Pipeline de dados construído no Databricks (PySpark + Delta Lake) replicando, em outro motor de processamento, a arquitetura que mantenho em produção em Oracle: ingestão incremental, tratamento de qualidade de dado e uma dimensão com histórico versionado (SCD Type 2).

O objetivo aqui não foi aprender Spark do zero num tutorial genérico. É pegar problemas que eu já resolvi de verdade num ambiente (incidente de reconciliação financeira, corte de dia mal tratado, dimensão sem controle de vigência) e provar que sei resolver a mesma classe de problema em Auto Loader / Delta Lake, sem depender do Oracle.

Nenhum dado real da empresa onde trabalho foi usado. Todo o dataset é sintético, gerado com anomalias propositais (ver seção "Dataset sintético" abaixo), para simular exatamente os cenários de erro que eu precisava tratar.

## Por que esse projeto existe

Minha stack principal é Oracle + PL/SQL + Apache Airflow. É sólida, mas 100% on-premise. As vagas de engenharia de dados que hoje me interessam pedem PySpark, Delta Lake e processamento distribuído — tecnologia que eu não uso no dia a dia. Em vez de fechar essa lacuna com um projeto de dataset público (tipo Titanic ou e-commerce fake), decidi portar a lógica de negócio de um pipeline que já opero de verdade (integração Uber Central da empresa) para Databricks, mantendo a estrutura de dado e os tipos de problema reais, mas com dado 100% sintético.

## Arquitetura

```
Bronze (dado bruto, como veio da fonte)
  |-- bronze_daily_trips        <- Auto Loader, ingestao incremental
  \-- bronze_dim_unit_snapshots <- leitura batch, 3 snapshots periodicos

Silver (dado limpo e reconciliado)
  \-- silver_daily_trips        <- dedup + correcao de data de evento

Gold (modelo dimensional final)
  |-- gold_dim_unit             <- SCD Type 2 (MERGE, recompute completo)
  \-- gold_fact_trips           <- fato com join temporal contra a dimensao
```

Cada camada é um notebook separado, não um notebook monolítico. Mesma lógica que já aplico no Airflow separando DAGs por fonte (`dag_teknisa`, `dag_meta`, `dag_gold_master`) — reprocessar a dimensão não deveria obrigar reprocessar o fato inteiro, e vice-versa.

Os 5 notebooks são orquestrados via Databricks Workflows, com dependência explícita em grafo, não em sequência linear: `01_bronze_trips` e `02_bronze_dim_unit` rodam em paralelo (não dependem um do outro), convergindo em `03_silver_trips` e `04_gold_dim_unit_scd2` respectivamente, que por sua vez alimentam `05_gold_fact_trips` (que depende dos dois). É o mesmo desenho de dependência que já uso com Airflow Datasets, só que declarado na UI do Job em vez de código.

![Grafo de orquestração do Job](docs/job_orchestration_graph.png)

## Dataset sintético

Os arquivos de trips (`daily_trips-YYYY_MM_DD.csv`) simulam 90 dias de operação, com três anomalias injetadas de propósito:

- **Arquivo faltante** em 2 dias, simulando falha de carga/SFTP.
- **Duplicidade** de ~8% dos registros em 5 dias, simulando reprocessamento indevido de arquivo.
- **Corte de fuso/dia**: corridas entre 23:45 e 23:59 gravadas no arquivo do dia seguinte, mas com a data do evento (`request_date`/`pickup_datetime`) mantendo o dia correto. Isso replica, de forma controlada, o mesmo tipo de divergência entre "arquivo de chegada" e "data do evento" que causou uma discrepância de R$ 141 num fechamento real que precisei investigar e corrigir.

A dimensão de unidade (`dim_unit_snapshot-YYYY_MM_DD.csv`) simula 3 extrações completas (equivalente a um extrato periódico do sistema de origem), com duas mudanças propositais de atributo entre elas — transferência de centro de custo responsável e uma reorganização (nome + região). O gabarito completo de cada anomalia está documentado em `ANOMALIES.md` e `DIM_UNIT_ANOMALIES.md`, usado para validar se cada camada tratou o problema certo.

## Decisões técnicas e por quê

**Schema explícito em vez de inferência automática.** A primeira versão do pipeline usava `inferSchema`/`inferColumnTypes` e isso corrompeu silenciosamente a chave `unit_code` — códigos como `01001` viraram o inteiro `1001`, perdendo o zero à esquerda. Sem validação linha a linha isso teria passado despercebido até o join da Gold falhar sem erro nenhum (silenciosamente não casando nada, ou pior, casando errado). Corrigi com `cloudFiles.schemaHints` no Auto Loader (força o tipo de uma coluna específica sem desabilitar a evolução de schema automática do resto) e com `StructType` explícito na leitura batch da dimensão. Isso virou regra que eu aplico agora por padrão: qualquer campo que pareça número mas seja código de negócio nunca fica com tipo inferido.

**Auto Loader nas trips, leitura batch simples na dimensão.** As trips chegam como fluxo incremental diário (ou deveriam chegar, num cenário real) — faz sentido pagar o overhead de checkpoint e schema evolution do Auto Loader. A dimensão são só 3 snapshots pontuais; usar Auto Loader ali seria complexidade sem ganho.

**Silver com `overwrite`, Gold com `MERGE`.** Silver recomputa do zero a cada execução a partir de Bronze — não existe estado histórico para preservar nessa camada, então overwrite completo é a opção mais simples e correta. A Gold da dimensão precisa de `MERGE` porque ali sim existe histórico (versões antigas não podem ser apagadas, só marcadas como não vigentes).

**Surrogate key por hash determinístico (`sha2(unit_code + valid_from)`), não `IDENTITY`.** Como o pipeline inteiro é recompute-completo (nada aqui é append incremental de verdade), uma sequência autoincremento correria o risco de gerar uma SK diferente para a mesma versão lógica cada vez que eu recriasse a tabela do zero durante o desenvolvimento. Hash é uma função pura — mesma entrada, mesma saída, sempre.

**A dimensão de unidade não passa por Silver, de propósito.** As trips têm uma Silver dedicada porque a fonte chega suja (duplicidade, corte de fuso). Os snapshots da dimensão não têm nenhum desses problemas — são extrações completas e determinísticas, sem duplicidade nem necessidade de reconciliação de data. Adicionar uma Silver nesse ramo seria uma cópia 1:1 de Bronze sem nenhuma transformação real, burocracia sem função. O tratamento de qualidade acontece direto no `MERGE` de SCD2 da Gold.

**A Gold da dimensão recria a tabela do zero a cada execução, não faz `MERGE` incremental sobre estado acumulado.** Essa decisão nasceu de um bug que eu mesmo cometi durante o desenvolvimento: a primeira versão do notebook só fazia `MERGE` contra o estado atual da tabela, sem nunca recriá-la. Rodei o notebook três vezes ao longo da sessão (um teste manual, uma execução de Job que falhou por outro motivo, e o repair run que corrigiu aquele erro) e cada execução comparou os mesmos 3 snapshots contra uma tabela que já carregava o resultado da execução anterior — o histórico foi duplicado a cada rodada. `01001` e `02002` (as únicas unidades com mudança de atributo) acabaram com 6 versões cada, quando deveriam ter 2. A fato, que faz join contra a dimensão, herdou a multiplicação: uma corrida que devia casar com 1 versão passou a casar com 3.

A causa é falta de idempotência: o `MERGE` só sabe comparar contra o presente (`is_current`), nunca contra "esse snapshot específico já foi processado alguma vez no passado?". A correção foi tratar a Gold da dimensão como recompute completo, igual à Silver — `DROP TABLE` + `CREATE TABLE` vazia no início do notebook, processando o histórico inteiro do zero a cada execução. Com histórico pequeno (3 snapshots, 5 unidades) isso é barato, e a surrogate key por hash determinístico garante que recriar do zero não muda nenhuma chave entre execuções. Validado rodando o Job completo (orquestrado, não manual) mais de uma vez e confirmando que a contagem de versões por unidade se mantém estável.

**Join temporal na fato, não join simples por chave de negócio.** Esse é o ponto mais importante do projeto. `gold_dim_unit` guarda várias versões da mesma unidade ao longo do tempo. Se o join do fato com a dimensão usasse só `unit_code = unit_code`, cada corrida bateria com todas as versões da unidade, ou (filtrando só a versão vigente) toda corrida herdaria o atributo *atual* da dimensão, mesmo que tivesse acontecido num período em que o atributo era outro. É a mesma classe de erro que gera `ORA-30926` nas minhas procedures reais quando uma dimensão SCD2 é usada num MERGE ou join sem filtro de vigência. A correção foi comparar a data do evento contra o intervalo `valid_from`/`valid_to` da dimensão, tratando o `NULL` de `valid_to` (versão ainda vigente) com `COALESCE` para uma data futura, permitindo que o `BETWEEN` funcione tanto para versões expiradas quanto para a corrente.

## Validação

Toda alteração de camada foi validada com query, não só "rodou sem erro":

- Arquivo faltante: `LEFT ANTI JOIN` entre calendário completo e datas presentes em Bronze confirma exatamente os 2 dias esperados, nem mais nem menos.
- Duplicidade: `COUNT(*) > 1` por `trip_uuid` retorna 0 linhas em Silver, confirmando que o dedup por `ROW_NUMBER()` funcionou.
- Corte de fuso: cruzamento de `_source_file` contra `request_date`/`event_date` confirma que o arquivo físico (D+1) e a data do evento (D) divergem exatamente nos 5 dias esperados em Bronze, e que `event_date` reconcilia isso corretamente em Silver.
- SCD2: a unidade `01001` (mudança de um único atributo) e `02002` (mudança de dois atributos simultâneos) geraram exatamente 2 versões cada, com `valid_from`/`valid_to` corretos; as demais unidades, sem mudança, mantiveram 1 versão só (controle negativo).
- Join temporal: a mesma unidade (`01001`), em datas diferentes, retorna `sk_unit` e atributos diferentes na fato — uma corrida de abril traz o centro de custo antigo, uma corrida de maio traz o novo, provando que o join pegou a versão vigente na data do evento, não a versão atual.
- Idempotência do Job: rodei a orquestração completa mais de uma vez e confirmei que a contagem de versões por unidade na dimensão se mantém estável entre execuções, depois de corrigir o bug de recompute descrito acima.

## Estrutura do repositório

```
docs/
  job_orchestration_graph.png  - grafo de dependencia do Job, com as 5 tasks concluidas
notebooks/
  01_bronze_trips.py           - Auto Loader, schema explicito via schemaHints
  02_bronze_dim_unit.py        - leitura batch dos snapshots de dimensao
  03_silver_trips.py           - dedup + event_date reconciliado
  04_gold_dim_unit_scd2.py     - MERGE de SCD2, recompute completo a cada execucao
  05_gold_fact_trips.py        - fato final, join temporal contra a dimensao
data_generation/
  gerar_dados_uber_trips.py    - gerador do dataset sintetico de trips, com anomalias
  gerar_dados_uber_units.py    - gerador dos snapshots da dimensao de unidade
  ANOMALIES.md                 - gabarito das anomalias de trips
  DIM_UNIT_ANOMALIES.md        - gabarito das mudancas historicas da dimensao
```

## Próximos passos

- Avaliar `MERGE` incremental de verdade na Gold da dimensão (com controle de idempotência por snapshot já processado), caso o histórico cresça o suficiente para o recompute completo deixar de ser barato.
- Estender o join temporal para considerar múltiplas dimensões versionadas ao mesmo tempo (hoje só `dim_unit` tem SCD2).
- Adicionar teste automatizado (não só query manual) que rode o gabarito de anomalias como parte do próprio Job.
