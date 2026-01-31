# 🎣 Análise de Dados da Pesca em Stardew Valley

Este projeto é uma análise completa end-to-end do sistema de pesca do jogo **Stardew Valley**, desenvolvida com foco em **engenharia de dados**, **modelagem analítica** e **storytelling visual**.  
O objetivo foi transformar dados brutos do jogo em **insights reais, estruturados e analisáveis**, revelando padrões econômicos, mecânicos e ambientais que não são facilmente perceptíveis ao jogador comum.

O projeto cobre todas as etapas do ciclo de dados: ingestão, limpeza, padronização, enriquecimento, modelagem analítica e visualização final em dashboard.

Os dados originais para o projeto foram retiados de: https://www.kaggle.com/datasets/jessicaebrown/stardew-valley-full-catelog
e podem também ser encontrados na pasta:

```text
├── raw_data
│   └── csvs_originais
```
do projeto.

---

## 🧠 Motivação

Stardew Valley possui uma mecânica de pesca rica e complexa, envolvendo:
- múltiplos ambientes e regiões
- diferentes comportamentos de peixes
- influência de clima, horário e estação
- progressão econômica
- impacto direto das profissões do jogador

Apesar disso, o jogo não apresenta essas relações de forma explícita.  
Este projeto nasce da curiosidade de responder perguntas como:
- Quais peixes realmente compensam em cada fase do jogo?
- Como a economia evolui ao longo da progressão?
- Qual o impacto real das profissões Fisher e Angler?
- O jogo privilegia variedade mecânica ou repete padrões?
- Existem ambientes mais lucrativos ou apenas mais difíceis?

---

## 🛠️ Tecnologias Utilizadas

- **Databricks**
- **PySpark**
- **Spark SQL**
- **Python**
- **Notebooks estruturados em camadas**
- **Dashboard Databricks SQL**

---

## 🏗️ Arquitetura do Projeto

O projeto foi estruturado seguindo uma arquitetura em camadas inspirada no padrão **Medallion Architecture**:

### 🔹 Bronze
Camada de ingestão dos dados brutos.
- Leitura dos CSVs originais
- Preservação do formato original
- Nenhuma regra de negócio aplicada

### 🔹 Silver
Camada de limpeza e padronização.
- Normalização de nomes e textos
- Padronização de colunas categóricas
- Tratamento de cardinalidade
- Correção de inconsistências semânticas
- Reconstrução correta das relações entre localizações, regiões e qualificadores

### 🔹 Gold
Camada analítica e de consumo.
- Enriquecimento de dados
- Criação de métricas analíticas
- Flags de progressão e acessibilidade
- Modelagem pensada para dashboards e storytelling

---

## 🔄 Pipeline de Dados

A pipeline foi pensada para ser:
- **Reprodutível**
- **Idempotente**
- **Modular**
- **Fácil de manter e evoluir**

Principais características:
- Transformações organizadas por camada
- Uso de funções utilitárias compartilhadas
- Notebooks facilmente convertíveis para scripts
- Separação clara entre lógica de ingestão, transformação e análise
- Uso combinado de PySpark e SQL conforme o contexto

---

## ✨ Enriquecimento dos Dados

Na camada Gold, os dados foram enriquecidos com diversas novas informações analíticas, incluindo:

- **Effort Score** e esforço estimado
- **Complexidade de comportamento** dos peixes
- Flags como:
  - `is_best_early_game_fish`
  - `is_beginner_friendly`
- Métricas de progressão por fase do jogo
- Normalização e expansão de localizações em:
  - location_type
  - region
  - special_area
  - rules
- Simulação de valores de venda considerando:
  - Qualidade do peixe (Normal, Silver, Gold, Iridium)
  - Profissões Fisher e Angler
- Integração de dados econômicos, mecânicos e ambientais em uma única visão analítica

---

## 📊 Dashboard e Storytelling

O dashboard final foi organizado em **abas temáticas**, cada uma contando uma parte da história do jogo.

### 🔹 Overview
Visão geral e contextual:
- Distribuição de comportamentos
- Complexidade percebida
- Relação entre dificuldade e esforço
- Introdução às mecânicas sem sobrecarregar com números

### 🔹 Economia
Análises econômicas profundas:
- Evolução do valor dos peixes por progressão
- Impacto real das profissões
- Comparações entre qualidades
- Relação entre XP, valor e risco
- Visualizações como stacked charts, heatmaps, linhas, scatter e sankey

### 🔹 Ambiente e Exploração
Foco em mundo e exploração:
- Distribuição de peixes por regiões
- Relação entre ambiente, estação e disponibilidade
- Influência de clima e horário
- Análise espacial e ambiental do sistema de pesca

### 🔹 Mecânica
Análises mecânicas e comportamentais:
- Complexidade dos comportamentos
- Relação entre dificuldade e progressão
- Acessibilidade para iniciantes
- Regras especiais e exceções do sistema

Cada gráfico foi acompanhado de **descrições curtas e orientadas a storytelling**, guiando a leitura e interpretação dos dados.

---

## 📁 Estrutura do Repositório

```text
├── notebooks
│   ├── bronze
│   │   └── *.ipynb
│   ├── silver
│   │   └── *.ipynb
│   └── gold
│       └── *.ipynb
│
├── raw_data
│   └── csvs_originais
│
├── utils
│   └── functions.py
│
├── dashboards
│   ├── pdfs
│   │   └── *.pdf
│   |── images
│   │   └── Overview
│   │     └── *.png
│   |   └── Análise de Economia
│   │     └── *.png
│   |   └── Análise de Ambiente e Exploração
│   │     └── *.png
│   |   └── Análise de Mecânica
│   │     └── *.png
│
└── README.md
```

---

## 🚀 Resultados

- Construção de uma pipeline analítica robusta

- Dados limpos, padronizados e semanticamente corretos

- Métricas que traduzem mecânicas do jogo em números reais

- Insights que revelam padrões invisíveis ao jogador comum

- Dashboards claros, visuais e orientadso a tomada de decisão

---

## 📌 Considerações Finais

Este projeto foi uma oportunidade de aplicar engenharia de dados de ponta a ponta em um contexto criativo, transformando dados de um jogo em uma análise rica, estruturada e cheia de significado.

Mais do que analisar números, o foco foi contar histórias com dados, respeitando o comportamento real do jogo e revelando padrões que normalmente passam despercebidos.

---

## 📸 Exemplos dos Resultados Obtidos:

<img width="906" height="592" alt="Valor Médio de Peixes Iridium por Ambiente e Progressão (1)" src="https://github.com/user-attachments/assets/607d54ac-7134-49a4-9105-64121a1dec39" />

<img width="906" height="472" alt="Diversidade de Peixes por Região e Horário" src="https://github.com/user-attachments/assets/3a5daa1b-4587-43f1-9ec5-6c5890c38c4b" />

<img width="1211" height="472" alt="Distribuição de Esforço Mecânico por Fase do Jogo" src="https://github.com/user-attachments/assets/ac7665b7-673b-49f0-9a1c-66c46cfb2db9" />

<img width="1211" height="532" alt="_Distribuição de Ouro Máximo por Ambiente e Clima (Pescador Especialista x Qualidade Iridium)" src="https://github.com/user-attachments/assets/7b2678d5-7387-4738-989c-37e61347820b" />

<img width="906" height="472" alt="Faixa de Tamanho dos Peixes por Nível de Dificuldade" src="https://github.com/user-attachments/assets/0968370d-6b17-491b-9a04-3a39bdc97a67" />

