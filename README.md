
# Projet bancaire de bout en bout avec Databricks
## Pipeline Lakehouse et analyse des données financières

Le scénario métier: 

Une entreprise du secteur financier possède ses données dans SQL Server. L’objectif est d’automatiser leur ingestion dans Databricks, de les nettoyer et les fiabiliser avec une architecture Bronze-Silver-Gold, puis de produire des indicateurs financiers et un dashboard pour l’analyse.

## Architecture du projet

SQL Server
↓
Extraction Python avec pyodbc
↓
Fichiers CSV
↓
Databricks Volume
↓
Bronze
↓
Silver
↓
Gold
↓
Workflow Databricks
↓
Dashboard BI

## Résultats

Le pipeline traite les données bancaires selon l'architecture Medallion :

- Bronze : ingestion et traçabilité des données sources
- Silver : nettoyage, typage et contrôles qualité
- Gold : agrégations et indicateurs métier
- Workflow : orchestration du pipeline
- Dashboard : visualisation des KPI et analyses financières

Le dashboard présente notamment :
- Nombre total de transactions
- Montant total des transactions
- Montant moyen
- Nombre de clients actifs
- Top 10 clients
- Analyse par canal
- Analyse CREDIT / DEBIT
