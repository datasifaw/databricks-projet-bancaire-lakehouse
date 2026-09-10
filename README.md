#Projet bancaire de bout en bout avec Databricks | Pipeline Lakehouse et analyse des données financières

Le scénario métier sera :

Une entreprise du secteur financier possède ses données dans SQL Server. L’objectif est d’automatiser leur ingestion dans Databricks, de les nettoyer et les fiabiliser avec une architecture Bronze-Silver-Gold, puis de produire des indicateurs financiers et un dashboard pour l’analyse.

Notre futur projet ressemblera à :

SQL Server
    ↓
Ingestion Databricks
    ↓
BRONZE
Données brutes
    ↓
SILVER
Nettoyage + qualité
    ↓
GOLD
Indicateurs financiers
    ↓
Dashboard BI
