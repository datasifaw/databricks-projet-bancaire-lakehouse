import csv
from pathlib import Path
import pyodbc

# ============================================
# CONFIGURATION
# ============================================

SERVER = "localhost,1433"
DATABASE = "Banque_Databricks"

TABLES = [
    "branches",
    "customers",
    "accounts",
    "transactions"
]

OUTPUT_DIR = Path("data/staging")


# ============================================
# CONNEXION SQL SERVER
# ============================================

connexion = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================
# EXTRACTION DES TABLES
# ============================================

for table in TABLES:

    print(f"Extraction de banking.{table}...")

    curseur = connexion.cursor()

    curseur.execute(
        f"SELECT * FROM banking.{table}"
    )

    colonnes = [
        colonne[0]
        for colonne in curseur.description
    ]

    fichier_csv = OUTPUT_DIR / f"{table}.csv"

    with open(
        fichier_csv,
        "w",
        newline="",
        encoding="utf-8"
    ) as fichier:

        writer = csv.writer(fichier)

        # En-têtes
        writer.writerow(colonnes)

        # Données
        writer.writerows(curseur.fetchall())

    print(f"OK : {fichier_csv}")


connexion.close()

print("\nExtraction terminée avec succès.")