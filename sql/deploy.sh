#!/bin/bash
# Usage: ./deploy.sh env/dev.env

# Charger les variables d'environnement
source $1

# Parcourir chaque dossier (chaque schéma)
for schema_dir in */sql/models/ ; do
    schema_name=$(basename "$schema_dir")
    echo "🔁 Déploiement du schéma : $schema_name"

    for sql_file in "$schema_dir"/*.sql; do
        echo "📄 Exécution : $sql_file"
        snowsql \
            -a "$SNOWFLAKE_ACCOUNT" \
            -u "$SNOWFLAKE_USER" \
            -r "$SNOWFLAKE_ROLE" \
            -w "$SNOWFLAKE_WAREHOUSE" \
            -d "$SNOWFLAKE_DATABASE" \
            -s "$schema_name" \
            -f "$sql_file"
    done
done
