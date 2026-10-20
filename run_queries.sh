
#!/usr/bin/env bash
set -euo pipefail

CONTAINER="novacommerce-postgres"
DB="novacommerce_oltp"
USER="novacommerce"

echo "=================================="
echo " DATA-001 / T03 - SQL Test Runner"
echo "=================================="

for file in database/queries/*.sql; do
    echo ""
    echo "▶ Running: $file"

    start=$SECONDS

    if docker exec -i "$CONTAINER" \
        psql -X -v ON_ERROR_STOP=1 \
        -U "$USER" -d "$DB" < "$file"; then

        duration=$((SECONDS - start))
        echo "✓ PASSED - ${duration}s"
    else
        echo "✗ FAILED - $file"
        exit 1
    fi
done

echo ""
echo "✓ All SQL scripts executed successfully!"
