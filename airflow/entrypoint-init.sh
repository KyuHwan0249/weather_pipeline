#!/bin/bash
set -e

echo "🔍 Waiting for Postgres to be ready..."

# wait for postgres
until PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U $POSTGRES_USER \
  -d $POSTGRES_DB -c "\q" 2>/dev/null; do
  echo "⏳ Postgres not ready yet... retrying in 2s"
  sleep 2
done

echo "✅ Postgres is ready!"


echo "🔍 Checking if Airflow DB is initialized..."

# Check if Airflow meta table exists
TABLE_EXISTS=$(PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U $POSTGRES_USER \
  -d $POSTGRES_DB -tAc "SELECT 1 FROM information_schema.tables WHERE table_name='dag';")

if [ "$TABLE_EXISTS" == "1" ]; then
    echo "✔ Airflow database already initialized. Skipping."
else
    echo "🚀 Running Airflow DB init..."
    airflow db init

    echo "🚀 Creating Airflow admin user..."
    airflow users create \
      --username admin \
      --password admin \
      --firstname admin \
      --lastname admin \
      --role Admin \
      --email admin@example.com

    echo "🎉 Airflow DB Initialization Complete!"
fi

exit 0
