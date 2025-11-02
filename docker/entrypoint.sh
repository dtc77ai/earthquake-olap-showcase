#!/bin/bash
set -e

echo "🌍 Earthquake OLAP Showcase - Starting..."

# Always run incremental ETL (it will skip already-loaded years)
echo "📥 Running incremental ETL pipeline..."
python scripts/run_etl_incremental.py

if [ $? -eq 0 ]; then
    echo "✅ ETL pipeline completed successfully"
else
    echo "❌ ETL pipeline failed"
    exit 1
fi

# Start Streamlit
echo "🚀 Starting Streamlit application..."
exec streamlit run src/app/main.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false \
    --server.fileWatcherType=none
