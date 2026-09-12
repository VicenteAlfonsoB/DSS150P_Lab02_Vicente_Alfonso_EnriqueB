# DSS150P Weeks 2-3 Laboratory Package

Start with the Word laboratory guide. This repository is intentionally incomplete.

## Quick start
1. `python -m venv .venv`
2. Activate `.venv`
3. `pip install -r requirements.txt`
4. `docker compose up -d`
5. Load PostgreSQL seed: `docker exec -i dss150p-w23-postgres psql -U dss150p -d dss150p < sql/seed_support_tickets.sql`
6. Terminal A: `python src/local_api_server.py`
7. Terminal B: complete/run profiling and ingestion scripts.

Do not commit `.env`, generated raw data, or watermark state unless specifically instructed.
