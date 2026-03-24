# 🧠 PG AI Query Engine v3.0

**NLP-to-SQL Assistant** — Unlock the power of your PostgreSQL database with natural language. Ask questions, discover trends, and visualize data without writing a single line of SQL.

Built with **FastAPI**, **pgai**, and **pgvector**, this engine converts plain English into optimized, safe, and context-aware SQL queries using automatic schema discovery and semantic search context.

---

## 🌟 Key Features

### 🧠 Intelligent SQL Generation
- **Context-Aware**: Uses `pgai` loaders and renderers to feed high-quality database schema context to the AI.
- **Semantic Search**: Native support for **pgvector** operations (`<=>`, `<->`, etc.) for proximity-based queries.
- **Blazing Fast**: Powered by **Pollinations AI** for high-speed, keyless inference.

### 🍱 Modern Web Experience
- **Premium UI**: Sleek, responsive design with glassmorphism elements and smooth transitions.
- **Dual Theme**: Support for Dark and Light modes with a persistent theme system.
- **PWA Ready**: Installable as a progressive web app (mobile-friendly with offline caching).

### 🔒 Enterprise-Grade Security
- **Read-Only Enforcement**: Strictly blocks non-SELECT queries (`UPDATE`, `DELETE`, `DROP`, etc.).
- **Automatic Validation**: AI-generated SQL is pre-validated for safety before execution.
- **Schema Isolation**: Only queries the public schema by default, with custom table selection support.

---

## 🛠️ Tech Stack

- **Backend**: Python 3.10+, FastAPI, `psycopg` (Async), `pgai`, `requests`.
- **Inference**: Pollinations AI (OpenAI-compatible).
- **Database**: PostgreSQL (Optimized for **Neon.tech**).
- **Frontend**: Vanilla JS (ES6+), CSS3 (Custom Variables), HTML5.
- **Deployment**: Vercel-ready with PWA support.

---

## 📐 Architecture

```mermaid
graph TD
    User([User Prompt]) --> Frontend[PWA Web UI]
    Frontend --> API[FastAPI Engine]
    API --> Schema[pgai Schema Discovery]
    Schema --> AI{Inference Engine}
    AI --> Validate[SQL Safety Filter]
    Validate --> DB[(Neon PostgreSQL)]
    DB --> Exec[Async Execution]
    Exec --> Result[JSON Response]
    Result --> Frontend
```

---

## 🏁 Quick Start

### 1. Requirements
Ensure you have Python 3.10+ installed:
```bash
pip install -r requirements.txt
```

### 2. Configure Database
Set your connection string in `api.py` or as an environment variable:
```bash
# Environment Variable
export DATABASE_URL="postgresql://user:pass@host/db"
```

### 3. Run Locally
```bash
python app.py
```
*The application will be available at `http://localhost:8001`.*

---

## 📡 API Reference

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/query-data` | POST | Core NL-to-SQL engine — converts prompt to executed results |
| `/table-metadata` | GET | Returns discovered schema mapping for the UI |
| `/health-check` | GET | Verifies DB connection and active extensions (pgvector/pgai) |

### Sample Request (`/query-data`)
```json
{
  "prompt": "Show me top 5 high-spending users from last month",
  "selected_tables": ["users", "orders"],
  "role": "viewer"
}
```

---

## 📱 Mobile & PWA
This application is fully responsive and supports PWA features:
- **Installable**: Add to home screen on iOS/Android.
- **Offline Access**: Service worker caching for UI assets.
- **Native Look**: Transparent status bars and app-like navigation.

---

## ☁️ Deployment
Ready for one-click deployment to **Vercel**:
1. Connect your GitHub repository.
2. Add your `DATABASE_URL` to Environment Variables.
3. Deploy!

---

*Powered by [pgai](https://github.com/timescale/pgai) & [pgvector](https://github.com/pgvector/pgvector)*
