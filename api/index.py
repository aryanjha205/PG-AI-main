"""
PG AI Query Engine — Powered by pgai and pgvector.
Simplified implementation using direct API calls.
"""
import os
import asyncio
import sys
import json
import requests
from typing import List, Dict, Any
import time
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import psycopg
from psycopg.rows import dict_row

# (Removed pgai due to 5GB size constraint — using custom discovery)

# BASE DIR for static files
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════
NEON_DB_URL = os.getenv("DATABASE_URL", "postgresql://neondb_owner:npg_5RdSgjpHCQ6P@ep-sweet-thunder-a1rqx5ar-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")

class Config:
    MODEL = "openai" 
    BASE_URL = "https://text.pollinations.ai/v1/chat/completions"

# ═══════════════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════════════
def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

async def get_connection():
    """Returns an async psycopg connection to the database."""
    try:
        conn = await psycopg.AsyncConnection.connect(NEON_DB_URL, autocommit=True, connect_timeout=15)
        return conn
    except Exception as e:
        print(f"[{get_timestamp()}] [PG AI] Database connection failed: {e}")
        raise

async def generate_sql_with_ai(prompt: str, schema_context: str) -> Dict[str, Any]:
    """Call Pollinations AI to generate high-quality SQL from natural language."""
    system_prompt = (
        "You are 'PG AI', a state-of-the-art PostgreSQL Data Assistant.\n"
        "Your mission: Transform natural language into optimized PostgreSQL SELECT queries.\n\n"
        "DATABASE CONTEXT:\n"
        f"{schema_context}\n\n"
        "CORE CAPABILITIES:\n"
        "1. pgvector: For semantic search on 'vector' columns, use <-> (L2), <#> (dot product), or <=> (cosine). Example: 'WHERE embedding <=> ai.openai_embed(...) < 0.2'.\n"
        "2. pgai Synergy: If the user asks for semantic relationships, leverage the schema metadata provided.\n"
        "3. SQL Quality: Use ILIKE for search, proper JOINs, and CTEs where beneficial.\n"
        "4. Safety: STRICTLY SELECT queries only. No DML/DDL.\n\n"
        "Respond in JSON format ONLY with:\n"
        "{ \"sql\": \"string\", \"explanation\": \"string\", \"confidence\": 0.95, \"visualization\": \"table|chart|map\" }"
    )
    
    payload = {
        "model": Config.MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"User Request: {prompt}"}
        ],
        "response_format": {"type": "json_object"}
    }
    
    try:
        print(f"[{get_timestamp()}] [PG AI] Requesting SQL for: {prompt[:50]}...")
        resp = await asyncio.to_thread(requests.post, Config.BASE_URL, json=payload, timeout=30)
        data = resp.json()
        content = data['choices'][0]['message']['content']
        print(f"[{get_timestamp()}] [PG AI] AI generated raw content: {content[:100]}...")
        return json.loads(content)
    except Exception as e:
        print(f"[{get_timestamp()}] [PG AI] AI Generation Error: {e}")
        return {
            "sql": "",
            "explanation": f"AI error: {str(e)}",
            "confidence": 0,
            "visualization": "table"
        }

# ═══════════════════════════════════════════════════════════════
# FASTAPI APP
# ═══════════════════════════════════════════════════════════════
app = FastAPI(title="PG AI Engine", description="Natural Language to SQL via pgai & pgvector")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    print(f"[{get_timestamp()}] [HTTP] {request.method} {request.url.path} - {response.status_code} ({duration:.2f}s)")
    return response

class QueryRequest(BaseModel):
    prompt: str
    selected_tables: List[str] = Field(default_factory=list)
    role: str = "viewer"

async def get_schema_context(conn, table_names: List[str]) -> str:
    """Retrieve schema details and sample data for the AI prompt."""
    context_lines = []
    for table in table_names:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (table,))
            columns = await cur.fetchall()
            
            try:
                await cur.execute(f"SELECT * FROM \"{table}\" LIMIT 2")
                samples = await cur.fetchall()
            except Exception:
                samples = []
            
            col_info = "\n  - ".join([f"{c['column_name']} ({c['data_type']})" for c in columns])
            sample_json = json.dumps(samples, default=str)
            context_lines.append(f"TABLE: {table}\nCOLUMNS:\n  - {col_info}\nSAMPLES: {sample_json}")
    return "\n\n".join(context_lines)

@app.get("/health-check")
async def health_check():
    """Verify system health and active extensions."""
    try:
        async with await get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT extname FROM pg_extension")
                exts = [r[0] for r in await cur.fetchall()]
                print(f"[{get_timestamp()}] [PG AI] Health Check: Connected to database. Extensions: {exts}")
                return {
                    "status": "online",
                    "extensions": exts,
                    "pgvector_ready": "vector" in exts,
                    "pgai_ready": "ai" in exts or "pgai" in exts
                }
    except Exception as e:
        print(f"[{get_timestamp()}] [PG AI] Health Check FAILED: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/get-database-tables")
async def api_get_schema():
    try:
        async with await get_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public'")
                rows = await cur.fetchall()
                schema = {}
                for r in rows:
                    schema.setdefault(r['table_name'], {})[r['column_name']] = r['data_type']
                return schema
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate-query")
async def api_generate_query(req: QueryRequest):
    """NL -> pgai Schema Context -> AI -> executed Results."""
    try:
        async with await get_connection() as conn:
            # 1. Schema discovery (CUSTOM)
            async with conn.cursor() as cur:
                if req.selected_tables:
                    await cur.execute("SELECT relname FROM pg_class WHERE relname = ANY(%s) AND relkind = 'r'", (req.selected_tables,))
                else:
                    await cur.execute("""
                        SELECT c.relname FROM pg_class c 
                        JOIN pg_namespace n ON n.oid = c.relnamespace 
                        WHERE n.nspname = 'public' AND c.relkind = 'r' 
                        LIMIT 12
                    """)
                table_names = [r[0] for r in await cur.fetchall()]
            
            if not table_names:
                return {"success": False, "error": "No tables detected in the public schema."}

            print(f"[{get_timestamp()}] [PG AI] Incoming query from user with role: {req.role}")
            schema_context = await get_schema_context(conn, table_names)
            print(f"[{get_timestamp()}] [PG AI] Loaded context for {len(table_names)} tables.")
            
            # 2. AI SQL Generation
            ai_data = await generate_sql_with_ai(req.prompt, schema_context)
            sql_query = ai_data.get("sql", "").strip().rstrip(';') + ';'
            
            if not sql_query or "SELECT" not in sql_query.upper():
                return {"success": False, "error": f"Failed to generate valid SQL: {ai_data.get('explanation')}"}

            # 3. Security
            blocks = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "GRANT", "REVOKE", "CREATE"]
            if any(b in sql_query.upper() for b in blocks):
                print(f"[{get_timestamp()}] [PG AI] SAFETY BLOCK: {sql_query}")
                return {"success": False, "error": "Safety Block: Potential modification query detected."}

            # 4. Execute
            async with conn.cursor(row_factory=dict_row) as cur:
                print(f"[{get_timestamp()}] [PG AI] Executing: {sql_query}")
                await cur.execute(sql_query)
                dataset = await cur.fetchall()
                
                table_hint = "results"
                if req.selected_tables:
                    table_hint = req.selected_tables[0]
                elif dataset and len(dataset) > 0:
                    # Try to guess table from SQL
                    for t_name in (await api_get_schema()).keys():
                        if t_name.lower() in sql_query.lower():
                            table_hint = t_name; break
                
                return {
                    "success": True,
                    "query": sql_query,
                    "explanation": ai_data.get("explanation", ""),
                    "chat_answer": ai_data.get("explanation", ""),
                    "confidence": ai_data.get("confidence", 1.0),
                    "visualization": ai_data.get("visualization", "table"),
                    "results": {table_hint: dataset},
                    "tables": [table_hint] if dataset else []
                }

    except Exception as e:
        return {"success": False, "error": str(e)}

# Aliases
@app.get("/table-metadata")
async def meta(): return await api_get_schema()

@app.post("/query-data")
async def query(req: QueryRequest): return await api_generate_query(req)

@app.get("/")
async def home():
    return FileResponse(os.path.join(BASE_DIR, "..", "index.html"))

@app.get("/manifest.json")
async def manifest(): return FileResponse(os.path.join(BASE_DIR, "..", "manifest.json"))

@app.get("/sw.js")
async def sw(): return FileResponse(os.path.join(BASE_DIR, "..", "sw.js"))

if __name__ == "__main__":
    import uvicorn
    banner = f"""
    ╔══════════════════════════════════════════════════════════════╗
    ║                 PG AI Query Engine v3.0                      ║
    ║        Natural Language to SQL — Terminal Logging ON         ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [System] Starting PG AI Backend...")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [System] Database: {NEON_DB_URL.split('@')[-1]}")
    uvicorn.run(app, host="0.0.0.0", port=8001, loop="asyncio")
