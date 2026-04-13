#!/usr/bin/env python3
"""
REVOLVER Backend v2
FastAPI · WebSockets · SQLite · Async AI
Puerto: 8080
"""

import asyncio, json, os, secrets, smtplib, socket, ssl, logging, hashlib, hmac
from datetime import datetime, timedelta
from typing import Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import aiosqlite
import httpx
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks, Request, UploadFile, File, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════════════════

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DB_PATH   = os.path.join(BASE_DIR, 'revolver.db')
KEYS_PATH = os.path.join(BASE_DIR, 'keys.json')
PORT      = int(os.environ.get('PORT', 8080))

logging.basicConfig(level=logging.INFO, format='%(asctime)s  %(levelname)s  %(message)s')
log = logging.getLogger('revolver')


def load_keys() -> dict:
    """Load API keys from keys.json, with env-var overrides for production."""
    try:
        with open(KEYS_PATH, encoding='utf-8') as f:
            keys = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        keys = {}
    # Environment variables override file values (used in Railway / production)
    env_map = {
        'anthropic':      'ANTHROPIC_API_KEY',
        'openai':         'OPENAI_API_KEY',
        'google':         'GOOGLE_API_KEY',
        #xai':            'XAI_API_KEY',
        'deepseek':       'DEEPSEEK_API_KEY',
        'groq':           'GROQ_API_KEY',
        'gmail_user':     'GMAIL_USER',
        'gmail_password': 'GMAIL_PASSWORD',
        'admin_password': 'ADMIN_PASSWORD',
    }
    for key, env in env_map.items():
        val = os.environ.get(env)
        if val:
            keys[key] = val
    return keys


def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return 'localhost'


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ════════════════════════════════════════════════════════════════════════════

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.executescript("""
            -- ── Usuarios / Empresas ──────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS usuarios (
                id            TEXT PRIMARY KEY,
                nombre        TEXT NOT NULL,
                empresa       TEXT NOT NULL,
                cargo         TEXT DEFAULT '',
                email         TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                salt          TEXT NOT NULL,
                plan          TEXT DEFAULT 'free',
                logo_url      TEXT DEFAULT '',
                website       TEXT DEFAULT '',
                industria     TEXT DEFAULT '',
                created_at    TEXT NOT NULL
            );

            -- ── Tokens de autenticación ──────────────────────────────────────
            CREATE TABLE IF NOT EXISTS tokens_auth (
                token       TEXT PRIMARY KEY,
                usuario_id  TEXT NOT NULL,
                expires_at  TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
            );

            -- ── Sesiones ─────────────────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS sessiones (
                id         TEXT PRIMARY KEY,
                empresa     TEXT NOT NULL,
                problema    TEXT NOT NULL,
                contexto    TEXT DEFAULT '',
                impacto     TEXT DEFAULT '',
                restricciones TEXT DEFAULT '',
                criterios   TEXT DEFAULT '',
                facilitador TEXT DEFAULT 'El facilitador',
                dna         TEXT DEFAULT '{}',
                estado      TEXT DEFAULT 'activa',
                usuario_id  TEXT DEFAULT NULL,
                created_at  TEXT NOT NULL,
                bala        TEXT DEFAULT NULL,
                FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
            );

            -- ── Miembros ─────────────────────────────────────────────────────