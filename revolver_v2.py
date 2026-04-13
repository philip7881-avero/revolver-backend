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
# ══════════════════════════════════════════════════════════════════════════════

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DB_PATH   = os.path.join(BASE_DIR, 'revolver.db')
KEYS_PATH = os.path.join(BASE_DIR, 'keys.json')
PORT      = int(os.environ.get('PORT', 8080))

logging.basicConfig(level=logging.INFO, format='%(asctime)s  %(levelname)s  %(message)s')
log = logging.getLogger('revolver')