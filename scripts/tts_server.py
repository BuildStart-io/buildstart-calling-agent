#!/usr/bin/env python3
"""
Studio-Quality Sinhala & Multilingual Neural TTS Server
Powered by Microsoft Azure / Edge Neural Voices (si-LK-ThiliniNeural & si-LK-SameeraNeural)
with local Piper TTS (si_LK-sinhala-medium ONNX) as offline fallback.
"""

import asyncio
import base64
import io
import json
import os
import re
import sys
import threading
import wave
import websockets
import numpy as np
import requests
from flask import Flask, request, Response, jsonify

app = Flask(__name__)
piper_lock = threading.Lock()
dialog_lock = threading.Lock()

# Voice hierarchy:
# 1. dialog-nipunika (Dialog Axiata & UoM Lab 22.05 kHz Studio Female - #1 Natural Sinhala Voice)
# 2. piper-openslr (Google OpenSLR 30 High-Fidelity 22.05 kHz Neural Voice)
# 3. piper-ashoka (Ashoka Weerawardhana Studio Voice - 16 kHz)
# 4. gemini-aoede (Google AI Studio Gemini Flash Voice)
# 5. si-LK-ThiliniNeural (Microsoft Female Neural)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
DEFAULT_VOICE = os.environ.get("DEFAULT_VOICE", "dialog-nipunika")
RATE_MODIFIER = "+4%"  # Conversational pace
PITCH_MODIFIER = "+1Hz"
ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE_ID = os.environ.get("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")

# Pre-load Dialog Axiata Nipunika Studio VITS model (210,000 steps, 22.05 kHz)
dialog_synthesizer = None
dialog_romanizer = None

try:
    from unittest.mock import MagicMock
    sys.modules.setdefault("ko_speech_tools", MagicMock())

    # 1. Alias monotonic align FIRST
    VITS_TOOLS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools", "finetune-hf-vits")
    if VITS_TOOLS not in sys.path:
        sys.path.insert(0, VITS_TOOLS)
    try:
        import monotonic_align
        sys.modules.setdefault("monotonic_alignment_search", monotonic_align)
    except Exception:
        pass

    # 2. Shims for transformers and torch
    import torch
    import transformers.pytorch_utils
    transformers.pytorch_utils.isin_mps_friendly = getattr(torch, "isin", None)
    import transformers.utils.import_utils
    transformers.utils.import_utils.is_torchcodec_available = lambda: True

    from huggingface_hub import hf_hub_download
    from TTS.utils.synthesizer import Synthesizer
    import importlib.util

    print("🇱🇰 Initializing Dialog Axiata Nipunika Studio VITS (210,000 steps)...")
    d_cfg = hf_hub_download("dialoglk/SinhalaVITS-TTS-F1", "Nipunika_config.json")
    d_pth = hf_hub_download("dialoglk/SinhalaVITS-TTS-F1", "Nipunika_210000.pth")
    d_rom = hf_hub_download("dialoglk/SinhalaVITS-TTS-F1", "romanizer.py")

    dialog_synthesizer = Synthesizer(tts_checkpoint=d_pth, tts_config_path=d_cfg, use_cuda=False)
    # Tune VITS model parameters for fluent connected speech (avoids word-by-word pauses)
    if hasattr(dialog_synthesizer, "tts_model") and dialog_synthesizer.tts_model:
        dialog_synthesizer.tts_model.length_scale = 1.15
        dialog_synthesizer.tts_model.inference_noise_scale = 0.33

    spec = importlib.util.spec_from_file_location("romanizer", d_rom)
    dialog_romanizer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dialog_romanizer)

    print("✨ Dialog Nipunika Studio Female Voice (22,050 Hz) Ready (Expressive Human Tuning Active)!")
except Exception as e:
    print(f"⚠️ Dialog Nipunika initialization notice: {e}")

# Pre-load local Piper TTS models (OpenSLR & Ashoka)
piper_voices = {}
try:
    from huggingface_hub import hf_hub_download
    from piper import PiperVoice
    from piper.config import SynthesisConfig

    print("🇱🇰 Initializing Piper local neural models...")
    # 1. Intellisr Ashoka Weerawardhana model
    p_int_onnx = hf_hub_download('intellisr/sinhala-tts-piper-v1', 'model.onnx')
    p_int_json = hf_hub_download('intellisr/sinhala-tts-piper-v1', 'model.onnx.json')
    piper_voices['piper-intellisr'] = PiperVoice.load(p_int_onnx, config_path=p_int_json)
    piper_voices['piper-ashoka'] = piper_voices['piper-intellisr']

    # 2. UNICEF Ashoka medium model
    p_uni_onnx = hf_hub_download('unicef/piper-si_LK-ashoka-medium', 'si_LK-ashoka-medium.onnx')
    p_uni_json = hf_hub_download('unicef/piper-si_LK-ashoka-medium', 'si_LK-ashoka-medium.onnx.json')
    piper_voices['piper-unicef'] = PiperVoice.load(p_uni_onnx, config_path=p_uni_json)

    # 3. OpenSLR-30 high-fidelity 22.05 kHz Sinhala model (chan4lk/piper-tts-sinhala)
    p_chan_onnx = hf_hub_download('chan4lk/piper-tts-sinhala', 'si_LK-sinhala-medium.onnx')
    p_chan_json = hf_hub_download('chan4lk/piper-tts-sinhala', 'si_LK-sinhala-medium.onnx.json')
    piper_voices['piper-openslr'] = PiperVoice.load(p_chan_onnx, config_path=p_chan_json)
    piper_voices['piper-chan4lk'] = piper_voices['piper-openslr']

    print("✅ Local Piper Human Voice models ready: ['piper-openslr', 'piper-intellisr', 'piper-unicef']")
except Exception as e:
    print(f"⚠️ Piper models initialization notice: {e}")

try:
    import edge_tts
    print(f"✨ Microsoft Studio Neural TTS Engine Activated! (Default Voice: {DEFAULT_VOICE})")
except ImportError:
    print("❌ edge_tts library missing. Install via: pip install edge-tts")
    sys.exit(1)


NUM_MAP = {
    '0': 'බිංදුව', '1': 'එක', '2': 'දෙක', '3': 'තුන', '4': 'හතර',
    '5': 'පහ', '6': 'හය', '7': 'හත', '8': 'අට', '9': 'නවය'
}

# Automatic conversion of bookish/formal words into warm spoken Sinhala
COLLOQUIAL_MAP = [
    ('ඔබගේ', 'ඔයාගෙ'),
    ('ඔබේ', 'ඔයාගේ'),
    ('ඔබට', 'ඔයාට'),
    ('ඔබව', 'ඔයාව'),
    ('ඔබෙන්', 'ඔයාගෙන්'),
    ('ඔබ', 'ඔයා'),
    ('උදවු', 'උදව්'),
    ('පවසන්න', 'කියන්න'),
    ('පවසන්නකො', 'කියන්නකො'),
    ('හැකියි', 'පුළුවන්'),
    ('හැකිය', 'පුළුවන්'),
    ('හැකිද', 'පුළුවන්ද'),
    ('නැවත', 'ආයෙත්'),
    ('පැමිණෙන්න', 'එන්න'),
    ('විමසන්න', 'අහන්න'),
    ('ලබාගන්න', 'ගන්න'),
    ('සැපයිය හැකිය', 'දෙන්න පුළුවන්'),
    ('හලෝ', 'හෙලෝ'),
]


SRI_LANKAN_SPOKEN_LOANWORDS = {
    # Tech, AI & Channels
    "whatsapp": "වට්ස්ඇප්",
    "ai": "ඒඅයි",
    "buildstart": "බිල්ඩ්ස්ටාර්ට්",
    "sms": "එස්එම්එස්",
    "bot": "බොට්",
    "bots": "බොට්ස්",
    "chatbot": "චැට්බොට්",
    "chatbots": "චැට්බොට්ස්",
    "crm": "සීආර්එම්",
    "web": "වෙබ්",
    "website": "වෙබ්සයිට්",
    "websites": "වෙබ්සයිට්ස්",
    "app": "ඇප්",
    "apps": "ඇප්ස්",
    "software": "සොෆ්ට්වෙයාර්",
    "online": "ඔන්ලයින්",
    "offline": "ඕෆ්ලයින්",
    "cloud": "ක්ලවුඩ්",
    "server": "සර්වර්",
    "servers": "සර්වර්ස්",
    "system": "සිස්ටම්",
    "systems": "සිස්ටම්ස්",
    "link": "ලින්ක්",
    "links": "ලින්ක්ස්",
    "data": "ඩේටා",
    "dashboard": "ඩෑෂ්බෝඩ්",
    "api": "ඒපීඅයි",
    "tool": "ටූල්",
    "tools": "ටූල්ස්",
    "platform": "ප්ලැට්ෆෝම්",
    "platforms": "ප්ලැට්ෆෝම්ස්",
    "code": "කෝඩ්",
    "script": "ස්ක්‍රිප්ට්",
    "internet": "ඉන්ටර්නෙට්",
    "network": "නෙට්වර්ක්",
    "device": "ඩිවයිස්",
    "devices": "ඩිවයිසස්",

    # Calling, Sales & Business Operations
    "call": "කෝල්",
    "calls": "කෝල්ස්",
    "calling": "කෝලිං",
    "caller": "කෝලර්",
    "callers": "කෝලර්ස්",
    "customer": "කස්ටමර්",
    "customers": "කස්ටමර්ස්",
    "client": "ක්ලයන්ට්",
    "clients": "ක්ලයන්ට්ස්",
    "business": "බිස්නස්",
    "businesses": "බිස්නස්",
    "service": "සර්විස්",
    "services": "සර්විසස්",
    "team": "ටීම්",
    "teams": "ටීම්ස්",
    "member": "මෙම්බර්",
    "members": "මෙම්බර්ස්",
    "staff": "ස්ටාෆ්",
    "agent": "ඒජන්ට්",
    "agents": "ඒජන්ට්ස්",
    "lead": "ලීඩ්",
    "leads": "ලීඩ්ස්",
    "deal": "ඩීල්",
    "deals": "ඩීල්ස්",
    "close": "ක්ලෝස්",
    "closing": "ක්ලෝසින්",
    "closed": "ක්ලෝස්ඩ්",
    "sales": "සේල්ස්",
    "marketing": "මාකටින්",
    "package": "පැකේජ්",
    "packages": "පැකේජස්",
    "plan": "ප්ලෑන්",
    "plans": "ප්ලෑන්ස්",
    "starter": "ස්ටාටර්",
    "pro": "ප්‍රෝ",
    "enterprise": "එන්ටර්ප්‍රයිස්",
    "standard": "ස්ටෑන්ඩර්ඩ්",
    "demo": "ඩෙමෝ",
    "demos": "ඩෙමෝස්",
    "account": "එකවුන්ට්",
    "accounts": "එකවුන්ට්ස්",
    "order": "ඕඩර්",
    "orders": "ඕඩර්ස්",
    "ordering": "ඕඩරින්",
    "detail": "විස්තර",
    "details": "විස්තර",
    "support": "සපෝට්",
    "help": "උදව්",
    "check": "චෙක්",
    "setup": "සෙටප්",
    "update": "අප්ඩේට්",
    "updates": "අප්ඩේට්ස්",
    "updating": "අප්ඩේටින්",
    "confirm": "කන්ෆර්ම්",
    "confirmed": "කන්ෆර්ම්ඩ්",
    "confirmation": "කන්ෆර්මේෂන්",
    "cancel": "කැන්සල්",
    "canceled": "කැන්සල්ඩ්",
    "cancellation": "කැන්සලේෂන්",
    "booking": "බුකින්",
    "bookings": "බුකින්ස්",
    "book": "බුක්",
    "booked": "බුක්ඩ්",
    "appointment": "ඇපොයින්ට්මන්ට්",
    "appointments": "ඇපොයින්ට්මන්ට්ස්",
    "meeting": "මීටින්",
    "meetings": "මීටින්ස්",
    "schedule": "ෂෙඩියුල්",
    "schedules": "ෂෙඩියුල්ස්",
    "scheduled": "ෂෙඩියුල්ඩ්",
    "scheduling": "ෂෙඩියුලින්",
    "automate": "ඔටෝමේට්",
    "automates": "ඔටෝමේට්",
    "automation": "ඔටෝමේෂන්",
    "automations": "ඔටෝමේෂන්ස්",
    "automatic": "ඔටෝමැටික්",
    "automated": "ඔටෝමේටඩ්",
    "automating": "ඔටෝමේටින්",
    "workflow": "වර්ක්ෆ්ලෝ",
    "workflows": "වර්ක්ෆ්ලෝස්",
    "message": "මැසේජ්",
    "messages": "මැසේජස්",
    "messaging": "මැසේජින්",
    "connect": "කනෙක්ට්",
    "connected": "කනෙක්ටඩ්",
    "connecting": "කනෙක්ටින්",
    "response": "රිස්පොන්ස්",
    "responses": "රිස්පොන්සස්",
    "respond": "රිස්පොන්ඩ්",
    "instant": "ඉන්ස්ටන්ට්",
    "test": "ටෙස්ට්",
    "testing": "ටෙස්ටින්",
    "followup": "ෆලෝඅප්",
    "followups": "ෆලෝඅප්ස්",
    "follow": "ෆලෝ",
    "reminders": "රිමයින්ඩර්ස්",
    "reminder": "රිමයින්ඩර්",
    "screen": "ස්ක්‍රීන්",
    "qualify": "කොලිෆයි",
    "qualified": "කොලිෆයිඩ්",
    "qualifying": "කොලිෆයිං",
    "handle": "හැන්ඩ්ල්",
    "handling": "හැන්ඩ්ලින්",
    "retail": "රීටේල්",
    "admissions": "ඇඩ්මිෂන්ස්",
    "admission": "ඇඩ්මිෂන්",
    "hotel": "හොටෙල්",
    "hotels": "හොටෙල්ස්",
    "feature": "ෆීචර්",
    "features": "ෆීචර්ස්",
    "notification": "නොටිෆිකේෂන්",
    "notifications": "නොටිෆිකේෂන්ස්",
    "contact": "කොන්ටැක්ට්",
    "contacts": "කොන්ටැක්ට්ස්",
    "number": "නම්බර්",
    "numbers": "නම්බර්ස්",
    "direct": "ඩිරෙක්ට්",
    "price": "ගාණ",
    "prices": "ගණන්",
    "pricing": "ප්‍රයිසින්",
    "charge": "චාර්ජ්",
    "charges": "චාර්ජස්",
    "discount": "ඩිස්කවුන්ට්",
    "company": "කම්පැනි",
    "office": "ඔෆිස්",
    "channel": "චැනල්",
    "channels": "චැනල්ස්",
    "manager": "මැනේජර්",
    "management": "මැනේජ්මන්ට්",
    "manage": "මැනේජ්",
    "developer": "ඩිවලොපර්",
    "process": "ප්‍රොසෙස්",
    "product": "ප්‍රොඩක්ට්",
    "products": "ප්‍රොඩක්ට්ස්",
    "integration": "ඉන්ටග්‍රේෂන්",
    "integrations": "ඉන්ටග්‍රේෂන්ස්",

    # Conversational & Daily Spoken
    "ok": "ඕකේ",
    "okay": "ඕකේ",
    "yes": "ඔව්",
    "no": "නෑ",
    "hello": "හෙලෝ",
    "hi": "හායි",
    "hey": "හේයි",
    "bye": "බායි",
    "thanks": "තෑන්ක්ස්",
    "thank": "තෑන්ක්",
    "you": "යූ",
    "welcome": "වෙල්කම්",
    "sorry": "සොරි",
    "sure": "ෂුවර්",
    "super": "සුපර්",
    "perfect": "පර්ෆෙක්ට්",
    "good": "ගුඩ්",
    "fine": "ෆයින්",
    "nice": "නයිස්",
    "great": "ග්‍රේට්",
    "today": "අද",
    "tomorrow": "හෙට",
    "now": "දැන්",
    "live": "ලයිව්",
    "real": "රියල්",
    "ready": "රෙඩි",
    "start": "ස්ටාට්",
    "stop": "ස්ටොප්",
    "done": "ඩන්",
    "voice": "වොයිස්",
}


def transliterate_loanword_algorithmic(word: str) -> str:
    """Fallback phonetic conversion of unmapped English words into clean Sinhala characters for Nipunika."""
    w = word.lower().strip()
    if not w:
        return ""
    if w in SRI_LANKAN_SPOKEN_LOANWORDS:
        return SRI_LANKAN_SPOKEN_LOANWORDS[w]

    # Handle common prefixes
    if w.startswith("auto"):
        return "ඔටෝ" + transliterate_loanword_algorithmic(w[4:])
    elif w.startswith("tele"):
        return "ටෙලි" + transliterate_loanword_algorithmic(w[4:])
    elif w.startswith("inter"):
        return "ඉන්ටර්" + transliterate_loanword_algorithmic(w[5:])
    elif w.startswith("micro"):
        return "මයික්‍රෝ" + transliterate_loanword_algorithmic(w[5:])
    elif w.startswith("super"):
        return "සුපර්" + transliterate_loanword_algorithmic(w[5:])

    # Multi-letter phonetic patterns
    phonetic_patterns = [
        (r'tions?\b', 'ෂන්ස්' if w.endswith('s') else 'ෂන්'),
        (r'sions?\b', 'ෂන්ස්' if w.endswith('s') else 'ෂන්'),
        (r'ments?\b', 'මන්ට්ස්' if w.endswith('s') else 'මන්ට්'),
        (r'ings?\b', 'ඉන්ස්' if w.endswith('s') else 'ඉන්'),
        (r'ables?\b', 'බල්ස්' if w.endswith('s') else 'බල්'),
        (r'ibles?\b', 'බල්ස්' if w.endswith('s') else 'බල්'),
        (r'nesses?\b', 'නසස්' if w.endswith('es') else 'නස්'),
        (r'ness\b', 'නස්'),
        (r'ives?\b', 'ඉව්ස්' if w.endswith('s') else 'ඉව්'),
        (r'ers?\b', 'ර්ස්' if w.endswith('s') else 'ර්'),
        (r'ors?\b', 'ර්ස්' if w.endswith('s') else 'ර්'),
        (r'ists?\b', 'ඉස්ට්ස්' if w.endswith('s') else 'ඉස්ට්'),
        (r'ics?\b', 'ඉක්ස්' if w.endswith('s') else 'ඉක්'),
        (r'ities?\b', 'ඉටීස්' if w.endswith('ies') else 'ඉටි'),
        (r'ity\b', 'ඉටි'),
        (r'fully\b', 'ෆුලි'),
        (r'ful\b', 'ෆුල්'),
        (r'ed\b', 'ඩ්'),
    ]
    for pat, rep in phonetic_patterns:
        w = re.sub(pat, rep, w)

    # Phonetic mappings for non-Sinhala letters to ensure Nipunika vocabulary compatibility
    w = w.replace('w', 'v').replace('q', 'k').replace('x', 'ks').replace('z', 's')
    w = w.replace('th', 't').replace('sh', 's').replace('ch', 'c').replace('ph', 'f')
    return w


def enhance_prosody(t: str) -> str:
    """Ensure natural fluent phrasing without word-by-word chopping."""
    # Completely remove all apostrophes and single quotes (they trigger glottal stops in VITS)
    t = t.replace("'", "").replace("`", "").replace("’", "").replace("‘", "")

    # Natural conversational starter (only a single gentle comma at the start of sentence)
    starters = [
        ("ආ හරි", "ආ හරි,"),
        ("අහ් හරි", "අහ් හරි,"),
        ("හරි බලමුකො", "හරි බලමුකො,"),
        ("ඒක තමයි", "ඒක තමයි,"),
        ("ඔව් අනිවාර්යයෙන්ම", "ඔව් අනිවාර්යයෙන්ම,"),
        ("අනිවාර්යයෙන්ම", "අනිවාර්යයෙන්ම,"),
        ("ඕකේ", "ඕකේ,"),
        ("ඔව්", "ඔව්,"),
        ("හරි", "හරි,"),
        ("ආ", "ආ,"),
    ]
    starters.sort(key=lambda x: len(x[0]), reverse=True)

    for orig, rep in starters:
        t = re.sub(rf"^{orig}(\s+)(?![,])", rf"{rep} ", t)
        t = re.sub(rf"([.!?])\s*{orig}(\s+)(?![,])", rf"\1 {rep} ", t)

    # Clean double commas and trailing punctuation
    t = re.sub(r",\s*,+", ",", t)
    t = re.sub(r"\s+([,.!?])", r"\1", t)
    return t


def normalize_text(text: str) -> str:
    """Preprocess text for fluent, connected spoken Sinhala with co-articulation and no word-by-word pauses."""
    if not text:
        return ""

    # Strip markdown symbols, asterisks, brackets, quotes, and emojis
    text = re.sub(r'[*#_`~\'\"’‘]', '', text)

    # 1. Convert English loanwords to crisp natural Sri Lankan spoken pronunciation
    def _replace_loanword(m):
        raw = m.group(0).lower()
        if raw in SRI_LANKAN_SPOKEN_LOANWORDS:
            return " " + SRI_LANKAN_SPOKEN_LOANWORDS[raw] + " "
        return " " + transliterate_loanword_algorithmic(raw) + " "

    text = re.sub(r'[A-Za-z]+', _replace_loanword, text)

    # 2. Convert numeric digits to spoken Sinhala words
    for digit, word in NUM_MAP.items():
        text = text.replace(digit, f" {word} ")

    # 3. Normalize bookish/written words to natural spoken Sinhala
    for formal, spoken in COLLOQUIAL_MAP:
        text = text.replace(formal, spoken)

    # 4. Strip internal commas inside short lists to maintain continuous vocal flow
    text = re.sub(r'(?<=[^\s,.!?]),(?=[^\s,.!?])', ' ', text)

    # 5. Apply smooth prosody
    text = enhance_prosody(text)

    # 6. Normalize multiple dots into gentle pauses and clean whitespace
    text = re.sub(r'\.{2,}', ', ', text)
    text = re.sub(r'[-–—]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def synthesize_elevenlabs(text: str, voice_id: str = ELEVENLABS_VOICE_ID) -> bytes:
    """Synthesize speech using ElevenLabs Multilingual v2 if key is configured."""
    import requests
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}?output_format=mp3_44100_128"
    headers = {
        "xi-api-key": ELEVENLABS_API_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "text": text,
        "model_id": "eleven_multilingual_v2",
        "voice_settings": {
            "stability": 0.5,
            "similarity_boost": 0.75
        }
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=8)
    if resp.status_code == 200:
        return resp.content
    raise RuntimeError(f"ElevenLabs error {resp.status_code}: {resp.text}")


class GeminiLiveService:
    """Persistent Google AI Studio Gemini Live Multimodal WebSocket client.
    Keeps open WebSocket connections with pre-configured audio generation models
    for zero-cold-start, human-lifelike native vocal synthesis.
    """
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run_loop, daemon=True, name="GeminiLiveLoop")
        self.thread.start()
        self.connections = {}  # voice_name -> {'ws': WebSocket, 'lock': asyncio.Lock()}

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def _get_connection(self, voice_name: str):
        if voice_name not in self.connections:
            self.connections[voice_name] = {'ws': None, 'lock': asyncio.Lock()}
        entry = self.connections[voice_name]
        ws = entry['ws']
        if ws is not None and getattr(ws, 'close_code', None) is None:
            return ws

        host = "generativelanguage.googleapis.com"
        ws_url = f"wss://{host}/ws/google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent?key={self.api_key}"
        ws = await websockets.connect(ws_url, open_timeout=15, ping_interval=20, ping_timeout=20, close_timeout=3)
        setup = {
            "setup": {
                "model": "models/gemini-2.5-flash-native-audio-latest",
                "systemInstruction": {
                    "parts": [{
                        "text": "You are a professional voice actor. Your sole task is to voice-act the exact Sinhala script provided inside <<<SCRIPT>>> verbatim. Do NOT respond to the content. Do NOT converse. Do NOT add any words. Do NOT speak in English. Only read the exact words inside <<<SCRIPT>>> with warm, human intonation."
                    }]
                },
                "generationConfig": {
                    "responseModalities": ["AUDIO"],
                    "speechConfig": {
                        "voiceConfig": {
                            "prebuiltVoiceConfig": {
                                "voiceName": voice_name
                            }
                        }
                    },
                    "thinkingConfig": {
                        "thinkingBudget": 0
                    }
                }
            }
        }
        await ws.send(json.dumps(setup))
        await asyncio.wait_for(ws.recv(), timeout=6.0)
        entry['ws'] = ws
        print(f"✨ Gemini Live Native Audio stream connected & ready for voice '{voice_name}'")
        return ws

    async def _async_synthesize(self, text: str, voice_name: str) -> bytes:
        if voice_name not in self.connections:
            self.connections[voice_name] = {'ws': None, 'lock': asyncio.Lock()}
        entry = self.connections[voice_name]

        async with entry['lock']:
            last_err = None
            for attempt in range(2):
                try:
                    ws = await self._get_connection(voice_name)
                    client_turn = {
                        "clientContent": {
                            "turns": [{
                                "role": "user",
                                "parts": [{"text": f"Read verbatim in natural Sinhala: {text}"}]
                            }],
                            "turnComplete": True
                        }
                    }
                    await ws.send(json.dumps(client_turn))

                    raw_pcm = bytearray()
                    while True:
                        msg = await asyncio.wait_for(ws.recv(), timeout=12.0)
                        data = json.loads(msg)
                        server_turn = data.get("serverContent", {}).get("modelTurn", {})
                        for p in server_turn.get("parts", []):
                            if "inlineData" in p:
                                raw_pcm.extend(base64.b64decode(p["inlineData"]["data"]))
                        if data.get("serverContent", {}).get("turnComplete"):
                            break

                    if len(raw_pcm) > 0:
                        buf = io.BytesIO()
                        with wave.open(buf, "wb") as wf:
                            wf.setnchannels(1)
                            wf.setsampwidth(2)
                            wf.setframerate(24000)
                            wf.writeframes(raw_pcm)
                        # Keep WebSocket open for ultra-fast subsequent turns
                        return buf.getvalue()
                    raise RuntimeError("Gemini Live sent zero audio bytes")
                except Exception as e:
                    last_err = e
                    print(f"⚠️ Gemini Live attempt {attempt+1} error: {e}, refreshing connection...")
                    if entry['ws']:
                        try:
                            await entry['ws'].close()
                        except Exception:
                            pass
                    entry['ws'] = None

            raise last_err or RuntimeError("Gemini Live synthesis failed after retries")

    def prewarm(self, voice_name: str = "Aoede"):
        """Pre-warm WebSocket connection in the background."""
        def _warm():
            try:
                self.synthesize("හෙලෝ", voice_name, timeout=15.0)
                print(f"✨ Gemini Live prewarm completed for voice '{voice_name}'")
            except Exception as e:
                print(f"⚠️ Gemini Live prewarm notice: {e}")
        threading.Thread(target=_warm, daemon=True).start()

    def synthesize(self, text: str, voice: str = "Aoede", timeout: float = 25.0) -> bytes:
        v_map = {
            "gemini-aoede": "Aoede", "gemini-puck": "Puck", "gemini-charon": "Charon",
            "gemini-kore": "Kore", "gemini-fenrir": "Fenrir",
            "aoede": "Aoede", "puck": "Puck", "charon": "Charon",
            "kore": "Kore", "fenrir": "Fenrir",
        }
        voice_name = v_map.get(voice.lower(), "Aoede")
        fut = asyncio.run_coroutine_threadsafe(self._async_synthesize(text, voice_name), self.loop)
        return fut.result(timeout=timeout)


gemini_live_service = GeminiLiveService(GEMINI_API_KEY) if GEMINI_API_KEY else None
if gemini_live_service:
    gemini_live_service.prewarm("Aoede")


def synthesize_gemini_tts(text: str, voice: str = "Aoede") -> bytes:
    """Generate human-lifelike speech directly using Google AI Studio Gemini Live Multimodal WebSocket."""
    if not gemini_live_service:
        raise ValueError("GEMINI_API_KEY is not configured")
    return gemini_live_service.synthesize(text, voice=voice)


def synthesize_edge_tts(text: str, voice: str = DEFAULT_VOICE, rate: str = RATE_MODIFIER, pitch: str = PITCH_MODIFIER) -> bytes:
    """Synthesize speech using Microsoft Edge Neural voices (si-LK-ThiliniNeural / si-LK-SameeraNeural)."""
    async def _synth():
        comm = edge_tts.Communicate(text, voice, rate=rate, pitch=pitch)
        buf = io.BytesIO()
        async for chunk in comm.stream():
            if chunk.get("type") == "audio" and "data" in chunk:
                buf.write(chunk["data"])
        return buf.getvalue()

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(_synth())
    finally:
        loop.close()


def synthesize_dialog_nipunika(text: str) -> bytes:
    """Synthesize speech using Dialog Axiata Nipunika VITS (22,050 Hz Studio Quality)."""
    if not dialog_synthesizer or not dialog_romanizer:
        raise RuntimeError("Dialog Nipunika model is not loaded")
    roman_text = dialog_romanizer.sinhala_to_roman(text)
    with dialog_lock:
        wav = dialog_synthesizer.tts(roman_text)
        buf = io.BytesIO()
        dialog_synthesizer.save_wav(wav, buf)
    buf.seek(0)
    return buf.read()


def synthesize_piper(text: str, voice_key: str = "piper-openslr") -> bytes:
    """Synthesize speech using local Piper ONNX models."""
    p_voice = (
        piper_voices.get(voice_key)
        or piper_voices.get("piper-openslr")
        or piper_voices.get("piper-intellisr")
        or piper_voices.get("piper-unicef")
    )
    if not p_voice:
        raise RuntimeError(f"Piper voice '{voice_key}' not found and no piper voices loaded")

    with piper_lock:
        chunks = list(p_voice.synthesize(text))
    if not chunks:
        return b""

    # Use audio_int16_bytes or audio_int16_array
    if hasattr(chunks[0], "audio_int16_bytes"):
        all_raw = b"".join(c.audio_int16_bytes for c in chunks)
    else:
        all_raw = np.concatenate([c.audio_int16_array for c in chunks]).tobytes()

    s_rate = p_voice.config.sample_rate or 16000
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(s_rate)
        wav_file.writeframes(all_raw)

    buf.seek(0)
    return buf.read()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "primary_engine": "dialog-nipunika-vits" if dialog_synthesizer else "piper-neural",
        "default_voice": DEFAULT_VOICE,
        "available_voices": [
            "dialog-nipunika (Dialog Axiata & UoM Lab 22.05 kHz Studio Female - #1 Natural Voice)",
            "piper-openslr (Google OpenSLR 30 High-Fidelity 22.05 kHz Neural Voice)",
            "piper-ashoka (Ashoka Weerawardhana Studio Voice - 16 kHz)",
            "gemini-aoede (Google AI Studio Super-Natural Female)",
            "si-LK-ThiliniNeural (Microsoft Female Neural - Free)"
        ]
    })


@app.route("/v1/audio/speech", methods=["POST"])
@app.route("/synthesize", methods=["POST"])
def synthesize():
    data = request.get_json(force=True, silent=True) or {}
    raw_text = data.get("input") or data.get("text") or ""
    voice = data.get("voice") or DEFAULT_VOICE

    text = normalize_text(raw_text)
    if not text:
        return Response(b"", mimetype="audio/mpeg")

    print(f"🎙️ [TTS] In: {raw_text!r} -> Norm: {text!r}")

    has_english = bool(re.search(r'[A-Za-z]{2,}', text))

    # 0. ElevenLabs Custom Voice Clone (Multilingual v2)
    if (voice.lower() == "elevenlabs" or DEFAULT_VOICE == "elevenlabs") and ELEVENLABS_API_KEY:
        try:
            audio_mp3 = synthesize_elevenlabs(text, voice_id=ELEVENLABS_VOICE_ID)
            if audio_mp3 and len(audio_mp3) > 100:
                print(f"✨ [TTS ELEVENLABS] Synthesized {len(audio_mp3)} bytes")
                return Response(audio_mp3, mimetype="audio/mpeg")
        except Exception as el_err:
            print(f"⚠️ ElevenLabs voice clone error: {el_err}, falling back...")

    # 1. Primary: Dialog Axiata Nipunika Studio Voice (22.05 kHz Studio Human Quality)
    if "dialog" in voice.lower() or "nipunika" in voice.lower() or voice.lower() == "dialog-nipunika" or (dialog_synthesizer and not any(k in voice.lower() for k in ["gemini", "piper", "ashoka", "si-lk", "thilini", "sameera"])):
        try:
            audio_wav = synthesize_dialog_nipunika(text)
            if audio_wav and len(audio_wav) > 100:
                print(f"✨ [TTS NIPUNIKA] Synthesized {len(audio_wav)} bytes")
                return Response(audio_wav, mimetype="audio/wav")
        except Exception as d_err:
            print(f"⚠️ Dialog Nipunika error: {d_err}, falling back to Piper OpenSLR...")

    # 3. Offline High-Fidelity Neural Fallback: Piper OpenSLR / Ashoka
    if "piper" in voice.lower() or "ashoka" in voice.lower() or "openslr" in voice.lower():
        try:
            audio_wav = synthesize_piper(text, voice_key=voice)
            if audio_wav:
                return Response(audio_wav, mimetype="audio/wav")
        except Exception as p_err:
            print(f"⚠️ Piper synthesis error: {p_err}, falling back to Edge Neural...")

    # 4. Optional: Google AI Studio Gemini Flash Native Voice
    if GEMINI_API_KEY and ("gemini" in voice.lower() or voice.lower() in ["aoede", "puck", "charon", "kore", "fenrir"]):
        try:
            audio_wav = synthesize_gemini_tts(text, voice=voice)
            if audio_wav and len(audio_wav) > 100:
                return Response(audio_wav, mimetype="audio/wav")
        except Exception as g_err:
            print(f"❌ Gemini Flash AI Voice error: {g_err}")

    # 5. Secondary: High-fidelity Microsoft Neural Voice (Thilini / Sameera)
    edge_voice = voice if "si-lk" in voice.lower() else "si-LK-ThiliniNeural"
    try:
        audio_mp3 = synthesize_edge_tts(text, voice=edge_voice)
        if audio_mp3 and len(audio_mp3) > 100:
            return Response(audio_mp3, mimetype="audio/mpeg")
    except Exception as edge_err:
        print(f"⚠️ Edge Neural TTS error: {edge_err}, falling back to Piper...")

    # 6. Ultimate Fallback: Local Piper ONNX
    try:
        audio_wav = synthesize_piper(text)
        if audio_wav:
            return Response(audio_wav, mimetype="audio/wav")
    except Exception as piper_err:
        print(f"❌ Piper fallback error: {piper_err}")

    return jsonify({"error": "Synthesis failed on all engines"}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    print(f"🚀 Studio Neural TTS server listening on http://127.0.0.1:{port} (Voice: {DEFAULT_VOICE})")
    app.run(host="127.0.0.1", port=port, debug=False)
