"""
Bale Messenger Bot — Downloader + YouTube Downloader + Telegram File Sync
"""

import asyncio
import concurrent.futures
import io
import os
import logging
import math
import tempfile
import zipfile
from urllib.parse import urlparse, quote as url_quote

import aiohttp
from balethon import Client
from balethon.objects.message import Message
from balethon.conditions import private, text
from balethon.conditions.condition import Condition

import config as cfg

# ──────────────────────────────────────────────
# Setup
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("bale-bot")
os.makedirs(cfg.Config.DOWNLOAD_DIR, exist_ok=True)

bot = Client(cfg.Config.BALE_TOKEN)

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
SPLIT_THRESHOLD = 20 * 1024 * 1024   # split files > 20 MB
CHUNK_SIZE      = 19 * 1024 * 1024   # each zip chunk ≤ 19 MB

# ──────────────────────────────────────────────
# State Machine  (simple dict-based)
# ──────────────────────────────────────────────
_states  = {}   # user_id → state string
_yt_urls = {}   # user_id → youtube url (while picking resolution)


def set_state(msg, state):
    _states[msg.author.id] = state

def clear_state(msg):
    _states.pop(msg.author.id, None)


class _AtState(Condition):
    """Condition: true when user's state equals *state*."""
    def __init__(self, state):
        super().__init__(can_process=(Message,))
        self._state = state

    async def __call__(self, client, event):
        if not event or not getattr(event, "author", None):
            return False
        return _states.get(event.author.id) == self._state


def at_state(state):
    return _AtState(state)


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def safe_reply(message, text):
    """Balethon's message.reply() only accepts (text, reply_markup)."""
    return message.reply(text)


def get_filename_from_url(url, default="download"):
    path = urlparse(url).path
    return os.path.basename(path) or default


MIME_EXT = {
    "video/mp4": ".mp4", "video/webm": ".webm", "video/quicktime": ".mov",
    "audio/mpeg": ".mp3", "audio/ogg": ".ogg", "audio/wav": ".wav",
    "audio/mp4": ".m4a", "audio/x-m4a": ".m4a",
    "image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif",
    "image/webp": ".webp",
    "application/pdf": ".pdf", "application/zip": ".zip",
    "application/x-rar-compressed": ".rar", "application/octet-stream": ".bin",
}


async def download_from_url(session, url):
    """Download from URL → (bytes, filename, mime, size)."""
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=300)) as resp:
        if resp.status != 200:
            raise Exception(f"HTTP {resp.status}: {resp.reason}")

        total = int(resp.headers.get("Content-Length", 0))
        mime  = resp.headers.get("Content-Type", "application/octet-stream").split(";")[0].strip()
        fname = get_filename_from_url(url)

        # Telegram bot API limit is 50 MB — don't block larger files here
        # Bale splitting handles the 20 MB Bale limit separately
        TG_BOT_LIMIT = 50 * 1024 * 1024
        if total and total > cfg.Config.MAX_FILE_SIZE:
            raise Exception(
                f"Too large: {total/1024/1024:.1f} MB "
                f"(max {cfg.Config.MAX_FILE_SIZE/1024/1024:.0f} MB)"
            )

        data = bytearray()
        async for chunk in resp.content.iter_chunked(65536):
            data.extend(chunk)
            if len(data) > TG_BOT_LIMIT:
                raise Exception(f"Exceeds {TG_BOT_LIMIT/1024/1024:.0f} MB (Telegram bot limit)")

        if not os.path.splitext(fname)[1]:
            ext = MIME_EXT.get(mime)
            if ext:
                fname += ext

        return bytes(data), fname, mime, len(data)


# ──────────────────────────────────────────────
# File splitting (zip parts)
# ──────────────────────────────────────────────
def split_to_parts(data, filename):
    """Split *data* into zip parts each < CHUNK_SIZE."""
    total   = len(data)
    n_parts = math.ceil(total / CHUNK_SIZE)
    base, ext = os.path.splitext(filename)
    parts = []

    for i in range(n_parts):
        chunk = data[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE]
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            inner = f"{base}.part{i+1}of{n_parts}{ext}" if ext else f"{base}.part{i+1}of{n_parts}"
            zf.writestr(inner, chunk)
        parts.append((buf.getvalue(), f"{base}.part{i+1}of{n_parts}.zip"))

    return parts


async def send_reply(message, data, filename, media_type="document", prefix="📁"):
    """Send file via message reply; auto-split if > SPLIT_THRESHOLD."""
    caption = f"{prefix} {filename}"

    if len(data) <= SPLIT_THRESHOLD:
        fn = {
            "video":   message.reply_video,
            "audio":   message.reply_audio,
            "photo":   message.reply_photo,
        }.get(media_type, message.reply_document)
        await fn(data, caption=caption)
    else:
        parts = split_to_parts(data, filename)
        total = len(parts)
        await safe_reply(message,
            f"📦 File too large ({len(data)/1024/1024:.1f} MB), "
            f"sending in {total} parts…")
        for i, (pd, pn) in enumerate(parts):
            await message.reply_document(
                pd,
                caption=f"{prefix} {filename} — Part {i+1}/{total}"
            )
        await safe_reply(message,
            f"✅ Done — {total} parts.\n"
            "Extract each zip, then join the inner files:\n"
            "  Linux/Mac:  cat part1* part2* > file\n"
            "  Windows:    copy /b part1+part2 file")


# ──────────────────────────────────────────────
# Main event loop reference (for thread-safe Bale sends from TG sync)
# ──────────────────────────────────────────────
_main_loop = None


async def send_to_bale_safe(bale_client, bale_id, data, filename,
                            media_type="document", prefix="📁"):
    """Send file to Bale, routing through main event loop if needed.
    The TG sync thread runs its own event loop, but Balethon client
    methods must run on the main loop. This bridges the gap."""
    global _main_loop
    if _main_loop is None or _main_loop is asyncio.get_running_loop():
        await send_to_chat(bale_client, bale_id, data, filename, media_type, prefix)
    else:
        coro = send_to_chat(bale_client, bale_id, data, filename, media_type, prefix)
        future = asyncio.run_coroutine_threadsafe(coro, _main_loop)
        # Wrap the concurrent.futures.Future into an awaitable asyncio.Future
        await asyncio.wrap_future(future)


async def send_to_chat(client, chat_id, data, filename,
                       media_type="document", prefix="📁"):
    """Send file via client (for TG sync); auto-split if large."""
    caption = f"{prefix} {filename}"

    if len(data) <= SPLIT_THRESHOLD:
        fn = {
            "video": client.send_video,
            "audio": client.send_audio,
            "photo": client.send_photo,
        }.get(media_type, client.send_document)
        buf = io.BytesIO(data)
        buf.name = filename  # so Balethon/httpx uses real filename
        await fn(chat_id, buf, caption=caption)
    else:
        parts = split_to_parts(data, filename)
        total = len(parts)
        await client.send_message(chat_id,
            f"📦 Splitting {filename} ({len(data)/1024/1024:.1f} MB) "
            f"into {total} parts…")
        for i, (pd, pn) in enumerate(parts):
            buf = io.BytesIO(pd)
            buf.name = pn  # set filename on each part
            await client.send_document(
                chat_id, buf,
                caption=f"{prefix} {filename} — Part {i+1}/{total}"
            )
        await client.send_message(chat_id,
            f"✅ {total} parts sent. Extract zips and join inner files.")


# ──────────────────────────────────────────────
# Error handler
# ──────────────────────────────────────────────
@bot.on_error()
async def error_handler(client, error):
    log.error("Handler error: %s: %s", type(error).__name__, error)


# ──────────────────────────────────────────────
# Utility commands
# ──────────────────────────────────────────────
@bot.on_command(private)
async def cancel(*, message):
    clear_state(message)
    _yt_urls.pop(message.author.id, None)
    await safe_reply(message, "❌ Cancelled.")


@bot.on_command(private)
async def start(*, message):
    clear_state(message)
    _yt_urls.pop(message.author.id, None)
    await safe_reply(message,
        "✨ Welcome to Downloader Bot!\n\n"
        "📹 /video  — Download video from URL\n"
        "🎵 /audio  — Download audio from URL\n"
        "🖼 /photo  — Download image from URL\n"
        "📁 /file   — Download any file from URL\n"
        "▶️ /yt     — YouTube video (pick quality)\n"
        "🎵 /ytaudio — YouTube audio (mp3)\n"
        "🔄 /tgsync — Telegram ↔ Bale sync info\n\n"
        "❌ /cancel — Cancel current action\n"
        "ℹ️ /help   — All commands")


@bot.on_command(private, name="help")
async def help_command(*, message):
    clear_state(message)
    _yt_urls.pop(message.author.id, None)
    await safe_reply(message,
        "📚 Commands:\n\n"
        "/video   — Download video from URL\n"
        "/audio   — Download audio from URL\n"
        "/photo   — Download image from URL\n"
        "/file    — Download any file from URL\n"
        "/yt      — YouTube video (choose quality)\n"
        "/ytaudio — YouTube audio (mp3)\n"
        "/tgsync  — TG ↔ Bale sync status\n"
        "/cancel  — Cancel current action")


# ──────────────────────────────────────────────
# Download commands  (set state, ask for URL)
# ──────────────────────────────────────────────
@bot.on_command(private)
async def video(*, message):
    clear_state(message)
    _yt_urls.pop(message.author.id, None)
    set_state(message, "s_video")
    await safe_reply(message, "📹 Send me the video URL:")


@bot.on_command(private)
async def audio(*, message):
    clear_state(message)
    _yt_urls.pop(message.author.id, None)
    set_state(message, "s_audio")
    await safe_reply(message, "🎵 Send me the audio URL:")


@bot.on_command(private)
async def photo(*, message):
    clear_state(message)
    _yt_urls.pop(message.author.id, None)
    set_state(message, "s_photo")
    await safe_reply(message, "🖼 Send me the image URL:")


@bot.on_command(private, name="file")
async def file_download(*, message):
    clear_state(message)
    _yt_urls.pop(message.author.id, None)
    set_state(message, "s_file")
    await safe_reply(message, "📁 Send me the file URL:")


@bot.on_command(private, name="yt")
async def yt(*, message):
    clear_state(message)
    _yt_urls.pop(message.author.id, None)
    set_state(message, "s_yt")
    await safe_reply(message, "▶️ Send me the YouTube URL:")


@bot.on_command(private, name="ytaudio")
async def ytaudio(*, message):
    clear_state(message)
    _yt_urls.pop(message.author.id, None)
    set_state(message, "s_yta")
    await safe_reply(message, "🎵 Send me the YouTube URL for audio (mp3):")


@bot.on_command(private, name="tgsync")
async def tgsync(*, message):
    clear_state(message)
    if not cfg.Config.TG_TOKEN:
        await safe_reply(message,
            "⚠️ Telegram sync disabled.\nSet TG_TOKEN in .env.")
        return
    mapping = "\n".join(
        f"  • TG {t} → Bale {b}"
        for t, b in cfg.Config.USER_MAPPING.items()
    ) or "  (none)"
    await safe_reply(message,
        f"🔄 Telegram ↔ Bale Sync\n\n"
        f"Status: ✅ Active\nUsers:\n{mapping}\n\n"
        "Send files to your Telegram bot to forward to Bale.")


# ──────────────────────────────────────────────
# State handlers  (receive URL / options)
# ──────────────────────────────────────────────
async def _safe_download_handler(message, url, label, media_type, prefix):
    """Generic handler for URL downloads with timeout & full error safety."""
    try:
        status = await safe_reply(message, f"⏳ Downloading {label}…")
    except Exception as e:
        log.error("safe_reply failed: %s", e)
        clear_state(message)
        return

    try:
        async with aiohttp.ClientSession() as session:
            data, fname, mime, size = await asyncio.wait_for(
                download_from_url(session, url), timeout=90)
    except asyncio.TimeoutError:
        log.error("%s download timeout: %s", label, url)
        try:
            await status.edit_text(f"❌ Download timed out (90s). Try a shorter URL or direct link.")
        except Exception:
            pass
        return
    except Exception as e:
        log.error("%s err: %s", label, e)
        try:
            err_msg = str(e)[:200]
            await status.edit_text(f"❌ {err_msg}")
        except Exception:
            pass
        return

    log.info("%s: %s (%d bytes)", label, fname, size)
    try:
        await status.edit_text(f"✅ {fname} ({size/1024/1024:.1f} MB)…")
        await send_reply(message, data, fname, media_type, prefix)
        try:
            await status.delete()
        except Exception:
            pass
    except Exception as e:
        log.error("%s send err: %s", label, e)
        try:
            await status.edit_text(f"❌ Send failed: {e}")
        except Exception:
            pass


@bot.on_message(private & text & at_state("s_video"))
async def handle_video(*, message):
    clear_state(message)
    url = message.text.strip()
    if url.startswith("/"):
        return
    await _safe_download_handler(message, url, "Video", "video", "📹")


@bot.on_message(private & text & at_state("s_audio"))
async def handle_audio(*, message):
    clear_state(message)
    url = message.text.strip()
    if url.startswith("/"):
        return
    await _safe_download_handler(message, url, "Audio", "audio", "🎵")


@bot.on_message(private & text & at_state("s_photo"))
async def handle_photo(*, message):
    clear_state(message)
    url = message.text.strip()
    if url.startswith("/"):
        return
    await _safe_download_handler(message, url, "Photo", "photo", "🖼")


@bot.on_message(private & text & at_state("s_file"))
async def handle_file(*, message):
    clear_state(message)
    url = message.text.strip()
    if url.startswith("/"):
        return
    await _safe_download_handler(message, url, "File", "document", "📁")


# ── YouTube video (two-step: URL → resolution) ──

@bot.on_message(private & text & at_state("s_yt"))
async def handle_yt_url(*, message):
    url = message.text.strip()
    if url.startswith("/"):
        clear_state(message)
        return

    _yt_urls[message.author.id] = url
    set_state(message, "s_yt_res")
    await safe_reply(message,
        "📐 Select video quality:\n\n"
        "1 — Best (default)\n"
        "2 — 480p\n"
        "3 — 720p\n"
        "4 — 1080p\n\n"
        "Send 1-4 or type resolution (480/720/1080)")


@bot.on_message(private & text & at_state("s_yt_res"))
async def handle_yt_res(*, message):
    uid  = message.author.id
    url  = _yt_urls.pop(uid, "")
    clear_state(message)

    choice = message.text.strip().lower()
    res_map = {
        "1": None, "best": None, "default": None,
        "2": 480, "480": 480,
        "3": 720, "720": 720,
        "4": 1080, "1080": 1080,
    }

    if choice not in res_map:
        _yt_urls[uid] = url
        set_state(message, "s_yt_res")
        await safe_reply(message, "⚠️ Invalid. Send 1-4 or 480/720/1080")
        return

    res = res_map[choice]
    res_label = f"{res}p" if res else "best"

    try:
        status = await safe_reply(message, f"⏳ Downloading YouTube ({res_label})…")
    except Exception as e:
        log.error("yt safe_reply failed: %s", e)
        return

    try:
        fpath = await asyncio.wait_for(
            asyncio.to_thread(_yt_video, url, res), timeout=120)
        size  = os.path.getsize(fpath)
        with open(fpath, "rb") as f:
            data = f.read()
        os.remove(fpath)

        fname = os.path.basename(fpath)
        log.info("YT video: %s (%d bytes)", fname, size)
        await status.edit_text(f"✅ {fname} ({size/1024/1024:.1f} MB)…")
        await send_reply(message, data, fname, "video", "▶️")
        try:
            await status.delete()
        except Exception:
            pass
    except asyncio.TimeoutError:
        log.error("yt download timeout: %s", url)
        try:
            await status.edit_text("❌ YouTube download timed out (120s).")
        except Exception:
            pass
    except Exception as e:
        log.error("yt err: %s", e)
        try:
            await status.edit_text(f"❌ {e}")
        except Exception:
            pass


# ── YouTube audio ──

@bot.on_message(private & text & at_state("s_yta"))
async def handle_ytaudio(*, message):
    clear_state(message)
    url = message.text.strip()
    if url.startswith("/"):
        return

    try:
        status = await safe_reply(message, "⏳ Downloading YouTube audio…")
    except Exception as e:
        log.error("ytaudio safe_reply failed: %s", e)
        return

    try:
        fpath = await asyncio.wait_for(
            asyncio.to_thread(_yt_audio, url), timeout=120)
        size  = os.path.getsize(fpath)
        with open(fpath, "rb") as f:
            data = f.read()
        os.remove(fpath)

        fname = os.path.basename(fpath)
        log.info("YT audio: %s (%d bytes)", fname, size)
        await status.edit_text(f"✅ {fname} ({size/1024/1024:.1f} MB)…")
        await send_reply(message, data, fname, "audio", "🎵")
        try:
            await status.delete()
        except Exception:
            pass
    except asyncio.TimeoutError:
        log.error("ytaudio download timeout: %s", url)
        try:
            await status.edit_text("❌ YouTube audio download timed out (120s).")
        except Exception:
            pass
    except Exception as e:
        log.error("ytaudio err: %s", e)
        try:
            await status.edit_text(f"❌ {e}")
        except Exception:
            pass


# ──────────────────────────────────────────────
# Debug: catch-all for unhandled Bale messages (must be AFTER all handlers)
# ──────────────────────────────────────────────
@bot.on_message(private)
async def catch_all(*, message):
    log.info("📭 Unhandled Bale msg from %s: text=%r state=%s",
             getattr(message.author, 'id', '?'),
             getattr(message, 'text', None),
             _states.get(getattr(message.author, 'id', None)))


# ──────────────────────────────────────────────
# YouTube downloaders (run in thread)
# ──────────────────────────────────────────────
def _yt_video(url, height=None):
    import youtube_dl
    fmt = (f"bestvideo[height<={height}]+bestaudio/best[height<={height}]"
           if height else "best[ext=mp4]/best")
    opts = {
        "format": fmt,
        "outtmpl": os.path.join(cfg.Config.DOWNLOAD_DIR, "yt_%(title)s.%(ext)s"),
        "max_filesize": cfg.Config.MAX_FILE_SIZE,
        "noplaylist": True, "quiet": True, "no_warnings": True,
        "prefer_free_formats": True,
    }
    with youtube_dl.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        return ydl.prepare_filename(info)


def _yt_audio(url):
    import youtube_dl
    opts = {
        "format": "bestaudio/best",
        "outtmpl": os.path.join(cfg.Config.DOWNLOAD_DIR, "yt_%(title)s.%(ext)s"),
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }],
        "max_filesize": cfg.Config.MAX_FILE_SIZE,
        "noplaylist": True, "quiet": True, "no_warnings": True,
    }
    with youtube_dl.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        return os.path.join(cfg.Config.DOWNLOAD_DIR, f"yt_{info['title']}.mp3")


# ──────────────────────────────────────────────
# Web server
# ──────────────────────────────────────────────
async def start_web_server():
    from aiohttp import web

    async def index(r):
        return web.Response(text="✨ Bot running!", content_type="text/plain")
    async def health(r):
        return web.json_response({"status": "ok"})
    async def status_page(r):
        return web.json_response({
            "status": "running",
            "tg_sync": bool(cfg.Config.TG_TOKEN),
            "mapped_users": len(cfg.Config.USER_MAPPING),
        })

    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/health", health)
    app.router.add_get("/status", status_page)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, cfg.Config.WEB_HOST, cfg.Config.WEB_PORT).start()
    log.info("Web server on %s:%d", cfg.Config.WEB_HOST, cfg.Config.WEB_PORT)


# ──────────────────────────────────────────────
# Telegram → Bale file sync  (python-telegram-bot v20+)
# ──────────────────────────────────────────────
class TelegramSync:
    """Uses python-telegram-bot ApplicationBuilder for reliable file downloads."""

    def __init__(self, token, bale_client, mapping):
        self.token   = token
        self.bale    = bale_client
        self.mapping = mapping          # {str(tg_chat_id): str(bale_user_id)}

    # ── build & start the TG application ──
    async def run(self):
        from telegram.ext import (
            ApplicationBuilder, MessageHandler,
            CommandHandler, ContextTypes, filters,
        )

        log.info("TG sync starting (python-telegram-bot v22)…")

        # Reference to self for closures
        sync = self

        async def tg_start(update, context: ContextTypes.DEFAULT_TYPE):
            chat_id = update.effective_chat.id
            log.info("TG /start from %s", chat_id)
            await update.message.reply_text(
                "👋 TG → Bale sync active!\n"
                "Send a file / photo / video / audio to forward.\n"
                f"Max: {cfg.Config.MAX_FILE_SIZE//1024//1024} MB")

        async def tg_downloader(update, context: ContextTypes.DEFAULT_TYPE):
            await sync._handle_message(update, context)

        app = (
            ApplicationBuilder()
            .token(self.token)
            .build()
        )
        app.add_handler(CommandHandler("start", tg_start))
        app.add_handler(MessageHandler(
            filters.Document.ALL | filters.PHOTO |
            filters.VIDEO | filters.AUDIO | filters.VOICE,
            tg_downloader,
        ))

        # Start polling in the background (non-blocking)
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        log.info("TG sync polling started (users: %s)",
                 list(self.mapping.keys()))

        # Keep running forever
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            log.info("TG sync shutting down…")
            await app.updater.stop()
            await app.stop()
            await app.shutdown()

    # ── handle incoming media message ──
    async def _handle_message(self, update, context):
        msg     = update.message
        chat_id = msg.chat.id

        # Check authorization
        bale_id = self.mapping.get(str(chat_id))
        if not bale_id:
            log.warning("TG unauthorized: %s", chat_id)
            await msg.reply_text("⛔ Not authorized.")
            return

        # Determine file info & media type
        mtype, filename = self._identify_media(msg)
        if not mtype:
            await msg.reply_text("❌ Please send a file, photo, video, or audio.")
            return

        await msg.reply_text("⏳ Downloading from Telegram…")

        try:
            # Check file size first — TG Bot API hard limit is 20 MB for get_file()
            attachment = msg.effective_attachment
            tg_file_size = getattr(attachment, 'file_size', None) or 0
            TG_DOWNLOAD_LIMIT = 20 * 1024 * 1024  # 20 MB Telegram Bot API limit

            if tg_file_size and tg_file_size > TG_DOWNLOAD_LIMIT:
                await msg.reply_text(
                    f"⚠️ File is too large for Telegram Bot API to download.\n"
                    f"Size: {tg_file_size/1024/1024:.1f} MB\n"
                    f"Limit: {TG_DOWNLOAD_LIMIT/1024/1024:.0f} MB\n\n"
                    f"💡 Tip: Send files ≤ 20 MB, or use a direct URL with /file command instead."
                )
                return

            # Download using python-telegram-bot's get_file → download as bytes
            tg_file = await attachment.get_file()

            # Download directly to memory (faster than disk round-trip)
            data = await tg_file.download_as_bytearray()
            data = bytes(data)

            if not data:
                raise Exception("Downloaded file is empty (0 bytes)")

            size_mb = len(data) / 1024 / 1024
            log.info("TG downloaded: %s (%.1f MB)", filename, size_mb)
            await msg.reply_text(f"⏳ Sending to Bale… ({size_mb:.1f} MB)")

            # Forward to Bale (thread-safe via main event loop)
            await send_to_bale_safe(self.bale, int(bale_id), data,
                               filename, mtype, "📁")
            await msg.reply_text(
                f"✅ Sent to Bale! ({len(data)/1024/1024:.1f} MB)")

        except Exception as e:
            err_str = str(e)
            log.error("TG transfer error: %s", e)
            try:
                if "too big" in err_str.lower() or "too large" in err_str.lower():
                    await msg.reply_text(
                        "⚠️ File exceeds Telegram Bot API download limit (20 MB).\n\n"
                        "💡 Send files ≤ 20 MB directly, or share a download URL\n"
                        "and use /file command on the Bale bot instead."
                    )
                else:
                    await msg.reply_text(f"❌ Failed: {e}")
            except Exception:
                pass

    # ── identify media type and filename from TG message ──
    @staticmethod
    def _identify_media(msg):
        """Return (media_type, filename) or (None, None)."""
        if msg.document:
            fname = msg.document.file_name or "document"
            return "document", fname
        elif msg.photo:
            return "photo", "photo.jpg"
        elif msg.video:
            fname = msg.video.file_name or "video.mp4"
            return "video", fname
        elif msg.audio:
            title = msg.audio.title or ""
            performer = msg.audio.performer or ""
            fname = msg.audio.file_name
            if fname:
                return "audio", fname
            elif title:
                fname = f"{title} - {performer}.mp3" if performer else f"{title}.mp3"
                return "audio", fname
            return "audio", "audio.mp3"
        elif msg.voice:
            return "audio", "voice.ogg"
        return None, None


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
async def startup():
    global _main_loop
    _main_loop = asyncio.get_running_loop()

    await start_web_server()

    if cfg.Config.TG_TOKEN and cfg.Config.USER_MAPPING:
        tg = TelegramSync(cfg.Config.TG_TOKEN, bot, cfg.Config.USER_MAPPING)
        # Run TG sync in its own thread with its own event loop
        # (python-telegram-bot and Balethon can't share an event loop)
        import threading
        def _run_tg():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(tg.run())
            except KeyboardInterrupt:
                pass
            finally:
                loop.close()
        t = threading.Thread(target=_run_tg, daemon=True, name="tg-sync")
        t.start()
        log.info("TG sync thread started")

    await bot.start_polling()


def main():
    cfg.Config.load_user_mapping()
    cfg.Config.load_allowed_users()

    if not cfg.Config.validate():
        return

    log.info("Starting Bale Bot…")
    log.info("Web: %s:%d  TG sync: %s",
             cfg.Config.WEB_HOST, cfg.Config.WEB_PORT,
             "on" if cfg.Config.TG_TOKEN else "off")

    try:
        bot.connect()
        asyncio.get_event_loop().run_until_complete(startup())
    except KeyboardInterrupt:
        pass
    finally:
        if bot.dispatcher.is_started:
            bot.shutdown()
        bot.disconnect()


if __name__ == "__main__":
    main()
