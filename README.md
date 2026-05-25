# 🤖 Bale Bot

Minimal Bale Messenger bot with media downloaders, YouTube support, and Telegram ↔ Bale file sync.

## Features

| Command | Description |
|---------|-------------|
| `/video` | Download video from URL (interactive) |
| `/audio` | Download audio from URL (interactive) |
| `/photo` | Download image from URL (interactive) |
| `/file` | Download any file from URL (interactive) |
| `/yt` | Download YouTube video (pick quality: best/480/720/1080) |
| `/ytaudio` | Download YouTube audio (mp3) |
| `/tgsync` | View Telegram sync status |
| `/cancel` | Cancel current action |

## Interactive Mode

All commands use a step-by-step flow:
1. Send `/video` (or `/audio`, `/photo`, `/file`, `/yt`)
2. Bot asks for the URL
3. Send the URL
4. For `/yt`: bot asks for quality → pick 1-4 or type resolution
5. Bot downloads and sends the file

## Large File Handling

Bale limits uploads to **20 MB**. Files larger than 20 MB are automatically:
- Split into zip parts (~19 MB each)
- Sent sequentially with clear naming (`filename.part1of3.zip`, …)
- Include instructions to reassemble after extraction

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
sudo apt install ffmpeg
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your tokens
```

### 3. Get a Bale Bot Token

1. Talk to [@BotFather](https://t.me/BotFather) on Bale
2. Create a new bot and copy the token
3. Set `BALE_TOKEN` in `.env`

### 4. (Optional) Telegram Sync

1. Get a Telegram bot token from [@BotFather](https://t.me/BotFather)
2. Set `TG_TOKEN` in `.env`

### 5. Run

```bash
python3 bot.py
```

## Web Server

Runs on port 8080 alongside the bot:
- `GET /` — Status text
- `GET /health` — Health check (JSON)
- `GET /status` — Detailed status (JSON)

## Systemd Service (Optional)

```bash
sudo cp bale-bot.service /etc/systemd/system/
sudo systemctl enable bale-bot
sudo systemctl start bale-bot
```
