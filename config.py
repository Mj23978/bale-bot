"""Configuration for Bale Bot.Tokens and settings are loaded from environment variables or .env file."""
import os from dotenv 
import load_dotenv

load_dotenv()

class Config:    
    # Bale Bot    BALE_TOKEN: str = os.getenv("BALE_TOKEN", "")    
    # Telegram Bot (for sync feature)    TG_TOKEN: str = os.getenv("TG_TOKEN", "")    
    # Web Server    WEB_HOST: str = os.getenv("WEB_HOST", "0.0.0.0")    WEB_PORT: int = int(os.getenv("WEB_PORT", "8080"))    
    # Telegram -> Bale user mapping: {"telegram_user_id": "bale_user_id"}    USER_MAPPING: dict = {}    
    # File size limit for downloads (50 MB — Telegram bot max)    
    # Bale's 20 MB limit is handled by auto-splitting in bot.py    MAX_FILE_SIZE: int = 50 * 1024 * 1024    
    # Downloads directory    DOWNLOAD_DIR: str = os.getenv("DOWNLOAD_DIR", "/tmp/bale-bot-downloads")    
    # Allowed users (empty = allow all)    ALLOWED_USERS: list = []    
    
    @classmethod    
    def load_user_mapping(cls):        
        """Load user mapping from env: TG_BALE_USER_123456=987654321"""        
        mapping = {}        
        for key, value in os.environ.items():            
            if key.startswith("TG_BALE_USER_"):                
                tg_id = key.replace("TG_BALE_USER_", "")                
                mapping[tg_id] = value        
                # Also check JSON string in env        
                json_mapping = os.getenv("USER_MAPPING", "")        
            if json_mapping:            
                import json            
                try:                
                    mapping.update(json.loads(json_mapping))            
                except json.JSONDecodeError:                
                    pass        
        cls.USER_MAPPING = mapping    
        
        @classmethod    
        def load_allowed_users(cls):        
            users_str = os.getenv("ALLOWED_USERS", "")        
            if users_str:            
                cls.ALLOWED_USERS = [u.strip() for u in users_str.split(",") if u.strip()]    
                
        @classmethod    
        def validate(cls) -> bool:        
            if not cls.BALE_TOKEN:            
                print("ERROR: BALE_TOKEN is not set in .env or environment")            
                return False        
            return True
