import os
from dotenv import load_dotenv

load_dotenv()
class Config:
    APP_TITLE=os.getenv('APP_TITLE','HubSpot Deals ETL')
    APP_VERSION=os.getenv('APP_VERSION','1.0.0')
    SECRET_KEY=os.getenv('SECRET_KEY','change-me')
    HOST=os.getenv('HOST','0.0.0.0')
    PORT=int(os.getenv('PORT','5200'))
    DB_HOST=os.getenv('DB_HOST','localhost'); DB_PORT=int(os.getenv('DB_PORT','5432'))
    DB_NAME=os.getenv('DB_NAME','hubspot_deals_data'); DB_USER=os.getenv('DB_USER','postgres'); DB_PASSWORD=os.getenv('DB_PASSWORD','password123'); DB_SCHEMA=os.getenv('DB_SCHEMA','hubspot_deals')
    DLT_PIPELINE_NAME=os.getenv('DLT_PIPELINE_NAME','hubspot_deals_pipeline'); DLT_WORKING_DIR=os.getenv('DLT_WORKING_DIR','.dlt')
    HUBSPOT_API_BASE_URL=os.getenv('HUBSPOT_API_BASE_URL','https://api.hubapi.com').rstrip('/')
    HUBSPOT_API_TIMEOUT=int(os.getenv('HUBSPOT_API_TIMEOUT','30')); HUBSPOT_API_RATE_LIMIT=int(os.getenv('HUBSPOT_API_RATE_LIMIT','150'))
    HUBSPOT_RETRY_ATTEMPTS=int(os.getenv('HUBSPOT_RETRY_ATTEMPTS','4')); HUBSPOT_RETRY_DELAY=float(os.getenv('HUBSPOT_RETRY_DELAY','1'))
    CHECKPOINT_EVERY_N_PAGES=int(os.getenv('CHECKPOINT_EVERY_N_PAGES','10'))
    MAX_CONCURRENT_SCANS=int(os.getenv('MAX_CONCURRENT_SCANS','5'))
    @classmethod
    def db_url(cls):
        from urllib.parse import quote_plus
        return f"postgresql://{quote_plus(cls.DB_USER)}:{quote_plus(cls.DB_PASSWORD)}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
