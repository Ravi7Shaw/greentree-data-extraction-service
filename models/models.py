from datetime import datetime, timezone
from sqlalchemy import create_engine,Column,String,Integer,DateTime,Text,JSON,Index
from sqlalchemy.orm import declarative_base,sessionmaker
from config import Config
Base=declarative_base()
class Scan(Base):
    __tablename__='scans'
    id=Column(String(64),primary_key=True); tenant_id=Column(String(128),nullable=False,index=True); status=Column(String(30),nullable=False,index=True); records_extracted=Column(Integer,default=0); cursor=Column(String(255)); error=Column(Text); metadata_json=Column(JSON,default=dict); created_at=Column(DateTime(timezone=True),default=lambda:datetime.now(timezone.utc)); updated_at=Column(DateTime(timezone=True),default=lambda:datetime.now(timezone.utc),onupdate=lambda:datetime.now(timezone.utc))
    __table_args__=(Index('ix_scans_tenant_created','tenant_id','created_at'),)
engine=create_engine(Config.db_url(),pool_pre_ping=True)
SessionLocal=sessionmaker(bind=engine)
def init_db(): Base.metadata.create_all(engine)
