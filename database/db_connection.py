"""
Database connection utilities
"""
import os
import logging
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import sys

sys.path.append('..')  # Add parent directory to path

from config.settings import DB_CONFIG, LOG_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(LOG_DIR) / 'database.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create Base class for declarative models
Base = declarative_base()

def get_connection_string():
    """
    Build database connection string from configuration.
    
    Returns:
        str: Database connection string
    """
    engine = DB_CONFIG['engine']
    username = DB_CONFIG['username']
    password = DB_CONFIG['password']
    host = DB_CONFIG['host']
    port = DB_CONFIG['port']
    database = DB_CONFIG['database']
    
    return f"{engine}://{username}:{password}@{host}:{port}/{database}"

def get_engine():
    """
    Create and return SQLAlchemy engine.
    
    Returns:
        Engine: SQLAlchemy engine object
    """
    try:
        connection_string = get_connection_string()
        engine = create_engine(
            connection_string,
            pool_pre_ping=True,  # Check connection before using
            pool_recycle=3600,   # Recycle connections after 1 hour
            echo=False           # Set to True for SQL query logging
        )
        logger.info("Database engine created successfully")
        return engine
    except Exception as e:
        logger.error(f"Error creating database engine: {e}")
        raise

def get_session():
    """
    Create and return a new database session.
    
    Returns:
        Session: SQLAlchemy session object
    """
    try:
        engine = get_engine()
        Session = sessionmaker(bind=engine)
        return Session()
    except Exception as e:
        logger.error(f"Error creating database session: {e}")
        raise

def init_db():
    """
    Initialize database tables.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        engine = get_engine()
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully")
        return True
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        return False

# Database schema definitions (can be moved to separate file)
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, JSON
from sqlalchemy.orm import relationship

class Session(Base):
    __tablename__ = 'sessions'
    
    id = Column(Integer, primary_key=True)
    year = Column(Integer, nullable=False)
    round = Column(Integer, nullable=False)
    session_type = Column(String, nullable=False)  # FP1, FP2, FP3, Q, R
    event_name = Column(String, nullable=False)
    circuit_name = Column(String, nullable=False)
    date = Column(DateTime, nullable=False)
    source = Column(String, nullable=False)  # 'fastf1', 'openf1', or 'reconciled'
    
    # Relationships
    timing_data = relationship("TimingData", back_populates="session")
    telemetry_data = relationship("TelemetryData", back_populates="session")
    
    def __repr__(self):
        return f"<Session(year={self.year}, round={self.round}, type={self.session_type})>"

class TimingData(Base):
    __tablename__ = 'timing_data'
    
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('sessions.id'))
    driver_number = Column(String, nullable=False)
    driver_name = Column(String)
    team = Column(String)
    lap_number = Column(Integer)
    lap_time = Column(Float)
    sector_1_time = Column(Float)
    sector_2_time = Column(Float)
    sector_3_time = Column(Float)
    timestamp = Column(DateTime)
    position = Column(Integer)
    tyre_compound = Column(String)
    pit_stops = Column(Integer)
    is_personal_best = Column(Boolean)
    
    # Relationships
    session = relationship("Session", back_populates="timing_data")
    
    def __repr__(self):
        return f"<TimingData(driver={self.driver_number}, lap={self.lap_number})>"

class TelemetryData(Base):
    __tablename__ = 'telemetry_data'
    
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('sessions.id'))
    driver_number = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    lap_number = Column(Integer)
    speed = Column(Float)
    rpm = Column(Float)
    throttle = Column(Float)
    brake = Column(Boolean)
    gear = Column(Integer)
    drs = Column(Boolean)
    distance = Column(Float)
    x_position = Column(Float)
    y_position = Column(Float)
    
    # Relationships
    session = relationship("Session", back_populates="telemetry_data")
    
    def __repr__(self):
        return f"<TelemetryData(driver={self.driver_number}, time={self.timestamp})>"

class WeatherData(Base):
    __tablename__ = 'weather_data'
    
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('sessions.id'))
    timestamp = Column(DateTime, nullable=False)
    air_temp = Column(Float)
    track_temp = Column(Float)
    humidity = Column(Float)
    pressure = Column(Float)
    wind_speed = Column(Float)
    wind_direction = Column(Float)
    rainfall = Column(Boolean)
    
    def __repr__(self):
        return f"<WeatherData(time={self.timestamp})>"