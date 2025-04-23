import os
import fastf1
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, r2_score
from sqlalchemy import create_engine, text, inspect
import sqlite3
import streamlit as st
import math

# Create and enable FastF1 cache directory
cache_dir = 'cache'
os.makedirs(cache_dir, exist_ok=True)
fastf1.Cache.enable_cache(cache_dir)

# Streamlit UI for selecting year and GP
st.title("Formula 1 Race Outcome Predictor")

# User inputs for year and GP
year_options = list(range(2018, 2024))  # F1 data is best from 2018 onwards
selected_year = st.selectbox("Select Year", year_options, index=len(year_options)-1)  # Default to latest year

# Get available GPs for the selected year

@st.cache_data
def get_available_gps(year):
    try:
        # Get the F1 schedule for the selected year
        schedule = fastf1.get_event_schedule(year)
        # Return list of event names
        return schedule['EventName'].tolist()
    except Exception as e:
        st.error(f"Error fetching GP list for year {year}: {e}")
        return ["Bahrain Grand Prix"]  # Default fallback

gp_options = get_available_gps(selected_year)
selected_gp = st.selectbox("Select Grand Prix", gp_options, index=0)

# Step 2: Load race session with Streamlit spinner and caching
@st.cache_resource
def load_session(year, gp_name):
    with st.spinner(f'Loading {year} {gp_name} session data...'):
        try:
            session = fastf1.get_session(year, gp_name, 'R')
            session.load()
            return session
        except Exception as e:
            st.error(f"Error loading session: {e}")
            return None

# Function to ensure database tables exist with correct schema
def ensure_db_tables():
    db_path = 'f1_race_data.db'
    engine = create_engine(f'sqlite:///{db_path}', echo=False)
    
    with engine.connect() as conn:
        inspector = inspect(engine)
        
        # Check if race_data table exists
        if 'race_data' in inspector.get_table_names():
            # Check if Year and GrandPrix columns exist
            columns = [col['name'] for col in inspector.get_columns('race_data')]
            
            if 'Year' not in columns or 'GrandPrix' not in columns:
                st.warning("Existing race_data table has incompatible schema. Recreating table...")
                # Drop existing table and recreate
                conn.execute(text("DROP TABLE race_data"))
                conn.commit()
                # Table will be recreated below
        
        # Create the tables with proper schema if they don't exist or were dropped
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS race_data (
                Driver TEXT,
                GridPosition REAL,
                AvgLapTime REAL,
                PitStops INTEGER,
                AirTemperature REAL,
                Rainfall REAL,
                FinalPosition REAL,
                Year INTEGER,
                GrandPrix TEXT
            )
        """))
        
        # Check if predictions table exists and handle similarly
        if 'predictions' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('predictions')]
            
            if 'Year' not in columns or 'GrandPrix' not in columns:
                st.warning("Existing predictions table has incompatible schema. Recreating table...")
                conn.execute(text("DROP TABLE predictions"))
                conn.commit()
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS predictions (
                GridPosition REAL,
                AvgLapTime REAL,
                PitStops INTEGER,
                AirTemperature REAL,
                Rainfall REAL,
                Actual REAL,
                Predicted REAL,
                Year INTEGER,
                GrandPrix TEXT
            )
        """))
        conn.commit()
    
    return engine

# Only load session after user clicks "Analyze" button
analyze_button = st.button("Analyze Race Data")

if analyze_button:
    # Ensure database tables exist with correct schema
    engine = ensure_db_tables()
    
    session = load_session(selected_year, selected_gp)
    
    if session is not None:
        # Debug weather data
        with st.expander("Debug Weather Data"):
            if hasattr(session, 'weather_data') and session.weather_data is not None:
                st.write("Weather data columns:", session.weather_data.columns.tolist())
                st.dataframe(session.weather_data.head())
            else:
                st.write("No weather data available")

        # Step 3: Extract and clean data
        @st.cache_data
        def extract_driver_data(_session):
            data = []

            # Get weather data if available
            if hasattr(_session, 'weather_data') and _session.weather_data is not None:
                weather = _session.weather_data
                
                # Get temperature - check different possible column names
                if 'AirTemp' in weather.columns:
                    avg_temp = weather['AirTemp'].mean()
                elif 'Air' in weather.columns:
                    avg_temp = weather['Air'].mean()
                else:
                    avg_temp = 25.0  # default
                
                # Get rainfall if available
                if 'Rainfall' in weather.columns:
                    avg_rain = weather['Rainfall'].mean()
                else:
                    avg_rain = 0.0  # default
            else:
                avg_temp = 25.0
                avg_rain = 0.0
                st.warning("No weather data available, using default values")

            # Debug the results data structure
            with st.expander("Driver Data Debug"):
                st.write("Available driver abbreviations in results:")
                if hasattr(_session, 'results') and _session.results is not None:
                    st.write(_session.results['Abbreviation'].tolist())
                else:
                    st.write("No results data available")
            
            # Instead of using session.drivers, let's use the drivers from results directly
            if hasattr(_session, 'results') and _session.results is not None:
                driver_abbrevs = _session.results['Abbreviation'].tolist()
                
                for driver_abbrev in driver_abbrevs:
                    try:
                        st.write(f"Processing driver: {driver_abbrev}")
                        
                        # Get laps for this driver abbreviation
                        laps = _session.laps.pick_driver(driver_abbrev)
                        if laps.empty:
                            st.write(f"No lap data for driver {driver_abbrev}")
                            continue

                        if 'LapTime' not in laps.columns or laps['LapTime'].isna().all():
                            st.write(f"No valid lap times for driver {driver_abbrev}")
                            continue
                            
                        avg_lap_time = laps['LapTime'].dropna().mean().total_seconds()
                        pit_stops = len(laps[laps['PitOutTime'].notna()]) if 'PitOutTime' in laps.columns else 0
                        
                        # Get driver info from results
                        result_row = _session.results[_session.results['Abbreviation'] == driver_abbrev]
                        if result_row.empty:
                            st.write(f"No results for driver {driver_abbrev}")
                            continue
                        
                        # Get grid position and final position from results
                        grid_position = result_row['GridPosition'].values[0]
                        final_position = result_row['Position'].values[0]

                        data.append({
                            'Driver': driver_abbrev,
                            'GridPosition': grid_position,
                            'AvgLapTime': avg_lap_time,
                            'PitStops': pit_stops,
                            'AirTemperature': avg_temp,
                            'Rainfall': avg_rain,
                            'FinalPosition': final_position
                        })
                    except Exception as e:
                        st.write(f"Error processing driver {driver_abbrev}: {str(e)}")
                
            if not data:
                st.warning("No driver data could be processed")
                
            return pd.DataFrame(data)

        df = extract_driver_data(session)

        # Step 5: Store data in SQLite database
        if not df.empty:
            # Store with race info
            df['Year'] = selected_year
            df['GrandPrix'] = selected_gp
            
            # Check if there's already data for this race to avoid duplicates
            inspector = inspect(engine)
            if 'race_data' in inspector.get_table_names():
                columns = [col['name'] for col in inspector.get_columns('race_data')]
                if 'Year' in columns and 'GrandPrix' in columns:
                    try:
                        with engine.connect() as conn:
                            result = conn.execute(
                                text("SELECT COUNT(*) FROM race_data WHERE Year = :year AND GrandPrix = :gp"),
                                {"year": selected_year, "gp": selected_gp}
                            )
                            existing_count = result.scalar()
                        
                        if existing_count > 0:
                            # Delete existing data first
                            with engine.connect() as conn:
                                conn.execute(
                                    text("DELETE FROM race_data WHERE Year = :year AND GrandPrix = :gp"),
                                    {"year": selected_year, "gp": selected_gp}
                                )
                                conn.commit()
                                st.info(f"Replaced existing data for {selected_year} {selected_gp}")
                    except Exception as e:
                        st.warning(f"Error checking for existing data: {e}")
            
            # Now save the new data
            try:
                df.to_sql('race_data', con=engine, if_exists='append', index=False)
                st.success(f"Data for {selected_year} {selected_gp} saved to database")
            except Exception as e:
                st.error(f"Database error: {e}")
                # If error occurs, try to recreate the table
                try:
                    with engine.connect() as conn:
                        conn.execute(text("DROP TABLE IF EXISTS race_data"))
                        conn.commit()
                    df.to_sql('race_data', con=engine, if_exists='replace', index=False)
                    st.success(f"Database table recreated with data for {selected_year} {selected_gp}")
                except Exception as e2:
                    st.error(f"Failed to recover from database error: {e2}")
        else:
            st.warning("No data available to save to the database.")

        # Step 6: Prepare features and target
        if not df.empty:
            features = df[['GridPosition', 'AvgLapTime', 'PitStops', 'AirTemperature', 'Rainfall']]
            target = df['FinalPosition']

            # Check if we have enough data for training/testing split
            if len(df) >= 5:  # Need at least a few samples
                X_train, X_test, y_train, y_test = train_test_split(features, target, test_size=0.2, random_state=42)

                # Step 7: Train a regression model
                model = RandomForestRegressor(n_estimators=100, random_state=42)
                model.fit(X_train, y_train)

                y_pred = model.predict(X_test)

                # Step 8: Save predictions to database
                predictions_df = X_test.copy()
                predictions_df['Actual'] = y_test.values
                predictions_df['Predicted'] = y_pred
                predictions_df['Year'] = selected_year
                predictions_df['GrandPrix'] = selected_gp

                if not predictions_df.empty:
                    # Check if predictions table has correct schema
                    inspector = inspect(engine)
                    if 'predictions' in inspector.get_table_names():
                        columns = [col['name'] for col in inspector.get_columns('predictions')]
                        if 'Year' in columns and 'GrandPrix' in columns:
                            try:
                                # Delete existing predictions first
                                with engine.connect() as conn:
                                    conn.execute(
                                        text("DELETE FROM predictions WHERE Year = :year AND GrandPrix = :gp"),
                                        {"year": selected_year, "gp": selected_gp}
                                    )
                                    conn.commit()
                            except Exception as e:
                                st.warning(f"Error deleting existing predictions: {e}")
                    
                    # Save new predictions
                    try:
                        predictions_df.to_sql('predictions', con=engine, if_exists='append', index=False)
                        st.success("Predictions saved to database")
                    except Exception as e:
                        st.error(f"Error saving predictions: {e}")
                        # Try to recreate the table
                        try:
                            with engine.connect() as conn:
                                conn.execute(text("DROP TABLE IF EXISTS predictions"))
                                conn.commit()
                            predictions_df.to_sql('predictions', con=engine, if_exists='replace', index=False)
                            st.success("Predictions table recreated and data saved")
                        except Exception as e2:
                            st.error(f"Failed to recover from predictions database error: {e2}")
                else:
                    st.warning("No predictions to save.")

                # Step 9: Show results
                st.header(f"{selected_year} {selected_gp} Race Data")
                st.dataframe(df)

                st.header("Predictions vs Actual")
                st.dataframe(predictions_df)

                st.header("Model Evaluation")
                st.write(f"R^2 Score: {r2_score(y_test, y_pred):.2f}")
                
                # Calculate RMSE manually without using the squared parameter
                mse = mean_squared_error(y_test, y_pred)
                rmse = math.sqrt(mse)
                st.write(f"RMSE: {rmse:.2f}")

                st.header("Feature Importances")
                feat_importances = pd.Series(model.feature_importances_, index=features.columns)
                st.bar_chart(feat_importances)

                # Optional: Cross-validation scores
                cv_scores = cross_val_score(model, features, target, cv=min(5, len(df)))
                st.write("Cross-validation R^2 scores:", cv_scores)
                st.write("Average CV Score:", np.mean(cv_scores))
            else:
                st.warning("Not enough data for training/testing split. Need at least 5 drivers.")
                st.dataframe(df)
        else:
            st.error("No race data available for analysis.")
    else:
        st.error(f"Could not load session for {selected_year} {selected_gp}. Try another race.")

# Reset and create new database option
if st.button("Reset Database"):
    db_path = 'f1_race_data.db'
    if os.path.exists(db_path):
        try:
            engine = create_engine(f'sqlite:///{db_path}', echo=False)
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS race_data"))
                conn.execute(text("DROP TABLE IF EXISTS predictions"))
                conn.commit()
            st.success("Database tables reset successfully!")
        except Exception as e:
            st.error(f"Error resetting database: {e}")
    else:
        st.info("No database file found to reset.")

# Display stored data
st.header("Stored Race Data")
try:
    db_path = 'f1_race_data.db'
    if os.path.exists(db_path):
        # Use SQLAlchemy to safely check tables
        engine = create_engine(f'sqlite:///{db_path}', echo=False)
        inspector = inspect(engine)
        
        if 'race_data' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('race_data')]
            if 'Year' in columns and 'GrandPrix' in columns:
                # If we have the right columns, proceed
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT Year, GrandPrix, COUNT(*) as DriversCount FROM race_data GROUP BY Year, GrandPrix"))
                    stored_data = pd.DataFrame(result.fetchall(), columns=['Year', 'GrandPrix', 'DriversCount'])
                    
                    if not stored_data.empty:
                        st.dataframe(stored_data)
                    else:
                        st.info("No race data stored in database yet")
            else:
                st.warning("Database exists but has incorrect schema. Try resetting the database.")
        else:
            st.info("No race data table found in database")
    else:
        st.info("No race database found yet")
except Exception as e:
    st.warning(f"Error reading database: {e}")