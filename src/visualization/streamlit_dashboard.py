import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from pymongo import MongoClient
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

class FlightDashboard:
    def __init__(self):
        self.mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        self.mongo_db = os.getenv("MONGODB_DB", "skymonitor")
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
    
    def get_realtime_flights(self, limit: int = 100):
        """Fetch latest realtime flights from MongoDB."""
        collection = self.db["flights_realtime"]
        flights = list(
            collection.find(
                {"processed_at": {"$gte": datetime.utcnow() - timedelta(minutes=5)}}
            ).sort("processed_at", -1).limit(limit)
        )
        return pd.DataFrame(flights)
    
    def get_descent_alerts(self):
        """Get flights in rapid descent."""
        collection = self.db["flights_realtime"]
        alerts = list(
            collection.find(
                {
                    "descent_alert": True,
                    "processed_at": {"$gte": datetime.utcnow() - timedelta(minutes=1)}
                }
            ).sort("descent_rate", -1)
        )
        return pd.DataFrame(alerts)
    
    def render_map(self, flights_df):
        """Render live flight map."""
        if flights_df.empty:
            st.warning("No flight data available")
            return
        
        # Center on Vietnam
        m = folium.Map(
            location=[15.5, 105.5],
            zoom_start=6,
            tiles="OpenStreetMap"
        )
        
        for _, flight in flights_df.iterrows():
            if not pd.isna(flight.get("latitude")) and not pd.isna(flight.get("longitude")):
                color = "red" if flight.get("descent_alert") else "blue"
                popup_text = f"{flight.get('callsign', 'N/A')} - Alt: {flight.get('altitude', 0):.0f}m"
                
                folium.CircleMarker(
                    location=[flight["latitude"], flight["longitude"]],
                    radius=5,
                    popup=popup_text,
                    color=color,
                    fill=True,
                    fillColor=color
                ).add_to(m)
        
        st_folium(m, width=1200, height=600)
    
    def run(self):
        """Streamlit dashboard app."""
        st.set_page_config(page_title="SkyMonitor", layout="wide")
        st.title("🛫 SkyMonitor - Real-time Flight Tracking")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("Live Flight Map")
            flights = self.get_realtime_flights(500)
            self.render_map(flights)
        
        with col2:
            st.subheader("Descent Alerts")
            alerts = self.get_descent_alerts()
            if not alerts.empty:
                st.error(f"⚠️ {len(alerts)} aircraft in rapid descent!")
                st.dataframe(
                    alerts[["callsign", "altitude", "descent_rate", "origin_country"]].head(10),
                    use_container_width=True
                )
            else:
                st.success("No rapid descent alerts")
            
            st.subheader("Live Statistics")
            if not flights.empty:
                st.metric("Active Aircraft", len(flights["icao24"].unique()))
                st.metric("Max Altitude", f"{flights['altitude'].max():.0f}m")
                st.metric("Avg Velocity", f"{flights['velocity'].mean():.1f} m/s")

if __name__ == "__main__":
    dashboard = FlightDashboard()
    dashboard.run()