# COMPLETE SETUP - Add this to the top of your app.py file
# ========================================================

import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import io
import os
from googleapiclient.discovery import build

# ADD THESE IMPORTS FOR LOGGING:
import logging
from datetime import datetime

# FILE-ONLY LOGGING SETUP
# ========================

def setup_file_only_logging():
    """File-only logging - no console output"""
    
    # Create logs directory in your workspace
    logs_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger('booking_app')
    logger.setLevel(logging.INFO)
    logger.handlers = []  # Clear ALL existing handlers
    
    # Create daily log file
    today = datetime.now().strftime("%Y%m%d")
    log_file = os.path.join(logs_dir, f'booking_app_{today}.log')
    
    # ONLY file handler - NO console handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Silent test log (won't show in console)
    logger.info("📝 File-only logging started")
    
    return logger, log_file

# Initialize file-only logging
logger, log_file_path = setup_file_only_logging()

st.set_page_config(page_title="Dismac: Reserva de Entrega de Mercadería", layout="wide")

# ─────────────────────────────────────────────────────────────
# 1. Configuration
# ─────────────────────────────────────────────────────────────
try:
    # Email configuration
    EMAIL_HOST = os.getenv("EMAIL_HOST") or st.secrets["EMAIL_HOST"]
    EMAIL_PORT = int(os.getenv("EMAIL_PORT") or st.secrets["EMAIL_PORT"])
    EMAIL_USER = os.getenv("EMAIL_USER") or st.secrets["EMAIL_USER"]
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD") or st.secrets["EMAIL_PASSWORD"]
    
except KeyError as e:
    st.error(f"🔒 Falta configuración: {e}")
    st.stop()

# ─────────────────────────────────────────────────────────────
# 2. Google Sheets Functions - MIGRATED FROM SHAREPOINT
# ─────────────────────────────────────────────────────────────

@st.cache_resource
def setup_google_sheets():
    """Configurar conexión a Google Sheets"""
    try:
        credentials_info = dict(st.secrets["google_service_account"])
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        gc = gspread.authorize(credentials)
        return gc
    except Exception as e:
        logger.error(f"Error conectando a Google Sheets: {str(e)}")
        st.error(f"❌ Error conectando: {str(e)}")
        return None

@st.cache_data(ttl=60, show_spinner=False)
def download_sheets_to_memory():
    """Download all sheets from Google Sheets"""
    try:
        logger.info("Descargando datos de Google Sheets")
        gc = setup_google_sheets()
        if not gc:
            return None, None, None
        
        spreadsheet = gc.open(st.secrets["GOOGLE_SHEET_NAME"])
        
        # Load credentials sheet
        try:
            credentials_ws = spreadsheet.worksheet("proveedor_credencial")
            credentials_data = credentials_ws.get_all_records()
            if credentials_data:
                credentials_df = pd.DataFrame(credentials_data)
                for col in credentials_df.columns:
                    credentials_df[col] = credentials_df[col].astype(str)
            else:
                all_values = credentials_ws.get_all_values()
                if all_values and len(all_values) > 1:
                    credentials_df = pd.DataFrame(all_values[1:], columns=all_values[0])
                else:
                    credentials_df = pd.DataFrame(columns=['usuario', 'password', 'Email', 'cc'])
        except gspread.WorksheetNotFound:
            logger.warning("Hoja proveedor_credencial no encontrada")
            credentials_df = pd.DataFrame(columns=['usuario', 'password', 'Email', 'cc'])
        
        # Load reservas sheet
        try:
            reservas_ws = spreadsheet.worksheet("proveedor_reservas")
            reservas_data = reservas_ws.get_all_records()
            if reservas_data:
                reservas_df = pd.DataFrame(reservas_data)
            else:
                all_values = reservas_ws.get_all_values()
                if all_values and len(all_values) > 1:
                    reservas_df = pd.DataFrame(all_values[1:], columns=all_values[0])
                else:
                    reservas_df = pd.DataFrame(columns=['Fecha', 'Hora', 'Proveedor', 'Numero_de_bultos', 'Orden_de_compra'])
        except gspread.WorksheetNotFound:
            logger.warning("Hoja proveedor_reservas no encontrada")
            reservas_df = pd.DataFrame(columns=['Fecha', 'Hora', 'Proveedor', 'Numero_de_bultos', 'Orden_de_compra'])
        
        # Load or create gestion sheet
        try:
            gestion_ws = spreadsheet.worksheet("proveedor_gestion")
            gestion_data = gestion_ws.get_all_records()
            if gestion_data:
                gestion_df = pd.DataFrame(gestion_data)
            else:
                all_values = gestion_ws.get_all_values()
                if all_values and len(all_values) > 1:
                    gestion_df = pd.DataFrame(all_values[1:], columns=all_values[0])
                else:
                    gestion_df = pd.DataFrame(columns=[
                        'Orden_de_compra', 'Proveedor', 'Numero_de_bultos',
                        'Hora_llegada', 'Hora_inicio_atencion', 'Hora_fin_atencion',
                        'Tiempo_espera', 'Tiempo_atencion', 'Tiempo_total', 'Tiempo_retraso',
                        'numero_de_semana', 'hora_de_reserva'
                    ])
        except gspread.WorksheetNotFound:
            try:
                gestion_ws = spreadsheet.add_worksheet("proveedor_gestion", rows=100, cols=12)
                headers = [
                    'Orden_de_compra', 'Proveedor', 'Numero_de_bultos',
                    'Hora_llegada', 'Hora_inicio_atencion', 'Hora_fin_atencion',
                    'Tiempo_espera', 'Tiempo_atencion', 'Tiempo_total', 'Tiempo_retraso',
                    'numero_de_semana', 'hora_de_reserva'
                ]
                gestion_ws.update('A1:L1', [headers])
                gestion_df = pd.DataFrame(columns=headers)
                logger.info("Hoja proveedor_gestion creada")
            except Exception as e:
                logger.warning(f"No se pudo crear hoja de gestión: {e}")
                gestion_df = pd.DataFrame(columns=[
                    'Orden_de_compra', 'Proveedor', 'Numero_de_bultos',
                    'Hora_llegada', 'Hora_inicio_atencion', 'Hora_fin_atencion',
                    'Tiempo_espera', 'Tiempo_atencion', 'Tiempo_total', 'Tiempo_retraso',
                    'numero_de_semana', 'hora_de_reserva'
                ])
        
        logger.info("Datos de Google Sheets descargados exitosamente")
        return credentials_df, reservas_df, gestion_df
        
    except Exception as e:
        logger.error(f"Error descargando datos de Google Sheets: {str(e)}")
        st.error(f"Error descargando datos: {str(e)}")
        return None, None, None

# IMPROVED save_booking_to_sheets with logging
def save_booking_to_sheets(new_booking):
    """Save new booking to Google Sheets with comprehensive verification and logging"""
    try:
        logger.info(f"🔄 Iniciando proceso de guardado para {new_booking['Proveedor']}")
        
        # Clear cache and get fresh data for final check
        download_sheets_to_memory.clear()
        credentials_df, reservas_df, gestion_df = download_sheets_to_memory()
        
        if reservas_df is None:
            logger.error("No se pudo cargar los datos de reservas")
            st.error("❌ **Problemas de conexión**: No se pudo cargar los datos de reservas. Por favor, inténtelo nuevamente en unos minutos.")
            return False

        # 🔒 FINAL CHECK: Verify slot is still available
        fecha_reserva = new_booking['Fecha']
        hora_reserva = new_booking['Hora']
        
        logger.info(f"🔍 Verificando disponibilidad: {fecha_reserva} a las {hora_reserva}")
        
        existing_booking = reservas_df[
            (reservas_df['Fecha'].astype(str).str.contains(fecha_reserva.split(' ')[0], na=False)) & 
            (reservas_df['Hora'].astype(str) == hora_reserva)
        ]
        
        if not existing_booking.empty:
            logger.warning(f"⚠️ Conflicto de horario detectado para {fecha_reserva} a las {hora_reserva}")
            st.error("❌ Otro proveedor acaba de reservar este horario")
            download_sheets_to_memory.clear()
            return False
        
        logger.info("✅ Horario confirmado disponible, procediendo con el guardado")
        
        # Get Google Sheets connection
        gc = setup_google_sheets()
        if not gc:
            logger.error("No se pudo establecer conexión con Google Sheets")
            st.error("❌ **Problemas de conexión**: No se pudo conectar con el sistema de reservas. Por favor, inténtelo nuevamente en unos minutos.")
            return False
        
        spreadsheet = gc.open(st.secrets["GOOGLE_SHEET_NAME"])
        reservas_ws = spreadsheet.worksheet("proveedor_reservas")
        
        # Get current row count before insertion
        current_rows = len(reservas_ws.get_all_values())
        logger.info(f"📊 La hoja tiene {current_rows} filas antes de la inserción")
        
        # Prepare new row data
        new_row_data = [
            new_booking['Fecha'],
            new_booking['Hora'],
            new_booking['Proveedor'],
            str(new_booking['Numero_de_bultos']),
            new_booking['Orden_de_compra']
        ]
        
        logger.info(f"💾 Intentando guardar datos de reserva: {new_row_data}")
        
        # Attempt to append the new booking with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Append the new booking
                response = reservas_ws.append_row(new_row_data, value_input_option='RAW')
                logger.info(f"📝 Respuesta de la operación de guardado: {response}")
                
                # Small delay to ensure operation completes
                import time
                time.sleep(1)
                
                # VERIFICATION: Check if the booking was actually saved
                logger.info("🔍 Verificando que la reserva se guardó...")
                
                # Get updated row count
                new_rows = len(reservas_ws.get_all_values())
                logger.info(f"📊 La hoja ahora tiene {new_rows} filas después de la inserción")
                
                if new_rows <= current_rows:
                    logger.error(f"❌ El número de filas no aumentó! Esperado: {current_rows + 1}, Obtenido: {new_rows}")
                    if attempt < max_retries - 1:
                        logger.info(f"🔄 Reintentando... (Intento {attempt + 2}/{max_retries})")
                        wait_time = 2 ** (attempt + 1)
                        logger.info(f"⏱️ Esperando {wait_time} segundos antes de reintentar...")
                        time.sleep(wait_time)
                        continue
                    else:
                        st.error("❌ **Problemas de conexión**: No se pudo guardar la reserva debido a problemas de red. Por favor, inténtelo nuevamente en unos minutos.")
                        return False
                
                # Double-check: Try to find our specific booking
                logger.info("🔍 Buscando nuestra reserva específica...")
                
                # Clear cache and get fresh data to verify
                download_sheets_to_memory.clear()
                time.sleep(1)
                
                _, verification_df, _ = download_sheets_to_memory()
                
                if verification_df is not None:
                    # Look for our booking in the fresh data
                    matching_bookings = verification_df[
                        (verification_df['Fecha'].astype(str).str.contains(fecha_reserva.split(' ')[0], na=False)) & 
                        (verification_df['Hora'].astype(str) == hora_reserva) &
                        (verification_df['Proveedor'].astype(str) == new_booking['Proveedor']) &
                        (verification_df['Orden_de_compra'].astype(str) == new_booking['Orden_de_compra'])
                    ]
                    
                    if matching_bookings.empty:
                        logger.error("❌ Reserva no encontrada en verificación!")
                        if attempt < max_retries - 1:
                            logger.info(f"🔄 Reintentando... (Intento {attempt + 2}/{max_retries})")
                            wait_time = 2 ** (attempt + 1)
                            logger.info(f"⏱️ Esperando {wait_time} segundos antes de reintentar...")
                            time.sleep(wait_time)
                            continue
                        else:
                            st.error("❌ **Problemas de conexión**: No se pudo verificar que la reserva se guardó debido a problemas de red. Por favor, inténtelo nuevamente en unos minutos.")
                            return False
                    else:
                        logger.info(f"✅ Reserva verificada! Se encontraron {len(matching_bookings)} registros coincidentes")
                        break
                else:
                    logger.warning("⚠️ No se pudieron cargar datos de verificación, pero el guardado fue exitoso")
                    break
                    
            except Exception as e:
                logger.error(f"❌ Intento {attempt + 1} falló: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"🔄 Reintentando... (Intento {attempt + 2}/{max_retries})")
                    wait_time = 2 ** (attempt + 1)
                    logger.info(f"⏱️ Esperando {wait_time} segundos antes de reintentar...")
                    time.sleep(wait_time)
                    continue
                else:
                    st.error("❌ **Problemas de conexión**: No se pudo guardar la reserva debido a problemas de red. Por favor, inténtelo nuevamente en unos minutos.")
                    return False
        
        logger.info("✅ Proceso de guardado de reserva completado exitosamente")
        
        # Clear cache after successful save
        download_sheets_to_memory.clear()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error crítico en save_booking_to_sheets: {str(e)}")
        st.error("❌ **Problemas de conexión**: No se pudo guardar la reserva debido a problemas de red. Por favor, inténtelo nuevamente en unos minutos.")
        return False

# ADD LOG VIEWER TO YOUR MAIN FUNCTION
# ====================================

def main():
    st.title("🚚 Dismac: Reserva de Entrega de Mercadería")
    
    # Log viewer in sidebar
    with st.sidebar:
        st.subheader("📄 Log Viewer")
        
        if st.button("🔍 View Logs"):
            logs_dir = os.path.join(os.getcwd(), 'logs')
            
            if os.path.exists(logs_dir):
                log_files = [f for f in os.listdir(logs_dir) if f.endswith('.log')]
                
                if log_files:
                    # Sort files (newest first)
                    log_files.sort(reverse=True)
                    
                    # Select log file
                    selected_file = st.selectbox("Select log file:", log_files)
                    
                    if selected_file:
                        log_path = os.path.join(logs_dir, selected_file)
                        
                        # Read last 50 lines
                        try:
                            with open(log_path, 'r', encoding='utf-8') as f:
                                lines = f.readlines()
                                recent_lines = lines[-50:] if len(lines) > 50 else lines
                                log_content = ''.join(recent_lines)
                            
                            st.text_area(
                                f"Recent logs from {selected_file}:",
                                log_content,
                                height=400
                            )
                        except Exception as e:
                            st.error(f"Error reading log: {e}")
                else:
                    st.info("No log files found")
            else:
                st.error("Logs directory not found")
        
        # Test logging (silent - no console output)
        if st.button("✍️ Test Silent Logging"):
            logger.info("🧪 Silent test log entry")
            logger.warning("⚠️ Silent test warning")
            logger.error("❌ Silent test error")
            st.success("Silent logs written! Check log viewer.")
    
    # Silent logging on app start
    logger.info("🚀 App started silently")
    
    # Download Google Sheets data when app starts
    with st.spinner("Cargando datos..."):
        credentials_df, reservas_df, gestion_df = download_sheets_to_memory()
    
    if credentials_df is None:
        logger.error("Error al cargar datos al iniciar la aplicación")
        st.error("❌ Error al cargar datos")
        if st.button("🔄 Reintentar Conexión"):
            download_sheets_to_memory.clear()
            st.rerun()
        return
    
    # ... rest of your existing main() function code ...