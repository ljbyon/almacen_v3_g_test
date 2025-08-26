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

import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        st.error(f"❌ Error conectando: {str(e)}")
        return None

@st.cache_data(ttl=60, show_spinner=False)  # Reduced TTL for real-time booking
def download_sheets_to_memory():
    """Download all sheets from Google Sheets - REPLACES SharePoint Excel download"""
    try:
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
                # Ensure all columns are strings for consistency
                for col in credentials_df.columns:
                    credentials_df[col] = credentials_df[col].astype(str)
            else:
                # Fallback to raw values
                all_values = credentials_ws.get_all_values()
                if all_values and len(all_values) > 1:
                    credentials_df = pd.DataFrame(all_values[1:], columns=all_values[0])
                else:
                    credentials_df = pd.DataFrame(columns=['usuario', 'password', 'Email', 'cc'])
        except gspread.WorksheetNotFound:
            credentials_df = pd.DataFrame(columns=['usuario', 'password', 'Email', 'cc'])
        
        # Load reservas sheet
        try:
            reservas_ws = spreadsheet.worksheet("proveedor_reservas")
            reservas_data = reservas_ws.get_all_records()
            if reservas_data:
                reservas_df = pd.DataFrame(reservas_data)
            else:
                # Fallback to raw values
                all_values = reservas_ws.get_all_values()
                if all_values and len(all_values) > 1:
                    reservas_df = pd.DataFrame(all_values[1:], columns=all_values[0])
                else:
                    reservas_df = pd.DataFrame(columns=['Fecha', 'Hora', 'Proveedor', 'Numero_de_bultos', 'Orden_de_compra'])
        except gspread.WorksheetNotFound:
            reservas_df = pd.DataFrame(columns=['Fecha', 'Hora', 'Proveedor', 'Numero_de_bultos', 'Orden_de_compra'])
        
        # Load or create gestion sheet
        try:
            gestion_ws = spreadsheet.worksheet("proveedor_gestion")
            gestion_data = gestion_ws.get_all_records()
            if gestion_data:
                gestion_df = pd.DataFrame(gestion_data)
            else:
                # Fallback to raw values
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
            # Create gestion sheet if it doesn't exist
            try:
                gestion_ws = spreadsheet.add_worksheet("proveedor_gestion", rows=100, cols=12)
                # Add headers
                headers = [
                    'Orden_de_compra', 'Proveedor', 'Numero_de_bultos',
                    'Hora_llegada', 'Hora_inicio_atencion', 'Hora_fin_atencion',
                    'Tiempo_espera', 'Tiempo_atencion', 'Tiempo_total', 'Tiempo_retraso',
                    'numero_de_semana', 'hora_de_reserva'
                ]
                gestion_ws.update('A1:L1', [headers])
                gestion_df = pd.DataFrame(columns=headers)
            except Exception as e:
                st.warning(f"No se pudo crear hoja de gestión: {e}")
                gestion_df = pd.DataFrame(columns=[
                    'Orden_de_compra', 'Proveedor', 'Numero_de_bultos',
                    'Hora_llegada', 'Hora_inicio_atencion', 'Hora_fin_atencion',
                    'Tiempo_espera', 'Tiempo_atencion', 'Tiempo_total', 'Tiempo_retraso',
                    'numero_de_semana', 'hora_de_reserva'
                ])
        
        return credentials_df, reservas_df, gestion_df
        
    except Exception as e:
        st.error(f"Error descargando datos: {str(e)}")
        return None, None, None


def log_booking_attempt(action, details, success=None, error=None):
    """Centralized logging for booking operations - SERVER SIDE ONLY"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {action}: {details}"
    
    if success is not None:
        log_message += f" | Success: {success}"
    if error:
        log_message += f" | Error: {error}"
    
    # Log to console/server logs only - NOT visible to users
    if success is False or error:
        logger.error(log_message)
    else:
        logger.info(log_message)

def verify_booking_saved(spreadsheet, booking_data, max_retries=3):
    """Verify that booking was actually saved to Google Sheets"""
    try:
        for attempt in range(max_retries):
            log_booking_attempt("VERIFY_ATTEMPT", f"Attempt {attempt + 1}/{max_retries}")
            
            # Get fresh data from sheets
            reservas_ws = spreadsheet.worksheet("proveedor_reservas")
            all_data = reservas_ws.get_all_values()
            
            if len(all_data) <= 1:  # Only headers
                log_booking_attempt("VERIFY_FAILED", "No data found in sheet")
                continue
            
            # Check last few rows for our booking
            rows_to_check = min(5, len(all_data) - 1)  # Check last 5 rows
            for i in range(len(all_data) - rows_to_check, len(all_data)):
                row = all_data[i]
                if len(row) >= 5:  # Ensure row has enough columns
                    # Check if this row matches our booking
                    if (row[0] == booking_data['Fecha'] and 
                        row[1] == booking_data['Hora'] and 
                        row[2] == booking_data['Proveedor'] and 
                        row[3] == str(booking_data['Numero_de_bultos']) and 
                        row[4] == booking_data['Orden_de_compra']):
                        
                        log_booking_attempt("VERIFY_SUCCESS", f"Booking found in row {i + 1}")
                        return True, f"Booking verified in row {i + 1}"
            
            # If not found, wait and retry
            if attempt < max_retries - 1:
                log_booking_attempt("VERIFY_RETRY", f"Booking not found, waiting {attempt + 1} seconds")
                time.sleep(attempt + 1)  # Progressive delay
        
        return False, "Booking not found after verification attempts"
        
    except Exception as e:
        error_msg = f"Verification failed: {str(e)}"
        log_booking_attempt("VERIFY_ERROR", "", error=error_msg)
        return False, error_msg

def get_sheet_row_count(worksheet):
    """Get the current number of rows in the worksheet"""
    try:
        all_values = worksheet.get_all_values()
        # Subtract 1 for header row to get actual data rows
        data_rows = len(all_values) - 1 if all_values else 0
        return max(0, data_rows)
    except Exception as e:
        # Log error server-side only, don't show to user
        log_booking_attempt("ROW_COUNT_ERROR", "", error=f"Failed to get row count: {str(e)}")
        return -1

def save_booking_to_sheets_enhanced(new_booking):
    """
    Enhanced save function with row count and specific booking verification
    
    Error Codes for User Messages:
    - Error código 1: Database connection failures (can't connect to Google Sheets, can't load data)
    - Error código 2: API failures (Google Sheets API calls fail, general exceptions)
    - Error código 3: Row count verification failures (row count doesn't increase as expected)
    - Error código 4: Booking verification failures (can't find specific booking after saving)
    """
    booking_id = f"{new_booking['Proveedor']}_{new_booking['Fecha']}_{new_booking['Hora']}"
    
    try:
        log_booking_attempt("SAVE_START", f"Booking ID: {booking_id}")
        
        # Step 1: Clear cache and get fresh data
        log_booking_attempt("CACHE_CLEAR", "Clearing cached data")
        download_sheets_to_memory.clear()
        credentials_df, reservas_df, gestion_df = download_sheets_to_memory()
        
        if reservas_df is None:
            error_msg = "Failed to load data from Google Sheets"
            log_booking_attempt("DATA_LOAD_FAILED", booking_id, success=False, error=error_msg)
            st.error("❌ Debido a errores de servidor, no se pudo concretar la reserva. Por favor intentar luego después de unos minutos (Error código 1)")
            return False, error_msg

        log_booking_attempt("DATA_LOADED", f"Loaded {len(reservas_df)} existing reservations")

        # Step 2: Final availability check
        fecha_reserva = new_booking['Fecha']
        hora_reserva = new_booking['Hora']
        
        log_booking_attempt("AVAILABILITY_CHECK", f"Date: {fecha_reserva}, Time: {hora_reserva}")
        
        existing_booking = reservas_df[
            (reservas_df['Fecha'].astype(str).str.contains(fecha_reserva.split(' ')[0], na=False)) & 
            (reservas_df['Hora'].astype(str) == hora_reserva)
        ]
        
        if not existing_booking.empty:
            error_msg = "Slot already booked by another provider"
            log_booking_attempt("SLOT_TAKEN", booking_id, success=False, error=error_msg)
            st.error("❌ Otro proveedor acaba de reservar este horario")
            download_sheets_to_memory.clear()
            return False, error_msg

        log_booking_attempt("SLOT_AVAILABLE", f"Slot confirmed available for {booking_id}")

        # Step 3: Get Google Sheets connection
        log_booking_attempt("SHEETS_CONNECT", "Establishing Google Sheets connection")
        gc = setup_google_sheets()
        if not gc:
            error_msg = "Failed to connect to Google Sheets"
            log_booking_attempt("SHEETS_CONNECTION_FAILED", booking_id, success=False, error=error_msg)
            st.error("❌ Debido a errores de servidor, no se pudo concretar la reserva. Por favor intentar luego después de unos minutos (Error código 1)")
            return False, error_msg

        spreadsheet = gc.open(st.secrets["GOOGLE_SHEET_NAME"])
        reservas_ws = spreadsheet.worksheet("proveedor_reservas")
        
        log_booking_attempt("WORKSHEET_ACCESSED", "proveedor_reservas worksheet accessed")

        # Step 4: Get initial row count BEFORE saving
        initial_row_count = get_sheet_row_count(reservas_ws)
        if initial_row_count == -1:
            error_msg = "Failed to get initial row count"
            log_booking_attempt("INITIAL_COUNT_FAILED", booking_id, success=False, error=error_msg)
            st.error("❌ Debido a errores de servidor, no se pudo concretar la reserva. Por favor intentar luego después de unos minutos (Error código 2)")
            return False, error_msg
        
        log_booking_attempt("INITIAL_ROW_COUNT", f"Rows before save: {initial_row_count}")

        # Step 5: Prepare data for saving
        new_row_data = [
            new_booking['Fecha'],
            new_booking['Hora'],
            new_booking['Proveedor'],
            str(new_booking['Numero_de_bultos']),
            new_booking['Orden_de_compra']
        ]
    
        log_booking_attempt("DATA_PREPARED", f"Row data: {new_row_data}")

        # Attempt to save with retry logic
        max_save_attempts = 10
        save_success = False
        last_error = None
        
        for attempt in range(max_save_attempts):
            try:
                log_booking_attempt("SAVE_ATTEMPT", f"Attempt {attempt + 1}/{max_save_attempts} for {booking_id}")
                
                # Save to sheets
                #reservas_ws.append_row(new_row_data, value_input_option='RAW')
                #log_booking_attempt("APPEND_REQUESTED", f"append_row() request sent for {booking_id}")
                
                #new append starts
                all_values = reservas_ws.get_all_values()
                next_row = len(all_values) + 1
                col_range = f'A{next_row}:E{next_row}'
                reservas_ws.update(
                    range_name=col_range,
                    values=[new_row_data],
                    value_input_option='RAW'
                )                
                log_booking_attempt("APPEND_REQUESTED", f"Updated row {next_row} for {booking_id}")
                #new append ends

                # Wait a moment for Google Sheets to process
                time.sleep(5)
                
                # Step 5: Verify the specific booking was saved (CONTENT-ONLY VALIDATION)
                log_booking_attempt("PROCESSING_WAIT", f"Waiting for Google Sheets to process {booking_id}")
                
                verification_success, verification_message = verify_booking_saved(spreadsheet, new_booking)
                
                if verification_success:
                    log_booking_attempt("BOOKING_SAVE_SUCCESS", f"{booking_id} successfully saved and verified", success=True)
                    save_success = True
                    break
                else:
                    last_error = f"BOOKING_VERIFICATION_FAILED: {verification_message}"
                    log_booking_attempt("BOOKING_VERIFICATION_FAILED", f"{booking_id} save failed - content not found: {verification_message}", success=False)
                    
                    if attempt < max_save_attempts - 1:
                        wait_time = (attempt + 1) * 2
                        log_booking_attempt("SAVE_RETRY_WAIT", f"Waiting {wait_time} seconds before retry")
                        time.sleep(wait_time)
                
            except Exception as save_error:
                last_error = f"API_FAILURE: Save attempt {attempt + 1} failed: {str(save_error)}"
                log_booking_attempt("SAVE_ATTEMPT_ERROR", f"{booking_id}", error=last_error)
                
                if attempt < max_save_attempts - 1:
                    wait_time = (attempt + 1) * 2
                    time.sleep(wait_time)
        
        if save_success:
            # Clear cache after successful save
            download_sheets_to_memory.clear()
            log_booking_attempt("SAVE_COMPLETE", f"{booking_id} successfully saved and verified", success=True)
            return True, "Booking saved and verified successfully"
        else:
            # Determine error code based on the type of failure
            if "BOOKING_VERIFICATION_FAILED" in last_error:
                error_code = "4"  # Booking verification failure
            elif "API_FAILURE" in last_error:
                error_code = "2"  # API failure
            else:
                error_code = "2"  # Default to API failure
            
            error_msg = f"Failed to save after {max_save_attempts} attempts. Last error: {last_error}"
            log_booking_attempt("SAVE_FAILED_FINAL", booking_id, success=False, error=error_msg)
            
            # Show user-friendly error message with appropriate error code
            st.error(f"❌ Debido a errores de servidor, no se pudo concretar la reserva. Por favor intentar luego después de unos minutos (Error código {error_code})")
            
            return False, error_msg
        
    except Exception as e:
        error_msg = f"Unexpected error in save_booking_to_sheets_enhanced: {str(e)}"
        log_booking_attempt("SAVE_EXCEPTION", booking_id, success=False, error=error_msg)
        
        # Show user-friendly error message
        st.error("❌ Debido a errores de servidor, no se pudo concretar la reserva. Por favor intentar luego después de unos minutos (Error código 2)")
        
        return False, error_msg

def get_duration_and_slots_info(numero_bultos, selected_slot):
    """Get duration text and combined slots based on bultos"""
    if numero_bultos >= 8:
        # 60 minutes (3 x 20-minute slots)
        slot1 = selected_slot
        slot2 = get_next_slot(slot1)
        slot3 = get_next_slot(slot2)
        combined_hora = f"{slot1}:00, {slot2}:00, {slot3}:00"
        duration_text = " (60 minutos)"
        duration_minutes = 60
    elif numero_bultos >= 4:
        # 40 minutes (2 x 20-minute slots)
        slot1 = selected_slot
        slot2 = get_next_slot(slot1)
        combined_hora = f"{slot1}:00, {slot2}:00"
        duration_text = " (40 minutos)"
        duration_minutes = 40
    else:
        # 20 minutes (single slot)
        combined_hora = f"{selected_slot}:00"
        duration_text = " (20 minutos)"
        duration_minutes = 20
    
    return combined_hora, duration_text, duration_minutes

def enhanced_confirmation_process(selected_date, selected_slot, numero_bultos, valid_orders, supplier_name, supplier_email, supplier_cc_emails):
    """Enhanced confirmation process with proper error handling and logging"""
    
    log_booking_attempt("CONFIRMATION_START", f"User: {supplier_name}, Date: {selected_date}, Slot: {selected_slot}")
    
    # Final availability check
    with st.spinner("Verificando disponibilidad final..."):
        is_still_available, availability_message = check_slot_availability(selected_date, selected_slot, numero_bultos)
    
    if not is_still_available:
        log_booking_attempt("FINAL_CHECK_FAILED", f"{supplier_name}", success=False, error=availability_message)
        st.error(f"❌ {availability_message}")
        return False
    
    log_booking_attempt("FINAL_CHECK_PASSED", f"Slot still available for {supplier_name}")

    # Prepare booking data - MODIFIED FOR 20-MINUTE SLOTS
    orden_compra_combined = ', '.join(valid_orders)
    
    combined_hora, duration_text, _ = get_duration_and_slots_info(numero_bultos, selected_slot)
    
    booking_to_save = {
        'Fecha': selected_date.strftime('%Y-%m-%d') + ' 0:00:00',
        'Hora': combined_hora,
        'Proveedor': supplier_name,
        'Numero_de_bultos': numero_bultos,
        'Orden_de_compra': orden_compra_combined
    }
    
    log_booking_attempt("BOOKING_PREPARED", f"Data prepared for {supplier_name}: {booking_to_save}")

    # Attempt to save booking
    with st.spinner("Guardando reserva... (Esto puede tomar unos momentos)"):
        save_success, save_message = save_booking_to_sheets_enhanced(booking_to_save)
    
    if not save_success:
        log_booking_attempt("BOOKING_SAVE_FAILED", f"{supplier_name}", success=False, error=save_message)
        
        # User already saw the error message from save_booking_to_sheets_enhanced
        st.error("❌ No se enviará email de confirmación debido al error en el guardado")
        
        # Clear selected slot so user can try again
        if 'selected_slot' in st.session_state:
            del st.session_state.selected_slot
        
        st.info("💡 Puede intentar seleccionar otro horario o el mismo horario nuevamente después de unos minutos")
        
        return False
    
    # Only send email if save was successful and verified
    log_booking_attempt("BOOKING_SAVED", f"{supplier_name} - {save_message}", success=True)
    st.success("✅ Reserva confirmada y verificada!")
    
    # Send email
    if supplier_email:
        log_booking_attempt("EMAIL_START", f"Sending to {supplier_email}")
        
        with st.spinner("Enviando confirmación por email..."):
            email_sent, actual_cc_emails = send_booking_email(
                supplier_email,
                supplier_name,
                booking_to_save,
                supplier_cc_emails
            )
        
        if email_sent:
            log_booking_attempt("EMAIL_SUCCESS", f"Email sent to {supplier_email}, CC: {actual_cc_emails}", success=True)
            st.success(f"📧 Email de confirmación enviado a: {supplier_email}")
            if actual_cc_emails:
                st.success(f"📧 CC enviado a: {', '.join(actual_cc_emails)}")
        else:
            log_booking_attempt("EMAIL_FAILED", f"Failed to send email to {supplier_email}", success=False)
            st.warning("⚠️ Reserva guardada exitosamente pero error enviando email")
    else:
        log_booking_attempt("NO_EMAIL", f"No email configured for {supplier_name}")
        st.warning("⚠️ No se encontró email para enviar confirmación")
    
    return True

# ─────────────────────────────────────────────────────────────
# 3. Email Functions - MODIFIED FOR 20-MINUTE SLOTS
# ─────────────────────────────────────────────────────────────
def download_pdf_attachment():
    """Download PDF attachment from Google Drive"""
    try:
        # Get Google Drive service using same credentials as Sheets
        gc = setup_google_sheets()
        if not gc:
            return None, None
        
        # Get the underlying credentials for Drive API
        credentials_info = dict(st.secrets["google_service_account"])
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly"
        ]
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        
        # Build Drive service
        from googleapiclient.discovery import build
        drive_service = build('drive', 'v3', credentials=credentials)
        
        # Download the PDF file
        file_id = st.secrets["PDF_FILE_ID"]
        
        # Get file metadata
        file_metadata = drive_service.files().get(fileId=file_id).execute()
        filename = file_metadata.get('name', 'GUIA_DEL_SELLER_DISMAC_MARKETPLACE.pdf')
        
        # Download file content
        request = drive_service.files().get_media(fileId=file_id)
        
        # Execute download
        pdf_content = io.BytesIO()
        downloader = request.execute()
        pdf_content.write(downloader)
        pdf_content.seek(0)
        
        return pdf_content.getvalue(), filename
        
    except Exception as e:
        st.warning(f"No se pudo descargar el archivo adjunto: {str(e)}")
        return None, None

def send_booking_email(supplier_email, supplier_name, booking_details, cc_emails=None):
    """Send booking confirmation email - MODIFIED FOR 20-MINUTE SLOTS"""
    try:
        # Use provided CC emails or default
        if cc_emails is None or len(cc_emails) == 0:
            cc_emails = [ "ljbyon@dismac.com.bo"]
        else:
            # Add default email to the CC list if not already present
            if "marketplace@dismac.com.bo" not in cc_emails:
                cc_emails = cc_emails + [ "ljbyon@dismac.com.bo"]
        
        # Email content
        subject = "Confirmación de Reserva para Entrega de Mercadería"
        
        # Format dates for email display
        display_fecha = booking_details['Fecha'].split(' ')[0]  # Remove time part for display
        
        # Handle combined hora format for reservations - MODIFIED FOR 20-MINUTE SLOTS
        hora_field = booking_details['Hora']
        if ',' in hora_field:
            # Combined slots - show as range
            slots = [slot.strip() for slot in hora_field.split(',')]
            start_time = slots[0].rsplit(':', 1)[0]  # Remove seconds
            last_slot = slots[-1].split(':')
            end_hour = int(last_slot[0])
            end_minute = int(last_slot[1])
            
            # Add 20 minutes to get actual end time
            end_minute += 20
            if end_minute >= 60:
                end_hour += end_minute // 60
                end_minute = end_minute % 60
            
            end_time = f"{end_hour:02d}:{end_minute:02d}"
            display_hora = f"{start_time} - {end_time}"
            
            # Determine duration based on number of slots
            num_slots = len(slots)
            duration_minutes = num_slots * 20
            duration_info = f" (Duración: {duration_minutes} minutos)"
        else:
            # Single slot (20 minutes)
            display_hora = hora_field.rsplit(':', 1)[0]  # Remove seconds
            duration_info = " (Duración: 20 minutos)"
        
        body = f"""
        Hola {supplier_name},
        
        Su reserva de entrega ha sido confirmada exitosamente.
        
        DETALLES DE LA RESERVA:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        📅 Fecha: {display_fecha}
        🕐 Horario: {display_hora}{duration_info}
        📦 Número de bultos: {booking_details['Numero_de_bultos']}
        📋 Orden de compra: {booking_details['Orden_de_compra']}
        
        INSTRUCCIONES:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        • Respeta el horario reservado para tu entrega.
        • En caso de retraso, podrías tener que esperar hasta el próximo cupo disponible del día o reprogramar tu entrega.
        • Dismac no se responsabiliza por los tiempos de espera ocasionados por llegadas fuera de horario.
        • Además, según el tipo de venta, es importante considerar lo siguiente:
          - Venta al contado: Debes entregar el pedido junto con la factura a nombre del comprador y tres (3) copias de la orden de compra.
          - Venta en minicuotas: Debes entregar el pedido junto con la factura a nombre de Dismatec S.A. y una (1) copia de la orden de compra.
        
        REQUISITOS DE SEGURIDAD
        • Pantalón largo, sin rasgados
        • Botines de seguridad
        • Casco de seguridad
        • Chaleco o camisa con reflectivo
        • No está permitido manillas, cadenas, y principalmente masticar coca.

        Gracias por utilizar nuestro sistema de reservas.
        
        Saludos cordiales,
        Equipo de Almacén Dismac
        """
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = supplier_email
        msg['Cc'] = ', '.join(cc_emails)
        msg['Subject'] = subject
        
        # Add body
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # Download and attach PDF (optional)
        pdf_data, pdf_filename = download_pdf_attachment()
        if pdf_data:
            attachment = MIMEBase('application', 'octet-stream')
            attachment.set_payload(pdf_data)
            encoders.encode_base64(attachment)
            attachment.add_header(
                'Content-Disposition',
                f'attachment; filename= {pdf_filename}'
            )
            msg.attach(attachment)
        
        # Send email
        server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASSWORD)
        
        # Send to supplier + CC recipients
        all_recipients = [supplier_email] + cc_emails
        text = msg.as_string()
        server.sendmail(EMAIL_USER, all_recipients, text)
        server.quit()
        
        return True, cc_emails
        
    except Exception as e:
        st.error(f"Error enviando email: {str(e)}")
        return False, []

# ─────────────────────────────────────────────────────────────
# 4. Time Slot Functions - MODIFIED FOR 20-MINUTE SLOTS
# ─────────────────────────────────────────────────────────────
def parse_booked_slots(booked_hours):
    """Parse booked hours that may contain single or combined time slots"""
    all_booked_slots = []
    
    for booked_hora in booked_hours:
        hora_str = str(booked_hora).strip()
        
        # Skip empty or NaN values
        if not hora_str or hora_str.lower() in ['nan', 'none', '']:
            continue
        
        # Check if it contains comma (combined slots)
        if ',' in hora_str:
            # Split by comma and clean each slot
            slots = [slot.strip() for slot in hora_str.split(',')]
            for slot in slots:
                formatted_slot = format_time_slot(slot)
                if formatted_slot:
                    all_booked_slots.append(formatted_slot)
        else:
            # Single slot
            formatted_slot = format_time_slot(hora_str)
            if formatted_slot:
                all_booked_slots.append(formatted_slot)
    
    return all_booked_slots

def format_time_slot(time_str):
    """Format time string to HH:MM format, handling various input formats"""
    try:
        time_str = str(time_str).strip()
        
        # Handle different time formats that might come from Google Sheets
        if ':' in time_str:
            parts = time_str.split(':')
            if len(parts) >= 2:
                hour = int(parts[0])
                minute = int(parts[1])
                return f"{hour:d}:{minute:02d}"
        
        # Handle time objects or datetime objects
        if hasattr(time_str, 'hour') and hasattr(time_str, 'minute'):
            return f"{time_str.hour:02d}:{time_str.minute:02d}"
        
        return None
        
    except (ValueError, AttributeError, TypeError):
        return None

def generate_all_20min_slots():
    """Generate all possible 20-minute slots"""
    weekday_slots = []
    saturday_slots = []
    
    # Weekday slots (9:00-16:00)
    for hour in range(9, 16):
        for minute in [0, 20, 40]:
            start_time = f"{hour:d}:{minute:02d}"
            weekday_slots.append(start_time)
    
    # Saturday slots (9:00-12:00)
    for hour in range(9, 12):
        for minute in [0, 20, 40]:
            start_time = f"{hour:d}:{minute:02d}"
            saturday_slots.append(start_time)
    
    return weekday_slots, saturday_slots

def get_next_slot(slot_time):
    """Get the next 20-minute slot"""
    hour, minute = map(int, slot_time.split(':'))
    
    if minute == 0:
        next_slot = f"{hour:d}:20"
    elif minute == 20:
        next_slot = f"{hour:d}:40"
    else:  # minute == 40
        next_hour = hour + 1
        next_slot = f"{next_hour:d}:00"
    
    return next_slot

def find_contiguous_slots(all_slots, booked_slots, slots_needed):
    """Find available contiguous slots based on number of slots needed"""
    available_slots = []
    
    for i in range(len(all_slots) - (slots_needed - 1)):
        # Check if we have enough consecutive slots
        slots_to_check = []
        current = all_slots[i]
        slots_to_check.append(current)
        
        # Get the next slots needed
        for j in range(1, slots_needed):
            next_expected = get_next_slot(slots_to_check[-1])
            if i + j < len(all_slots) and all_slots[i + j] == next_expected:
                slots_to_check.append(all_slots[i + j])
            else:
                break
        
        # Check if we found all needed consecutive slots
        if len(slots_to_check) == slots_needed:
            # Check if all slots are available
            if all(slot not in booked_slots for slot in slots_to_check):
                available_slots.append(current)
    
    return available_slots

def get_available_slots(selected_date, reservas_df, numero_bultos):
    """Get available slots for a date based on bultos count"""
    weekday_slots, saturday_slots = generate_all_20min_slots()
    
    # Sunday = 6, no work
    if selected_date.weekday() == 6:
        return []
    
    # Saturday = 5
    if selected_date.weekday() == 5:
        all_20min_slots = saturday_slots
    else:
        all_20min_slots = weekday_slots
    
    # Get booked slots for this date
    target_date = selected_date.strftime('%Y-%m-%d')
    
    # Filter reservations for the selected date
    date_mask = reservas_df['Fecha'].astype(str).str.contains(target_date, na=False)
    booked_hours = reservas_df[date_mask]['Hora'].tolist()
    
    # Parse booked slots (handles combined slots)
    booked_slots = parse_booked_slots(booked_hours)
    
    if numero_bultos >= 8:
        # For 8+ bultos, find contiguous 60-minute slots (3 x 20 minutes)
        return find_contiguous_slots(all_20min_slots, booked_slots, 3)
    elif numero_bultos >= 4:
        # For 4-7 bultos, find contiguous 40-minute slots (2 x 20 minutes)
        return find_contiguous_slots(all_20min_slots, booked_slots, 2)
    else:
        # For 1-3 bultos, return available 20-minute slots
        return [slot for slot in all_20min_slots if slot not in booked_slots]

# ─────────────────────────────────────────────────────────────
# 5. Authentication Function - UPDATED FOR GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────
def authenticate_user(usuario, password):
    """Authenticate user against Google Sheets data and get email + CC emails"""
    credentials_df, _, _ = download_sheets_to_memory()
    
    if credentials_df is None:
        return False, "Error al cargar credenciales", None, None
    
    # Clean and compare (all data is already strings)
    df_usuarios = credentials_df['usuario'].str.strip()
    
    input_usuario = str(usuario).strip()
    input_password = str(password).strip()
    
    # Find user row
    user_row = credentials_df[df_usuarios == input_usuario]
    if user_row.empty:
        return False, "Usuario no encontrado", None, None
    
    # Get stored password and clean it
    stored_password = str(user_row.iloc[0]['password']).strip()
    
    # Compare passwords
    if stored_password == input_password:
        # Get email
        email = None
        try:
            email = user_row.iloc[0]['Email']
            if str(email) == 'nan' or email is None:
                email = None
        except:
            email = None
        
        # Get CC emails
        cc_emails = []
        try:
            cc_data = user_row.iloc[0]['cc']
            if str(cc_data) != 'nan' and cc_data is not None and str(cc_data).strip():
                # Parse semicolon-separated emails
                cc_emails = [email.strip() for email in str(cc_data).split(';') if email.strip()]
        except Exception as e:
            cc_emails = []
        
        return True, "Autenticación exitosa", email, cc_emails
    
    return False, "Contraseña incorrecta", None, None

# ─────────────────────────────────────────────────────────────
# 6. Fresh slot validation function - MODIFIED FOR 20-MINUTE SLOTS
# ─────────────────────────────────────────────────────────────
def check_slot_availability(selected_date, slot_time, numero_bultos):
    """Check if a specific slot is still available with fresh data from Google Sheets"""
    try:
        # Force fresh download
        download_sheets_to_memory.clear()
        _, fresh_reservas_df, _ = download_sheets_to_memory()
        
        if fresh_reservas_df is None:
            return False, "Error al verificar disponibilidad"
        
        # Get booked slots for this date
        target_date = selected_date.strftime('%Y-%m-%d')
        date_mask = fresh_reservas_df['Fecha'].astype(str).str.contains(target_date, na=False)
        booked_hours = fresh_reservas_df[date_mask]['Hora'].tolist()
        
        # Parse booked slots (handles combined slots)
        booked_slots = parse_booked_slots(booked_hours)
        
        if numero_bultos >= 8:
            # For 8+ bultos, check current and next 2 slots (60 minutes)
            slot1 = slot_time
            slot2 = get_next_slot(slot1)
            slot3 = get_next_slot(slot2)
            
            if slot1 in booked_slots:
                return False, "Otro proveedor acaba de reservar este horario. Por favor, elija otro."
            if slot2 in booked_slots:
                return False, "Uno de los horarios necesarios para su reserva de 60 minutos ya está ocupado."
            if slot3 in booked_slots:
                return False, "Uno de los horarios necesarios para su reserva de 60 minutos ya está ocupado."
                
        elif numero_bultos >= 4:
            # For 4-7 bultos, check current and next slot (40 minutes)
            slot1 = slot_time
            slot2 = get_next_slot(slot1)
            
            if slot1 in booked_slots:
                return False, "Otro proveedor acaba de reservar este horario. Por favor, elija otro."
            if slot2 in booked_slots:
                return False, "El horario siguiente necesario para su reserva de 40 minutos ya está ocupado."
        else:
            # For 1-3 bultos, check only current slot (20 minutes)
            if slot_time in booked_slots:
                return False, "Otro proveedor acaba de reservar este horario. Por favor, elija otro."
        
        return True, "Horario disponible"
        
    except Exception as e:
        return False, f"Error verificando disponibilidad: {str(e)}"

# ─────────────────────────────────────────────────────────────
# 7. Main App - MODIFIED FOR 20-MINUTE SLOTS
# ─────────────────────────────────────────────────────────────
def main():
    st.title("🚚 Dismac: Reserva de Entrega de Mercadería")
    
    # Download Google Sheets data when app starts
    with st.spinner("Cargando datos..."):
        credentials_df, reservas_df, gestion_df = download_sheets_to_memory()
    
    if credentials_df is None:
        st.error("❌ Error al cargar datos")
        if st.button("🔄 Reintentar Conexión"):
            download_sheets_to_memory.clear()
            st.rerun()
        return
    
    
    # Session state - UNCHANGED
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'supplier_name' not in st.session_state:
        st.session_state.supplier_name = None
    if 'supplier_email' not in st.session_state:
        st.session_state.supplier_email = None
    if 'supplier_cc_emails' not in st.session_state:
        st.session_state.supplier_cc_emails = []
    if 'slot_error_message' not in st.session_state:
        st.session_state.slot_error_message = None
    if 'orden_compra_list' not in st.session_state:
        st.session_state.orden_compra_list = ['']
    
    # Authentication - UNCHANGED LOGIC
    if not st.session_state.authenticated:
        st.subheader("🔐 Iniciar Sesión")
        
        with st.form("login_form"):
            usuario = st.text_input("Usuario")
            password = st.text_input("Contraseña", type="password")
            submitted = st.form_submit_button("Iniciar Sesión")
            
            if submitted:
                if usuario and password:
                    is_valid, message, email, cc_emails = authenticate_user(usuario, password)
                    
                    if is_valid:
                        st.session_state.authenticated = True
                        st.session_state.supplier_name = usuario
                        st.session_state.supplier_email = email
                        st.session_state.supplier_cc_emails = cc_emails
                        # Clear booking session data
                        st.session_state.orden_compra_list = ['']
                        if 'numero_bultos_input' in st.session_state:
                            del st.session_state.numero_bultos_input
                        if 'selected_slot' in st.session_state:
                            del st.session_state.selected_slot
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)
                else:
                    st.warning("Complete todos los campos")
    
    # Main interface after authentication
    else:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.subheader(f"Bienvenido, {st.session_state.supplier_name}")
        with col2:
            if st.button("Cerrar Sesión"):
                st.session_state.authenticated = False
                st.session_state.supplier_name = None
                st.session_state.supplier_email = None
                st.session_state.supplier_cc_emails = []
                # Clear booking session data
                st.session_state.orden_compra_list = ['']
                if 'numero_bultos_input' in st.session_state:
                    del st.session_state.numero_bultos_input
                if 'selected_slot' in st.session_state:
                    del st.session_state.selected_slot
                st.rerun()
        
        st.markdown("---")
        
        # STEP 1: Delivery Information - MODIFIED INFO MESSAGE
        st.subheader("📦 Información de Entrega")
        st.markdown('<p style="color: red; font-size: 14px; margin-top: -10px;">Esta aplicación permite programar entregas <strong>exclusivamente de pedidos Marketplace</strong>.<br>Las compras locales o corporativas deben coordinarse directamente con el almacén.</p>', unsafe_allow_html=True)        
        # Show permanent information about time slot durations - MODIFIED FOR 20-MINUTE SLOTS
        st.info("ℹ️ **La duración del horario de reserva dependerá de la cantidad de bultos:** 1-3 bultos = 20 minutos, 4-7 bultos = 40 minutos y 8+ bultos = 60 minutos")
        
        # Number of bultos (MANDATORY, NO DEFAULT)
        numero_bultos = st.number_input(
            "📦 Número de bultos *", 
            min_value=0, 
            value=None,
            key="numero_bultos_input",
            help="Cantidad de bultos o paquetes a entregar (obligatorio)",
            placeholder="Ingrese el número de bultos"
        )
        
        # Get value from session state (automatically updated by key)
        if 'numero_bultos_input' in st.session_state and st.session_state.numero_bultos_input:
            numero_bultos = st.session_state.numero_bultos_input
        
        # Multiple Purchase orders section - UNCHANGED
        st.write("📋 **Órdenes de compra** *")
        
        # Display current orden de compra inputs
        orden_compra_values = []
        for i, orden in enumerate(st.session_state.orden_compra_list):
            if len(st.session_state.orden_compra_list) == 1:
                # Single order - full width
                orden_value = st.text_input(
                    f"Orden {i+1}",
                    value=orden,
                    placeholder=f"Ej: 0000000",
                    key=f"orden_{i}"
                )
                orden_compra_values.append(orden_value)
            else:
                # Multiple orders - use columns for remove button
                col1, col2 = st.columns([5, 1])
                with col1:
                    orden_value = st.text_input(
                        f"Orden {i+1}",
                        value=orden,
                        placeholder=f"Ej: OC-2024-00{i+1}",
                        key=f"orden_{i}"
                    )
                    orden_compra_values.append(orden_value)
                with col2:
                    st.write("")  # Empty space for alignment
                    if st.button("🗑️", key=f"remove_{i}"):
                        st.session_state.orden_compra_list.pop(i)
                        st.rerun()
        
        # Update session state with current values
        st.session_state.orden_compra_list = orden_compra_values
        
        # Add button
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("➕ Agregar", use_container_width=True):
                st.session_state.orden_compra_list.append('')
                st.rerun()
        
        # Check if minimum requirements are met to proceed
        valid_orders = [orden.strip() for orden in orden_compra_values if orden.strip()]
        can_proceed = numero_bultos and numero_bultos > 0 and valid_orders
        
        if not can_proceed:
            st.warning("⚠️ Complete el número de bultos y al menos una orden de compra para continuar.")
            return
        
        st.markdown("---")
        
        # STEP 2: Date selection - UNCHANGED
        st.subheader("📅 Seleccionar Fecha")
        st.markdown('<p style="color: red; font-size: 14px; margin-top: -10px;">Le rogamos seleccionar la fecha y el horario con atención, ya que, una vez confirmados, no podrán ser modificados ni cancelados.</p>', unsafe_allow_html=True)
        today = datetime.now().date()
        max_date = today + timedelta(days=30)
        
        selected_date = st.date_input(
            "Fecha de entrega",
            min_value=today,
            max_value=max_date,
            value=today
        )
        
        # Check if Sunday
        if selected_date.weekday() == 6:
            st.warning("⚠️ No trabajamos los domingos")
            return
        
        # STEP 3: Time slot selection - MODIFIED FOR 20-MINUTE SLOTS
        st.subheader("🕐 Horarios Disponibles")
        
        # Show any persistent error message
        if st.session_state.slot_error_message:
            st.error(f"❌ {st.session_state.slot_error_message}")
        
        # Get ALL possible slots and determine availability - MODIFIED
        weekday_slots, saturday_slots = generate_all_20min_slots()
        
        if selected_date.weekday() == 5:  # Saturday
            all_20min_slots = saturday_slots
        else:  # Monday-Friday
            all_20min_slots = weekday_slots
        
        # Get booked slots for this date
        target_date = selected_date.strftime('%Y-%m-%d')
        date_mask = reservas_df['Fecha'].astype(str).str.contains(target_date, na=False)
        booked_hours = reservas_df[date_mask]['Hora'].tolist()
        booked_slots = parse_booked_slots(booked_hours)
        
        # Generate display slots based on bultos - MODIFIED FOR 20-MINUTE SLOTS
        if numero_bultos >= 8:
            # For 8+ bultos, show all possible 60-minute slots with availability
            display_slots = []
            slots_needed = 3
            for i in range(len(all_20min_slots) - (slots_needed - 1)):
                current_slot = all_20min_slots[i]
                slots_to_check = [current_slot]
                temp_slot = current_slot
                for j in range(1, slots_needed):
                    temp_slot = get_next_slot(temp_slot)
                    if i + j < len(all_20min_slots) and all_20min_slots[i + j] == temp_slot:
                        slots_to_check.append(temp_slot)
                    else:
                        break
                if len(slots_to_check) == slots_needed:
                    is_available = all(slot not in booked_slots for slot in slots_to_check)
                    display_slots.append((current_slot, is_available))
        elif numero_bultos >= 4:
            # For 4-7 bultos, show all possible 40-minute slots with availability
            display_slots = []
            slots_needed = 2
            for i in range(len(all_20min_slots) - (slots_needed - 1)):
                current_slot = all_20min_slots[i]
                next_slot = get_next_slot(current_slot)
                if i + 1 < len(all_20min_slots) and all_20min_slots[i + 1] == next_slot:
                    is_available = current_slot not in booked_slots and next_slot not in booked_slots
                    display_slots.append((current_slot, is_available))
        else:
            # For 1-3 bultos, show all 20-minute slots with availability
            display_slots = [(slot, slot not in booked_slots) for slot in all_20min_slots]
        
        if not display_slots:
            st.warning("❌ No hay horarios para esta fecha")
            return
        
        # Display slots (2 per row) - MODIFIED BUTTON TEXT
        selected_slot = None
        
        for i in range(0, len(display_slots), 2):
            col1, col2 = st.columns(2)
            
            # First slot
            slot1, is_available1 = display_slots[i]
            
            # Button text based on bultos and availability - MODIFIED
            if numero_bultos >= 8:
                button_text1 = f"✅ {slot1} (60min)" if is_available1 else f"🚫 {slot1} (Ocupado)"
            elif numero_bultos >= 4:
                button_text1 = f"✅ {slot1} (40min)" if is_available1 else f"🚫 {slot1} (Ocupado)"
            else:
                button_text1 = f"✅ {slot1} (20min)" if is_available1 else f"🚫 {slot1} (Ocupado)"
            
            with col1:
                if not is_available1:
                    st.button(button_text1, disabled=True, key=f"slot_{i}", use_container_width=True)
                else:
                    if st.button(button_text1, key=f"slot_{i}", use_container_width=True):
                        # FRESH CHECK ON CLICK
                        with st.spinner("Verificando disponibilidad..."):
                            is_available, message = check_slot_availability(selected_date, slot1, numero_bultos)
                        
                        if is_available:
                            selected_slot = slot1
                            st.session_state.slot_error_message = None
                        else:
                            st.session_state.slot_error_message = message
                            st.rerun()
            
            # Second slot (if exists)
            if i + 1 < len(display_slots):
                slot2, is_available2 = display_slots[i + 1]
                
                # Button text based on bultos and availability - MODIFIED
                if numero_bultos >= 8:
                    button_text2 = f"✅ {slot2} (60min)" if is_available2 else f"🚫 {slot2} (Ocupado)"
                elif numero_bultos >= 4:
                    button_text2 = f"✅ {slot2} (40min)" if is_available2 else f"🚫 {slot2} (Ocupado)"
                else:
                    button_text2 = f"✅ {slot2} (20min)" if is_available2 else f"🚫 {slot2} (Ocupado)"
                
                with col2:
                    if not is_available2:
                        st.button(button_text2, disabled=True, key=f"slot_{i+1}", use_container_width=True)
                    else:
                        if st.button(button_text2, key=f"slot_{i+1}", use_container_width=True):
                            # FRESH CHECK ON CLICK
                            with st.spinner("Verificando disponibilidad..."):
                                is_available, message = check_slot_availability(selected_date, slot2, numero_bultos)
                            
                            if is_available:
                                selected_slot = slot2
                                st.session_state.slot_error_message = None
                            else:
                                st.session_state.slot_error_message = message
                                st.rerun()
        
        # STEP 4: Enhanced Confirmation - MODIFIED FOR 20-MINUTE SLOTS
        if selected_slot or 'selected_slot' in st.session_state:
            if selected_slot:
                st.session_state.selected_slot = selected_slot
            
            st.markdown("---")
            st.subheader("✅ Confirmar Reserva")
            
            # Show summary - MODIFIED
            _, duration_text, _ = get_duration_and_slots_info(numero_bultos, st.session_state.selected_slot)
            st.info(f"📅 Fecha: {selected_date}")
            st.info(f"🕐 Horario: {st.session_state.selected_slot}{duration_text}")
            st.info(f"📦 Número de bultos: {numero_bultos}")
            st.info(f"📋 Órdenes de compra: {', '.join(valid_orders)}")
            
            # Confirm button
            if st.button("✅ Confirmar Reserva", use_container_width=True):
                success = enhanced_confirmation_process(
                    selected_date,
                    st.session_state.selected_slot,
                    numero_bultos,
                    valid_orders,
                    st.session_state.supplier_name,
                    st.session_state.supplier_email,
                    st.session_state.supplier_cc_emails
                )
                
                if success:
                    st.balloons()
                    
                    # Clear session data and log off user
                    log_booking_attempt("SESSION_CLEANUP", f"Clearing session for {st.session_state.supplier_name}")
                    st.session_state.orden_compra_list = ['']
                    if 'numero_bultos_input' in st.session_state:
                        del st.session_state.numero_bultos_input
                    st.info("Cerrando sesión automáticamente...")
                    st.session_state.authenticated = False
                    st.session_state.supplier_name = None
                    st.session_state.supplier_email = None
                    st.session_state.supplier_cc_emails = []
                    if 'selected_slot' in st.session_state:
                        del st.session_state.selected_slot
                    
                    # Wait a moment then rerun
                    time.sleep(2)
                    st.rerun()

                    
if __name__ == "__main__":
    main()