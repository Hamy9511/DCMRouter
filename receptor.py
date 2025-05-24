from pynetdicom import AE, evt, AllStoragePresentationContexts, VerificationPresentationContexts
from pydicom import dcmread
from pydicom.uid import generate_uid
import os
import logging
import re
from pathlib import Path
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

# Configuración de logging con rotación diaria
def setup_logging():
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    
    # Configurar el logger principal
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Handler para archivo con rotación diaria
    log_directory = "logs"
    os.makedirs(log_directory, exist_ok=True)
    
    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(log_directory, 'dicom_server.log'),
        when='midnight',  # Rotación a medianoche
        interval=1,       # Cada día
        backupCount=7,    # Conservar 7 días de logs
        encoding='utf-8'
    )
    file_handler.setFormatter(logging.Formatter(log_format))
    file_handler.suffix = "%Y-%m-%d"  # Sufijo con la fecha
    
    # Añadir handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

logger = setup_logging()

class DICOMServerConfig:
    OUTPUT_FOLDER = "estudios_recibidos"
    AE_TITLE = "MI_RECEPTOR"
    PORT = 11112
    MAX_PDU = 0  # Sin límite
    MAX_FOLDER_LENGTH = 50  # Longitud máxima para nombres de carpeta

def clean_filename(name):
    """Limpia el nombre para usarlo como nombre de carpeta"""
    if not name:
        return "Sin_nombre"
    
    # Convertir a string si es un objeto DICOM
    name = str(name)
    
    # Reemplazar caracteres problemáticos
    name = re.sub(r'[\\/*?:"<>|^]', "_", name)
    name = name.replace(" ", "_").strip()
    
    # Limitar longitud
    return name[:DICOMServerConfig.MAX_FOLDER_LENGTH]

def shorten_uid(uid):
    """Acorta los UIDs para nombres de carpeta"""
    return uid[-12:] if len(uid) > 12 else uid

def create_safe_path(base_folder, *subfolders):
    """
    Crea una ruta segura dentro de los límites del sistema
    Devuelve el objeto Path si tiene éxito, None si falla
    """
    path = Path(base_folder)
    for folder in subfolders:
        path = path / folder
        try:
            path.mkdir(exist_ok=True, parents=True)
        except Exception as e:
            logger.error(f"No se pudo crear directorio {path}: {str(e)}")
            return None
    return path

def get_patient_folder_name(ds):
    """Genera un nombre de carpeta seguro basado en el paciente"""
    patient_id = clean_filename(ds.get("PatientID", "Sin_ID"))
    patient_name = clean_filename(ds.get("PatientName", "Sin_nombre"))
    return f"{patient_name}_{patient_id}"

def handle_store(event):
    """Manejador para eventos C-STORE"""
    try:
        ds = event.dataset
        ds.file_meta = event.file_meta
        
        # Obtener información del paciente
        patient_folder = get_patient_folder_name(ds)
        
        # Acortar identificadores largos
        study_uid = shorten_uid(ds.get('StudyInstanceUID', generate_uid()))
        series_uid = shorten_uid(ds.get('SeriesInstanceUID', generate_uid()))
        sop_instance_uid = shorten_uid(ds.get('SOPInstanceUID', generate_uid()))
        
        # Obtener metadatos adicionales
        modality = ds.get("Modality", "XX")
        instance_number = str(ds.get("InstanceNumber", "0")).zfill(4)
        study_date = ds.get("StudyDate", "")
        
        # Crear estructura de directorios segura
        base_path = Path(DICOMServerConfig.OUTPUT_FOLDER).absolute()
        safe_path = create_safe_path(
            base_path,
            patient_folder,
            f"{study_date}_E{study_uid}",
            f"{modality}_S{series_uid}"
        )
        
        if not safe_path:
            logger.error("No se pudo crear la estructura de directorios")
            return 0xC001  # Processing failure
        
        # Nombre de archivo seguro
        filename = f"{modality}_{instance_number}_{sop_instance_uid}.dcm"
        filepath = safe_path / filename
        
        # Verificar si el archivo ya existe
        if filepath.exists():
            logger.warning(f"Archivo ya existe: {filepath}")
            return 0x0000  # Success
        
        # Guardar el archivo DICOM
        try:
            ds.save_as(filepath, write_like_original=True)
            logger.info(f"Archivo DICOM guardado: {filepath}")
            return 0x0000  # Success
        except Exception as save_error:
            logger.error(f"Error al guardar {filepath}: {str(save_error)}")
            try:
                filepath.unlink(missing_ok=True)  # Intentar eliminar archivo corrupto
            except:
                pass
            return 0xC002  # Unable to process
        
    except Exception as e:
        logger.error(f"Error en handle_store: {str(e)}", exc_info=True)
        return 0xC003  # Unable to process

def handle_echo(event):
    """Manejador para eventos C-ECHO"""
    logger.info("Solicitud C-ECHO recibida")
    return 0x0000  # Success

def start_server():
    """Inicia el servidor DICOM"""
    try:
        # Configuración inicial
        output_path = Path(DICOMServerConfig.OUTPUT_FOLDER).absolute()
        os.makedirs(output_path, exist_ok=True)
        logger.info(f"Carpeta de salida: {output_path}")
        
        # Crear Application Entity
        ae = AE(ae_title=DICOMServerConfig.AE_TITLE)
        ae.maximum_pdu_size = DICOMServerConfig.MAX_PDU
        
        # Añadir contextos soportados
        storage_contexts = AllStoragePresentationContexts
        verification_contexts = VerificationPresentationContexts
        
        ae.supported_contexts = storage_contexts
        for context in verification_contexts:
            ae.add_supported_context(context.abstract_syntax)
        
        # Handlers de eventos
        handlers = [
            (evt.EVT_C_STORE, handle_store),
            (evt.EVT_C_ECHO, handle_echo),
            (evt.EVT_ABORTED, lambda event: logger.warning("Conexión abortada")),
            (evt.EVT_CONN_OPEN, lambda event: logger.info("Conexión establecida")),
            (evt.EVT_CONN_CLOSE, lambda event: logger.info("Conexión cerrada"))
        ]
        
        # Iniciar servidor
        logger.info(f"Iniciando servidor DICOM en puerto {DICOMServerConfig.PORT}...")
        ae.start_server(
            ('', DICOMServerConfig.PORT),
            evt_handlers=handlers,
            block=True
        )
        
    except Exception as e:
        logger.critical(f"Error en el servidor: {str(e)}", exc_info=True)
    finally:
        logger.info("Servidor DICOM detenido")

if __name__ == "__main__":
    # Habilitar soporte para rutas largas en Windows si es necesario
    if os.name == 'nt':
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetFileAttributesW.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32]
        kernel32.SetFileAttributesW.restype = ctypes.c_uint32
        FILE_ATTRIBUTE_NORMAL = 0x80
        kernel32.SetFileAttributesW(DICOMServerConfig.OUTPUT_FOLDER, FILE_ATTRIBUTE_NORMAL)
    
    start_server()