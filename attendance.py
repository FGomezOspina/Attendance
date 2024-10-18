import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime
import os
import sys
import io

# Función para obtener la ruta de recursos, útil para PyInstaller
def resource_path(relative_path):
    """Obtiene la ruta absoluta al recurso, funciona en desarrollo y PyInstaller"""
    try:
        # PyInstaller crea una carpeta temporal y almacena la ruta en _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# Función para conectar a la base de datos
def connect_db():
    db_path = resource_path("identification_records.db")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    return conn

# Función para crear las tablas en la base de datos
def create_tables():
    with connect_db() as conn:
        cursor = conn.cursor()
        
        # Tabla de registros de identificación
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS records (
            id TEXT PRIMARY KEY,
            name TEXT,
            area TEXT
        )
        ''')
        
        # Datos iniciales (completa con todos los registros)
        records_data = [
            ('1088327667', 'ERIKA TATIANA GALLEGO VINASCO', 'POSCOSECHA'),
            ('1004520909', 'ANGIE VANESSA ARCE LOPEZ', 'POSCOSECHA'),
            ('94509907', 'JULIÁN ANDRÉS PÉREZ BETANCUR', 'ADMINISTRACIÓN'),
            ('22131943', 'ROSA AMELIA SOLORZANO SAMPEDRO', 'ASEO'),
            ('1089747022', 'JUAN ESTEBAN TORO VARGAS', 'MANTENIMIENTO'),
            ('1004733786', 'STEVEN ARCE VALLE', 'POSCOSECHA'),
            ('1004756825', 'LAURA RAMIREZ QUINTERO', 'ADMINISTRACIÓN'),
            ('1088305975', 'PAOLA ANDREA OSORIO GRISALES', 'POSCOSECHA'),
            ('1004751727', 'LAURA VANESSA LONDOÑO SILVA', 'POSCOSECHA'),
            ('1088256932', 'YEISON LEANDRO ARIAS MONTES', 'OPERACIONES'),
            ('1088346284', 'NATALIA VALENCIA CORTES', 'ADMINISTRACIÓN'),
            ('1117964380', 'YAMILETH HILARION OLAYA', 'POSCOSECHA'),
            ('5760588', 'JOSE AUGUSTO CORDOVEZ CHARAMA', 'POSCOSECHA'),
            ('1112770658', 'LUIS ALBERTO OROZCO RODRIGUEZ', 'POSCOSECHA'),
            ('1110480331', 'CLAUDIA YULIMA CUTIVA BETANCOURTH', 'POSCOSECHA'),
            ('25038229', 'YORME DE JESUS LOPEZ IBARRA', 'POSCOSECHA'),
            ('1128844585', 'YENIFFER MOSQUERA PEREA', 'POSCOSECHA'),
            ('1089930892', 'ALEXANDRA RIOS BUENO', 'POSCOSECHA'),
            ('1088295574', 'ANDRES FELIPE BEDOYA ROJAS', 'POSCOSECHA'),
            ('1004767653', 'BRAYAN ANDRES JARAMILLO URBANO', 'MANTENIMIENTO'),
            ('1093984174', 'CARLOS ANDRES SANCHEZ QUEBRADA', 'POSCOSECHA'),
            ('1193546514', 'MAYERLIN PARRA RIVEROS', 'POSCOSECHA'),
            ('1088316215', 'SERGIO MUÑOZ RAMIREZ', 'ADMINISTRACIÓN'),
            ('1128844863', 'BIVIAN YISET MOSQUERA PEREA', 'POSCOSECHA'),
            ('30356482', 'MAGOLA PATIÑO ECHEVERRY', 'POSCOSECHA'),
            ('1085816021', 'LEIDY CAROLINA JIMENEZ BERMUDEZ', 'POSCOSECHA'),
            ('1089599713', 'MARIA CAMILA COLORADO LONDOÑO', 'POSCOSECHA'),
            ('1007367459', 'FLOR NORELA VARGAS SERNA', 'POSCOSECHA'),
            ('1004668536', 'LAURA CAMILA ARIAS HERNANDEZ', 'ADMINISTRACIÓN'),
            ('1054926615', 'MARIA PAULA AGUIRRE OCHOA', 'ADMINISTRACIÓN'),
            ('1089598139', 'JHON MICHAEL GOMEZ RESTREPO', 'POSCOSECHA'),
            ('41214603', 'LUZ KARIME CONTRERAS BUITRAGO', 'POSCOSECHA'),
            ('1060270203', 'MARCELA LOPEZ RAMIREZ', 'POSCOSECHA'),
            ('1274327', 'WYNDIMAR YALUZ SANCHEZ HERRERA', 'POSCOSECHA'),
            ('1118287112', 'MARTHA LUCIA LOPEZ ARBOLEDA', 'POSCOSECHA'),
            ('5472144', 'NERYS CAROLINA HERNANDEZ GARCIA', 'POSCOSECHA'),
            ('63530730', 'NORIZA NIÑO PEDRAZA', 'POSCOSECHA'),
            ('1085717082', 'BRAYAN LEANDRO BELTRAN PIEDRAHITA', 'POSCOSECHA'),
            ('1004755939', 'FABIO ANDRES GOMEZ OSPINA', 'ADMINISTRACIÓN')
        ]
        
        cursor.executemany('''
        INSERT OR IGNORE INTO records (id, name, area) VALUES (?, ?, ?)
        ''', records_data)
        
        # Tabla de archivos de asistencia
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS attendance_files (
            file_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            upload_date TEXT,
            attendance_date TEXT
        )
        ''')
        
        # Tabla de resultados emparejados vinculados a archivos de asistencia
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS matched_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER,
            id_number TEXT,
            name TEXT,
            area TEXT,
            check_in TEXT,
            check_out TEXT,
            status TEXT,
            hours_worked TEXT,
            FOREIGN KEY (file_id) REFERENCES attendance_files(file_id)
        )
        ''')
        
        conn.commit()

# Horarios esperados para cada área
expected_schedules = {
    "ASEO": {"check_in": "06:15", "check_out": "15:00"},
    "MANTENIMIENTO": {"check_in": "07:15", "check_out": "17:00"},
    "ADMINISTRACIÓN": {"check_in": "07:45", "check_out": "17:00"},
    "POSCOSECHA": {"check_in": "06:40", "check_out": "15:30"},
    # Puedes agregar más áreas y horarios según sea necesario
}

# Función para procesar el archivo subido
def process_file(file_content, attendance_date, file_name):
    with connect_db() as conn:
        cursor = conn.cursor()
        
        # Insertar el archivo en attendance_files
        cursor.execute('''
        INSERT INTO attendance_files (file_name, upload_date, attendance_date)
        VALUES (?, ?, ?)
        ''', (file_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), attendance_date))
        
        file_id = cursor.lastrowid
        
        # Leer el contenido del archivo
        try:
            content = file_content.decode("utf-8").splitlines()
        except UnicodeDecodeError:
            st.error("Error al decodificar el archivo. Asegúrate de que esté en formato UTF-8.")
            return
        
        user_times = {}

        for line in content:
            fields = line.strip().split('\t')
            if len(fields) >= 2:
                id_number = fields[0].strip()
                date_time = fields[1].strip()
                
                # Filtrar IDs inválidos
                if len(id_number) < 4 or not id_number.isdigit():
                    continue
                
                if id_number not in user_times:
                    user_times[id_number] = []
                user_times[id_number].append(date_time)
        
        # Procesar los tiempos de cada usuario
        for id_number, times in user_times.items():
            cursor.execute("SELECT name, area FROM records WHERE id = ?", (id_number,))
            result = cursor.fetchone()

            if not result and len(id_number) == 9:
                cursor.execute(
                    "SELECT name, area FROM records WHERE id LIKE ?",
                    (f"{id_number}%",)
                )
                result = cursor.fetchone()
            
            if result:
                name, area = result
                times.sort()
                
                check_in = times[0]
                check_out = times[-1] if len(times) > 1 else None

                status = "N/A"
                hours_worked = "N/A"
                if area in expected_schedules:
                    expected_check_in_str = expected_schedules[area]["check_in"]
                    expected_check_in = datetime.strptime(expected_check_in_str, "%H:%M").time()

                    try:
                        actual_check_in_datetime = datetime.strptime(check_in, "%Y-%m-%d %H:%M:%S")
                        actual_check_in_time = actual_check_in_datetime.time()
                        
                        status = "TEMPRANO" if actual_check_in_time <= expected_check_in else "TARDE"
                    except ValueError:
                        status = "Formato de Hora Inválido"
                    
                    if check_out:
                        try:
                            actual_check_out_datetime = datetime.strptime(check_out, "%Y-%m-%d %H:%M:%S")
                            time_difference = actual_check_out_datetime - actual_check_in_datetime
                            total_hours = time_difference.total_seconds() / 3600
                            hours = int(total_hours)
                            minutes = int((total_hours - hours) * 60)
                            hours_worked = f"{hours}h {minutes}m"
                        except ValueError:
                            hours_worked = "Formato de Hora Inválido"
                else:
                    status = "Área no definida"
                    hours_worked = "N/A"

                # Insertar el resultado emparejado
                cursor.execute('''
                INSERT INTO matched_results (
                    file_id, id_number, name, area, check_in, check_out, status, hours_worked
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (file_id, id_number, name, area, check_in, check_out, status, hours_worked))
        
        conn.commit()
        st.success(f"Archivo '{file_name}' procesado y almacenado correctamente.")

# Función para obtener la lista de archivos de asistencia
def get_attendance_files():
    with connect_db() as conn:
        df = pd.read_sql_query("SELECT * FROM attendance_files ORDER BY attendance_date DESC", conn)
    return df

# Función para obtener los resultados emparejados de un archivo específico
def get_matched_results(file_id):
    with connect_db() as conn:
        query = '''
        SELECT id_number AS "ID", name AS "Nombre", area AS "Área",
               check_in AS "Check In", check_out AS "Check Out",
               status AS "Estado", hours_worked AS "Horas Trabajadas"
        FROM matched_results
        WHERE file_id = ?
        '''
        df = pd.read_sql_query(query, conn, params=(file_id,))
    return df

# Función para exportar los resultados emparejados a Excel
def export_to_excel(df, file_name):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados')
    processed_data = output.getvalue()
    return processed_data

# Configuración inicial de la base de datos
create_tables()

# Configuración de la aplicación Streamlit
st.set_page_config(page_title="Administrador de Asistencia", layout="wide")
st.title("Administrador de Asistencia con Base de Datos")

st.markdown("""
Esta aplicación permite subir archivos de asistencia, almacenarlos en una base de datos, 
ver los registros almacenados y exportarlos cuando lo desees.
""")

# Sección para subir un nuevo archivo de asistencia
st.header("Subir Archivo de Asistencia")
with st.form(key='upload_form'):
    uploaded_file = st.file_uploader("Selecciona un archivo de asistencia", type=["dat", "txt"])
    attendance_date = st.date_input("Fecha de Asistencia", datetime.today())
    submit_button = st.form_submit_button("Procesar y Almacenar")

    if submit_button:
        if uploaded_file is not None:
            file_name = uploaded_file.name
            file_content = uploaded_file.read()
            process_file(file_content, attendance_date.strftime("%Y-%m-%d"), file_name)
        else:
            st.error("Por favor, selecciona un archivo para subir.")

st.markdown("---")

# Sección para ver los archivos de asistencia almacenados
st.header("Archivos de Asistencia Almacenados")
attendance_files_df = get_attendance_files()

if not attendance_files_df.empty:
    st.dataframe(attendance_files_df[['file_id', 'file_name', 'attendance_date', 'upload_date']])
    
    # Seleccionar un archivo para ver sus detalles
    selected_file_id = st.selectbox(
        "Selecciona un archivo para ver los detalles", 
        attendance_files_df['file_id'].tolist(),
        format_func=lambda x: f"ID: {x} - {attendance_files_df.loc[attendance_files_df['file_id'] == x, 'file_name'].values[0]} - Fecha: {attendance_files_df.loc[attendance_files_df['file_id'] == x, 'attendance_date'].values[0]}"
    )
    
    if selected_file_id:
        st.subheader("Resultados Emparejados")
        results_df = get_matched_results(selected_file_id)
        if not results_df.empty:
            st.dataframe(results_df)
            
            # Botón para exportar a Excel
            excel_data = export_to_excel(results_df, f"Resultados_{selected_file_id}.xlsx")
            st.download_button(
                label="Descargar Resultados en Excel",
                data=excel_data,
                file_name=f"Resultados_{selected_file_id}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("No hay resultados emparejados para este archivo.")
else:
    st.info("No se han subido archivos de asistencia aún.")

st.markdown("---")

# Opcional: Mostrar todos los resultados emparejados
st.header("Todos los Resultados Emparejados")
with connect_db() as conn:
    query = '''
    SELECT 
        af.file_id AS "ID Archivo",
        af.file_name AS "Nombre Archivo",
        af.attendance_date AS "Fecha Asistencia",
        mr.id_number AS "ID",
        mr.name AS "Nombre",
        mr.area AS "Área",
        mr.check_in AS "Check In",
        mr.check_out AS "Check Out",
        mr.status AS "Estado",
        mr.hours_worked AS "Horas Trabajadas"
    FROM 
        matched_results mr
    JOIN 
        attendance_files af ON mr.file_id = af.file_id
    ORDER BY 
        af.attendance_date DESC
    '''
    all_results_df = pd.read_sql_query(query, conn)

if not all_results_df.empty:
    st.dataframe(all_results_df)
    
    # Botón para exportar todos los resultados a Excel
    excel_all = export_to_excel(all_results_df, "Todos_Los_Resultados.xlsx")
    st.download_button(
        label="Descargar Todos los Resultados en Excel",
        data=excel_all,
        file_name="Todos_Los_Resultados.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("No hay resultados emparejados para mostrar.")
