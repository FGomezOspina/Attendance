import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime
import os
import sys
import io

# **1. Configuración de la Página (Debe ser la Primera Llamada a Streamlit)**
st.set_page_config(page_title="Administrador de Asistencia", layout="wide")
st.title("Administrador de Asistencia con Base de Datos")

# **2. Función para Obtener la Ruta de Recursos (útil para PyInstaller)**
def resource_path(relative_path):
    """Obtiene la ruta absoluta al recurso, funciona en desarrollo y PyInstaller"""
    try:
        # PyInstaller crea una carpeta temporal y almacena la ruta en _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# **3. Función para Conectar a la Base de Datos**
def connect_db():
    db_path = resource_path("identification_records.db")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    return conn

# **4. Función para Crear las Tablas en la Base de Datos con Migración**
def create_tables():
    migration_done = False  # Variable para indicar si se realizó una migración
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
            # ... (otros registros)
            ('1004520909','ANGIE VANESSA ARCE LOPEZ','POSCOSECHA'),
            ('94509907','JULIÁN ANDRÉS PÉREZ BETANCUR','ADMINISTRACIÓN'),
            ('22131943','ROSA AMELIA SOLORZANO SAMPEDRO','ASEO'),
            ('1089747022','JUAN ESTEBAN TORO VARGAS','MANTENIMIENTO'),
            ('1004733786','STEVEN ARCE VALLE','POSCOSECHA'),
            ('1004756825','LAURA RAMIREZ QUINTERO','ADMINISTRACIÓN'),
            ('1088305975','PAOLA ANDREA OSORIO GRISALES','POSCOSECHA'),
            ('1004751727',"LAURA VANESSA LONDOÑO SILVA","POSCOSECHA"),
            ('1088256932',"YEISON LEANDRO ARIAS MONTES","OPERACIONES"),
            ('1088346284',"NATALIA VALENCIA CORTES","ADMINISTRACIÓN"),
            ('1117964380',"YAMILETH HILARION OLAYA","POSCOSECHA"),
            ('5760588',"JOSE AUGUSTO CORDOVEZ CHARAMA","POSCOSECHA"),
            ('1112770658',"LUIS ALBERTO OROZCO RODRIGUEZ","POSCOSECHA"),
            ('1110480331',"CLAUDIA YULIMA CUTIVA BETANCOURTH","POSCOSECHA"),
            ('25038229',"YORME DE JESUS LOPEZ IBARRA","POSCOSECHA"),
            ('1128844585',"YENIFFER MOSQUERA PEREA","POSCOSECHA"),
            ('1089930892',"ALEXANDRA RIOS BUENO","POSCOSECHA"),
            ('1088295574',"ANDRES FELIPE BEDOYA ROJAS","POSCOSECHA"),
            ('1093984174',"CARLOS ANDRES SANCHEZ QUEBRADA","POSCOSECHA"),
            ('1088316215',"SERGIO MUÑOZ RAMIREZ","ADMINISTRACIÓN"),
            ('1128844863',"BIVIAN YISET MOSQUERA PEREA","POSCOSECHA"),
            ('30356482',"MAGOLA PATIÑO ECHEVERRY","POSCOSECHA"),
            ('1085816021',"LEIDY CAROLINA JIMENEZ BERMUDEZ","POSCOSECHA"),
            ('1089599713',"MARIA CAMILA COLORADO LONDOÑO","POSCOSECHA"),
            ('1007367459',"FLOR NORELA VARGAS SERNA","POSCOSECHA"),
            ('1004668536',"LAURA CAMILA ARIAS HERNANDEZ","ADMINISTRACIÓN"),
            ('1054926615',"MARIA PAULA AGUIRRE OCHOA","ADMINISTRACIÓN"),
            ('1060270203',"MARCELA LOPEZ RAMIREZ","POSCOSECHA"),
            ('1274327',"WYNDIMAR YALUZ SANCHEZ HERRERA","POSCOSECHA"),
            ('1118287112',"MARTHA LUCIA LOPEZ ARBOLEDA","POSCOSECHA"),
            ('5472144',"NERYS CAROLINA HERNANDEZ GARCIA","POSCOSECHA"),
            ('63530730',"NORIZA NIÑO PEDRAZA","POSCOSECHA"),
            ('1004755939',"FABIO ANDRES GOMEZ OSPINA","ADMINISTRACIÓN"),
            ('1089601326',"LEIDY LAURA ESPINOZA OSPINA","POSCOSECHA"),
            ('1007554110',"ANGIE PAOLA OCAMPO HENAO","POSCOSECHA"),
            ('1032936469',"DANA CAROLINA SUAREZ GALEANO","POSCOSECHA"),
            ('1090332929',"MAIKOL JUNIOR CHIQUITO MONTOYA","POSCOSECHA"),
            ('42146393',"ANGELA MARIA ALARCON ESCOBAR","POSCOSECHA"),
            ('42146393', 'ANGELA MARIA ALARCON ESCOBAR', 'POSCOSECHA'),
            ('1007745486', 'ALEXANDRA CUELLAR ARTUNDUAGA', 'POSCOSECHA'),
            ('6060045', 'OSCARIANI DEL CARMEN AMARISTA GUZMAN', 'POSCOSECHA'),
            ('1088352316', 'ANGIE KATHERINE VALENCIA HEREDIA', 'POSCOSECHA'),
            ('1088282768', 'ELIANA VALENCIA GARCIA', 'ADMINISTRACIÓN'),
            ('1143384637', 'KELLY JOHANA DELGADO CASTILLO', 'ADMINISTRACIÓN'),
            ('1089930256', 'PAULINA GUERRERO CARVAJAL', 'ADMINISTRACIÓN'),
            ('1060010197', 'JUAN PABLO OSPINA VILLADA', 'POSCOSECHA'),
            ('1088034548', 'YURI LORENA GIRALDO BERRIO', 'POSCOSECHA'),
            ('1193263534','KAREN DAHIANA BERMUDEZ','POSCOSECHA'),
            ('1088325129','ROSA MARIA LOZANO TORRES','POSCOSECHA'),
            ('1004683651','SEBASTIAN OROZCO ECHEVERRY','POSCOSECHA'),
            ('6620175', 'JESUS DANIEL AMATIMA ASOCAR', 'POSCOSECHA'),
            ('1088029552', 'JAMES MORALES AGUADO', 'POSCOSECHA'),
            ('1090338866', 'YULIETH LADINO SUAREZ', 'POSCOSECHA'),
            ('1004686441', 'ANA YASMIN VELEZ GARCIA', 'POSCOSECHA'),
            ('1087560062', 'KELLY JOHANA LOPEZ GONZALEZ', 'POSCOSECHA'),
            ('1005021274', 'MANUELA HOLGUIN ARANGO', 'POSCOSECHA'),
            ('1088353499', 'MARIA ESMERALDA PAVAS BATERO', 'POSCOSECHA'),
            ('1059698941', 'JENIFER BAÑOL PESCADORS', 'POSCOSECHA')
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
            attendance_date TEXT,  -- Añadido campo de fecha
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
        
        # **Migración: Añadir 'attendance_date' si no existe**
        cursor.execute("PRAGMA table_info(matched_results)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'attendance_date' not in columns:
            cursor.execute("ALTER TABLE matched_results ADD COLUMN attendance_date TEXT")
            migration_done = True
        else:
            migration_done = False
        
        conn.commit()
    
    return migration_done

# **5. Horarios Esperados para Cada Área**
expected_schedules = {
    "ASEO": {"check_in": "06:15", "check_out": "15:00"},
    "MANTENIMIENTO": {"check_in": "07:15", "check_out": "17:00"},
    "ADMINISTRACIÓN": {"check_in": "07:45", "check_out": "17:00"},
    "POSCOSECHA": {"check_in": "06:40", "check_out": "15:30"},
    # Puedes agregar más áreas y horarios según sea necesario
}

# **7. Función para Procesar el Archivo Subido**
def process_file(file_content, attendance_date, file_name):
    with connect_db() as conn:
        cursor = conn.cursor()
        
        # Insertar el archivo en attendance_files
        cursor.execute('''
        INSERT INTO attendance_files (file_name, upload_date, attendance_date)
        VALUES (?, ?, ?)
        ''', (file_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), attendance_date))
        
        file_id = cursor.lastrowid
        
        # Determinar el tipo de archivo y extraer el contenido de texto
        if file_name.lower().endswith(('.dat', '.txt')):
            try:
                content = file_content.decode("utf-8").splitlines()
            except UnicodeDecodeError:
                st.error("Error al decodificar el archivo. Asegúrate de que esté en formato UTF-8.")
                return
        else:
            st.error("Formato de archivo no soportado. Por favor, sube un archivo .dat o .txt.")
            return  # Mover el return dentro del bloque else
        
        user_times = {}
        
        for line in content:
            fields = line.strip().split('\t')
            if len(fields) >= 2:
                id_number = fields[0].strip()
                date_time_str = fields[1].strip()
                
                # Filtrar IDs inválidos
                if len(id_number) < 4 or not id_number.isdigit():
                    continue
                
                # Parsear el datetime
                try:
                    date_time = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")
                    date_str = date_time.strftime("%Y-%m-%d")
                except ValueError:
                    st.warning(f"Formato de fecha y hora inválido para el ID {id_number}: {date_time_str}")
                    continue
                
                if id_number not in user_times:
                    user_times[id_number] = {}
                if date_str not in user_times[id_number]:
                    user_times[id_number][date_str] = []
                user_times[id_number][date_str].append(date_time)
        
        # Procesar los tiempos de cada usuario por día
        for id_number, dates in user_times.items():
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
                for date_str, times in dates.items():
                    times.sort()
                    
                    check_in_datetime = times[0]
                    check_out_datetime = times[-1] if len(times) > 1 else None
                    
                    check_in = check_in_datetime.strftime("%Y-%m-%d %H:%M:%S")
                    check_out = check_out_datetime.strftime("%Y-%m-%d %H:%M:%S") if check_out_datetime else None

                    status = "N/A"
                    hours_worked = "N/A"
                    if area in expected_schedules:
                        expected_check_in_str = expected_schedules[area]["check_in"]
                        expected_check_in = datetime.strptime(expected_check_in_str, "%H:%M").time()

                        try:
                            actual_check_in_time = check_in_datetime.time()
                            
                            status = "TEMPRANO" if actual_check_in_time <= expected_check_in else "TARDE"
                        except ValueError:
                            status = "Formato de Hora Inválido"
                        
                        if check_out_datetime:
                            try:
                                time_difference = check_out_datetime - check_in_datetime
                                total_hours = time_difference.total_seconds() / 3600
                                hours = int(total_hours)
                                minutes = int((total_hours - hours) * 60)
                                hours_worked = f"{hours}h {minutes}m"
                            except ValueError:
                                hours_worked = "Formato de Hora Inválido"
                    else:
                        status = "Área no definida"
                        hours_worked = "N/A"

                    # Insertar el resultado emparejado con la fecha
                    cursor.execute('''
                    INSERT INTO matched_results (
                        file_id, attendance_date, id_number, name, area, check_in, check_out, status, hours_worked
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (file_id, date_str, id_number, name, area, check_in, check_out, status, hours_worked))
        
        conn.commit()
        st.success(f"Archivo '{file_name}' procesado y almacenado correctamente.")

# **8. Función para Obtener la Lista de Archivos de Asistencia**
def get_attendance_files():
    with connect_db() as conn:
        df = pd.read_sql_query("SELECT * FROM attendance_files ORDER BY upload_date DESC", conn)
    return df

# **9. Función para Obtener los Resultados Emparejados de un Archivo Específico**
def get_matched_results(file_id):
    with connect_db() as conn:
        query = '''
        SELECT 
            mr.attendance_date AS "Fecha",
            mr.id_number AS "ID", 
            mr.name AS "Nombre", 
            mr.area AS "Área",
            mr.check_in AS "Check In", 
            mr.check_out AS "Check Out",
            mr.status AS "Estado", 
            mr.hours_worked AS "Horas Trabajadas"
        FROM matched_results mr
        WHERE mr.file_id = ?
        ORDER BY mr.attendance_date, mr.id_number
        '''
        df = pd.read_sql_query(query, conn, params=(file_id,))
    return df

# **10. Función para Exportar los Resultados Emparejados a Excel**
def export_to_excel(df, file_name):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados')
    processed_data = output.getvalue()
    return processed_data

# **11. Crear las Tablas y Realizar la Migración si es Necesario**
migration_done = create_tables()

# **12. Mostrar Mensaje de Migración si se Realizó**
if migration_done:
    st.info("Se ha añadido la columna 'attendance_date' a la tabla 'matched_results'.")


# **14. Sección para Subir un Nuevo Archivo de Asistencia**
st.header("Subir Archivo de Asistencia")
with st.form(key='upload_form'):
    uploaded_file = st.file_uploader("Selecciona un archivo de asistencia", type=["dat", "txt"])  # Añadir 'docx' a los tipos permitidos
    # El campo de fecha ahora puede representar la semana o puede omitirse si se usa la fecha de cada registro
    # Para mantener compatibilidad, lo mantenemos pero puede ser opcional
    attendance_week = st.date_input("Fecha de Asistencia (Semana)", datetime.today())
    submit_button = st.form_submit_button("Procesar y Almacenar")

    if submit_button:
        if uploaded_file is not None:
            file_name = uploaded_file.name
            file_content = uploaded_file.read()
            process_file(file_content, attendance_week.strftime("%Y-%m-%d"), file_name)
        else:
            st.error("Por favor, selecciona un archivo para subir.")

st.markdown("---")

# **15. Sección para Ver los Archivos de Asistencia Almacenados**
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

# **16. Opcional: Mostrar Todos los Resultados Emparejados**
st.header("Todos los Resultados Emparejados")
with connect_db() as conn:
    query = '''
    SELECT 
        mr.attendance_date AS "Fecha",
        af.file_id AS "ID Archivo",
        af.file_name AS "Nombre Archivo",
        af.attendance_date AS "Fecha de Asistencia del Archivo",
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
        mr.attendance_date DESC, af.upload_date DESC
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
