import sqlite3
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk
import sys
import os
from datetime import datetime

# Function to get the resource path, useful for PyInstaller
def resource_path(relative_path):
    """ Get the absolute path to the resource, works for dev and PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# Function to create the database and insert initial records
def create_database():
    # Use resource_path to ensure correct path after packaging
    db_path = resource_path("identification_records.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table for storing identification numbers, names, and areas
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS records (
        id TEXT PRIMARY KEY,
        name TEXT,
        area TEXT
    )
    ''')
    
    # Provided data for insertion (now including area)
    records_data = [
        ('1088327667', 'ERIKA TATIANA GALLEGO VINASCO', 'POSCOSECHA'),
        ('1004520909', 'ANGIE VANESSA ARCE LOPEZ', 'POSCOSECHA'),
        ('94509907', 'JULIÁN ANDRÉS PÉREZ BETANCUR', 'ADMINISTRACIÓN'),
        ('22131943', 'ROSA AMELIA SOLORZANO SAMPEDRO', 'ASEO'),
        ('1089747022', 'JUAN ESTEBAN TORO VARGAS', 'CONSTRUCCION'),
        ('1004733786', 'STEVEN ARCE VALLE', 'POSCOSECHA'),
        ('1004756825', 'LAURA RAMIREZ QUINTERO', 'ADMINISTRACIÓN'),
        ('1088305975', 'PAOLA ANDREA OSORIO GRISALES', 'POSCOSECHA'),
        ('1004751727', 'LAURA VANESSA LONDOÑO SILVA', 'POSCOSECHA'),
        ('1088256932', 'YEISON LEANDRO ARIAS MONTES', 'OPERACIONES'),
        ('1088346284', 'NATALIA VALENCIA CORTES', 'ADMINISTRACIÓN'),
        ('1117964380', 'YAMILETH HILARION OLAYA', 'POSCOSECHA'),
        ('PPT 5760588', 'JOSE AUGUSTO CORDOVEZ CHARAMA', 'POSCOSECHA'),
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
    
    # Insert data into the table
    cursor.executemany('''
    INSERT OR IGNORE INTO records (id, name, area) VALUES (?, ?, ?)
    ''', records_data)
    
    # Create table for matched results with area, status, and hours_worked
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS matched_results (
        id TEXT,
        name TEXT,
        area TEXT,
        check_in TEXT,
        check_out TEXT,
        status TEXT,
        hours_worked TEXT,
        PRIMARY KEY (id)
    )
    ''')
    
    conn.commit()
    conn.close()

# Expected schedules for each area
expected_schedules = {
    "ASEO": {"check_in": "06:15", "check_out": "15:00"},
    "MANTENIMIENTO": {"check_in": "07:15", "check_out": "17:00"},
    "ADMINISTRACIÓN": {"check_in": "07:45", "check_out": "17:00"},
    "POSCOSECHA": {"check_in": "06:40", "check_out": "15:30"},
    # You can add more areas and schedules as needed
}

# Function to process the selected file and perform ID matching
def process_file(file_path):
    # Use resource_path to ensure correct path after packaging
    db_path = resource_path("identification_records.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    with open(file_path, "r", encoding="utf-8", errors="ignore") as file:
        content = file.readlines()
        
        # Temporary storage for all times a user appears
        user_times = {}

        for line in content:
            fields = line.strip().split('\t')
            if len(fields) >= 2:
                id_number = fields[0].strip()
                date_time = fields[1].strip()
                
                # Skip IDs with less than 4 digits or that are not fully numeric
                if len(id_number) < 4 or not id_number.isdigit():
                    continue
                
                # Store the times for each user
                if id_number not in user_times:
                    user_times[id_number] = []
                user_times[id_number].append(date_time)
        
        # Process each user's times to determine check_in and check_out
        for id_number, times in user_times.items():
            # Get the user's name and area from the database
            cursor.execute("SELECT name, area FROM records WHERE id = ?", (id_number,))
            result = cursor.fetchone()

            # If not found directly, try using a prefix for a 9-digit ID
            if not result and len(id_number) == 9:
                cursor.execute(
                    "SELECT name, area FROM records WHERE id LIKE ?",
                    (f"{id_number}%",)
                )
                result = cursor.fetchone()
            
            if result:
                name, area = result
                times.sort()  # Sort the times to ensure chronological order
                
                # Determine check_in and check_out based on the number of appearances
                check_in = times[0]  # First appearance
                check_out = times[-1] if len(times) > 1 else None  # Last appearance or None

                # Determine status based on expected schedule
                status = "N/A"
                hours_worked = "N/A"
                if area in expected_schedules:
                    expected_check_in_str = expected_schedules[area]["check_in"]
                    expected_check_in = datetime.strptime(expected_check_in_str, "%H:%M").time()
                    
                    # Parse the actual check-in time
                    try:
                        actual_check_in_datetime = datetime.strptime(check_in, "%Y-%m-%d %H:%M:%S")
                        actual_check_in_time = actual_check_in_datetime.time()
                        
                        # Compare times
                        if actual_check_in_time <= expected_check_in:
                            status = "TEMPRANO"
                        else:
                            status = "TARDE"
                    except ValueError:
                        # Handle parsing errors
                        status = "Invalid Time Format"

                    # Calculate hours worked if check_out is available
                    if check_out and check_out != "N/A":
                        try:
                            actual_check_out_datetime = datetime.strptime(check_out, "%Y-%m-%d %H:%M:%S")
                            # Calculate the difference
                            time_difference = actual_check_out_datetime - actual_check_in_datetime
                            # Convert to total hours worked
                            total_hours = time_difference.total_seconds() / 3600
                            # Format to hours and minutes
                            hours = int(total_hours)
                            minutes = int((total_hours - hours) * 60)
                            hours_worked = f"{hours}h {minutes}m"
                        except ValueError:
                            hours_worked = "Invalid Time Format"
                    else:
                        hours_worked = "N/A"
                else:
                    status = "Área no definida"
                    hours_worked = "N/A"

                # Insert or update the result in matched_results
                cursor.execute('''
                INSERT OR REPLACE INTO matched_results (id, name, area, check_in, check_out, status, hours_worked)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (id_number, name, area, check_in, check_out, status, hours_worked))
    
    # Commit changes and close the connection
    conn.commit()

    # Fetch all matched results
    cursor.execute("SELECT * FROM matched_results")
    matched_results = cursor.fetchall()
    conn.close()

    # Display the results in the table
    display_results(matched_results)

# Function to display results in a table
def display_results(matched_results):
    # Clear previous rows
    for row in result_table.get_children():
        result_table.delete(row)
    
    # Insert new rows into the table, replacing None with "N/A"
    for result in matched_results:
        id_number, name, area, check_in, check_out, status, hours_worked = result
        check_out = check_out if check_out is not None else "N/A"
        result_table.insert('', 'end', values=(id_number, name, area, check_in, check_out, status, hours_worked))

# Function to select a file
def select_file():
    file_path = filedialog.askopenfilename(
        title="Select a file",
        filetypes=[("DAT files", "*.dat"), ("All files", "*.*")]
    )
    if file_path:
        process_file(file_path)

# Function to export data to Excel
def export_to_excel():
    # Prompt the user to select a file location
    file_path = filedialog.asksaveasfilename(
        defaultextension=".xlsx",
        filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
        title="Save as"
    )
    
    if file_path:
        try:
            # Use resource_path to ensure correct path after packaging
            db_path = resource_path("identification_records.db")
            # Connect to the database and fetch the data
            conn = sqlite3.connect(db_path)
            df = pd.read_sql_query("SELECT * FROM matched_results", conn)
            conn.close()
            
            # Replace None values with "N/A" in the DataFrame
            df['check_out'] = df['check_out'].fillna("N/A")
            
            # Write the DataFrame to an Excel file
            df.to_excel(file_path, index=False)
            messagebox.showinfo("Success", "Data exported successfully!")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to export data: {e}")

# Set up the main window
root = tk.Tk()
root.title("ID Matcher")
root.geometry("1100x500")

# Button to load the file
load_button = tk.Button(root, text="Select File", command=select_file)
load_button.pack(pady=10)

# Button to export the results to Excel
export_button = tk.Button(root, text="Export to Excel", command=export_to_excel)
export_button.pack(pady=10)

# Table to display matched results, now including area, status, and hours_worked
columns = ("ID", "Name", "Area", "Check In", "Check Out", "Estado", "Horas Trabajadas")
result_table = ttk.Treeview(root, columns=columns, show="headings")
for col in columns:
    result_table.heading(col, text=col)
    result_table.column(col, width=150)
result_table.pack(fill="both", expand=True)

# Initialize the database
create_database()

# Start the Tkinter event loop
root.mainloop()