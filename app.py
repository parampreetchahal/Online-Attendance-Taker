from flask import Flask, request, render_template, send_file
import pandas as pd
from sqlalchemy import create_engine
import joblib
import os
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.platypus import Table, TableStyle
import io

app = Flask(__name__)

def process_csv(file, valid_minutes, format):
    df = pd.read_csv(file)
    df.columns = df.columns.str.strip()
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%m/%d/%Y, %I:%M:%S %p')
    df_joined = df[df['User Action'] == 'Joined'].rename(columns={'Timestamp': 'Joined Timestamp'})
    df_left = df[df['User Action'] == 'Left'].rename(columns={'Timestamp': 'Left Timestamp'})
    df_combined = pd.merge(df_joined[['Full Name', 'Joined Timestamp']],
                           df_left[['Full Name', 'Left Timestamp']],
                           on='Full Name', how='outer')

    def calculate_total_meeting_time(df_group):
        df_sorted = df_group.sort_values(by='Joined Timestamp')
        total_time = pd.Timedelta(0)
        current_start = None
        current_end = None

        for _, row in df_sorted.iterrows():
            if current_start is None:
                current_start = row['Joined Timestamp']
                current_end = row['Left Timestamp']
            elif row['Joined Timestamp'] <= current_end:
                current_end = max(current_end, row['Left Timestamp'])
            else:
                total_time += current_end - current_start
                current_start = row['Joined Timestamp']
                current_end = row['Left Timestamp']

        if current_start is not None:
            total_time += current_end - current_start

        return round(total_time.total_seconds() / 60, 1)

    df_total_time = df_combined.groupby('Full Name').apply(calculate_total_meeting_time).reset_index()
    df_total_time.columns = ['Full Name', 'Total Meeting Duration']
    df_filtered = df_total_time[df_total_time['Total Meeting Duration'] >= valid_minutes]

    def fetch_section_and_roll(full_names, db_connection_string):
        engine = create_engine(db_connection_string)
        names_tuple = tuple(full_names)
        query = f"""
        SELECT name, section, rollno
        FROM record
        WHERE name IN {names_tuple}
        """
        section_roll_df = pd.read_sql(query, engine)
        return section_roll_df

    db_connection_string = 'mysql+pymysql://root:@127.0.0.1:3306/collegeid'
    full_names = df_filtered['Full Name'].tolist()
    section_roll_df = fetch_section_and_roll(full_names, db_connection_string)
    df_final = pd.merge(df_filtered, section_roll_df, left_on='Full Name', right_on='name', how='left')
    df_final = df_final[['Full Name', 'Total Meeting Duration', 'section', 'rollno']].sort_values(by=['section', 'rollno'])

    if format == 'pdf':
        output_file_path = 'final.pdf'
        generate_pdf(df_final, output_file_path)
    elif format == 'excel':
        output_file_path = 'final.xlsx'
        generate_excel(df_final, output_file_path)

    joblib.dump(df_final, 'final.pkl')
    return output_file_path

def generate_pdf(df, output_file_path):
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    # Draw title
    title = "Attendance Sheet"
    c.setFont("Helvetica-Bold", 18)
    c.drawCentredString(width / 2.0, height - 40, title)

    # Create table data
    data = [['Full Name', 'Total Time (Minutes)', 'Section', 'Roll No']] + df.values.tolist()

    # Create Table
    table = Table(data, colWidths=[200, 100, 100, 100])
    style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ])
    table.setStyle(style)

    # Add table to canvas
    table.wrapOn(c, width, height)
    table.drawOn(c, 50, height - 220)  # Adjust vertical position for title visibility

    c.save()
    buffer.seek(0)

    with open(output_file_path, 'wb') as f:
        f.write(buffer.getbuffer())

def generate_excel(df, output_file_path):
    df.to_excel(output_file_path, index=False)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return 'No file part'
    file = request.files['file']
    if file.filename == '':
        return 'No selected file'
    if file:
        valid_minutes = int(request.form['valid_minutes'])
        format = request.form['format']
        output_file_path = process_csv(file, valid_minutes, format)
        return send_file(output_file_path, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
