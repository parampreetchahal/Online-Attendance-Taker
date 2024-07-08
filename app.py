from flask import Flask, request, render_template, send_file
import pandas as pd
from sqlalchemy import create_engine
import joblib
import os

app = Flask(__name__)


def process_csv(file):
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

        return total_time.total_seconds() / 60

    df_total_time = df_combined.groupby('Full Name').apply(calculate_total_meeting_time).reset_index()
    df_total_time.columns = ['Full Name', 'Total Meeting Duration']
    df_filtered = df_total_time[df_total_time['Total Meeting Duration'] >= 50]

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
    df_final = pd.merge(df_filtered[['Full Name']], section_roll_df, left_on='Full Name', right_on='name', how='left')
    output_file_path = 'final.csv'
    df_final[['Full Name', 'section', 'rollno']].to_csv(output_file_path, index=False)
    joblib.dump(df_final, 'final.pkl')
    return output_file_path


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
        output_file_path = process_csv(file)
        return send_file(output_file_path, as_attachment=True)


if __name__ == '__main__':
    app.run(debug=True)
