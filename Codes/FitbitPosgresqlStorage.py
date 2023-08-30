import requests
from flask import Flask, request, redirect

app = Flask(__name__)

# Fitbit application credentials
CLIENT_ID = '23R5GJ'
CLIENT_SECRET = '22e8c4628029beeb4ae388ed87f6f5f5'
CALLBACK_URL = 'http://localhost:5000/callback'  # Update with your actual callback URL

#PostgreSQL database credentials
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'test'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'

# Fitbit API endpoints
AUTHORIZATION_URL = 'https://www.fitbit.com/oauth2/authorize'
TOKEN_URL = 'https://api.fitbit.com/oauth2/token'
API_URL = 'https://api.fitbit.com/1/user/-/activities/steps/date/today/1w.json'

# PostgreSQL connection
import psycopg2

def create_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# @app.route('/')
def home():
    # Redirect to Fitbits authorization page
    return redirect(f'{AUTHORIZATION_URL}?response_type=code&client_id={CLIENT_ID}&redirect_uri={CALLBACK_URL}&scope=activity')

# @app.route('/callback')
def callback():
    # Handle callback from Fitbit
    code = request.args.get('code')

    # # Exchange authorization code for access token
    # response = requests.post(TOKEN_URL, data={
    #     'code': code,
    #     'client_id': CLIENT_ID,
    #     'client_secret': CLIENT_SECRET,
    #     'redirect_uri': CALLBACK_URL,
    #     'grant_type': 'authorization_code'
    # })

    # data = response.json()
    # access_token = data['access_token']

    # # Retrieve Fitbit data using access token
    # headers = {'Authorization': f'Bearer {access_token}'}
    # response = requests.get(API_URL, headers=headers)
    # fitbit_data = response.json()

    #creer un tableau ici, et simuler à l'intérieur, puis commenter tout ce qui précède.
    fitbit_data = {
        'activities-steps': [
            ('2023-08-01', 5000),
            ('2023-08-02', 7500),
            # ... autres données ...
        ]
    }
    # Store Fitbit data in PostgreSQL database
    connection = create_db_connection()
    cursor = connection.cursor()

    for date, value in fitbit_data['activities-steps']:
        query = "INSERT INTO fitbit_steps (date, steps) VALUES (%s, %s)"
        cursor.execute(query, (date, value))

    connection.commit()
    cursor.close()
    connection.close()

    return 'Fitbit data stored successfully!'

if __name__ == '__main__':
    app.run(debug=True)