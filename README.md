# DreamFit

### Prerequisites
- Python 3.x installed
- Pip for installing Python packages

### Installation Steps
1. Clone the repository or download the source code where the Python script is located.
2. Navigate to the project directory in your terminal or command prompt.
3. Install required dependencies by running:
    ```
    pip install -r requirements.txt
    ```
    Ensure `requirements.txt` includes `requests`, `tqdm`, `pandas` and `python-dotenv`.

4. Set up your `.env` file in the root of your project directory with the following content, replacing the placeholders with your actual Fitbit API credentials:

    - (I) Log in to development Fitbit web. [Fitbit Web](https://dev.fitbit.com/login)
    - (II) Create an APP with dummy data. The only requirement is that you insert “http://localhost:8000” for the “Callback URL” field
    - (III) Retrieve the data from the newly created APP
    - (IV) Repeat the process for the second email account

    Once those steps have been completed, you can go on and set the values in a .env file inside the root, next to the .env.example provided.
   
    ```
        CLIENT_EMAIL_1=<Client Email for the first account>
        CLIENT_ID_1=<OAuth 2.0 Client ID for the first email FITBIT registered APP.>
        CLIENT_SECRET_1=<Client Secret for the first email FITBIT registered APP.>

        CLIENT_EMAIL_2=<Client Email for the second account>
        CLIENT_ID_2=<OAuth 2.0 Client ID for the second email FITBIT registered APP.>
        CLIENT_SECRET_2=<Client Secret for the second email FITBIT registered APP.>

        REDIRECT_URI_1=http://localhost:8000 
        PORT=8000
    ```

### Running the Script
1. Start the script by running:
    ```
    python main.py
    ```
2. Authenticate with Fitbit when the web browser opens and redirects you to the Fitbit login page. Log in and authorize the application to access your Fitbit data.
3. Wait for the script to retrieve and format your Fitbit data. It will save the data in JSON, CSV, and TXT formats in a newly created data directory.
4. Check the data directory for your Fitbit data files ( `csvs`, `fitbit_data.json`, `fitbit_data.txt`) inside csvs you can find the data separated in several csv files.

### Viewing Documentation
The project documentation can be generated and viewed using Sphinx. To generate the documentation, navigate to the `docs` (cd docs) directory and run:
    ```
    make html
    ```
After running this command, the documentation will be generated in the `docs/build/html` directory. You can open the `index.html` file in your web browser to view the documentation.

Alternatively, you can run the script with an argument to automatically open the documentation in your default web browser:
    ```
    python main.py --docs
    ```

### Troubleshooting
- If you encounter any authentication errors, double-check your `.env` file for the correct API credentials and redirect URL.
- Ensure your Fitbit app's redirect URI matches the one in your `.env` file and is allowed in your Fitbit app settings on the Fitbit website.

### Notes
- The script logs its operations, so you can check `records.log` for detailed execution logs.
- You can modify the script to change the data directory or to retrieve different data types from Fitbit.