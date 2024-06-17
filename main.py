import argparse
from datetime import datetime, timedelta
import os
import logging
import sys
import json
from dotenv import load_dotenv
from Auth import FitbitAuth
from DataFormatting import DataExtractor
from DataRetrieval import FitbitDataRetriever
from DataStorage import DataStorage
from RateLimiter import RateLimitManager
import webbrowser

def check_dependencies():
        """
        Checks for required dependencies and ensures they are installed.

        Exits the program if any required dependency is missing or has an incorrect version.
        """
        required_packages = {
            'requests': None,
            'pandas': '1.1.5',
            'tqdm': None
        }
        issues = False
        for package, version in required_packages.items():
            try:
                mod = __import__(package)
                if version:
                    installed_version = mod.__version__
                    if installed_version < version:
                        print(f"Warning: {package} version {version} required, but {installed_version} is installed.")
                        issues = True
            except ImportError:
                print(f"Error: Required package {package} is not installed.")
                issues = True

        if issues:
            print("Resolve the issues before running the application.")
            print("Run: pip install -r requirements.txt to install all required packages. ")
            sys.exit(1)
        else:
            print("All dependencies are satisfied.")
  
class FitbitDataApplication(RateLimitManager):
    """
    Main application class for Fitbit data retrieval and processing.
    """
    def __init__(self):
        """
        Initializes the application, sets up logging, loads environment variables, and prompts user for credentials.
        """
        self.device_id = None
        self.logger = self.init_logger()
        load_dotenv()
        self.redirect_uri = os.getenv("REDIRECT_URI") or input("Enter FitBit App Redirect URL (e.g., http://localhost:8000): ")
        self.port = int(os.getenv("PORT") or input("Enter the App Redirect URL Port (e.g., 8000): "))
        self.access_token = None
        self.user_id = None
        self.user_dir = None
        self.data_storage = None
        self.data_retriever = None
        self.max_days = 30
      
    @staticmethod    
    def init_logger():
      """
      Initializes and configures the logger.

      Returns:
          logger (logging.Logger): Configured logger instance.
      """
      logger = logging.getLogger(__name__)
      logger.setLevel(logging.DEBUG)
      
      file_handler = logging.FileHandler('records.log')
      file_handler.setLevel(logging.DEBUG)
      formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
      file_handler.setFormatter(formatter)
      logger.addHandler(file_handler)
      
      console_handler = logging.StreamHandler()
      console_handler.setLevel(logging.INFO)
      simple_formatter = logging.Formatter('%(message)s')
      console_handler.setFormatter(simple_formatter)
      logger.addHandler(console_handler)
      
      return logger
  
    def list_and_select_dataset(self):
      """
      Lists available datasets and prompts the user to select one.

      Returns:
          str: Path to the selected dataset or None if no datasets are available.
      """
      user_data_dir = os.path.join('data', self.user_dir)
      if not os.path.exists(user_data_dir):
          self.logger.info("No datasets available.")
          return None

      datasets = [os.path.join(user_data_dir, entry) for entry in os.listdir(user_data_dir) if os.path.isdir(os.path.join(user_data_dir, entry))]
      
      for index, dataset in enumerate(datasets, start=1):
          print(f"[{index}] {dataset}")

      choice = input("Select a dataset number to process or 'exit' to quit: ")
      
      if choice.isdigit() and 1 <= int(choice) <= len(datasets):
          return datasets[int(choice) - 1]
      elif choice.lower() == 'exit':
          sys.exit(0)
      else:
          self.logger.info("Invalid choice. Please try again.")
          return self.list_and_select_dataset()

    def choose_credentials(self):
        """
        Prompts the user to choose Fitbit account credentials.
        """
        email_1 = os.getenv("CLIENT_EMAIL_1")
        email_2 = os.getenv("CLIENT_EMAIL_2")
        
        print("Select the Fitbit account to use:")
        print(f"[1] {email_1}")
        print(f"[2] {email_2}")
        choice = input("Please select the account by entering the number or 'exit' to cancel: ").strip()
        if choice == '1':
            self.client_id = os.getenv("CLIENT_ID_1")
            self.client_secret = os.getenv("CLIENT_SECRET_1")
            self.user_dir = email_1.split("@")[0]
        elif choice == '2':
            self.client_id = os.getenv("CLIENT_ID_2")
            self.client_secret = os.getenv("CLIENT_SECRET_2")
            self.user_dir = email_2.split("@")[0]
        elif choice == 'exit' or choice == 'EXIT':
              print("Exiting...")
              sys.exit(1)
        else:
            print("Unknown choice.")
            self.choose_credentials()
    
    def authorize(self):
        """
        Handles the Fitbit authorization process and obtains access token and user ID.
        """
        fitbit_auth = FitbitAuth(self.client_id, self.client_secret, self.redirect_uri, self.logger, self.port)
        email = os.getenv("CLIENT_EMAIL_1") if self.client_id == os.getenv("CLIENT_ID_1") else os.getenv("CLIENT_EMAIL_2")
        token_data = fitbit_auth.load_token(email)

        def set_tokens_and_device(tokens):
            self.access_token = tokens['access_token']
            self.user_id = tokens['user_id']
            self.data_retriever = FitbitDataRetriever(self.access_token, self.logger)
            fitbit_auth.save_token(email, tokens)

        if token_data:
            self.access_token = token_data['access_token']
            self.user_id = token_data['user_id']
            if fitbit_auth.validate_token(self.access_token):
                self.logger.info("Using stored token.")
                return
            else:
                self.logger.warning("Stored token is invalid or expired, trying to refresh.")
                try:
                    new_tokens = fitbit_auth.refresh_access_token(token_data['refresh_token'])
                    set_tokens_and_device(new_tokens)
                    self.logger.info("Token refreshed successfully.")
                    return
                except Exception as refresh_exception:
                    self.logger.warning("Token refresh failed, performing full authorization.")

        try:
            code = fitbit_auth.get_authorization_code()
            tokens = fitbit_auth.exchange_code_for_token(code)
            set_tokens_and_device(tokens)
        except KeyboardInterrupt:
            self.logger.info("Server shutdown.")
        except Exception as e:
            self.logger.error(f"Authorization failed: {e}")
            
    def request_date_input(self):
        """
        Request user to input the date range again.
        """
        choice = input("Please enter the date range in the format 'YYYY-MM-DD to YYYY-MM-DD': ")
        return choice

    def validate_and_parse_dates(self, start_date, end_date):
        """
        Validate the start and end dates and check for various conditions.

        Args:
            start_date (str): The start date in 'YYYY-MM-DD' format.
            end_date (str): The end date in 'YYYY-MM-DD' format.

        Returns:
            tuple: A tuple containing the parsed start and end dates (datetime objects) if valid, otherwise None.
        """
        try:
            start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            self.logger.error("Invalid date format. Please use 'YYYY-MM-DD'.")
            return None

        if start_date_dt.year < 2000 or end_date_dt.year < 2000:
            self.logger.error("Year must be greater than 2000.")
            return None

        if start_date_dt.month < 1 or start_date_dt.month > 12 or end_date_dt.month < 1 or end_date_dt.month > 12:
            self.logger.error("Month must be between 1 and 12.")
            return None

        if start_date_dt.day < 1 or start_date_dt.day > 31 or end_date_dt.day < 1 or end_date_dt.day > 31:
            self.logger.error("Day must be between 1 and 31.")
            return None

        if end_date_dt > datetime.now() - timedelta(days=1):
            self.logger.error("End date cannot be in the future or today. Please enter a valid range.")
            return None

        if start_date_dt > end_date_dt:
            self.logger.error("Start date cannot be greater than end date. Please enter a valid range.")
            return None

        if (end_date_dt - start_date_dt).days > self.max_days:
            self.logger.error(f"Date range must not exceed {self.max_days} days. Please enter a valid range.")
            return None

        return start_date_dt, end_date_dt
      
    def retrieve_and_save_data(self):
      """
      Retrieves and saves data from Fitbit.
      """
      if not self.data_storage:
          self.data_storage = DataStorage(base_dir='data', user_dir=self.user_dir)
      if not self.access_token or not self.user_id:
          self.logger.error("Access token or user ID missing. Aborting data retrieval.")
          return

      try:
          self.data_retriever = FitbitDataRetriever(self.access_token, self.logger)
          device_id = self.data_retriever.get_device_id(self.user_id)
          if device_id is None:
              self.logger.error("Failed to retrieve device ID. Aborting data retrieval.")
              return
      except Exception as e:
          self.logger.error(f"Failed to initialize data retrieval: {e}")
          return

      while True:
        choice = input("Enter number of days to retrieve data or a date range in format 'YYYY-MM-DD to YYYY-MM-DD': ")

        if "to" in choice:
            date_range = choice.split(" to ")
            if len(date_range) == 2:
                    start_date = date_range[0].strip()
                    end_date = date_range[1].strip()
                    
                    dates = self.validate_and_parse_dates(start_date, end_date)
                    if dates:
                        start_date_dt, end_date_dt = dates
                        all_data = self.data_retriever.get_data_for_dates(self.user_id, device_id, start_date, end_date)
                        break
                    else:
                        continue

            else:
                self.logger.error("Invalid date range format. Please use 'YYYY-MM-DD to YYYY-MM-DD'.")
                continue
                
        else:
            try:
                days = int(choice)
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days - 1)
                all_data = self.data_retriever.get_data_for_dates(self.user_id, device_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                break
            except ValueError:
               self.logger.error("Invalid input. Please enter a number of days or a date range.")
               continue

      try:
          self.data_storage.save_data_to_json(all_data, 'fitbit_data.json')
          self.data_storage.save_data_to_txt(all_data, 'fitbit_data.txt')
          self.data_retriever.summary()
      except Exception as e:
          self.logger.error(f"Failed to fetch all data: {e}")
          return

    def process_data(self, dataset_path):
      """
      Processes existing dataset.

      Args:
          dataset_path (str): Path to the dataset to be processed.
      """
      json_file_path = os.path.join(dataset_path, 'fitbit_data.json')
      if not os.path.exists(json_file_path):
          self.logger.error("ERROR: fitbit_data.json not found inside chosen directory.")
          action = input("Do you want to (R)erun or (E)xit? [R/E]: ").upper()
          if action == 'R':
              dataset_path = self.list_and_select_dataset()
              if dataset_path:
                  self.process_data(dataset_path)
              return
          elif action == 'E':
              return
          else:
              self.logger.info("Invalid option entered. Exiting.")
              return
      try:
        with open(json_file_path, 'r') as file:
            all_data = json.load(file)
        self.logger.info("Data loaded from local file successfully.")
        device_id = all_data.get('device_id', None)
        if device_id is None:
            self.logger.error("Device ID missing from data file.")
            return

        if not self.data_storage:
            self.data_storage = DataStorage(base_dir='data', user_dir=self.user_dir, specific_data_dir=dataset_path)
        extractor = DataExtractor(all_data, device_id, self.data_storage, self.logger)
        extractor.process_data()
        self.logger.info("Data processed successfully.")
      except json.JSONDecodeError as e:
          self.logger.error(f"Error decoding JSON from file: {e}")
      except Exception as e:
          self.logger.error(f"An error occurred while processing data: {e}")
    
    def choose_action(self):
      action = input("Do you want to (R)etrieve new data or (P)rocess existing data or (E)xit? [R/P/E]: ").upper()
      if action == 'R':
          self.authorize()
          self.retrieve_and_save_data()
      elif action == 'P':
          dataset_path = self.list_and_select_dataset()
          if dataset_path:
              self.process_data(dataset_path)
          else:
              self.logger.info("No dataset selected or found.")
      elif action == 'E':
          print("Exiting...")
          sys.exit(1)  
      else:
        print("Invalid action selected. Please choose either 'R' or 'P'.")
        self.choose_action()
               
    def run(self):
        """
        Runs the main application loop, prompting the user to retrieve new data or process existing data.
        """
        parser = argparse.ArgumentParser(description="Fitbit Data Application")
        parser.add_argument('--docs', action='store_true', help="Open the documentation in a web browser")
        args = parser.parse_args()

        if args.docs:
            docs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'docs', 'build', 'html', 'index.html'))
            webbrowser.open(f'file://{docs_path}')
            print(f"Documentation opened at {docs_path}")
            sys.exit(0)
            
        self.choose_credentials()
        self.choose_action()

                 
if __name__ == '__main__':
    check_dependencies()
    app = FitbitDataApplication()
    app.run()