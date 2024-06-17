from datetime import datetime, timedelta
import sys
import time
import requests
from tqdm import tqdm
from RateLimiter import RateLimitManager

class FitbitDataRetriever(RateLimitManager):
  """
  Retrieves data from the Fitbit API.
  """
  def __init__(self, access_token, logger):
    """
    Initializes the FitbitDataRetriever class.

    Args:
        access_token (str): The access token for the Fitbit API.
        logger (logging.Logger): The logger instance.
    """
    self.base_url = 'https://api.fitbit.com'
    self.headers = {'Authorization': f'Bearer {access_token}'}
    self.logger = logger
    self.total_requests = 0
    self.successful_requests = {resource: 0 for resource in ['steps', 'heart', 'calories', 'sleep', 'spo2', 'rate', 'devices']}
    self.failed_requests = {resource: 0 for resource in ['steps', 'heart', 'calories', 'sleep', 'spo2', 'rate', 'devices']}
    
  def get_device_id(self, user_id):
    """
    Retrieves the device ID for the given user ID.

    Args:
        user_id (str): The user ID for the Fitbit API.

    Returns:
        str: The selected device ID or None if no valid selection.
    """
    endpoint = f"/1/user/{user_id}/devices.json"
    self.check_not_exceeded(['devices'], 1)

    devices_data = self.get_data(endpoint, 'devices')
    

    if devices_data is not None and len(devices_data) > 0:
        print("Available devices:")
        for index, device in enumerate(devices_data):
            print(f"{index + 1}. {device.get('deviceVersion')} (ID: {device['id']})")

        device_number = int(input("Please select the device by entering the number: ")) - 1
        if 0 <= device_number < len(devices_data):
            selected_device_id = devices_data[device_number]['id']
            self.logger.info(f"Selected Device ID: {selected_device_id}")
            return selected_device_id
        else:
            self.logger.warning("Invalid device selection.")
            return None
    else:
        self.logger.warning("No devices found or failed to fetch devices.")
        return None
      
  def log_error_details(self, response, attempt, resource_type):
        """
        Logs error details from the API response.

        Args:
            response (requests.Response): The response object from the API request.
            attempt (int): The attempt number of the request.
            resource_type (str): The type of resource being requested.
        """
        error_message = "Unknown Error"
        try:
            error_details = response.json()
            if 'errors' in error_details:
                error_message = ', '.join([error['message'] for error in error_details['errors']])
        except ValueError:
            error_message = "Failed to parse JSON response"
        self.logger.error(f"Attempt {attempt + 1}: Error fetching {resource_type} - {response.status_code} {response.reason} - {error_message}")
              
  def get_data(self, endpoint, resource_type, retries=3, backoff_factor=1.5):
      """
      Retrieves data from the Fitbit API with retries and exponential backoff.

      Args:
          endpoint (str): The API endpoint to request data from.
          resource_type (str): The type of resource being requested.
          retries (int): Number of retries for the request.
          backoff_factor (float): Factor for exponential backoff between retries.

      Returns:
          dict: The JSON response from the API or None if the request fails.
      """
      attempt = 0
      while attempt < retries:
          response = requests.get(f"{self.base_url}/{endpoint}", headers=self.headers)
          super().update_rate_limit(response.headers)

          if response.status_code == 200:
              self.successful_requests[resource_type] += 1
              self.total_requests += 1
              return response.json()
            
          elif response.status_code == 429:
              reset_time = int(super().rate_limit['reset'])
              remaining_time = reset_time
              self.logger.info(f"Rate limit reached, resets in {remaining_time} seconds. Waiting...")
              if remaining_time > 0:
                  time.sleep(remaining_time + 1)  # Sleep for remaining time plus an extra second
              else:
                  time.sleep((attempt + 1) * backoff_factor)
              attempt += 1
      
          else:
              self.log_error_details(response, attempt, resource_type)
              time.sleep((attempt + 1) * backoff_factor)
              attempt += 1
              self.failed_requests[resource_type] += 1
              self.total_requests += 1
      self.logger.error(f"Failed to fetch {resource_type} data after {retries} attempts.")
      return None
      
  def summary(self):
        """
        Logs a summary of the API requests and rate limit information.
        """
        self.logger.info('-' * 100)
        self.logger.info("Summary of API Requests:")
        self.logger.info(f"Total requests: {self.total_requests}")
        for resource, count in self.successful_requests.items():
            self.logger.info(f"{resource.capitalize()}: Success: {count}, Failed: {self.failed_requests[resource]}")
        self.logger.info(f"API Rate Limit: {super().rate_limit['limit']}")
        self.logger.info(f"API Rate Limit Remaining: {super().rate_limit['remaining']}")
        self.logger.info(f"API Rate Limit Reset (seconds): {super().rate_limit['reset']}")
        self.logger.info('-' * 100)
        
  def construct_endpoint(self, user_id, resource, date):
      """
      Constructs the API endpoint for a given resource and date.

      Args:
          user_id (str): The user ID for the Fitbit API.
          resource (str): The resource type (e.g., steps, heart).
          date (str): The date for which to retrieve data.

      Returns:
          str: The constructed API endpoint.
      """
      if resource in ['steps', 'heart', 'calories']:
          return f"/1/user/{user_id}/activities/{resource}/date/{date}/1d.json"
      elif resource == 'sleep':
          return f"/1.2/user/{user_id}/sleep/date/{date}.json"
      elif resource == 'spo2':
          return f"/1/user/{user_id}/spo2/date/{date}.json"
      elif resource == 'rate':
          return f"/1/user/{user_id}/activities/heart/date/{date}/1d/1sec.json" 
  
  def get_all_data_for_date_ranges(self, user_id, device_id, start_date, end_date):
      """
      Retrieves all data for specified date ranges.

      Args:
          user_id (str): The user ID for the Fitbit API.
          device_id (str): The ID of the Fitbit device.
          start_date (str): The start date in YYYY-MM-DD format.
          end_date (str): The end date in YYYY-MM-DD format.

      Returns:
          dict: A dictionary containing data for all resources.
      """
      resources = ['steps', 'heart', 'calories', 'sleep', 'spo2', 'rate']
      data = {resource: [] for resource in resources}
      data['device_id'] = device_id

      start = datetime.strptime(start_date, '%Y-%m-%d')
      end = datetime.strptime(end_date, '%Y-%m-%d')
      num_days = (end - start).days + 1
      self.check_not_exceeded(resources, num_days)

      progress = tqdm(total=num_days * len(resources), desc="Fetching Data")
      for single_date in (start + timedelta(n) for n in range(num_days)):
          date_str = single_date.strftime('%Y-%m-%d')
          for resource in resources:
              endpoint = self.construct_endpoint(user_id, resource, date_str)
              daily_data = self.get_data(endpoint, resource)
              if daily_data:
                  data[resource].append(daily_data)
              progress.update(1)
      progress.close()
      return data

  def check_not_exceeded(self, resources, num_days):
      total_requests = len(resources) * num_days
      remaining_requests = int(super().rate_limit['remaining'])
      
      if total_requests > remaining_requests:
        self.logger.info(f"Remaining requests are {remaining_requests}, which is less than the total requests of {total_requests}.")
        sys.exit(1)
    
  def get_data_for_dates(self, user_id, device_id, start_date, end_date):
      """
      Retrieves data for a specific date range.

      Args:
          user_id (str): The user ID for the Fitbit API.
          device_id (str): The ID of the Fitbit device.
          start_date (str): The start date in YYYY-MM-DD format.
          end_date (str): The end date in YYYY-MM-DD format.

      Returns:
          dict: A dictionary containing data for the specified date range.
      """
      return self.get_all_data_for_date_ranges(user_id, device_id, start_date, end_date)