import base64
import json
import os
import webbrowser
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from urllib.parse import quote, parse_qs, urlparse
from RateLimiter import RateLimitManager

class FitbitAuth(RateLimitManager):
  """
  Handles Fitbit OAuth2 authentication.
  """
  def __init__(self, client_id, client_secret, redirect_uri, logger, port=8000):
    """
    Initializes the FitbitAuth class.

    Args:
        client_id (str): The client ID for the Fitbit app.
        client_secret (str): The client secret for the Fitbit app.
        redirect_uri (str): The redirect URI for the Fitbit app.
        logger (logging.Logger): The logger instance.
        port (int): The port number for the redirect URI.
    """
    self.logger = logger
    self.client_id = client_id
    self.client_secret = client_secret
    self.redirect_uri = redirect_uri
    self.port = port
    self.auth_url = f"https://www.fitbit.com/oauth2/authorize?response_type=code&client_id={self.client_id}&redirect_uri={quote(self.redirect_uri)}&scope=activity%20heartrate%20location%20nutrition%20profile%20settings%20sleep%20social%20weight%20oxygen_saturation"
    self.server_running = True
    self.query_string = None
  
  def validate_token(self, access_token):
        """
        Validates the current access token using the introspection endpoint.

        Args:
            access_token (str): The access token to validate.

        Returns:
            bool: True if the token is valid, False otherwise.
        """
        introspect_url = 'https://api.fitbit.com/1.1/oauth2/introspect'
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        body = {
            'token': access_token
        }
        response = requests.post(introspect_url, headers=headers, data=body)
        if response.status_code == 200:
            token_info = response.json()
            return token_info.get('active', False)
        else:
            self.logger.error(f"Token introspection failed: {response.text}")
            return False
  
  def save_token(self, email, token_data):
      """
      Saves the token data to a file.

      Args:
          email (str): The email associated with the token.
          token_data (dict): The token data to save.
      """
      token_file = f'token_{email}.json'
      with open(token_file, 'w') as f:
          json.dump(token_data, f)

  def load_token(self, email):
        """
        Loads the token data from a file.

        Args:
            email (str): The email associated with the token.

        Returns:
            dict: The token data, or None if the file doesn't exist.
        """
        token_file = f'token_{email}.json'
        if os.path.exists(token_file):
            with open(token_file, 'r') as f:
                return json.load(f)
        return None
      
  def refresh_access_token(self, refresh_token):
        """
        Refreshes the access token using the refresh token.

        Args:
            refresh_token (str): The refresh token.

        Returns:
            dict: The response JSON containing the new access token and other details.
        
        Raises:
            Exception: If the token refresh fails.
        """
        token_url = 'https://api.fitbit.com/oauth2/token'
        credentials = f"{self.client_id}:{self.client_secret}"
        headers = {
            'Authorization': f'Basic {base64.b64encode(credentials.encode()).decode()}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        body = {
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token'
        }
        
        response = requests.post(token_url, headers=headers, data=body)
        super().update_rate_limit(response.headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            if response.status_code == 429:
                self.summary()
                raise Exception(f"Too many requests, try again in: {response.headers['Fitbit-Rate-Limit-Reset']} seconds.")
            self.logger.error(f"Error refreshing token: {response.text}") 
            raise Exception(f"Failed to refresh token: {response.text}")
  
  def _request_handler_factory(self):
    """
    Creates a custom request handler class for the OAuth callback.

    Returns:
        CustomRequestHandler (BaseHTTPRequestHandler): A request handler class for handling OAuth callback.
    """
    auth_instance = self
    class CustomRequestHandler(BaseHTTPRequestHandler):
      def do_GET(self):
        nonlocal auth_instance
        if self.path.startswith("/?code="):
          auth_instance.query_string = self.path
          auth_instance.server_running = False
          self.send_response(200)
          self.send_header('Content-type', 'text/html')
          self.end_headers()
          self.wfile.write("Authentication successful. You can close this tab.".encode())
        else:
          self.send_response(404)
          self.end_headers()
          self.wfile.write("Not Found.".encode())
    return CustomRequestHandler

  def _start_server(self):
    """
    Starts a local HTTP server to handle the OAuth callback.
    """
    server = HTTPServer(('localhost', self.port), self._request_handler_factory())
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    self.logger.info("Server started, waiting for authentication...")
    while self.server_running:
      pass
    server.shutdown()
    server.server_close()
       
  def summary(self):
        """
        Logs the rate limit information.
        """
        self.logger.info('-' * 100)
        self.logger.info("TOO MANY REQUESTS:")
        self.logger.info(f"API Rate Limit: {super().rate_limit['limit']}")
        self.logger.info(f"API Rate Limit Remaining: {super().rate_limit['remaining']}")
        self.logger.info(f"API Rate Limit Reset (seconds): {super().rate_limit['reset']}")
        self.logger.info('-' * 100)
        
  def get_authorization_code(self):
      """
      Opens the Fitbit authorization URL in the user's default web browser and starts the local server to receive the authorization code.

      Returns:
          str: The authorization code received from Fitbit.
      """
      self.auth_url += "&prompt=login%20consent"
      webbrowser.open(self.auth_url)
      self._start_server()
      if not self.query_string:
          raise Exception("No se obtuvo el código de autorización. Asegúrese de haber iniciado sesión correctamente.")
      code = parse_qs(urlparse(self.query_string).query)['code'][0]
      return code
  
  def exchange_code_for_token(self, code):
    """
    Exchanges the authorization code for an access token.

    Args:
        code (str): The authorization code received from Fitbit.

    Returns:
        dict: The response JSON containing the access token and other details.
    
    Raises:
        Exception: If the token exchange fails or if rate limits are exceeded.
    """
    token_url = 'https://api.fitbit.com/oauth2/token'
    credentials = f"{self.client_id}:{self.client_secret}"
    headers = {
      'Authorization': f'Basic {base64.b64encode(credentials.encode()).decode()}',
      'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    body = {
      'code': code,
      'redirect_uri': self.redirect_uri,
      'grant_type': 'authorization_code'
    }
    
    response = requests.post(token_url, headers=headers, data=body)
    super().update_rate_limit(response.headers)
    if response.status_code == 200:
        return response.json()
    else:
        if response.status_code == 429:
            self.summary()
            raise Exception(f"Too many requests, try again in: {response.headers.rate_limit['reset']} seconds.")
        self.logger.error(f"Error token: {response.text}") 
        raise Exception(f"Failed to exchange credentials: {response.text}")
