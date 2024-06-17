import json
import os
from datetime import datetime

class DataStorage:
    """
    Handles storage of Fitbit data in various formats.

    Attributes:
        base_dir (str): Base directory for data storage.
        user_dir (str): User-specific directory for storing data.
        data_dir (str): Specific directory for storing data, either provided or created.
    """
    def __init__(self, base_dir='data', user_dir='default', specific_data_dir=None):
      """
      Initializes the DataStorage class.

      Args:
          base_dir (str): Base directory for data storage.
          user_dir (str): User-specific directory for storing data.
          specific_data_dir (str, optional): Specific directory for storing data. If not provided, a new directory is created.
      """
      self.base_dir = base_dir
      self.user_dir = user_dir
      if specific_data_dir:
        self.data_dir = specific_data_dir
      else:
        self.data_dir = self.create_data_dir()

    def create_data_dir(self):
        """
        Creates a new directory for storing data with a timestamp.

        Returns:
            str: Path to the created directory.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        data_dir = os.path.join(self.base_dir, self.user_dir, timestamp)
        os.makedirs(data_dir, exist_ok=True)
        return data_dir

    def save_data_to_json(self, data, filename='fitbit_data.json'):
        """
        Saves data to a JSON file.

        Args:
            data (dict): Data to be saved.
            filename (str): Name of the JSON file.
        """
        filepath = os.path.join(self.data_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"JSON data saved to {filepath}")

    def save_data_to_txt(self, data, filename='fitbit_data.txt'):
        """
        Saves data to a text file.

        Args:
            data (dict): Data to be saved.
            filename (str): Name of the text file.
        """
        filepath = os.path.join(self.data_dir, filename)
        with open(filepath, 'w') as f:
            for key, value in data.items():
                f.write(f"{key}: {value}\n")
        print(f"Text data saved to {filepath}")
          
    def save_data_to_csv(self, df, filename):
        """
        Saves data to a CSV file.

        Args:
            df (pandas.DataFrame): DataFrame containing the data to be saved.
            filename (str): Name of the CSV file.
        """
        filepath = os.path.join(self.data_dir, filename)
        df.to_csv(filepath, index=False)
        print(f"Data saved to {filepath}")
