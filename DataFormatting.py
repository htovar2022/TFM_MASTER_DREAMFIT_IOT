import pandas as pd
from functools import reduce
from datetime import datetime, timedelta

class DataExtractor:
    """
    Extracts and processes data from Fitbit.
    """
    def __init__(self, data, device_id, data_storage, logger):
        """
        Initializes the DataExtractor class.

        Args:
            data (dict): The raw data from Fitbit.
            device_id (str): The ID of the Fitbit device.
            data_storage (DataStorage): An instance of the DataStorage class.
            logger (logging.Logger): The logger instance.
        """
        self.logger = logger
        self.data = data
        self.device_id = device_id
        self.data_storage = data_storage
    
    def transform_minutes_to_readable(self, df, minute_columns):
        """
        Transform minutes into readable hours and minutes format.

        Args:
            df (pd.DataFrame): The dataframe containing minute columns.
            minute_columns (list): List of columns with minute values.

        Returns:
            pd.DataFrame: The dataframe with readable time format.
        """
        self.logger.info("Transforming minutes to readable format...")
        for column in minute_columns:
            if column in df.columns:
                df[column + '_readable'] = df[column].apply(lambda x: f"{x // 60} hours {x % 60} minutes" if x > 0 else "0 hours 0 minutes")
        return df
 
    def minutes_to_hours(self, df, minute_column):
        """
        Convert minutes to hours with one decimal place.

        Args:
            df (pd.DataFrame): The dataframe containing the minute column.
            minute_column (str): The column with minute values.

        Returns:
            pd.DataFrame: The dataframe with hours.
        """
        if minute_column in df.columns:
            df[minute_column + '_hours'] = df[minute_column].apply(lambda x: round(x / 60, 1))
        return df
  
    def format_duration(self, total_seconds):
        """
        Format total seconds into hours, minutes, and seconds.

        Args:
            total_seconds (int): Total duration in seconds.

        Returns:
            str: Formatted duration string.
        """
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours} hours {minutes} minutes {seconds} seconds"
          
    def transform_date_format(self, df, date_column):
        """
        Convert date format from YYYY-MM-DD to DD/MM/YYYY.

        Args:
            df (pd.DataFrame): The dataframe containing the date column.
            date_column (str): The column with date values.

        Returns:
            pd.DataFrame: The dataframe with formatted dates.
        """
        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column]).dt.strftime('%d/%m/%Y')
        return df

    def round_decimal_values(self, df, columns):
        """
        Round decimal values in specific columns to 4 decimal places.

        Args:
            df (pd.DataFrame): The dataframe containing decimal columns.
            columns (list): List of columns to be rounded.

        Returns:
            pd.DataFrame: The dataframe with rounded values.
        """
        for column in columns:
            if column in df.columns:
                df[column] = df[column].round(4)
        return df

    def calculate_duration(self, start_time, end_time, time_format='%H:%M:%S'):
        """
        Calculate the duration between two time points.

        Args:
            start_time (str): The start time in HH:MM:SS format.
            end_time (str): The end time in HH:MM:SS format.
            time_format (str): The time format string.

        Returns:
            int: Duration in seconds.
        """
        if start_time == 'N/A' or end_time == 'N/A':
            return 0
        start = datetime.strptime(start_time, time_format)
        end = datetime.strptime(end_time, time_format)
        if end < start:
            end += timedelta(days=1)
        duration = (end - start).total_seconds()
        return duration
    
    def calculate_period_durations(self, data):
        """
        Calculate total active and resting periods.

        Args:
            data (list): List of data entries with time and value.

        Returns:
            tuple: Total active duration and total resting duration in a readable format.
        """
        active_duration_total = 0
        resting_duration_total = 0
        current_period_start = None
        current_period_type = None 

        for i, entry in enumerate(data):
            current_type = 'active' if entry['value'] > 110 else 'resting'
            if current_period_type != current_type:
                if current_period_start is not None:
                    duration = self.calculate_duration(current_period_start, data[i-1]['time'])
                    if current_period_type == 'active':
                        active_duration_total += duration
                    else:
                        resting_duration_total += duration
                current_period_start = entry['time']
                current_period_type = current_type
        
        if current_period_start is not None and len(data) > 0:
            duration = self.calculate_duration(current_period_start, data[-1]['time'])
            if current_period_type == 'active':
                active_duration_total += duration
            else:
                resting_duration_total += duration

        return self.format_duration(active_duration_total), self.format_duration(resting_duration_total)

    def extract_sleep_data(self):
        """
        Extract sleep data from the raw data.

        Returns:
            pd.DataFrame: DataFrame containing processed sleep data.
        """
        sleep_records = []
        processed_days = set()

        for record in self.data.get('sleep', []):
            daily_sleeps = record.get('sleep', [])
            main_sleeps = {entry.get('dateOfSleep', 'N/A'): entry for entry in daily_sleeps if entry.get('isMainSleep', False)}

            for sleep_entry in record.get('sleep', []):
                date_of_sleep = sleep_entry.get('dateOfSleep', 'N/A')
                is_main_sleep = sleep_entry.get('isMainSleep', False)
                sleep_type = sleep_entry.get('type', 'N/A')

                if date_of_sleep not in processed_days and sleep_entry == main_sleeps.get(date_of_sleep, {}):
                    processed_days.add(date_of_sleep)
                    
                    extracted_data = {
                        'Id_dispositivo': self.device_id,
                        'logId': sleep_entry.get('logId', 'N/A'),
                        'DateTime': date_of_sleep,
                        'startTime': sleep_entry.get('startTime', 'N/A'),
                        'endTime': sleep_entry.get('endTime', 'N/A'),
                        'duration': sleep_entry.get('duration', 0),
                        'efficiency': sleep_entry.get('efficiency', 0),
                        'minutesAsleep': sleep_entry.get('minutesAsleep', 0),
                        'minutesAwake': sleep_entry.get('minutesAwake', 0),
                        'timeInBed': sleep_entry.get('timeInBed', 0),
                        'isMainSleep': is_main_sleep,
                        'type': sleep_type
                    }

                    levels_summary = sleep_entry.get('levels', {}).get('summary', {})
                    for stage in ['deep', 'wake', 'light', 'rem']:
                        stage_data = levels_summary.get(stage, {})
                        extracted_data[f'{stage}_minutes'] = stage_data.get('minutes', 0)
                        extracted_data[f'{stage}_count'] = stage_data.get('count', 0)
                        extracted_data[f'{stage}_thirtyDayAvgMinutes'] = stage_data.get('thirtyDayAvgMinutes', 0)

                    sleep_records.append(extracted_data)

        df = pd.DataFrame(sleep_records)
        df = self.transform_date_format(df, 'DateTime')
        df = self.minutes_to_hours(df, 'minutesAsleep')
        df = self.transform_minutes_to_readable(df, ['minutesAsleep', 'minutesAwake', 'timeInBed', 'wake_minutes', 'rem_minutes',  'light_minutes', 'deep_minutes'])

        return df
      
    def extract_steps_data(self):
      """
      Extract steps data from the raw data.

      Returns:
          pd.DataFrame: DataFrame containing processed steps data.
      """
      steps_records = []
      for entry in self.data.get('steps', []):
        steps_total = entry.get('activities-steps', [])

        for step_entry in steps_total:
            date = step_entry.get('dateTime', 'N/A')
            total_steps = step_entry.get('value', 0)

            daily_steps = {
                'Id_dispositivo': self.device_id,
                'DateTime': date,
                'TotalSteps': int(total_steps),
            }

            steps_records.append(daily_steps)
            
      df = pd.DataFrame(steps_records)
      df = self.transform_date_format(df, 'DateTime')
      return df

    def extract_calories_data(self):
        """
        Extract calories data from the raw data.

        Returns:
            pd.DataFrame: DataFrame containing processed calories data.
        """
        calories_records = []
        for entry in self.data.get('calories', []):
            for calorie_entry in entry.get('activities-calories', []):
                date = calorie_entry.get('dateTime', 'N/A')
                total_calories = calorie_entry.get('value', 0)

                calories_data = {
                    'Id_dispositivo': self.device_id,
                    'DateTime': date,
                    'Values_calorias quemadas': int(total_calories)
                }
                calories_records.append(calories_data)
        df = pd.DataFrame(calories_records)
        df = self.transform_date_format(df, 'DateTime')
        return df

    def extract_resting_heart_rate_data(self):
      """
      Extract resting heart rate data from the raw data.

      Returns:
          pd.DataFrame: DataFrame containing processed resting heart rate data.
      """
      resting_heart_records = []
      heart_activities = self.data.get('rate', [])

      for entry in heart_activities:
          for heart_entry in entry.get('activities-heart', []):
              date = heart_entry.get('dateTime', 'N/A')
              resting_heart_rate = heart_entry.get('value', {}).get('restingHeartRate')

              if resting_heart_rate is not None:
                  resting_data = {
                      'Id_dispositivo': self.device_id,
                      'DateTime': date,
                      'RestingHeartRate': resting_heart_rate
                  }
                  resting_heart_records.append(resting_data)
              else:
                  self.logger.info(f"No resting heart rate data available for {date}.")
                  
      df = pd.DataFrame(resting_heart_records)
      df = self.transform_date_format(df, 'DateTime')
      return df
    
    def extract_spo2_data(self):
        """
        Extract SPO2 data from the raw data.

        Returns:
            pd.DataFrame: DataFrame containing processed SPO2 data.
        """
        extracted_data = []
        spo2_entries = self.data.get('spo2', [])
        spo2_entries = self.data.get('spo2', [])

        for entry in spo2_entries:
            dateTime = entry.get('dateTime')
            value = entry.get('value', {})
            avg_spo2 = value.get('avg', None)
            min_spo2 = value.get('min', None)
            max_spo2 = value.get('max', None)

            extracted_data.append({
                'Id_dispositivo': self.device_id,
                'DateTime': dateTime,
                'Average_SP02': avg_spo2,
                'SPO2_Min': min_spo2,
                'SPO2_Max': max_spo2
            })

        df = pd.DataFrame(extracted_data)
        df = self.transform_date_format(df, 'DateTime')
        return df
      
    def extract_heart_rate_zones_data(self):
      """
      Extract heart rate zones data from the raw data.

      Returns:
          pd.DataFrame: DataFrame containing processed heart rate zones data.
      """
      heart_rate_zone_records = []
      heart_activities = self.data.get('heart', [])

      for entry in heart_activities:
          for heart_entry in entry.get('activities-heart', []):
              date = heart_entry.get('dateTime', 'N/A')
              zones = heart_entry.get('value', {}).get('heartRateZones', [])

              heart_data = {
                  'Id_dispositivo': self.device_id,
                  'DateTime': date
              }

              for zone in zones:
                  zone_name = zone['name'].replace(" ", "")
                  heart_data[f'{zone_name}_Min'] = zone.get('min', 0)
                  heart_data[f'{zone_name}_Max'] = zone.get('max', 0)
                  heart_data[f'{zone_name}_CaloriesOut'] = zone.get('caloriesOut', 0)
                  heart_data[f'{zone_name}_Minutes'] = zone.get('minutes', 0)

              heart_rate_zone_records.append(heart_data)
              
      df = pd.DataFrame(heart_rate_zone_records)
      df = self.transform_date_format(df, 'DateTime')
      decimal_columns = [col for col in df.columns if 'CaloriesOut' in col]
      df = self.round_decimal_values(df, decimal_columns)
      return df

    def extract_average_rate_data(self):
        """
        Extract average heart rate data from the raw data.

        Returns:
            pd.DataFrame: DataFrame containing processed average heart rate data.
        """
        average_rate_records = []
        rate_entries = self.data.get('rate', [])

        for entry in rate_entries:
            if 'activities-heart-intraday' in entry:
                date = entry['activities-heart'][0]['dateTime'] if 'activities-heart' in entry and entry['activities-heart'] else 'N/A'
                intraday_data = entry['activities-heart-intraday']
                dataset = intraday_data.get('dataset', [])
                interval = intraday_data.get('datasetInterval', 1)
                dataset_type = intraday_data.get('datasetType', 'minute')
                max_rate_on_rest = 110
                night_start = '00:00'
                night_end = '09:00'
                
                if dataset:
      
                    night_data = [d for d in dataset if night_start <= d['time'] < night_end]
                    day_data = [d for d in dataset if night_end <= d['time']]
                    
                    is_complete = lambda data: all(x['value'] for x in data)
                    day_complete = is_complete(day_data)
                    night_complete = is_complete(night_data)
                    # Check if no heart rate value above 110 is found
                    no_rate_above_110 = all(d['value'] <= max_rate_on_rest for d in dataset)
        
                    if day_complete and night_complete and no_rate_above_110:
                      avg_heart_rate_active = avg_heart_rate_night_active = 120
                      
                    active_data = [d['value'] for d in dataset if d['value'] > max_rate_on_rest]
                    resting_data = [d['value'] for d in dataset if d['value'] <= max_rate_on_rest]
                    
                    time_start_day = min(d['time'] for d in day_data) if day_data else None
                    time_end_day = max(d['time'] for d in day_data) if day_data else None
                    time_start_night = min(d['time'] for d in night_data) if night_data else None
                    time_end_night = max(d['time'] for d in night_data) if night_data else None
                    
                    active_day_data = [d['value'] for d in day_data if d['value'] > max_rate_on_rest]
                    resting_day_data = [d['value'] for d in day_data if d['value'] <= max_rate_on_rest]

                    active_night_data = [d['value'] for d in night_data if d['value'] > max_rate_on_rest]
                    resting_night_data = [d['value'] for d in night_data if d['value'] <= max_rate_on_rest]
                    
                    time_start = dataset[0]['time'] if dataset else None
                    time_end = dataset[-1]['time'] if dataset else None
                    duration = self.format_duration(self.calculate_duration(time_start, time_end))
                    
                    duration_day =  self.format_duration(self.calculate_duration(time_start_day, time_end_day) if day_data else 0)
                    duration_night = self.format_duration(self.calculate_duration(time_start_night, time_end_night) if night_data else 0)

                    avg_day_active_duration, avg_day_resting_duration = self.calculate_period_durations(day_data)
                    avg_night_active_duration, avg_night_resting_duration = self.calculate_period_durations(night_data)
                    
                    avg_heart_rate = sum(d['value'] for d in dataset) / len(dataset) if dataset else 0
                    avg_heart_rate_active = sum(active_data) / len(active_data) if active_data else 120
                    avg_heart_rate_resting = sum(resting_data) / len(resting_data) if resting_data else 80
                    
                    avg_heart_rate_day = sum(d['value'] for d in day_data) / len(day_data) if day_data else 0
                    avg_heart_rate_day_active = sum(active_day_data) / len(active_day_data) if active_day_data else 120
                    avg_heart_rate_day_resting = sum(resting_day_data) / len(resting_day_data) if resting_day_data else 80
                    
                    avg_heart_rate_night = sum(d['value'] for d in night_data) / len(night_data) if night_data else 0
                    avg_heart_rate_night_active = sum(active_night_data) / len(active_night_data) if active_night_data else 120
                    avg_heart_rate_night_resting = sum(resting_night_data) / len(resting_night_data) if resting_night_data else 80
                    
                    # Dejar 0 decimales
                    blood_pressure = f"{round(avg_heart_rate_active)} / {round(avg_heart_rate_resting)}"
                    
                    average_rate_records.append({
                        'Id_dispositivo': self.device_id,
                        'DateTime': date,
                        'TimeStart': time_start,
                        'TimeEnd': time_end,
                        "Duration": duration,
                        'TimeStartDay': time_start_day,
                        'TimeEndDay': time_end_day,
                        "DurationDay": duration_day,
                        'TimeStartNight': time_start_night,
                        'TimeEndNight': time_end_night,
                        "DurationNight": duration_night,
                        'dataset_Interval': interval,
                        'dataset_type': dataset_type,
                        'average_HeartValue': round(avg_heart_rate, 4),
                        'average_HeartValue_Activity': round(avg_heart_rate_active, 2),
                        'average_HeartValue_Resting': round(avg_heart_rate_resting, 2),
                        'average_HeartValue_Day': round(avg_heart_rate_day, 2),
                        'average_HeartValue_Day_Activity': round(avg_heart_rate_day_active, 2),
                        'average_HeartValue_Day_Activity_Duration': avg_day_active_duration,
                        'average_HeartValue_Day_Resting': round(avg_heart_rate_day_resting, 2),
                        'average_HeartValue_Day_Resting_Duration': avg_day_resting_duration,
                        'average_HeartValue_Night': round(avg_heart_rate_night, 2),
                        'average_HeartValue_Night_Activity': round(avg_heart_rate_night_active, 2),
                        'average_HeartValue_Night_Activity_Duration': avg_night_active_duration,
                        'average_HeartValue_Night_Resting': round(avg_heart_rate_night_resting, 2),
                        'average_HeartValue_Night_Resting_Duration': avg_night_resting_duration,
                        'BloodPreassure': blood_pressure
                    })
                    
        df_average_rate = pd.DataFrame(average_rate_records)
        df_average_rate = self.transform_date_format(df_average_rate, 'DateTime')
        return df_average_rate
  
    def join_and_save_combined_data(self):
        """
        Joins and saves all extracted data into complete and incomplete datasets.
        """
        self.logger.info("Joining all data tables...")
        data_frames = [
            self.extract_sleep_data(),
            self.extract_steps_data(),
            self.extract_calories_data(),
            self.extract_resting_heart_rate_data(),
            self.extract_spo2_data(),
            self.extract_heart_rate_zones_data(),
            self.extract_average_rate_data()
        ]
        
        non_empty_data_frames = [df for df in data_frames if not df.empty]
        
        if non_empty_data_frames:
            combined_df = reduce(lambda left, right: pd.merge(left, right, on=['Id_dispositivo', 'DateTime'], how='outer'), non_empty_data_frames)
            # Checking if 'logId' column is present
            if 'logId' not in combined_df.columns:
                combined_df['logId'] = None
        
            complete_mask = combined_df['logId'].notnull()
            complete_df = combined_df[complete_mask]
            incomplete_df = combined_df[~complete_mask]
                
            self.data_storage.save_data_to_csv(complete_df, 'Merged.csv')
            self.logger.info("Complete data table saved successfully.")
                
            self.data_storage.save_data_to_csv(incomplete_df, 'Registros_Incompletos.csv')
            self.logger.info("Incomplete data table saved successfully.")
        else:
            self.logger.error("No data available to merge.")
  
    def process_data(self):
        """
        Main function to process all the extracted data.
        """
        self.logger.info("Processing data...")
        
        df_sleep = self.extract_sleep_data()
        self.data_storage.save_data_to_csv(df_sleep, 'Sleep.csv')
        
        df_steps = self.extract_steps_data()
        self.data_storage.save_data_to_csv(df_steps, 'Steps.csv')

        df_calories = self.extract_calories_data()
        self.data_storage.save_data_to_csv(df_calories, 'Calories.csv')
        
        df_resting_heart_rate = self.extract_resting_heart_rate_data()
        self.data_storage.save_data_to_csv(df_resting_heart_rate, 'RestingHeartRate.csv')
        
        df_spo2 = self.extract_spo2_data()
        self.data_storage.save_data_to_csv(df_spo2, 'SPO2.csv')
        
        df_heart_rate_zones = self.extract_heart_rate_zones_data()
        self.data_storage.save_data_to_csv(df_heart_rate_zones, 'HeartRateData.csv')
        
        df_average_rate = self.extract_average_rate_data()
        self.data_storage.save_data_to_csv(df_average_rate, 'AverageRate.csv')
        
        self.join_and_save_combined_data()