from datetime import date, timedelta
import time

class TimeRangeProvider:
    def __init__(self, debug = False):
        self.mondays = self.get_all_weekdays(0)
        self.tuesdays = self.get_all_weekdays(1)
        self.wednesdays = self.get_all_weekdays(2)
        self.thursdays = self.get_all_weekdays(3)
        self.fridays = self.get_all_weekdays(4)
        self.saturdays = self.get_all_weekdays(5)
        self.sundays = self.get_all_weekdays(6)
        
        self.mondays_timestamp_ranges = self.get_timestamp_ranges(self.mondays)
        self.tuesdays_timestamp_ranges = self.get_timestamp_ranges(self.tuesdays)
        self.wednesdays_timestamp_ranges = self.get_timestamp_ranges(self.wednesdays)
        self.thursdays_timestamp_ranges = self.get_timestamp_ranges(self.thursdays)
        self.fridays_timestamp_ranges = self.get_timestamp_ranges(self.fridays)
        self.saturdays_timestamp_ranges = self.get_timestamp_ranges(self.saturdays)
        self.sundays_timestamp_ranges = self.get_timestamp_ranges(self.sundays)
        
        if debug:
            self.print_debug()
            
    def print_debug(self):
        print("Mondays: ", self.mondays)
        print("Monday ranges: ", self.mondays_timestamp_ranges)
        print("Tuesdays: ", self.tuesdays)
        print("Tuesday ranges: ", self.tuesdays_timestamp_ranges)
        print("Wednesdays: ", self.wednesdays)
        print("Wednesday ranges: ", self.wednesdays_timestamp_ranges)
        print("Thursdays: ", self.thursdays)
        print("Thursday ranges: ", self.thursdays_timestamp_ranges)
        print("Fridays: ", self.fridays)
        print("Friday ranges: ", self.fridays_timestamp_ranges)
        print("Saturdays: ", self.saturdays)
        print("Saturday ranges: ", self.saturdays_timestamp_ranges)
        print("Sundays: ", self.sundays)
        print("Sunday ranges: ", self.sundays_timestamp_ranges)

    def get_all_weekdays(self, day):
        """
        Returns all weekdays in september and october 2023
        day:
        0 = Monday,
        1 = Tuesday,
        2 = Wednesday,
        3 = Thursday,
        4 = Friday,
        5 = Saturday,
        6 = Sunday
        """
        days = []
        d = date(2023, 9, 1) # September 1st 
        delta = day - d.weekday()
        if delta < 0:
            delta += 7
        d += timedelta(days = delta)  # First weekday we are looking for
        while d.month == 9 or d.month == 10:
            days.append(d)
            d += timedelta(days = 7)
        return days
    
    def get_timestamp_ranges(self, days):
        """
        Returns a list of tuples with start and end timestamps for each day
        """
        timestamp_ranges = []
        for day in days:
            midnight_start = time.mktime(day.timetuple())
            end = day + timedelta(days = 1)
            midnight_end = time.mktime(end.timetuple())
            timestamp_ranges.append((midnight_start, midnight_end))
        return timestamp_ranges
    
    def get_example_hour_timestamp_range(self):
        """
        Returns the noon hour of 4th of October 2023
        """
        day = date(2023, 10, 4)
        noon_start = time.mktime(day.timetuple()) + 12 * 60 * 60
        noon_end = noon_start + 60 * 60
        return (noon_start, noon_end)
        