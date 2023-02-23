from sqlite3 import Connection
from devices import Device
from smarthouse import Room
from typing import Optional, List, Dict, Tuple
from datetime import date, datetime


class SmartHousePersistence:

    def __init__(self, db_file: str):
        self.db_file = db_file
        self.connection = Connection(db_file)
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.connection.rollback()
        self.connection.close()

    def save(self):
        self.connection.commit()

    def reconnect(self):
        self.connection.close()
        self.connection = Connection(self.db_file)
        self.cursor = self.connection.cursor()

    def check_tables(self) -> bool:
        self.cursor.execute("SELECT name FROM sqlite_schema WHERE type = 'table';")
        result = set()
        for row in self.cursor.fetchall():
            result.add(row[0])
        return 'rooms' in result and 'devices' in result and 'measurements' in result


class SmartHouseAnalytics:

    def __init__(self, persistence: SmartHousePersistence):
        self.persistence = persistence

    def get_most_recent_sensor_reading(self, sensor: Device) -> Optional[float]:
        """
        Retrieves the most recent (i.e. current) value reading for the given
        sensor device.
        Function may return None if the given device is an actuator or
        if there are no sensor values for the given device recorded in the database.
        """
        self.persistence.cursor.execute(f"SELECT value FROM measurements WHERE device = {sensor.db_id} ORDER BY datetime(time_stamp) DESC limit 1;")
        result = self.persistence.cursor.fetchone()
        if result:
            return result[0]
        else:
            return None

    def get_coldest_room(self) -> Optional[str]:
        """
        Retrieves name of the room, which has the lowest temperature on average.
        """
        self.persistence.cursor.execute("SELECT r.name, min(value) FROM measurements m inner join devices d on d.id = m.device inner JOIN rooms r on r.id = d.room ")
        result = self.persistence.cursor.fetchone()
        if result:
            return result[0]
        else:
            return None

    def get_sensor_readings_in_timespan(self, sensor: Device, from_ts: datetime, to_ts: datetime) -> List[float]:
        """
        Returns a list of sensor measurements (float values) for the given device in the given timespan.
        """
        self.persistence.cursor.execute(f"""
        SELECT value FROM measurements m 
        WHERE device = {sensor.db_id} AND DATETIME(time_stamp) 
        BETWEEN DATETIME('{from_ts}') AND DATETIME('{to_ts}')""")
        results = self.persistence.cursor.fetchall()
        return [t[0] for t in results]

    def describe_temperature_in_rooms(self) -> Dict[str, Tuple[float, float, float]]:
        """
        Returns a dictionary where the key are room names and the values are triples
        containing three floating point numbers:
        - The first component [index=0] being the _minimum_ temperature of the room.
        - The second component [index=1] being the _maximum_ temperature of the room.
        - The third component [index=2] being the _average_ temperature of the room.

        This function can be seen as a simplified version of the DataFrame.describe()
        function that exists in Pandas:
        https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.describe.html?highlight=describe
        """
        self.persistence.cursor.execute("""
        SELECT r.name, min(value), max(value), avg(value) 
        FROM measurements m 
        INNER JOIN devices d ON m.device = d.id 
        INNER JOIN rooms r ON r.id = d.room 
        WHERE d.type = 'Temperatursensor' 
        GROUP BY r.name;""")
        rows = self.persistence.cursor.fetchall()
        result = {}
        for row in rows:
            result[row[0]] = (row[1], row[2], row[3])
        return result

    def get_hours_when_humidity_above_average(self, room_name: str, day: date) -> List[int]:
        """
        This function determines during which hours of the given day
        there were more than three measurements in that hour having a humidity measurement that is above
        the average recorded humidity in that room at that particular time.
        The result is a (possibly empty) list of number representing hours [0-23].
        """
        self.persistence.cursor.execute(f"""
        SELECT  STRFTIME('%H', DATETIME(m.time_stamp)) AS hours 
        FROM measurements m 
        INNER JOIN devices d ON m.device = d.id 
        INNER JOIN rooms r ON r.id = d.room 
        WHERE 
        r.name = '{room_name}'
        AND d.type = 'Fuktighetssensor' 
        AND DATE(time_stamp) = DATE('{day}')
        AND m.value > (SELECT AVG(value) FROM measurements m WHERE m.device = 21 AND DATE(time_stamp) = DATE('{day}'))
        GROUP BY hours
        HAVING COUNT(m.value) > 3;
        """)
        rows = self.persistence.cursor.fetchall()
        return [int(r[0]) for r in rows]
