import pandas as pd
import sqlite3
import datetime
from memory_profiler import profile
import time



class DataProcessor:
    def __init__(self, db_file):
        self.db_file = db_file

    @profile(stream=open('memory_profile.txt', 'w+'))
    def process_data(self, date):
        try:
            # Connect to the database
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()

            # Create the metatable if it doesn't exist
            cursor.execute('''CREATE TABLE IF NOT EXISTS metatable (
                                timestamp INTEGER,
                                error_id TEXT,
                                player_id INTEGER,
                                json_server TEXT,
                                event_id INTEGER,
                                json_client TEXT
                            )''')

            client_data = self.load_csv_data('client.csv', date)
            server_data = self.load_csv_data('server.csv', date)

            combined_data = self.combine_data(client_data, server_data)

            filtered_data = self.filter_cheaters(combined_data, cursor)

            self.insert_into_metatable(filtered_data, cursor)

            conn.commit()
            conn.close()
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")

    def load_csv_data(self, file_name, date):
        df = pd.read_csv(file_name)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df = df[df['timestamp'].dt.date == date]
        return df.values.tolist()

    def combine_data(self, client_data, server_data):
        client_df = pd.DataFrame(client_data, columns=['timestamp',  'error_id', 'player_id', 'description'])
        client_df["timestamp"] = client_df["timestamp"].apply(lambda x: int(x.timestamp()))

        server_df = pd.DataFrame(server_data, columns=['timestamp', 'event_id', 'error_id', 'description'])
        server_df["timestamp"] = server_df["timestamp"].apply(lambda x: int(x.timestamp()))

        combined_df = pd.merge(client_df, server_df, on='error_id')
        combined_df = combined_df.drop(combined_df.columns[[4]], axis=1)
        combined_data = combined_df.values.tolist()
        return combined_data

    def filter_cheaters(self, combined_data, cursor):
        filtered_data = []
        for row in combined_data:
            player_id = row[2]
            ban_time = self.get_ban_time(player_id, cursor)
            if not self.is_banned(ban_time, row[0]):
                filtered_data.append(row)
        return filtered_data

    def get_ban_time(self, player_id, cursor):
        cursor.execute("SELECT ban_time FROM cheaters WHERE player_id = ?", (player_id,))
        result = cursor.fetchone()
        if result:
            return result[0]
        return None

    def is_banned(self, ban_time, timestamp):
        if ban_time is None:
            return False
        ban_date = datetime.datetime.strptime(ban_time, "%Y-%m-%d %H:%M:%S").date()
        check_date = datetime.datetime.fromtimestamp(timestamp).date() - datetime.timedelta(days=1)
        return ban_date >= check_date

    def insert_into_metatable(self, data, cursor):
        cursor.executemany("INSERT INTO metatable VALUES (?, ?, ?, ?, ?, ?)", data)



db_file = "cheaters.db"
processor = DataProcessor(db_file)
time_a = time.time()

processor.process_data(datetime.date(2021, 1, 1))
delta = time.time() - time_a
print(delta)
with open('memory_profile.txt', 'r') as f:
    print(f.read())