import sqlite3
import os
import uuid
import random

CUSTOMER_ID_LOOKUP = (
    set()
)  

CUSTOMER_ID_COUNTER = 0


async def build_db(CURRENT_SERVER_TYPE):
    db_directory = "./database"
    if not os.path.exists(db_directory):
        os.makedirs(db_directory)
    db_path = f"./database/{CURRENT_SERVER_TYPE}.db"
    if os.path.exists(db_path):
        # return  # Database already exists
        os.remove(db_path)
        # return # Database already exists

    # Connect to SQLite database (if it doesn't exist, it will be created)
    conn = sqlite3.connect(f"./database/{CURRENT_SERVER_TYPE}.db")

    # Create a cursor object using the cursor method
    cursor = conn.cursor()
    if CURRENT_SERVER_TYPE == "w":
        # Create the 'Cameras' table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS Cameras (
            camera_id INTEGER PRIMARY KEY,
            model_name TEXT NOT NULL,
            resolution TEXT,
            lens_type TEXT,
            price REAL
        )
        """
        )
        # Create the 'Orders' table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS Orders (
            order_id INTEGER PRIMARY KEY,
            quantity_ordered INTEGER,
            customer_id Customers , -- FOREIGN KEY (customer_id) REFERENCES Customers (customer_id),
            camera_id Cameras --FOREIGN KEY (camera_id) REFERENCES Cameras (camera_id)
        )
        """
        )
        cameras_data = [
            (
                i,
                f"Camera Model {i}",
                random.choice(["1080p", "4K", "720p"]),
                random.choice(["wide", "telephoto", "standard"]),
                i * 100,
            )
            for i in range(1, 11)
        ]
        cursor.executemany("INSERT INTO Cameras VALUES (?,?,?,?,?)", cameras_data)

        orders_data = [
            (i, random.randint(10, 100), random.randint(1, 6), random.randint(1, 6))
            for i in range(1, 11)
        ]
        cursor.executemany("INSERT INTO Orders VALUES (?,?,?,?)", orders_data)

        # Print the contents of Cameras and Orders tables
        # cursor.execute('SELECT * FROM Cameras')
        # print("Cameras Table:")
        # for row in cursor.fetchall():
        #     print(row)

        # cursor.execute('SELECT * FROM Orders')
        # print("\nOrders Table:")
        # for row in cursor.fetchall():
        #     print(row)

    elif CURRENT_SERVER_TYPE in ["c1", "c2"]:
        # Create the 'Customers' table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS Customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            address TEXT
        )
        """
        )
        global CUSTOMER_ID_COUNTER, CUSTOMER_ID_LOOKUP
        if CURRENT_SERVER_TYPE == "c1":
            customers_data = [
                (
                    i,
                    f"Customer {i}",
                    f"customer{i}@example.com",
                    f"{i * 100} Main Street",
                )
                for i in [1, 3, 5]
            ]
            CUSTOMER_ID_LOOKUP = set([1, 3, 5])
            CUSTOMER_ID_COUNTER = 7
        else:
            customers_data = [
                (
                    i,
                    f"Customer {i}",
                    f"customer{i}@example.com",
                    f"{i * 100} Main Street",
                )
                for i in [2, 4, 6]
            ]
            CUSTOMER_ID_LOOKUP = set([2, 4, 6])
            CUSTOMER_ID_COUNTER = 8

        cursor.executemany("INSERT INTO Customers VALUES (?,?,?,?)", customers_data)

        # Print the contents of the Customers table
        # cursor.execute("SELECT * FROM Customers")
        # print("Customers Table:")
        # for row in cursor.fetchall():
        #     print(row)

    conn.commit()
    conn.close()


class Database:
    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.connection.isolation_level = (
            None  # This line is to enable manual transaction control
        )
        self.cursor = self.connection.cursor()

    def execute(self, query, params=None) -> bool:
        try:
            self.cursor.execute("BEGIN;")
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.cursor.execute("COMMIT;")
            return True
        except sqlite3.Error as e:
            print(f"An SQL error occurred: {e}")
            self.cursor.execute("ROLLBACK;")
            return False

    def fetchone(self, query, params=None):
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)  # 没有 params
        return self.cursor.fetchone()

    def fetchall(self, query, params=None):
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def close(self):
        self.connection.close()


def get_row_count(db, table_name):
    query = f"SELECT COUNT(*) FROM {table_name};"
    return db.fetchone(query)[0]


# customers_data = [(i, f'Customer {i}', f'customer{i}@example.com', f'{i * 100} Main Street') for i in [2,4,6]]

# cursor.executemany('INSERT INTO Customers VALUES (?,?,?,?)', customers_data)


def transaction_1(db, name, email, address):
    global CUSTOMER_ID_COUNTER, CUSTOMER_ID_LOOKUP
    db.execute(
        "INSERT INTO Customers VALUES (?, ?, ?, ?);",
        (CUSTOMER_ID_COUNTER, name, email, address),
    )
    CUSTOMER_ID_LOOKUP.add(CUSTOMER_ID_COUNTER)
    # print('SQL info: T1 successed, customer_count',get_row_count(db,'Customers'), 'CUSTOMER_ID_COUNTER: ',CUSTOMER_ID_COUNTER, 'CUSTOMER_ID_LOOKUP: ', CUSTOMER_ID_LOOKUP)
    CUSTOMER_ID_COUNTER += 2
    return True


def transaction_2(db, model_name, resolution, lens_type, price):
    db.execute(
        "INSERT INTO Cameras (model_name, resolution, lens_type, price) VALUES (?, ?, ?, ?);",
        (model_name, resolution, lens_type, price),
    )
    # print("SQL info: T2 successed, customer_count", get_row_count(db, "Cameras"))
    return True


def transaction_3_hop1(db, customer_id):
    customer_count = db.fetchone(
        "SELECT COUNT(*) FROM Customers WHERE customer_id = ?;", (customer_id,)
    )[0]
    # if not customer_count:
    #     print("SQL info: T3_1 failed, customer_id: ", customer_id)
    # print('SQL info: T3_1 successed, customer_count: ', customer_count)
    return True


def transaction_3_hop2(db, customer_id, quantity):
    result = db.execute(
        "INSERT INTO Orders (customer_id, camera_id, quantity_ordered) VALUES (?, ?, ?);",
        (customer_id, 1, quantity),
    )  # Assuming a default camera_id of 1
    # if not result:
    #     print("SQL info: T3_2 failed, customer_id: ", customer_id)
    # print('SQL info: T3_2 successed,  result: ', result)
    return True


def transaction_4_hop1(db, camera_id):
    camera_count = db.fetchone(
        "SELECT COUNT(*) FROM Cameras WHERE camera_id = ?;", (camera_id,)
    )[0]
    # print('SQL info: T4_1 successed, camera_count info: ', camera_count)
    return True


def transaction_4_hop2(db, camera_id, quantity):
    result = db.execute(
        "INSERT INTO Orders (customer_id, camera_id, quantity_ordered) VALUES (?, ?, ?);",
        (1, camera_id, quantity),
    )  # Assuming a default customer_id of 1
    # print('SQL info: T4_2 successed, result info:', result)
    return True


def transaction_5(db, customer_id):
    address = db.fetchone(
        "SELECT address FROM Customers WHERE customer_id = ?;", (customer_id,)
    )
    # print('SQL info: T7 successed, address info:',address)
    return True


def transaction_6(db, camera_id):
    price = db.fetchone("SELECT price FROM Cameras WHERE camera_id = ?;", (camera_id,))
    # print('SQL info: T7 successed, price info:',price)
    return True


def transaction_7(db, order_id):
    order = db.fetchall("SELECT * FROM Orders WHERE order_id = ?;", (order_id,))
    # print('SQL info: T7 successed, order info:',order)
    return True
