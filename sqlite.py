import sqlite3
import os


def build_db():
    db_path = 'shop.db'
    if os.path.exists(db_path):
        return # Database already exists
    
    # Connect to SQLite database (if it doesn't exist, it will be created)
    conn = sqlite3.connect('shop.db')

    # Create a cursor object using the cursor method
    cursor = conn.cursor()

    # Create the 'Cameras' table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Cameras (
        camera_id INTEGER PRIMARY KEY,
        model_name TEXT NOT NULL,
        resolution TEXT,
        lens_type TEXT,
        price REAL
    )
    ''')

    # Create the 'Customers' table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Customers (
        customer_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        address TEXT
    )
    ''')

    # Create the 'Orders' table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        camera_id INTEGER,
        quantity_ordered INTEGER,
        FOREIGN KEY (customer_id) REFERENCES Customers (customer_id),
        FOREIGN KEY (camera_id) REFERENCES Cameras (camera_id)
    )
    ''')

    cameras_data = [
        (1, 'Canon EOS 1500D', '24.1MP', 'EF-S', 350.00),
        (2, 'Nikon D3500', '24.2MP', 'AF-P DX', 400.00),
        (3, 'Sony Alpha A6000', '24.3MP', 'E-mount', 480.00)
    ]
    cursor.executemany('INSERT INTO Cameras VALUES (?,?,?,?,?)', cameras_data)

    # Insert dummy data into 'Customers'
    customers_data = [
        (1, 'John Doe', 'john@example.com', '123 Elm Street'),
        (2, 'Jane Smith', 'jane@example.com', '456 Oak Street'),
        (3, 'Emily Johnson', 'emily@example.com', '789 Maple Street')
    ]
    cursor.executemany('INSERT INTO Customers VALUES (?,?,?,?)', customers_data)

    # Insert dummy data into 'Orders'
    orders_data = [
        (1, 1, 1, 2),  # John Doe ordered 2 Canon EOS 1500D cameras
        (2, 2, 2, 1),  # Jane Smith ordered 1 Nikon D3500 camera
        (3, 3, 3, 1)   # Emily Johnson ordered 1 Sony Alpha A6000 camera
    ]
    cursor.executemany('INSERT INTO Orders VALUES (?,?,?,?)', orders_data)

    conn.commit()
    conn.close()

class Database:
    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.connection.isolation_level = None  # This line is to enable manual transaction control
        self.cursor = self.connection.cursor()

    def execute(self, query, params=None)-> bool:
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

def get_row_count( db, table_name):
    query = f"SELECT COUNT(*) FROM {table_name};"
    return db.fetchone(query)[0]



def transaction_1(db, name, email, address):
    return db.execute("INSERT INTO Customers (name, email, address) VALUES (?, ?, ?);", (name, email, address))

def transaction_2(db, model_name, resolution, lens_type, price):
    return db.execute("INSERT INTO Cameras (model_name, resolution, lens_type, price) VALUES (?, ?, ?, ?);", (model_name, resolution, lens_type, price))

def transaction_3_hop1(db, customer_id):
    customer_count = db.fetchone("SELECT COUNT(*) FROM Customers WHERE customer_id = ?;", (customer_id,))[0]
    return customer_count > 0

def transaction_3_hop2(db, customer_id, quantity):
    return db.execute("INSERT INTO Orders (customer_id, camera_id, quantity_ordered) VALUES (?, ?, ?);", (customer_id, 1, quantity))  # Assuming a default camera_id of 1

def transaction_4_hop1(db, camera_id):
    camera_count = db.fetchone("SELECT COUNT(*) FROM Cameras WHERE camera_id = ?;", (camera_id,))[0]
    return camera_count > 0

def transaction_4_hop2(db, camera_id, quantity):
    return db.execute("INSERT INTO Orders (customer_id, camera_id, quantity_ordered) VALUES (?, ?, ?);", (1, camera_id, quantity))  # Assuming a default customer_id of 1

def transaction_5(db, customer_id):
    address = db.fetchone("SELECT address FROM Customers WHERE customer_id = ?;", (customer_id,))
    if address:
        print(address)
        return True
    return False

def transaction_6(db, camera_id):
    price = db.fetchone("SELECT price FROM Cameras WHERE camera_id = ?;", (camera_id,))
    if price:
        print(price)
        return True
    return False

def transaction_7(db, order_id):
    order = db.fetchall("SELECT * FROM Orders WHERE order_id = ?;", (order_id,))
    print('order info:',order)
    return bool(order)



