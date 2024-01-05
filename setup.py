import csv
import Connection as cn

PW = "<your password>"  # Put your MySQL Terminal password here.
ROOT = "root"
DB = "ecommerce_data"  # name of the database we will create in the next step
LOCALHOST = "localhost"  # considering you have installed MySQL server on your computer

RELATIVE_CONFIG_PATH = '<path of the file containing the csv files>'

USERS = 'users'
PRODUCTS = 'products'
ORDERS = 'orders'

connection = cn.create_server_connection(LOCALHOST, ROOT, PW)

# creating the schema in the DB
cn.create_and_switch_database(connection, DB, DB)

# Create the tables through python code here
create_users_table = """
    CREATE TABLE users (
        user_id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(50) NOT NULL,
        email VARCHAR(100) NOT NULL,
        password VARCHAR(100) NOT NULL,
        address VARCHAR(255),
        phone_number VARCHAR(20)
    )"""

create_orders_table = """
    CREATE TABLE orders (
        order_id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT,
        product_id INT,
        quantity INT,
        order_date DATE,
        total_amount DECIMAL(10, 2),
        payment_status VARCHAR(20),
        FOREIGN KEY (user_id) REFERENCES Users(user_id),
        FOREIGN KEY (product_id) REFERENCES Products(product_id)
	) """

create_products_table = """
    CREATE TABLE products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    description TEXT,
    category VARCHAR(50),
    stock_quantity INT
    )"""

cn.create_table(connection, create_users_table)
print("User table created")

cn.create_table(connection, create_products_table)
print("Products table created!")

cn.create_table(connection, create_orders_table)
print("Orders table created!")


with open(RELATIVE_CONFIG_PATH + USERS + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))

    sql = '''
    INSERT INTO users (user_id, username, email, password, address, phone_number)
    VALUES( %s, %s, %s, %s, %s, %s)
    '''
    val.pop(0)
    cn.insert_many_records(connection, sql, val)
    print("User Data Inserted!\n")


with open(RELATIVE_CONFIG_PATH + PRODUCTS + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    sql = '''
    INSERT INTO products (product_id,product_name,price,description,category, stock_quantity)
    VALUES( %s, %s, %s, %s, %s, %s)
    '''
    val.pop(0)
    cn.insert_many_records(connection, sql, val)
    print("Products Data Inserted!\n")


with open(RELATIVE_CONFIG_PATH + ORDERS + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))

    sql = '''
    INSERT INTO orders (order_id,user_id,product_id,quantity,order_date,total_amount,payment_status)
    VALUES( %s, %s, %s, %s, %s, %s, %s)
    '''

    val.pop(0)
    cn.insert_many_records(connection, sql, val)
    print("Orders Data Inserted!\n")

