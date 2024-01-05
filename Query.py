import Connection as cn
import setup as set

if __name__ == "__main__":

    PW = "<your password>"  # Put your MySQL terminal password here.
    ROOT = "root"
    DB = "ecommerce_data"
    # you like.
    LOCALHOST = "localhost"
    connection = cn.create_server_connection(LOCALHOST, ROOT, PW)

    # creating the schema in the DB
    connection_DB = cn.create_db_connection(LOCALHOST, ROOT, PW, DB)
# -------------------------------------------------------------------
    # QUERIES FOR UNDERSTANDING THE DATA MORE CLEARLY
    # 1. Aggregation nnd Grouping:

    ''' Total sales of any specific product'''
    query_1 = """
        SELECT SUM(quantity) AS total_sales 
        FROM orders 
        WHERE product_id = 3 ; """

    total_order_sales = cn.select_query(connection_DB, query_1)
    print(total_order_sales)

    '''Find the user with the highest number of orders:'''
    query_2 = """
        SELECT user_id, COUNT(order_id) AS order_count 
        FROM Orders GROUP BY user_id 
        ORDER BY order_count DESC LIMIT 1;
        """
    highest_number_order = cn.select_query(connection_DB, query_2)
    print(f"The highest number of order by user:'{highest_number_order}'")

    ''' Get the total number of orders placed in a January month'''
    query_3 = """
        SELECT COUNT(order_id) AS total_orders FROM Orders WHERE MONTH(order_date) = 1;
        """
    orders_january = cn.select_query(connection_DB, query_3)
    print(f"Number of Orders in January:'{orders_january}'")

    # 2. Joins and Relationships

    ''' Retrieve orders with associated user and product details:'''
    query_4 = """
        SELECT o.order_id, u.username, p.product_name
        FROM orders o
        INNER JOIN users u ON o.user_id = u.user_id
        INNER JOIN products p ON o.product_id = p.product_id;
        """
    user_product_details = cn.select_query(connection_DB, query_4)
    print("Orders with associated Users and Products detail:")
    print(user_product_details)

    '''List users who have not paid for orders: '''
    query_5 = """
        SELECT  u.username
        FROM users u
        LEFT JOIN orders o ON u.user_id = o.user_id
        WHERE o.payment_status = 'Not Paid';
        """
    users_no_order = cn.select_query(connection_DB, query_5)
    print("Users who have not paid for the orders:")
    print(users_no_order)

    ''' Find products that have never been ordered: '''
    query_6 = """
        SELECT p.product_id, p.product_name, count(o.product_id)
        FROM products p
        JOIN orders o ON p.product_id = o.product_id
        GROUP BY o.product_id 
        HAVING count(o.product_id) > 1;
        """
    more_than_one_order = cn.select_query(connection_DB, query_6)
    print("Products ordered more than once")
    print(more_than_one_order)

    # 3. Complex Queries

    ''' Identify users who have spent more than 1000 '''
    query_7 = """
            SELECT u.username, SUM(o.total_amount) AS total_spent
            FROM users u
            INNER JOIN orders o ON u.user_id = o.user_id
            GROUP BY u.user_id
            HAVING SUM(o.total_amount) > 1000;
            """
    amount_spent = cn.select_query(connection_DB, query_7)
    print("More than 1000 was spent by:")
    print(amount_spent)

    ''' Determine the most popular product based on the number of orders: '''
    query_8 = """
            SELECT o.product_id, p.product_name, COUNT(o.order_id) AS order_count
            FROM orders o
            JOIN products p
            ON o.product_id = p.product_id
            GROUP BY o.product_id
            ORDER BY order_count DESC
            LIMIT 1;
            """

    most_ordered = cn.select_query(connection_DB, query_8)
    print("Most Ordered Product is:")
    print(most_ordered)

    ''' Calculate average order quantity per user: '''
    query_9 = """
            SELECT o.user_id, u.username, AVG(o.quantity) AS avg_order_quantity
            FROM orders o 
            JOIN users u
            GROUP BY o.user_id;
            """
    avg_order = cn.select_query(connection_DB, query_9)
    print("Average Order:")
    print(avg_order)