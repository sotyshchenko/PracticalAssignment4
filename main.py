import duckdb
import uuid
from faker import Faker
import random


fake = Faker()


def create_connection():
    """Create a DuckDB database connection"""
    try:
        connection = duckdb.connect('bookrestaurant_duckdb.duckdb')
        print("Connection to DuckDB successful")
        return connection
    except Exception as e:
        print(f"The error '{e}' occurred")
        return None


def execute_many_queries(connection, query, data, table_name):
    """Execute multiple queries"""
    try:
        print(f"Inserting into {table_name}...")
        connection.executemany(query, data)
        print(f"Inserted into {table_name}.")
    except Exception as e:
        print(f"The error '{e}' occurred")


def create_tables(connection):
    """Create tables in DuckDB"""
    try:

        connection.execute('DROP TABLE IF EXISTS raw_authors_books')
        connection.execute('DROP TABLE IF EXISTS raw_book_orders')
        connection.execute('DROP TABLE IF EXISTS raw_menu_orders')
        connection.execute('DROP TABLE IF EXISTS raw_shifts')
        connection.execute('DROP TABLE IF EXISTS raw_salary')
        connection.execute('DROP TABLE IF EXISTS raw_authors')
        connection.execute('DROP TABLE IF EXISTS raw_books')
        connection.execute('DROP TABLE IF EXISTS raw_menu')
        connection.execute('DROP TABLE IF EXISTS raw_menu_categories')
        connection.execute('DROP TABLE IF EXISTS raw_customers')
        connection.execute('DROP TABLE IF EXISTS raw_employees')

        # Create raw_customers table
        connection.execute('''
            CREATE TABLE IF NOT EXISTS raw_customers (
                id VARCHAR PRIMARY KEY,
                first_name VARCHAR,
                last_name VARCHAR,
                email VARCHAR,
                phone VARCHAR,
                birth_date DATE,
                regular BOOLEAN
            )
        ''')
        print("Table raw_customers created successfully")

        # Create raw_employees table
        connection.execute('''
            CREATE TABLE IF NOT EXISTS raw_employees (
                id VARCHAR PRIMARY KEY,
                first_name VARCHAR,
                last_name VARCHAR,
                email VARCHAR,
                phone VARCHAR,
                birth_date DATE,
                position VARCHAR
            )
        ''')
        print("Table raw_employees created successfully")

        # Create raw_shifts table
        connection.execute('''
            CREATE TABLE IF NOT EXISTS raw_shifts (
                shift_id VARCHAR PRIMARY KEY,
                employee_id VARCHAR,
                shift_name VARCHAR,
                start_time TIME,
                end_time TIME,
                FOREIGN KEY(employee_id) REFERENCES raw_employees(id)
            )
        ''')
        print("Table raw_shifts created successfully")

        # Create raw_salary table
        connection.execute('''
            CREATE TABLE IF NOT EXISTS raw_salary (
                employee_id VARCHAR,
                salary_amount DECIMAL,
                salary_date DATE,
                FOREIGN KEY(employee_id) REFERENCES raw_employees(id)
            )
        ''')
        print("Table raw_salary created successfully")

        # Create raw_authors table
        connection.execute('''
            CREATE TABLE IF NOT EXISTS raw_authors (
                id VARCHAR PRIMARY KEY,
                first_name VARCHAR,
                last_name VARCHAR,
                biography TEXT
            )
        ''')
        print("Table raw_authors created successfully")

        # Create raw_books table
        connection.execute('''
            CREATE TABLE IF NOT EXISTS raw_books (
                id VARCHAR PRIMARY KEY,
                title VARCHAR,
                genre VARCHAR,
                price DECIMAL,
                isbn VARCHAR,
                publication_year INTEGER
            )
        ''')
        print("Table raw_books created successfully")

        # Create raw_authors_books table
        connection.execute('''
            CREATE TABLE IF NOT EXISTS raw_authors_books (
                author_id VARCHAR,
                book_id VARCHAR,
                FOREIGN KEY(author_id) REFERENCES raw_authors(id),
                FOREIGN KEY(book_id) REFERENCES raw_books(id)
            )
        ''')
        print("Table raw_authors_books created successfully")

        # Create raw_book_orders table
        connection.execute('''
            CREATE TABLE IF NOT EXISTS raw_book_orders (
                id VARCHAR PRIMARY KEY,
                book_id VARCHAR,
                customer_id VARCHAR,
                handled_by VARCHAR,
                order_date DATE,
                status VARCHAR,
                quantity INT,
                FOREIGN KEY(book_id) REFERENCES raw_books(id),
                FOREIGN KEY(customer_id) REFERENCES raw_customers(id),
                FOREIGN KEY(handled_by) REFERENCES raw_employees(id)
            )
        ''')
        print("Table raw_book_orders created successfully")

        # Create raw_menu_categories table
        connection.execute('''
            CREATE TABLE IF NOT EXISTS raw_menu_categories (
                id VARCHAR PRIMARY KEY,
                name VARCHAR,
                description TEXT
            )
        ''')
        print("Table raw_menu_categories created successfully")

        # Create raw_menu table
        connection.execute('''
            CREATE TABLE IF NOT EXISTS raw_menu (
                id VARCHAR PRIMARY KEY,
                name VARCHAR,
                description TEXT,
                price DECIMAL,
                category_id VARCHAR,
                is_vegan BOOLEAN,
                is_gluten_free BOOLEAN,
                ingredients TEXT,
                date_added DATE,
                is_available BOOLEAN,
                preparation_time INTEGER,
                calories INTEGER,
                image_url TEXT,
                spicy_level INTEGER,
                allergens TEXT,
                FOREIGN KEY(category_id) REFERENCES raw_menu_categories(id)
            )
        ''')
        print("Table raw_menu created successfully")

        # Create raw_menu_orders table
        connection.execute('''
            CREATE TABLE IF NOT EXISTS raw_menu_orders (
                id VARCHAR PRIMARY KEY,
                order_date TIMESTAMP,
                customer_id VARCHAR,
                menu_id VARCHAR,
                quantity INTEGER,
                employee_id VARCHAR,
                order_status VARCHAR,
                FOREIGN KEY(customer_id) REFERENCES raw_customers(id),
                FOREIGN KEY(menu_id) REFERENCES raw_menu(id),
                FOREIGN KEY(employee_id) REFERENCES raw_employees(id)
            )
        ''')
        print("Table raw_menu_orders created successfully")

    except Exception as e:
        print(f"The error '{e}' occurred during table creation")


def insert_data():
    connection = create_connection()

    if connection is None:
        return

    # Create tables
    create_tables(connection)

    # Insert data into raw_customers table
    customer_insert_query = """
    INSERT INTO raw_customers (id, first_name, last_name, email, phone, birth_date, regular) 
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    customers_data = [
        (str(uuid.uuid4()), fake.first_name(), fake.last_name(), fake.email(), fake.phone_number(),
         fake.date_of_birth(minimum_age=14, maximum_age=100), random.choice([True, False]))
        for _ in range (20000)
    ]
    execute_many_queries(connection, customer_insert_query, customers_data, "raw_customers")

    try:
        connection.execute("COPY (SELECT * FROM raw_customers) TO 'seeds/raw_customers.csv' (HEADER, DELIMITER ',');")
        print("Data exported to raw_customers.csv")
    except Exception as e:
        print(f"The error '{e}' occurred during data export")

    # Insert data into raw_employees table
    employee_insert_query = """
    INSERT INTO raw_employees (id, first_name, last_name, email, phone, birth_date, position) 
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    positions = ['Manager', 'Cashier', 'Chef', 'Waiter', 'Cleaner']
    employees_data = [
        (str(uuid.uuid4()), fake.first_name(), fake.last_name(), fake.email(), fake.phone_number(),
         fake.date_of_birth(minimum_age=18, maximum_age=65), random.choice(positions))
        for _ in range(5000)
    ]
    execute_many_queries(connection, employee_insert_query, employees_data, "raw_employees")

    try:
        connection.execute("COPY (SELECT * FROM raw_employees) TO 'seeds/raw_employees.csv' (HEADER, DELIMITER ',');")
        print("Data exported to raw_employees.csv")
    except Exception as e:
        print(f"The error '{e}' occurred during data export")

    # Insert data into raw_shifts table
    shifts_insert_query = """
        INSERT INTO raw_shifts (shift_id, employee_id, shift_name, start_time, end_time) 
        VALUES (?, ?, ?, ?, ?)
    """
    employee_ids = [employee[0] for employee in employees_data]
    shifts_data = [
        (str(uuid.uuid4()), random.choice(employee_ids), fake.word(),
         f"{start_hour:02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}",
         f"{end_hour:02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
         )
        for _ in range(10000)
        for start_hour in [random.randint(0, 20)]
        for end_hour in [random.randint(start_hour + 3, 23)]
    ]
    execute_many_queries(connection, shifts_insert_query, shifts_data, "raw_shifts")

    try:
        connection.execute("COPY (SELECT * FROM raw_shifts) TO 'seeds/raw_shifts.csv' (HEADER, DELIMITER ',');")
        print("Data exported to raw_shifts.csv")
    except Exception as e:
        print(f"The error '{e}' occurred during data export")

    # Insert data into raw_salary table
    salaries_insert_query = """
        INSERT INTO raw_salary (employee_id, salary_amount, salary_date) 
        VALUES (?, ?, ?)
    """
    salaries_data = [
        (employee[0], round(random.uniform(1000, 10000), 2), fake.date_this_year())
        for employee in employees_data
    ]
    execute_many_queries(connection, salaries_insert_query, salaries_data, "raw_salary")

    try:
        connection.execute("COPY (SELECT * FROM raw_salary) TO 'seeds/raw_salary.csv' (HEADER, DELIMITER ',');")
        print("Data exported to raw_salary.csv")
    except Exception as e:
        print(f"The error '{e}' occurred during data export")

    # Insert data into raw_authors table
    author_insert_query = """
    INSERT INTO raw_authors (id, first_name, last_name, biography) 
    VALUES (?, ?, ?, ?)
    """
    authors_data = [
        (str(uuid.uuid4()), fake.first_name(), fake.last_name(), fake.text(max_nb_chars=200))
        for _ in range(15000)
    ]
    execute_many_queries(connection, author_insert_query, authors_data, "raw_authors")

    try:
        connection.execute("COPY (SELECT * FROM raw_authors) TO 'seeds/raw_authors.csv' (HEADER, DELIMITER ',');")
        print("Data exported to raw_authors.csv")
    except Exception as e:
        print(f"The error '{e}' occurred during data export")

    # Insert data into raw_books table
    book_insert_query = """
    INSERT INTO raw_books (id, title, genre, price, isbn, publication_year) 
    VALUES (?, ?, ?, ?, ?, ?)
    """
    genres = ['Dystopian', 'Fantasy', 'Science Fiction', 'Non-Fiction', 'Mystery']
    books_data = [
        (str(uuid.uuid4()), fake.catch_phrase(), random.choice(genres), round(random.uniform(5.99, 29.99), 2),
         fake.isbn13(), fake.year())
        for _ in range(40000)
    ]
    execute_many_queries(connection, book_insert_query, books_data, "raw_books")

    try:
        connection.execute("COPY (SELECT * FROM raw_books) TO 'seeds/raw_books.csv' (HEADER, DELIMITER ',');")
        print("Data exported to raw_books.csv")
    except Exception as e:
        print(f"The error '{e}' occurred during data export")

    # Insert data into raw_authors_books table
    authors_books_query = """
    INSERT INTO raw_authors_books (author_id, book_id) 
    VALUES (?, ?)
    """
    authors_books_data = [
        (random.choice(authors_data)[0], random.choice(books_data)[0])
        for _ in range(40000)
    ]
    execute_many_queries(connection, authors_books_query, authors_books_data, "raw_authors_books")

    try:
        connection.execute("COPY (SELECT * FROM raw_authors_books) TO 'seeds/raw_authors_books.csv' (HEADER, DELIMITER ',');")
        print("Data exported to raw_authors_books.csv")
    except Exception as e:
        print(f"The error '{e}' occurred during data export")

    # Insert data into raw_book_orders table
    book_orders_query = """
    INSERT INTO raw_book_orders (id, book_id, customer_id, handled_by, order_date, status, quantity) 
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    order_statuses = ['Completed', 'Pending', 'Cancelled']
    book_orders_data = [
        (str(uuid.uuid4()), random.choice(books_data)[0], random.choice(customers_data)[0],
         random.choice(employees_data)[0], fake.date_this_year(), random.choice(order_statuses), random.randint(1, 100))
        for _ in range(70000)
    ]
    execute_many_queries(connection, book_orders_query, book_orders_data, "raw_book_orders")

    try:
        connection.execute("COPY (SELECT * FROM raw_book_orders) TO 'seeds/raw_book_orders.csv' (HEADER, DELIMITER ',');")
        print("Data exported to raw_book_orders.csv")
    except Exception as e:
        print(f"The error '{e}' occurred during data export")


    # Insert data into raw_menu_categories table
    menu_categories_query = """
    INSERT INTO raw_menu_categories (id, name, description) 
    VALUES (?, ?, ?)
    """
    category_names = ['Appetizers', 'Entrees', 'Sides', 'Desserts', 'Beverages']
    menu_categories_data = [
        (str(uuid.uuid4()), name, fake.text(max_nb_chars=100))
        for name in category_names
    ]
    execute_many_queries(connection, menu_categories_query, menu_categories_data, "raw_menu_categories")

    try:
        connection.execute("COPY (SELECT * FROM raw_menu_categories) TO 'seeds/raw_menu_categories.csv' (HEADER, DELIMITER ',');")
        print("Data exported to raw_menu_categories.csv")
    except Exception as e:
        print(f"The error '{e}' occurred during data export")

    # Insert data into raw_menu table
    menu_query = """
    INSERT INTO raw_menu (id, name, description, price, category_id, is_vegan, is_gluten_free, ingredients, date_added, is_available, preparation_time, calories, image_url, spicy_level, allergens) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    spicy_levels = [0, 1, 2, 3]
    menu_data = [
        (str(uuid.uuid4()), fake.word(), fake.text(max_nb_chars=200), round(random.uniform(5.99, 25.99), 2),
         random.choice(menu_categories_data)[0], fake.boolean(), fake.boolean(), ' '.join(fake.words(nb=5)),
         fake.date_this_year(), fake.boolean(), random.randint(5, 30), random.randint(100, 900), fake.image_url(),
         random.choice(spicy_levels), ' '.join(fake.words(nb=3)))
        for _ in range(50)
    ]
    execute_many_queries(connection, menu_query, menu_data, "raw_menu")

    try:
        connection.execute("COPY (SELECT * FROM raw_menu) TO 'seeds/raw_menu.csv' (HEADER, DELIMITER ',');")
        print("Data exported to raw_menu.csv")
    except Exception as e:
        print(f"The error '{e}' occurred during data export")

    # Insert data into raw_menu_orders table
    menu_orders_query = """
    INSERT INTO raw_menu_orders (id, order_date, customer_id, menu_id, quantity, employee_id, order_status) 
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    menu_orders_data = [
        (str(uuid.uuid4()), fake.date_time_this_year(), random.choice(customers_data)[0], random.choice(menu_data)[0],
         random.randint(1, 10), random.choice(employees_data)[0], random.choice(order_statuses))
        for _ in range(20000)
    ]
    execute_many_queries(connection, menu_orders_query, menu_orders_data, "raw_menu_orders")

    try:
        connection.execute("COPY (SELECT * FROM raw_menu_categories) TO 'seeds/raw_menu_orders.csv' (HEADER, DELIMITER ',');")
        print("Data exported to raw_menu_orders.csv")
    except Exception as e:
        print(f"The error '{e}' occurred during data export")

    connection.close()


if __name__ == "__main__":
    insert_data()
