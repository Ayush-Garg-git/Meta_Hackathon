SCHEMA_DDL = """
CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    country TEXT NOT NULL,
    tier TEXT NOT NULL DEFAULT 'standard',
    created_at TEXT NOT NULL
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price REAL NOT NULL,
    stock INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE reviews (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    rating INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);
"""

SEED_SQL = """
INSERT INTO customers VALUES
(1, 'Alice Johnson',  'alice@example.com',  'US', 'gold',     '2022-03-10'),
(2, 'Bob Smith',      'bob@example.com',    'UK', 'standard', '2022-07-22'),
(3, 'Carlos Rivera',  'carlos@example.com', 'MX', 'silver',   '2023-01-05'),
(4, 'Diana Patel',    'diana@example.com',  'IN', 'gold',     '2023-04-18'),
(5, 'Eve Chen',       'eve@example.com',    'US', 'standard', '2023-08-30'),
(6, 'Frank Mueller',  'frank@example.com',  'DE', 'silver',   '2024-01-12'),
(7, 'Grace Kim',      'grace@example.com',  'KR', 'standard', '2024-02-20');

INSERT INTO products VALUES
(1, 'Wireless Headphones', 'Electronics',  79.99, 150),
(2, 'Running Shoes',       'Footwear',     59.99, 300),
(3, 'Coffee Maker',        'Appliances',   49.99,  80),
(4, 'Yoga Mat',            'Sports',       29.99, 200),
(5, 'Desk Lamp',           'Office',       34.99, 120),
(6, 'Mechanical Keyboard', 'Electronics', 119.99,  60),
(7, 'Protein Powder',      'Sports',       44.99, 175),
(8, 'Notebook Set',        'Office',       14.99, 500);

INSERT INTO orders VALUES
(101, 1, 'completed', '2024-01-10'),
(102, 2, 'completed', '2024-01-15'),
(103, 1, 'active',    '2024-02-01'),
(104, 3, 'completed', '2024-02-14'),
(105, 4, 'refunded',  '2024-03-05'),
(106, 5, 'active',    '2024-03-20'),
(107, 3, 'completed', '2024-04-01'),
(108, 6, 'completed', '2024-04-10'),
(109, 1, 'completed', '2024-05-05'),
(110, 4, 'completed', '2024-05-18'),
(111, 2, 'active',    '2024-06-01'),
(112, 7, 'completed', '2024-06-15');

INSERT INTO order_items VALUES
(1,  101, 1, 1,  79.99),
(2,  101, 2, 1,  59.99),
(3,  102, 2, 2,  59.99),
(4,  103, 3, 1,  49.99),
(5,  104, 1, 2,  79.99),
(6,  104, 6, 1, 119.99),
(7,  105, 4, 1,  29.99),
(8,  106, 1, 1,  79.99),
(9,  106, 4, 2,  29.99),
(10, 107, 3, 2,  49.99),
(11, 108, 5, 3,  34.99),
(12, 108, 8, 5,  14.99),
(13, 109, 6, 1, 119.99),
(14, 109, 7, 2,  44.99),
(15, 110, 1, 1,  79.99),
(16, 110, 6, 2, 119.99),
(17, 111, 2, 1,  59.99),
(18, 112, 4, 3,  29.99),
(19, 112, 7, 1,  44.99);

INSERT INTO reviews VALUES
(1, 1, 1, 5, '2024-01-20'),
(2, 1, 2, 4, '2024-01-21'),
(3, 2, 2, 3, '2024-01-25'),
(4, 3, 1, 5, '2024-02-20'),
(5, 4, 4, 2, '2024-03-10'),
(6, 5, 1, 4, '2024-03-25'),
(7, 6, 5, 5, '2024-04-15'),
(8, 1, 6, 5, '2024-05-10'),
(9, 4, 6, 4, '2024-05-25'),
(10,7, 4, 3, '2024-06-20');
"""

SCHEMA_TABLES = [
    {
        "name": "customers",
        "columns": [
            "id INTEGER PK",
            "name TEXT",
            "email TEXT UNIQUE",
            "country TEXT",
            "tier TEXT  -- values: standard | silver | gold",
            "created_at TEXT  -- YYYY-MM-DD",
        ],
        "sample_rows": [
            {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "country": "US", "tier": "gold", "created_at": "2022-03-10"},
            {"id": 2, "name": "Bob Smith",     "email": "bob@example.com",   "country": "UK", "tier": "standard", "created_at": "2022-07-22"},
        ],
    },
    {
        "name": "products",
        "columns": [
            "id INTEGER PK",
            "name TEXT",
            "category TEXT",
            "price REAL",
            "stock INTEGER",
        ],
        "sample_rows": [
            {"id": 1, "name": "Wireless Headphones", "category": "Electronics", "price": 79.99,  "stock": 150},
            {"id": 2, "name": "Running Shoes",       "category": "Footwear",    "price": 59.99,  "stock": 300},
        ],
    },
    {
        "name": "orders",
        "columns": [
            "id INTEGER PK",
            "customer_id INTEGER FK -> customers.id",
            "status TEXT  -- values: active | completed | refunded",
            "created_at TEXT  -- YYYY-MM-DD",
        ],
        "sample_rows": [
            {"id": 101, "customer_id": 1, "status": "completed", "created_at": "2024-01-10"},
            {"id": 102, "customer_id": 2, "status": "completed", "created_at": "2024-01-15"},
        ],
    },
    {
        "name": "order_items",
        "columns": [
            "id INTEGER PK",
            "order_id INTEGER FK -> orders.id",
            "product_id INTEGER FK -> products.id",
            "quantity INTEGER",
            "unit_price REAL",
        ],
        "sample_rows": [
            {"id": 1, "order_id": 101, "product_id": 1, "quantity": 1, "unit_price": 79.99},
            {"id": 2, "order_id": 101, "product_id": 2, "quantity": 1, "unit_price": 59.99},
        ],
    },
    {
        "name": "reviews",
        "columns": [
            "id INTEGER PK",
            "customer_id INTEGER FK -> customers.id",
            "product_id INTEGER FK -> products.id",
            "rating INTEGER  -- 1 to 5",
            "created_at TEXT  -- YYYY-MM-DD",
        ],
        "sample_rows": [
            {"id": 1, "customer_id": 1, "product_id": 1, "rating": 5, "created_at": "2024-01-20"},
            {"id": 2, "customer_id": 1, "product_id": 2, "rating": 4, "created_at": "2024-01-21"},
        ],
    },
]

TASKS = [
    {
        "task_id": "task_01_syntax_fix",
        "level": "easy",
        "query": "SELECT id name email country FROM customers WHERE country = 'US' AND tier = gold",
        "instructions": (
            "This query has two bugs:\n"
            "1. Missing commas between column names in the SELECT list.\n"
            "2. The string value 'gold' is not quoted — SQL treats it as a column name, which causes an error.\n\n"
            "Fix both bugs. The query should return id, name, email, and country "
            "for all customers in the US whose tier is 'gold'."
        ),
        "reference_query": "SELECT id, name, email, country FROM customers WHERE country = 'US' AND tier = 'gold'",
        "reference_columns": {"id", "name", "email", "country"},
        "reference_where_tokens": {"country", "us", "tier", "gold"},
    },
    {
        "task_id": "task_02_join_logic",
        "level": "medium",
        "query": (
            "SELECT c.name, c.email, o.id AS order_id, o.status "
            "FROM customers c "
            "LEFT JOIN orders o ON c.id = o.customer_id "
            "WHERE o.status = 'active'"
        ),
        "instructions": (
            "This query is logically broken in a subtle way.\n\n"
            "It uses a LEFT JOIN to get all customers even if they have no orders, "
            "but then the WHERE clause filters on o.status — which silently eliminates "
            "all NULLs and makes the LEFT JOIN behave like an INNER JOIN anyway, "
            "while confusing the query planner.\n\n"
            "Fix it by:\n"
            "1. Changing LEFT JOIN to INNER JOIN (making the intent explicit).\n"
            "2. Adding ORDER BY c.name ASC so results are deterministic.\n"
            "3. Selecting only: customer name, customer email, order id, order status.\n\n"
            "The query should return only customers who have active orders."
        ),
        "reference_query": (
            "SELECT c.name, c.email, o.id AS order_id, o.status "
            "FROM customers c "
            "INNER JOIN orders o ON c.id = o.customer_id "
            "WHERE o.status = 'active' "
            "ORDER BY c.name ASC"
        ),
        "reference_columns": {"name", "email", "order_id", "status"},
        "reference_where_tokens": {"status", "active"},
    },
    {
        "task_id": "task_03_aggregation_fix",
        "level": "medium",
        "query": (
            "SELECT c.name, COUNT(o.id) AS order_count, SUM(oi.quantity * oi.unit_price) AS revenue "
            "FROM customers c "
            "JOIN orders o ON c.id = o.customer_id "
            "JOIN order_items oi ON o.id = oi.order_id "
            "WHERE o.status = 'completed' "
            "HAVING revenue > 100"
        ),
        "instructions": (
            "This query will fail at runtime.\n\n"
            "It uses aggregate functions (COUNT, SUM) but is missing the GROUP BY clause. "
            "Without GROUP BY, the database does not know how to group rows per customer "
            "before applying the aggregation.\n\n"
            "Fix it by:\n"
            "1. Adding GROUP BY c.id, c.name after the WHERE clause.\n"
            "2. Keeping the HAVING revenue > 100 filter.\n"
            "3. Adding ORDER BY revenue DESC so the highest-revenue customers appear first.\n\n"
            "The query should return each customer's name, number of completed orders, "
            "and total revenue from those orders, for customers whose revenue exceeds 100."
        ),
        "reference_query": (
            "SELECT c.name, COUNT(o.id) AS order_count, SUM(oi.quantity * oi.unit_price) AS revenue "
            "FROM customers c "
            "JOIN orders o ON c.id = o.customer_id "
            "JOIN order_items oi ON o.id = oi.order_id "
            "WHERE o.status = 'completed' "
            "GROUP BY c.id, c.name "
            "HAVING revenue > 100 "
            "ORDER BY revenue DESC"
        ),
        "reference_columns": {"name", "order_count", "revenue"},
        "reference_where_tokens": {"completed"},
    },
    {
        "task_id": "task_04_correlated_subquery",
        "level": "hard",
        "query": (
            "SELECT id, name, email "
            "FROM customers c "
            "WHERE ("
            "SELECT SUM(oi.quantity * oi.unit_price) "
            "FROM orders o JOIN order_items oi ON o.id = oi.order_id "
            "WHERE o.customer_id = c.id AND o.status = 'completed'"
            ") > 200"
        ),
        "instructions": (
            "This query produces correct results but is a performance disaster at scale.\n\n"
            "It uses a correlated subquery in the WHERE clause — the inner SELECT re-executes "
            "once for every row in the customers table, resulting in O(n) full scans. "
            "On a table with millions of customers this will time out.\n\n"
            "Rewrite it to:\n"
            "1. Use an explicit INNER JOIN between customers, orders, and order_items.\n"
            "2. Filter to completed orders in a WHERE clause.\n"
            "3. Group by customer (c.id, c.name, c.email).\n"
            "4. Use HAVING SUM(oi.quantity * oi.unit_price) > 200 to replace the subquery filter.\n"
            "5. Include the computed total as a column aliased as total_spent.\n"
            "6. Order by total_spent DESC.\n\n"
            "The result set must include the same customers as the original query."
        ),
        "reference_query": (
            "SELECT c.id, c.name, c.email, SUM(oi.quantity * oi.unit_price) AS total_spent "
            "FROM customers c "
            "INNER JOIN orders o ON c.id = o.customer_id "
            "INNER JOIN order_items oi ON o.id = oi.order_id "
            "WHERE o.status = 'completed' "
            "GROUP BY c.id, c.name, c.email "
            "HAVING SUM(oi.quantity * oi.unit_price) > 200 "
            "ORDER BY total_spent DESC"
        ),
        "reference_columns": {"id", "name", "email", "total_spent"},
        "reference_where_tokens": {"completed"},
    },
    {
        "task_id": "task_05_multi_table_report",
        "level": "hard",
        "query": (
            "SELECT p.category, SUM(oi.quantity * oi.unit_price) "
            "FROM order_items oi, products p, orders o "
            "WHERE oi.product_id = p.id "
            "GROUP BY p.category"
        ),
        "instructions": (
            "This query is a rough draft of a revenue-by-category report. It has several problems:\n\n"
            "1. It uses implicit comma-join syntax (old style) instead of explicit JOINs.\n"
            "2. It is missing the join condition between order_items and orders "
            "   (oi.order_id = o.id), causing a cartesian product with the orders table.\n"
            "3. It doesn't filter to only 'completed' orders, inflating revenue with "
            "   active and refunded orders.\n"
            "4. The SUM column has no alias.\n"
            "5. Results are not sorted.\n\n"
            "Rewrite it to:\n"
            "1. Use explicit INNER JOINs with ON conditions.\n"
            "2. Join order_items → orders → products correctly.\n"
            "3. Filter to o.status = 'completed'.\n"
            "4. Group by p.category.\n"
            "5. Alias the revenue column as total_revenue.\n"
            "6. Order by total_revenue DESC.\n"
            "7. Return category and total_revenue columns."
        ),
        "reference_query": (
            "SELECT p.category, SUM(oi.quantity * oi.unit_price) AS total_revenue "
            "FROM order_items oi "
            "INNER JOIN orders o ON oi.order_id = o.id "
            "INNER JOIN products p ON oi.product_id = p.id "
            "WHERE o.status = 'completed' "
            "GROUP BY p.category "
            "ORDER BY total_revenue DESC"
        ),
        "reference_columns": {"category", "total_revenue"},
        "reference_where_tokens": {"completed"},
    },
]
