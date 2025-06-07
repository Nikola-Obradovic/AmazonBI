DB_CONFIG = {
    'dbname': 'amazon_products',
    'user': 'postgres',
    'password': 'admin123',  # Replace with environment variables later
    'host': 'localhost',
    'port': '5432'
}

EXCHANGE_RATE_API = {
    'url': 'https://api.exchangerate.host/latest',
    'key': '9a11331ddcc59c7d805af3a7',
    'base_currency': 'USD'  # Assuming CSV uses Indian Rupees (₹)
}

