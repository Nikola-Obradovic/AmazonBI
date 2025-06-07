import csv
import random

# Define countries and their cities
country_cities = {
    "USA": ["New York", "Los Angeles", "Chicago"],
    "India": ["Mumbai", "Delhi", "Bangalore"],
    "UK": ["London", "Manchester", "Birmingham"],
    "Germany": ["Berlin", "Munich", "Hamburg"],
    "France": ["Paris", "Lyon", "Marseille"],
    "Japan": ["Tokyo", "Osaka", "Kyoto"],
    "Canada": ["Toronto", "Vancouver", "Montreal"],
    "Australia": ["Sydney", "Melbourne", "Brisbane"],
    "Brazil": ["Sao Paulo", "Rio de Janeiro", "Brasilia"],
    "China": ["Beijing", "Shanghai", "Shenzhen"]
}

# Read the input file
input_filename = 'amazon_products.csv'
output_filename = 'amazon_products_cleaned.csv'

with open(input_filename, mode='r', encoding='utf-8') as infile, \
        open(output_filename, mode='w', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(infile)

    # Add new fields to fieldnames
    fieldnames = reader.fieldnames + ['currency', 'country', 'city']

    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        # Assign USD as currency
        row['currency'] = 'USD'

        # Randomly select a country and city
        country = random.choice(list(country_cities.keys()))
        city = random.choice(country_cities[country])

        row['country'] = country
        row['city'] = city

        writer.writerow(row)

print(f"Processed file saved as {output_filename}")