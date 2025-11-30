import csv
import json
import random
import uuid
from faker import Faker
from datetime import datetime
import os

fake = Faker("nl_NL")   # Dutch locale for fake user generation

NUM_CUSTOMERS = 500
OUTPUT_DIR = "/Workspace/Users/v.tibor+dbx1@gmail.com/fraud_detection/source/generated_customers"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Dutch cities
CITIES = [
    "Amsterdam", "Rotterdam", "Utrecht", "Den Haag", "Eindhoven",
    "Tilburg", "Groningen", "Almere", "Breda", "Nijmegen", "Enschede",
    "Haarlem", "Arnhem", "Amersfoort", "Apeldoorn", "Zwolle",
    "Maastricht", "Leiden", "Dordrecht", "Zoetermeer",   
    "Hoorn", "Purmerend", "Delft", "Hilversum", "Veenendaal",
    "Venlo", "Sittard", "Heerlen", "Roermond", "Middelburg",
    "Gouda", "Oss", "Helmond", "Deventer", "Assen"
]

def random_birthdate():
    """Between age 18 and 80"""
    start_date = datetime(1945, 1, 1)
    end_date = datetime(2007, 12, 31)
    return fake.date_between(start_date=start_date, end_date=end_date).isoformat()

def random_spending_level():
    return random.randint(1, 3)


# Generate customers
customers = []

for i in range(NUM_CUSTOMERS):
    first = fake.first_name()
    last = fake.last_name()

    customer = {
        "customer_id": 1000 + i,
        "firstname": first,
        "lastname": last,
        "address": fake.street_address(),
        "city": random.choice(CITIES),
        "country": "Netherlands",
        "phone_number": '+316'+''.join(random.choices('0123456789', k=8)),
        "email_address": f"{first.lower()}.{last.lower()}@example.com",
        "birthdate": random_birthdate(),
        "spending_level": random_spending_level()
    }

    customers.append(customer)


# Save as json
json_path = os.path.join(OUTPUT_DIR, "customers.json")
with open(json_path, "w", encoding="utf-8") as f:
    for c in customers:
        f.write(json.dumps(c) + "\n")

print(f"Generated {NUM_CUSTOMERS} customers.")
print(f"JSON saved → {json_path}")
