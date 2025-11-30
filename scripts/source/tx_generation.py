import json
import uuid
import random
import time
from datetime import datetime, timedelta
import os
from faker import Faker



OUTPUT_DIR = './generated_transactions'
BATCH_SIZE = 50             # transactions per batch
SLEEP_SECONDS = 30          # batch interval
FRAUD_PROB = 0.05           # 5% suspicious transactions

os.makedirs(OUTPUT_DIR, exist_ok=True)

fake = Faker()              #library for generating fake geospatial data
FAKER_COUNTRIES = [
    'de_DE', 'fr_FR', 'es_ES', 'it_IT', 'pt_PT',
    'uk_UA', 'pl_PL', 'no_NO', 'fi_FI', 'dk_DK',
    'en_GB', 'ro_RO', 'bg_BG', 'cz_CZ', 'sk_SK',

    'ar_EG', 'fr_FR',

    'en_US', 'en_CA', 'es_MX', 'es_CO', 'es_AR', 'es_CL', 'pt_BR',

    'en_TH', 'en_IN', 'en_BD'
]

CITIES = [
    'Amsterdam', 'Rotterdam', 'Utrecht', 'Den Haag', 'Eindhoven',
    'Tilburg', 'Groningen', 'Breda', 'Nijmegen', 'Haarlem',
    'Arnhem', 'Enschede', 'Zwolle', 'Maastricht', 'Leiden',
    'Dordrecht', 'Amersfoort', 'Apeldoorn', 'Zoetermeer', 'Almere',
    'Hoorn', 'Purmerend', 'Delft', 'Hilversum', 'Veenendaal',
    'Venlo', 'Sittard', 'Heerlen', 'Roermond', 'Middelburg',
    'Gouda', 'Oss', 'Helmond', 'Deventer', 'Assen'
]


CATEGORIES = [
    'GROCERIES', 'RESTAURANT', 'CLOTHING', 'TECH', 'TRAVEL',
    'WITHDRAWAL', 'TRANSFER', 'ONLINE_MARKET',
    'UNKNOWN', 'SUPERMARKET'                  
]

HIGH_RISK_CATEGORIES = ['GAMBLING', 'LUXURY', 'BLACK-MARKET']


current_txn_number = 1

def generate_transaction_id():
    global current_txn_number
    prefix = 'TX'  
    num = str(current_txn_number).zfill(4)
    current_txn_number += 1
    return prefix + num

# Generate realistic payment amounts 
def random_amount():
    if random.random() < 0.8:
        return round(random.uniform(3, 300), 2)
    return round(random.uniform(0.01, 10000), 2)

# Normal transaction
def generate_normal_transaction():
    city = Faker('nl_NL').city()

    return {
        'transaction_id': generate_transaction_id(),
        'customer_id': random.randint(1000, 1500),
        'city': city,
        'country': 'NL',
        'category': random.choice(CATEGORIES),
        'amount': random_amount(),
        'timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M'),
        'pin_mistakes': random.choices([0,1,2],weights=[0.9,0.05,0.05])[0],
        'tx_succesful': random.choices([True,False],weights=[0.9,0.1])[0]
    }

# Suspicious transaction
def generate_fraudulent_transaction():
    fraud_types = [
        'high_amount',
        'high_risk_category',
        'night_time',
        'weird_location',
        'pin_block'
    ]

    fraud_type = random.sample(fraud_types, k=random.randint(1, 3))

    base = generate_normal_transaction()

    if 'high_amount' in fraud_type:
        base['amount'] = round(random.uniform(3000, 10000), 2)

    if 'high_risk_category' in fraud_type:
        base['category'] = random.choices(HIGH_RISK_CATEGORIES)[0]

    if 'night_time' in fraud_type:
        dt = datetime.utcnow().replace(hour=random.randint(0, 4), minute=random.randint(0, 59))
        base['timestamp'] = dt.strftime('%Y-%m-%d %H:%M')

    if 'weird_location' in fraud_type:
        location = random.choice(FAKER_COUNTRIES)
        base['city'] = Faker(location).city()
        base['country'] = location[-2:]

    if 'pin_block' in fraud_type:
        base['pin_mistakes'] = '3'
        base['tx_succesful'] = False
   
    return base


# Main loop
print('Starting transaction generator')

while True:
    batch = []

    for i in range(BATCH_SIZE):
        if random.random() < FRAUD_PROB:
            batch.append(generate_fraudulent_transaction())
        else:
            batch.append(generate_normal_transaction())

    filename = f'{OUTPUT_DIR}/transactions_{int(time.time())}.json'
    with open(filename, 'w') as f:
        for row in batch:
            f.write(json.dumps(row) + '\n')

    print(f'Generated {len(batch)} transactions → {filename}')
    time.sleep(SLEEP_SECONDS)
