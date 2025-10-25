import json
import random
from faker import Faker
from datetime import datetime, timedelta
from pathlib import Path

fake = Faker()
Faker.seed(42)

SERVICES = ["auth-service", "payment-service", "order-service", "search-service", "inventory-service"]
LEVELS = ["INFO", "WARN", "ERROR"]
ENVS = ["prod", "staging", "dev"]

def generate_log_record():
    service = random.choice(SERVICES)
    level = random.choices(LEVELS, weights=[0.7, 0.2, 0.1])[0]
    response_time = round(random.uniform(10, 2000), 2)
    status_code = random.choice([200, 400, 401, 403, 404, 500])
    timestamp = fake.date_time_between(start_date="-2d", end_date="now")
    message = f"{service} handled request with status {status_code}"

    return {
        "timestamp": timestamp.isoformat(),
        "service": service,
        "level": level,
        "message": message,
        "responseTime": response_time,
        "statusCode": status_code,
        "host": fake.ipv4(),
        "env": random.choice(ENVS)
    }

def write_logs(output_dir: str, num_files: int = 5, logs_per_file: int = 200_000):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    total = num_files * logs_per_file
    print(f"Generating {total:,} logs across {num_files} files...")

    for i in range(num_files):
        filename = Path(output_dir) / f"logs_{i}.json"
        with open(filename, "w") as f:
            for _ in range(logs_per_file):
                log = generate_log_record()
                f.write(json.dumps(log) + "\n")

        print(f"✅ File {i+1}/{num_files} written: {filename}")

if __name__ == "__main__":
    write_logs("../data/raw_logs", num_files=10, logs_per_file=500_000)
