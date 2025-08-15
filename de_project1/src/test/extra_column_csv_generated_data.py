import os
import csv
import random
from datetime import datetime

customer_ids = list(range(1, 51))
store_ids = list(range(121, 131))
product_data = {
    "quaker oats": 212,
    "sugar": 50,
    "flour": 20,
    "gram flour": 52,
    "refined oil": 110,
    "shampoo": 1.5,
    "toothpaste": 100,
    "nutrella": 40
}
sales_persons = {
    121: [1, 2, 3],
    122: [4, 5, 6],
    123: [7, 8, 9],
    124: [10, 11, 12],
    125: [12, 13],
    126: [14, 15],
    127: [16, 17],
    128: [18, 19,20],
    129: [21,22],
    130: [23,24,25]
}

file_location = "C:\\Users\\Rinkal Patidar\\Pycharm_Project_data\\spark_data"

if not os.path.exists(file_location):
    os.makedirs(file_location)

input_date_str = input("Enter the date for which you want to generate (YYYY-MM-DD): ")
input_date = datetime.strptime(input_date_str, "%Y-%m-%d")

csv_file_path = os.path.join(file_location, f"sales_data_{input_date_str}.csv")
with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost", "payment_mode"])

    for _ in range(100000):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = input_date
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        price = product_data[product_name]
        total_cost = price * quantity
        payment_mode = random.choice(["cash", "UPI"])

        csvwriter.writerow(
            [customer_id, store_id, product_name, sales_date.strftime("%Y-%m-%d"), sales_person_id, price, quantity, total_cost, payment_mode])

    print("CSV file generated successfully:", csv_file_path)
