import pandas as pd
import random

class GetSampleData:
    def __init__(self):
        # create sample data
        dates = pd.date_range(start="2022-01-01", end="2022-12-31", freq="D")
        order_numbers = list(range(1, 101))
        client_numbers = [
            random.randint(1, 10) for _ in range(len(dates) * len(order_numbers))
        ]
        product_numbers = [
            random.randint(1, 100) for _ in range(len(dates) * len(order_numbers))
        ]
        skus = [random.randint(1, 10) for _ in range(len(dates) * len(order_numbers))]
        quantities = [
            random.randint(1, 100) for _ in range(len(dates) * len(order_numbers))
        ]

        # insert some null values
        for i in range(10):
            idx = random.randint(0, len(dates) * len(order_numbers) - 1)
            quantities[idx] = None

        for i in range(10):
            idx = random.randint(0, len(dates) * len(order_numbers) - 1)
            product_numbers[idx] = None

        # create a pandas DataFrame from the data
        data = {
            "date": dates.repeat(len(order_numbers)),
            "order_number": order_numbers * len(dates),
            "client_number": client_numbers,
            "product_number": product_numbers,
            "SKU": skus,
            "qty": quantities,
        }
        # df = pd.DataFrame(data)
        self.get_df = pd.DataFrame(data)
