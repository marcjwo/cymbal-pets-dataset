import json, random, typing, itertools, os
from datetime import date, datetime, timedelta, time
from dataclasses import dataclass, field, InitVar

from google.cloud import bigquery, storage
from faker import Faker

# ==== INITIALIZATION ============================
fake = Faker()
bq_client = bigquery.Client()
storage_client = storage.Client()
# ================================================

# TODO: these should be env variables
DATASET_ID = "cymbal_pets"
BUCKET_NAME = "4711_test_cymbal_pets"
# DAILY_ORDERS = 2
# MIN_LOCATIONS = 2
# MAX_LOCATIONS = 3
NUM_OF_CUSTOMERS = 1050
# DATASET_ID = os.getenv("DATASET_ID")
# BUCKET_NAME = os.getenv("BUCKET_NAME")
# DAILY_ORDERS = int(os.getenv("DAILY_ORDERS"))
# MIN_LOCATIONS = int(os.getenv("MIN_LOCATIONS"))
# MAX_LOCATIONS = int(os.getenv("MAX_LOCATIONS"))
# NUM_OF_CUSTOMERS = int(os.getenv("NUM_OF_CUSTOMERS"))
# ================================================


class DataHandling:
    @staticmethod
    def read_json(bucket_name: str, file_name: str, city_name: str = None) -> dict:
        # client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        file = f"data/{file_name}.json"
        blob = bucket.blob(file)

        try:
            data = json.loads(blob.download_as_text())
        except Exception as e:
            print(f"Error: {str(e)}")
            return []

        if file_name == "city_addresses":
            city_data = data["cities"].get(city_name)
            if city_data:
                return city_data["addresses"]
            else:
                print(f"Error: No address data found for {city_name}")
                return []

        if file_name == "products":
            return data

        print(f"Error: Unsupported file name: {file_name}")
        return []

    def serialize(obj):
        """Custom JSON serializer for objects not serializable by default."""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()  # Convert datetime/date to ISO format string
        raise TypeError(f"Type {type(obj).__name__} not serializable")

    def json_to_gcs(bucket_name, file_name, data_list):
        """Saves a Python list as a JSON file to Google Cloud Storage (GCS).

        Args:
            bucket_name (str): The name of your GCS bucket.
            file_name (str): The name of the JSON file to be saved.
            data_list (list): The Python list to be converted to JSON.
        """
        # Get the bucket
        bucket = storage_client.bucket(bucket_name)

        # Create a blob object
        blob = bucket.blob(file_name)

        # Convert the list to newline-delimited JSON
        json_data = "\n".join(
            [json.dumps(record, default=DataHandling.serialize) for record in data_list]
        )

        # Upload the JSON data to GCS
        blob.upload_from_string(json_data, content_type="application/json")

        print(f"List saved as JSON to gs://{bucket_name}/{file_name}")

    def load_gcs_to_bq(
        data_name: str,
        source_bucket: str,
        dataset_id: str,
    ):
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            # autodetect=True,
        )
        uri = f"gs://{source_bucket}/{data_name}.json"
        table_ref = bq_client.dataset(dataset_id).table(data_name)
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()
        if load_job.errors:
            print(load_job.errors)
        else:
            print(f"loaded {load_job.output_rows} to {data_name} successfully")


class DataUtils:
    SEASONAL_WEIGHTS = {
        1: 0.11,  # January
        2: 0.09,  # February
        3: 0.08,  # March
        4: 0.11,  # April
        5: 0.11,  # May
        6: 0.13,  # June
        7: 0.14,  # July
        8: 0.10,  # August
        9: 0.1,  # September
        10: 0.08,  # October
        11: 0.09,  # November
        12: 0.1,  # December
    }

    @staticmethod
    def generate_age(
        lower_bound: int = 17,
        upper_bound: int = 80,
        mean: int = 38,
        std_dev: int = 20,
    ) -> int:
        while True:
            age = np.random.normal(mean, std_dev)
            if lower_bound <= age <= upper_bound:
                return round(age)

    @staticmethod
    def child_created_at(parent_date: date, month_weights: dict = None) -> datetime:
        if month_weights is None:
            month_weights = DataUtils.SEASONAL_WEIGHTS
        time_between_dates = (date.today() - parent_date).days
        time_between_dates = max(time_between_dates, 2)
        # if time_between_dates <= 1:
        #     time_between_dates = 2
        random_number_of_days = random.randrange(1, time_between_dates)
        random_date = parent_date + timedelta(days=random_number_of_days)
        random_month = random_date.month
        adjusted_weight = month_weights[random_month]
        if random.random() > adjusted_weight:
            return DataUtils.child_created_at(parent_date)  # Retry if not accepted

        random_time = time(
            random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
        )
        created_at_datetime = datetime.combine(random_date, random_time)
        return created_at_datetime


@dataclass
class Product:
    id: int
    name: str
    category: str
    subcategory: str
    brand: str
    price: float
    description: str
    img_url: str
    inventory_level: int
    supplier_id: int
    average_rating: float
    nutritional_info: str


@dataclass
class Store:
    store_id: int
    store_name: str
    location: str
    latitude: float
    longitude: float
    opening_hours: dict
    manager_id: int


@dataclass
class Supplier:
    supplier_id: int
    supplier_name: str
    contact_name: str
    email: str
    phone_number: str
    address: str


@dataclass
class Customer:
    id: int = field(default_factory=itertools.count(start=1).__next__)
    first_name: str = field(init=False)
    last_name: str = field(init=False)
    email: str = field(init=False)
    # age: int = field(init=False)
    gender: str = field(init=False)
    # address_city: InitVar[typing.Any] = None #TODO: commenting this out, no necessity to make sure the customer comes from the city of the store
    address_city: str = field(init=False)
    loyalty_member: bool = field(init=False)
    # last_purchase_date: date = field(init=False) # No need to, customer since will be the first order

    def __post_init__(self, address_city=None):
        self.gender = random.choices(["m", "f"], weights=[0.37, 0.63])[0]
        if self.gender == "m":
            self.first_name = fake.first_name_male()
        else:
            self.first_name = fake.first_name_female()
        self.last_name = fake.last_name()
        self.email = f"{self.first_name.lower()}{self.last_name.lower()}@{fake.safe_domain_name()}"
        self.address_city = fake.real_city_name()
        # self.age = DataUtils.generate_age()
        # self.age = min(80, max(17, round(np.random.normal(38, 20))))
        rand = random.randint(0, len(address_city) - 1)
        self.address_city = address_city[rand]
        self.loyalty_member = random.choices([True, False], weights=[0.31, 0.69])[0]


@dataclass
class Employee:
    id: int = field(default_factory=itertools.count(start=1).__next__)
    first_name: str = field(init=False)
    last_name: str = field(init=False)
    job_title: str = field(init=False)
    department: str = field(init=False)
    hire_date: date = field(init=False)
    salary: float = field(init=False)


@dataclass
class Order:
    # location_id: int
    customer_id: int
    order_date: date = field(init=False)
    # completed_datetime: datetime = field(init=False)
    id: int = field(default_factory=itertools.count(start=1).__next__)
    order_type: str = field(init=False)
    payment_method: str = field(init=False)
    shipping_address_city: str = field(init=False)
    # dine_in: bool = field(init=False)
    cymbal_pets_start_date: InitVar[typing.Any] = None

    def __post_init__(self, cymbal_pets_start_date=None):
        self.order_date = DataUtils.child_created_at(cymbal_pets_start_date)
        # self.created_datetime = DataUtils.child_created_at(location_founding_date)
        # if self.location_id < 3:
        #     self.completed_datetime = self.created_datetime + timedelta(
        #         minutes=random.randint(10, 30)
        #     )
        # else:
        #     self.completed_datetime = self.created_datetime + timedelta(
        #         minutes=random.randint(3, 12)
        #     )
        # self.dine_in = random.choices([True, False], weights=[0.67, 0.33])[0]


@dataclass
class OrderItem:
    id: int = field(default_factory=itertools.count(start=1).__next__)
    order_id: int
    product_id: int
    quantity: int = field(init=False)
    price: float

    def __post_init__(self):
        id: int = field(default_factory=itertools.count(start=1).__next__)


# ===== GENERATION FUNCTIONS =======================================================


def generate_customers(num_of_customers: int):
    customers = []
    for i in range(num_of_customers):
        customers.append(Customer().__dict__)
    return customers


def generate_stores():
    stores = []
    for store in DataHandling.read_json(bucket_name=BUCKET_NAME, file_name="stores"):
        stores.append(
            Store(
                store_id=store["store_id"],
                store_name=store["store_name"],
                location=store["location"],
                latitude=store["latitude"],
                longitude=store["longitude"],
                opening_hours=store["opening_hours"],
                manager_id=store["manager_id"],
            ).__dict__
        )

    return stores


def generate_products():
    products = []
    for product in DataHandling.read_json(
        bucket_name=BUCKET_NAME, file_name="products"
    ):
        products.append(
            Product(
                id=product["id"],
                name=product["name"],
                category=product["category"],
                subcategory=product["subcategory"],
                brand=product["brand"],
                price=product["price"],
                description=product["description"],
                img_url=product["img_url"],
                inventory_level=product["inventory_level"],
                supplier_id=product["supplier_id"],
                average_rating=product["average_rating"],
                nutritional_info=product["nutritional_info"],
            ).__dict__
        )
    return products


def generate_suppliers():
    suppliers = []
    for supplier in DataHandling.read_json(
        bucket_name=BUCKET_NAME, file_name="supplier"
    ):
        suppliers.append(
            Supplier(
                supplier_id=supplier["supplier_id"],
                supplier_name=supplier["supplier_name"],
                contact_name=supplier["contact_name"],
                email=supplier["email"],
                phone_number=supplier["phone_number"],
                address=supplier["address"],
            ).__dict__
        )
    return suppliers


def generate_orders():
    orders = []
    orders.append(Order(customer_id=customer_id).__dict__)
    return orders


def generate_order_items(orders: list, products: list):
    order_items = []
    for order in orders:
        num_of_items = random.randint(1, 5)
        for _ in range(num_of_items):
            rand_product = random.choice(products)
            order_items.append(
                OrderItem(order_id=order["id"], product_id=rand_product["id"]).__dict__
            )
    return order_items


# ==================================================================================
