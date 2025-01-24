import json, random, typing, itertools, os
from datetime import date, datetime, timedelta, time
from dataclasses import dataclass, field, InitVar
from decimal import Decimal

from google.cloud import bigquery, storage
from faker import Faker

# ==== INITIALIZATION ============================
fake = Faker()
bq_client = bigquery.Client()
storage_client = storage.Client()
# ================================================

# TODO: these should be env variables
# DATASET_ID = "4711_test_cymbal_pets"
# BUCKET_NAME = "4711_test_cymbal_pets"
# DAILY_ORDERS = 2
# NUM_OF_CUSTOMERS = 100
CYMBAL_PETS_START_DATE = date(2023, 1, 1)
DATASET_ID = os.getenv("DATASET_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
DAILY_ORDERS = int(os.getenv("DAILY_ORDERS"))
# MIN_LOCATIONS = int(os.getenv("MIN_LOCATIONS"))
# MAX_LOCATIONS = int(os.getenv("MAX_LOCATIONS"))
NUM_OF_CUSTOMERS = int(os.getenv("NUM_OF_CUSTOMERS"))
# CYMBAL_PETS_START_DATE = int(os.getenv("START_DATE"))
# ================================================


class DataHandling:
    @staticmethod
    def read_json(bucket_name: str, file_name: str, city_name: str = None) -> dict:
        bucket = storage_client.bucket(bucket_name)
        file = f"data/{file_name}.json"
        blob = bucket.blob(file)

        try:
            data = json.loads(blob.download_as_text())
            return data
        except Exception as e:
            print(f"Error: {str(e)}")
            return []

        # if file_name == "city_addresses":
        #     city_data = data["cities"].get(city_name)
        #     if city_data:
        #         return city_data["addresses"]
        #     else:
        #         print(f"Error: No address data found for {city_name}")
        #         return []

        # if file_name == "products":
        #     return data

        # print(f"Error: Unsupported file name: {file_name}")
        # return []

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

    # @staticmethod
    # def generate_age(
    #     lower_bound: int = 17,
    #     upper_bound: int = 80,
    #     mean: int = 38,
    #     std_dev: int = 20,
    # ) -> int:
    #     while True:
    #         age = np.random.normal(mean, std_dev)
    #         if lower_bound <= age <= upper_bound:
    #             return round(age)

    @staticmethod
    def child_created_at(parent_date: date, month_weights: dict = None) -> date:
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

        # random_time = time(
        #     random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
        # )
        # created_at_datetime = datetime.combine(random_date, random_time)
        # return created_at_datetime
        return random_date


@dataclass
class Product:
    product_id: int
    product_name: str
    category: str
    subcategory: str
    brand: str
    price: float
    description: str
    image_url: str
    inventory_level: int
    supplier_id: int
    average_rating: float
    nutritional_info: dict


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
    customer_id: int = field(default_factory=itertools.count(start=1).__next__)
    first_name: str = field(init=False)
    last_name: str = field(init=False)
    email: str = field(init=False)
    gender: str = field(init=False)
    # address_city: InitVar[typing.Any] = None #TODO: commenting this out, no necessity to make sure the customer comes from the city of the store
    address_city: str = field(init=False)
    loyalty_member: bool = field(init=False)

    def __post_init__(self, address_city=None):
        self.gender = random.choices(["m", "f"], weights=[0.37, 0.63])[0]
        if self.gender == "m":
            self.first_name = fake.first_name_male()
        else:
            self.first_name = fake.first_name_female()
        self.last_name = fake.last_name()
        self.email = f"{self.first_name.lower()}{self.last_name.lower()}@{fake.safe_domain_name()}"
        self.address_city = fake.city()
        self.loyalty_member = random.choices([True, False], weights=[0.31, 0.69])[0]


@dataclass
class Employee:
    employee_id: int = field(default_factory=itertools.count(start=100).__next__)
    first_name: str = field(init=False)
    last_name: str = field(init=False)
    job_title: str = field(init=False)
    # department: str = field(init=False)
    hire_date: date = field(init=False)
    gender: str = field(init=False)
    salary: float = field(init=False)

    def __post_init__(self):
        job_titles = [
            "Sales Associate",
            "Cashier",
            "Pet Care Specialist",
            "Groomer",
            "Inventory Manager" "Customer Service Representative",
        ]
        self.job_title = random.choice(job_titles)
        self.gender = random.choices(["m", "f"], weights=[0.37, 0.63])[0]
        if self.gender == "m":
            self.first_name = fake.first_name_male()
        else:
            self.first_name = fake.first_name_female()
        self.last_name = fake.last_name()
        self.hire_date = DataUtils.child_created_at(CYMBAL_PETS_START_DATE)
        # print(date.today())
        # print(self.hire_date)
        days_since_hire = (date.today() - self.hire_date).days
        base_salary = 67300
        salary_increase_per_month = 855
        salary_increase = (days_since_hire // 30) * salary_increase_per_month
        self.salary = base_salary + salary_increase


@dataclass
class Order:
    # location_id: int
    customer_id: int
    shipping_address_city: str
    store_id: int
    order_date: date = field(init=False)
    order_id: int = field(default_factory=itertools.count(start=1).__next__)
    order_type: str = field(init=False)
    payment_method: str = field(init=False)
    # cymbal_pets_start_date: InitVar[date] = None

    def __post_init__(self):
        self.order_date = DataUtils.child_created_at(CYMBAL_PETS_START_DATE)
        self.order_type = random.choices(["Online", "Offline"], weights=[0.61, 0.39])[0]
        if self.order_type == "Offline":
            self.payment_method = random.choices(
                ["Cash", "Credit Card"], weights=[0.35, 0.65]
            )[0]
            self.shipping_address_city = None
        else:
            self.payment_method = random.choices(
                ["Credit Card", "Paypal", "Invoice"], weights=[0.41, 0.33, 0.26]
            )[0]
            self.shipping_address_city = self.shipping_address_city
            self.store_id = None


@dataclass
class OrderItem:
    order_id: int
    product_id: int
    order_item_id: int = field(default_factory=itertools.count(start=1).__next__)
    quantity: int = field(init=False)
    price: InitVar[Decimal] = None

    def __post_init__(self, price: Decimal = None):
        self.quantity = random.randint(1, 5)
        self.price = self.quantity * price


@dataclass
class CustomerService:
    customer_id: int
    case_id: int = field(default_factory=itertools.count(start=1).__next__)
    case_type: str = field(init=False)
    case_status: str = field(init=False)
    resolution_notes: str = field(init=False)
    agent_id: int = field(init=False)

    def __post_init__(self):
        case_types = ["Return", "Complaint", "Product Question", "Payment Issue"]
        self.case_type = random.choices(case_types, weights=(0.1, 0.5, 0.25, 0.15))[0]
        case_status = ["Open", "In Progress", "Closed", "Escalated"]
        self.case_status = random.choices(
            case_status, weights=(0.45, 0.15, 0.45, 0.05)
        )[0]
        self.resolution_notes = fake.sentence(nb_words=10)
        self.agent_id = random.randint(1, 7)


@dataclass
class NutritionAgent:
    food_id: int
    food_name: str
    nutritional_info: dict


@dataclass
class PetProfile:
    customer_id: int
    pet_id: int = field(default_factory=itertools.count(start=1).__next__)
    pet_name: str = field(init=False)
    pet_type: str = field(init=False)
    age: int = field(init=False)
    weight: int = field(init=False)
    activity_level: str = field(init=False)
    dietary_needs: str = field(init=False)

    def __post_init__(self):
        self.pet_name = fake.first_name()
        pet_type = ["Cat", "Dog", "Fish", "Bird", "Reptile", "Other"]
        self.pet_type = random.choice(pet_type)
        self.age = random.randint(1, 10)
        self.weight = random.randint(1, 20)
        activity_level = ["Low", "Medium", "High"]
        self.activity_level = random.choice(activity_level)
        self.dietary_needs = fake.sentence(nb_words=10)


# ===== GENERATION FUNCTIONS =======================================================


def generate_pet_profiles(customers: list, num_of_pet_profiles: int):
    pet_profiles = []
    for _ in range(num_of_pet_profiles):
        rand_cust = random.choice(customers)
        pet_profiles.append(PetProfile(customer_id=rand_cust["customer_id"]).__dict__)

    return pet_profiles


def generate_customers(num_of_customers: int):
    customers = []
    for i in range(num_of_customers):
        customers.append(Customer().__dict__)
    return customers


def generate_stores():
    stores = []
    for store in DataHandling.read_json(
        bucket_name=BUCKET_NAME, file_name="stores_data"
    ):
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
        bucket_name=BUCKET_NAME, file_name="products_data"
    ):
        products.append(
            Product(
                product_id=product["product_id"],
                product_name=product["product_name"],
                category=product["category"],
                subcategory=product["subcategory"],
                brand=product["brand"],
                price=product["price"],
                description=product["description"],
                image_url=product["image_url"],
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
        bucket_name=BUCKET_NAME, file_name="suppliers_data"
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


def generate_orders(
    customer_id: int = None, address_city: str = None, store_id: int = None
):
    orders = []
    orders.append(
        Order(
            customer_id=customer_id,
            shipping_address_city=address_city,
            store_id=store_id,
        ).__dict__
    )
    return orders


def generate_employees(num_of_employees: int = None):
    employees = []
    for _ in range(num_of_employees):
        employees.append(Employee().__dict__)
    return employees


def generate_order_items(orders: list, products: list):
    order_items = []
    for order in orders:
        num_of_items = random.randint(1, 5)
        for _ in range(num_of_items):
            rand_product = random.choice(products)
            order_items.append(
                OrderItem(
                    order_id=order["order_id"],
                    product_id=rand_product["product_id"],
                    price=rand_product["price"],
                ).__dict__
            )
    return order_items


def generate_customer_service(customers: list, num_of_customer_services: int):
    customer_service = []
    for _ in range(num_of_customer_services):
        rand_cust = random.choice(customers)
        customer_service.append(
            CustomerService(
                customer_id=rand_cust["customer_id"],
            ).__dict__
        )
    return customer_service


def generate_nutrition_agent(products: list):
    nutrition_information = []
    for product in products:
        if product["category"] == "Food":
            nutrition_information.append(
                NutritionAgent(
                    food_name=product["product_name"],
                    food_id=product["product_id"],
                    nutritional_info=product["nutritional_info"],
                ).__dict__
            )
    return nutrition_information


# ==================================================================================


def main(
    num_of_customers: int,
    daily_orders: int,
):
    print("Generating products data")
    products = generate_products()
    print("Generated " + str(len(products)) + " products data successfully")
    print("Generating stores data")
    stores = generate_stores()
    print("Generated " + str(len(stores)) + " stores data successfully")
    print("Generating suppliers data")
    suppliers = generate_suppliers()
    print("Generated " + str(len(suppliers)) + " suppliers data successfully")
    print("Generating customers data")
    customers = generate_customers(num_of_customers=num_of_customers)
    num_of_employees = len(stores) * 7
    print("Generating employees data")
    employees = generate_employees(num_of_employees=num_of_employees)
    print("Generated " + str(len(employees)) + " employees data successfully")
    print("Generating nutritional data")
    nutritional_data = generate_nutrition_agent(products=products)
    print("Generated " + str(len(nutritional_data)) + " nutritional data successfully")
    print("Generated " + str(len(customers)) + " customers data successfully")
    print("Generating pet profiles data")
    pet_profiles = generate_pet_profiles(
        customers=customers, num_of_pet_profiles=(round(len(customers) / 12))
    )
    print("Generated " + str(len(pet_profiles)) + " pet profiles data successfully")
    print("Generating customer service data")
    num_of_customer_services = round(len(customers) / 12)
    customer_service = generate_customer_service(
        customers=customers, num_of_customer_services=num_of_customer_services
    )
    print(
        "Generated "
        + str(len(customer_service))
        + " customer services data successfully"
    )
    print("Generating orders and order items")
    orders = []
    num_of_orders = (date.today() - CYMBAL_PETS_START_DATE).days * round(daily_orders)
    for _ in range(num_of_orders):
        has_customer_id = random.choices([True, False], weights=[0.67, 0.33])[0]
        rand_store = random.randint(0, len(stores) - 1)
        if has_customer_id:
            rand_cust = random.randint(0, len(customers) - 1)
            customer_id = customers[rand_cust]["customer_id"]
            address_city = customers[rand_cust]["address_city"]
        else:
            customer_id = None
            address_city = None
        orders.extend(
            generate_orders(
                customer_id=customer_id,
                address_city=address_city,
                store_id=stores[rand_store]["store_id"],
            )
        )
    order_items = generate_order_items(orders=orders, products=products)
    print(
        "Generated "
        + str(len(orders))
        + " orders and "
        + str(len(order_items))
        + " order items successfully"
    )
    data_list = {
        "products": products,
        "stores": stores,
        "suppliers": suppliers,
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
        "customer_service": customer_service,
        "nutritional_data": nutritional_data,
        "employees": employees,
        "pet_profiles": pet_profiles,
    }
    for name, data in data_list.items():
        file_name = f"{name}.json"
        DataHandling.json_to_gcs(
            bucket_name=BUCKET_NAME,
            file_name=file_name,
            data_list=data,
        )
        DataHandling.load_gcs_to_bq(
            data_name=name,
            source_bucket=BUCKET_NAME,
            dataset_id=DATASET_ID,
        )
    print("Cymbal Pets Dataset generation successfully completed!")


main(num_of_customers=NUM_OF_CUSTOMERS, daily_orders=DAILY_ORDERS)


def hello_http(request):
    main(num_of_customers=NUM_OF_CUSTOMERS, daily_orders=DAILY_ORDERS)
    return "Function successfully finished"
