import math
import pandas as pd

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer

from employee import Employee

import os


employee_topic_name = "bf_employee_salary"
csv_file = os.path.join(os.path.dirname(__file__), "resources", "Employee_Salaries.csv")


class salaryProducer(Producer):
    def __init__(self, host="localhost", port="29092"):
        producerConfig = {
            "bootstrap.servers": f"{host}:{port}",
            "acks": "all"
        }
        super().__init__(producerConfig)


class DataHandler:

    VALID_DEPARTMENTS = {"ECC", "CIT", "EMS"}

    @staticmethod
    def get_employees():
        df = pd.read_csv(csv_file)
        print("Total rows in CSV:", len(df))
        print("Departments found:")
        print(df["Department"].unique())

        result = []

        for _, row in df.iterrows():
            department = str(row["Department"]).strip()
            if department not in DataHandler.VALID_DEPARTMENTS:
                continue
            hire_date = pd.to_datetime(
                row["Initial Hire Date"],
                format="%d-%b-%Y",
                errors="coerce"
            )
            # check the hire date
            if pd.isna(hire_date):
                print("Bad hire date:", row["Initial Hire Date"])
                continue
            if hire_date.year <= 2010:
                continue

            salary = row["Salary"]
            try:
                salary = int(math.floor(float(salary)))
            except Exception:
                print("Bad salary:", salary)
                continue
            # required columns
            employee = Employee(
                department=department,
                department_division=str(row["Department Division"]),
                position_title=str(row["Position Title"]),
                hire_date=hire_date.strftime("%Y-%m-%d"),
                salary=salary
            )

            result.append(employee)
        print("\nRecords after filtering:", len(result))
        return result


if __name__ == "__main__":

    encoder = StringSerializer("utf_8")
    producer = salaryProducer()
    employees = DataHandler.get_employees()

    for emp in employees:
        producer.produce(
            topic=employee_topic_name,
            key=encoder(emp.department),
            value=encoder(emp.to_json()) # send the json 
        )
        producer.poll(0)

    producer.flush()

    print(f"Sent {len(employees)} records.")

    producer.close()