"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project 1 Assignment.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import json
import psycopg2

from confluent_kafka import Consumer

from employee import Employee
from producer import employee_topic_name


class SalaryConsumer(Consumer):
    def __init__(
        self,
        host="localhost",
        port="29092",
        group_id="salary_consumer_group"
    ):
        conf = {
            "bootstrap.servers": f"{host}:{port}",
            "group.id": group_id,
            "enable.auto.commit": True,
            "auto.offset.reset": "earliest"
        }
        super().__init__(conf)
        self.keep_running = True

    def consume(self, topics, processing_func):
        try:
            self.subscribe(topics)
            while self.keep_running:
                msg = self.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(msg.error())
                    continue
                processing_func(msg)

        finally:
            self.close()


class ConsumingMethods:

    @staticmethod
    def add_salary(msg):
        e = Employee(**json.loads(msg.value()))

        conn = None
        cur = None

        try:
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="postgres",
                port="5432"
            )
            conn.autocommit = True
            cur = conn.cursor()
            # create tables if not exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS department_employee(
                    department VARCHAR(200),
                    department_division VARCHAR(200),
                    position_title VARCHAR(200),
                    hire_date DATE,
                    salary DECIMAL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS department_employee_salary(
                    department VARCHAR(200) PRIMARY KEY,
                    total_salary INT
                );
            """)

            # insert employee record
            cur.execute(
                """
                INSERT INTO department_employee
                (
                    department,
                    department_division,
                    position_title,
                    hire_date,
                    salary
                )
                VALUES (%s,%s,%s,%s,%s)
                """,
                (
                    e.department,
                    e.department_division,
                    e.position_title,
                    e.hire_date,
                    e.salary
                )
            )

            # update department total
            cur.execute(
                """
                INSERT INTO department_employee_salary
                (
                    department,
                    total_salary
                )
                VALUES (%s,%s)
                ON CONFLICT(department)
                DO UPDATE SET
                total_salary =
                department_employee_salary.total_salary + %s
                """,
                (
                    e.department,
                    int(e.salary),
                    int(e.salary)
                )
            )
            print(
                f"Inserted {e.department} salary={e.salary}"
            )

        except Exception as err:
            print(err)

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()


if __name__ == '__main__':
    consumer = SalaryConsumer(group_id="salary-consumer-clean") #what is the group id here?
    consumer.consume([employee_topic_name],ConsumingMethods.add_salary) #what is the topic here?