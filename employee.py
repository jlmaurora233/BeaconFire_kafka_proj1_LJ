import json


class Employee:
    def __init__(
        self,
        department='',
        department_division='',
        position_title='',
        hire_date='',
        salary=0
    ):
        self.department = department
        self.department_division = department_division
        self.position_title = position_title
        self.hire_date = hire_date
        self.salary = salary

    def to_json(self):
        return json.dumps(self.__dict__)