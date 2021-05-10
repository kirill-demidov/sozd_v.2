import pyodata
import requests

# SERVICE_URL ='http://knesset.gov.il/Odata/Parliamentinfo.svc/'

SERVICE_URL = 'http://services.odata.org/V2/Northwind/Northwind.svc/'

northwind = pyodata.Client(SERVICE_URL, requests.Session())


# count = northwind.entity_sets.KNS_Bill.get_entities().count().execute()

# print(count)
employees = northwind.entity_sets.Employees.get_entities()
employees_t = employees.execute()
for employee in employees:
    print(employee)