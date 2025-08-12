from db import db_resulth
from datetime import date
from db_data import insert_data


# Database search result
resulth = db_resulth()

# Billing, overdue accounts, receivable
billing = round(resulth[0], ndigits=2)
overdue = round(resulth[1], ndigits=2)
to_receive = round(resulth[2], ndigits=2)

# Calculus Memorial
average_receipt_period = ((overdue + to_receive) / billing) * 365
spin = 365 / average_receipt_period
delay = overdue / (overdue + to_receive)

# Data to be written to the SQLite database
data1 = (date.today(), average_receipt_period, spin, delay)
data2 = (date.today(), overdue, to_receive, billing)
insert_data(data1, data2)
