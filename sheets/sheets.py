# aggregation / pivot / summary
import datetime
import os
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

def process_receipts(data):
    daily = {}
    weekly = {}
    monthly = {}

    for row in data:
        #check each row
        print("Date:", row["date"])
        print("Vendor:", row["vendor"])
        print("Total:", row["total"])
        print("Tax:", row["tax"])
        print("Currency:", row["currency"])
        print("Items:", row["item"])
        print("User_ID:", row["user_id"])
        print("------------")

        # Currency conversion
        if row["currency"] != "THB":
            from sheets.currency_utils import convert_currency
            conversion = convert_currency(row["total"], row["currency"], "THB")
            # print(f"Converted {row['total']} {row['currency']} to {conversion['converted_amount']:.2f} THB at rate {conversion['rate']:.4f}")
            row["total"] = conversion['converted_amount']

        #sum by day
        if row["date"] not in daily:
            daily[row["date"]] = {}
            daily[row["date"]]["expense"] = 0
            daily[row["date"]]["expense_ratio"] = 0
        daily[row["date"]]["expense"] += row["total"]

        date_str = row["date"]  # e.g., "2025-10-06"
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        year = date_obj.year
        if year not in weekly:
            weekly[year] = {} 
            monthly[year] = {} 

        #sum by week
        week = date_obj.isocalendar()[1]
        if week not in weekly[year]:
            weekly[year][week] = {}
            weekly[year][week]["expense"] = 0
            weekly[year][week]["expense_ratio"] = 0
        weekly[year][week]["expense"] += row["total"]

        #sum by month
        month = date_obj.month  # directly get month number
        if month not in monthly[year]:
            monthly[year][month] = {}
            monthly[year][month]["expense"] = 0
            monthly[year][month]["expense_ratio"] = 0
        monthly[year][month]["expense"] += row["total"]

    return daily, weekly, monthly


def save_receipt(data, type):
    print(f"Mock save to Supabase ({type}):", data)

    if f"{type}".lower() == "date":
        data_list = list(map(lambda x: {
            f"{type}".lower(): x[0], 
            "expense(THB)": x[1].get('expense', 0),        # use .get() to avoid KeyError
            "expense_ratio": x[1].get('expense_ratio', 0)
            }, data.items()))
        
        for i in range(1, len(data_list)):
            prev = data_list[i-1]
            curr = data_list[i]
 
            ratio = (curr["expense(THB)"]- prev["expense(THB)"]) / prev["expense(THB)"]
            curr["expense_ratio"] = round(ratio * 100, 2) # percentage change
        response = supabase.table(f"Sum by {type}").upsert(data_list).execute()
        return 0
    else:
        data_list = [
        {
            "year": year,
            f"{type}".lower(): t,
            "expense(THB)": type_data.get("expense", 0),
            "expense_ratio": type_data.get("expense_ratio", 0)
        }
        for year, type_ in data.items()        # outer dict
        for t, type_data in type_.items()   # inner dict
]
        
        if len(data_list) == 1 :
            for i in range(1, len(data_list[1])):
                prev = data_list[1][i-1]
                curr = data_list[1][i]
                ratio = (curr["expense(THB)"]- prev["expense(THB)"]) / prev["expense(THB)"]

            curr["expense_ratio"] = round(ratio * 100, 2) # percentage change
        else :
            for i in range(1, len(data_list)):
                prev = data_list[i-1]
                curr = data_list[i]
                ratio = (curr["expense(THB)"]- prev["expense(THB)"]) / prev["expense(THB)"]

                curr["expense_ratio"] = round(ratio * 100, 2) # percentage change
    for item in data_list:
        response = supabase.table(f"Sum by {type}").upsert(item).execute()
    print(response.data)

    print(f"Completed saving receipt and items to Supabase ({type}).")

def save_items_pivot(data):
    print("Mock save to Supabase (Item Pivot):", data)
    stored_items = {}
    for row in data:
        items = row["item"]

        if not items:
            continue
        for item in items:
            name = item.get("name")
            qty = item.get("qty", 1)

            # Currency conversion
            if row["currency"] != "THB":
                from sheets.currency_utils import convert_currency
                conversion = convert_currency(row["unit_price(THB)"], row["currency"], "THB")
                # print(f"Converted {row['total']} {row['currency']} to {conversion['converted_amount']:.2f} THB at rate {conversion['rate']:.4f}")
                print(conversion)
                row["unit_price(THB)"] = conversion['converted_amount']
            unit_price = item.get("unit_price(THB)", 0)
            if name not in stored_items:
                stored_items[name] = {"qty": 0, "unit_price(THB)": item.get("unit_price", 0)}
            stored_items[name]["qty"] += qty
        
        data_list = list(map(lambda x: {
            "name": x[0], 
            "qty": x[1].get('qty', 0),        # use .get() to avoid KeyError
            "unit_price(THB)": x[1].get('unit_price(THB)', 0),
            "expense(THB)": x[1].get('qty', 0) * x[1].get('unit_price(THB)', 0)
            }, stored_items.items()))
        response = supabase.table("Items_pivot").upsert(data_list).execute()
        print(response.data)

    print("Completed saving item pivot to Supabase.")

load_dotenv()

url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

response = supabase.table("Receipts").select("*").execute()
data = response.data

day_dict, week_dict, month_dict = process_receipts(data)

save_receipt(day_dict, "Date")
save_receipt(week_dict, "Week")
save_receipt(month_dict, "Month")

save_items_pivot(data)