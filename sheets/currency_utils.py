import requests

def convert_currency(amount, from_currency, to_currency):
    url = f"https://api.exchangerate.host/convert?from={from_currency}&to={to_currency}"
    response = requests.get(url)
    data = response.json()
    
    if data.get("result"):
        rate = data["info"]["rate"]
        converted_amount = amount * rate
        return {
            "rate": rate,
            "converted_amount": converted_amount
        }
    else:
        raise ValueError("Failed to get conversion rate")

# Example usage:
# result = convert_currency(1000, "THB", "USD")