_Required Fields from Exchange Rate source_

**SOURCE:** Frankfuter data API

**Required Fields**

- **date** : Calendar date for which the exchange rate applies
- **inr_rate** : Amount of Indian Rupees (INR) per 1 US Dollar (USD) on the given date
- **usd_value** : Base amount in US Dollars (USD) used for the rate (typically 1)

---

# _Validation Constraints_

| Field     | Data Type  | Allowed Range / Values    | Can be Negative?       | Additional Constraints                                                  |
|-----------|------------|---------------------------|------------------------|-------------------------------------------------------------------------|
| date      | date       | ≤ today                   | No                     | Not null; cannot be a future date                                       |
| inr_rate  | decimal    | greater than 0            | No                     | Not null; maximum of 3 decimal places; represents INR per 1 USD         |
| usd_value | decimal    | greater than 0            | No                     | Not null; typically `1`  for base currency amount; positive values only |
