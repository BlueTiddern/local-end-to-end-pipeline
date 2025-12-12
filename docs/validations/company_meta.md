_Required Fields from Company Metadata source_

**SOURCE:** FMP data API

**Required Fields**

- **company_name** : Official registered name of the company
- **ticker** : Publicly traded stock ticker symbol
- **price** : Latest available market price per share
- **market_cap** : Total market capitalization (price × number of outstanding shares)
- **sector** : High-level sector classification (e.g., Technology, Healthcare)
- **industry** : More granular industry classification inside the sector

---

# _Validation Constraints_

| Field               | Data Type | Allowed Range / Values                                 | Can be Negative? | Additional Constraints                                        |
|---------------------|-----------|--------------------------------------------------------|------------------|---------------------------------------------------------------|
| company_name        | string    | 1–200 characters                                       | No               | Not null, not empty; can have ",.-" and numbers               |
| ticker              | string    | 1–7 alphanumeric (UPPERCASE preferred)                 | No               | Not null, not empty; must match known exchange-listed symbols |
| price               | decimal   | > (greater than) 0                                     | No               | Not null; usually 2 decimal places                            |
| market_cap          | decimal   | ≥ 0                                                    | No               | Not null; realistic ranges vary widely by company size        |
| sector              | string    | Valid sector taxonomy (e.g., Technology, Energy, etc.) | No               | Not null; must map to approved sector list                    |
| industry            | string    | Valid industry classification within a sector          | No               | Not null; must match approved industry hierarchy              |
