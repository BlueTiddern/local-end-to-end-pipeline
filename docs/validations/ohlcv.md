_Required Fields from Open High Low Close Volume source_

**SOURCE:** Yahoo Finance Data API - yfinanace

**Required Fields**
- `ticker : Company's stock ticker`
- `date : Date of the record`
- `open : First trade price of the period`
- `high : Highest price reached`
- `low : Lowest price reached`
- `close : The final trade price of the period`
- `volume : Total shares traded`

_Validation Constraints_

| Field  | Data Type | Allowed Range / Values           | Can be Negative? | Additional Constraints |
|--------|-----------|----------------------------------|------------------|------------------------|
| ticker | string    | 1–7 alphanumeric (usually UPPER) | -                | Not null, not empty    |
| date   | date      | Valid trading date (YYYY-MM-DD)  | -                | Not null, ≤ today      |
| open   | decimal   | > 0                              | No               | Not null               |
| high   | decimal   | ≥ max(open, low)                 | No               | Not null, high ≥ low   |
| low    | decimal   | > 0                              | No               | Not null, low ≤ high   |
| close  | decimal   | low ≤ close ≤ high               | No               | Not null               |
| volume | decimal   | ≥ 0                              | No               | Not null               |

**Prices usually 2–6 decimal places depending on exchange**

