_Required Fields from Macroeconomic Data source_

**SOURCE:** Macroeconomic data provider (country-level time series)

**Required Fields**

- **country_id** : Unique country identifier (ISO-2 or ISO-3 country code)
- **year** : Calendar year of the observation (e.g., 2020)
- **country_name** : Human-readable country name (e.g., "United States")
- **nominal_gdp** : Nominal Gross Domestic Product level for the year (USD millions)
- **real_gdp** : Real Gross Domestic Product level for the year (price-adjusted USD millions)
- **inflation** : Annual inflation rate (CPI, in percent)
- **unemployment** : Unemployment rate (in percent of the labor force)
- **short_term_rate** : Short-term interest rate (might be policy rate, 3-month T-bill, in percent)
- **long_term_rate** : Long-term interest rate (might be 10-year government bond yield, in percent)

---

# _Validation Constraints_

| Field            | Data Type   | Allowed Range / Values                       | Can be Negative?   | Additional Constraints                                                             |
|------------------|-------------|----------------------------------------------|--------------------|------------------------------------------------------------------------------------|
| country_id       | string      | 2–10 characters                              | No                 | Not null; not empty                                                                |
| year             | integer     | ≥ 1900 and ≤ current year                    | No                 | Not null; represents calendar year (e.g., 2020)                                    |
| country_name     | string      | 1–200 characters                             | No                 | Not null; not empty; should map consistently to `country_id`                       |
| nominal_gdp      | decimal     | ≥ 0                                          | No                 | Nullable; when present, non-negative; units must be consistent across dataset      |
| real_gdp         | decimal     | ≥ 0                                          | No                 | Nullable; when present, non-negative; same currency/unit convention as nominal_gdp |
| inflation        | decimal     | Reasonable macro range (e.g., -50 to 200%)   | Yes                | Nullable; when present, represents annual inflation rate in percent                |
| unemployment     | decimal     | 0–100                                        | No                 | Nullable; when present, percentage of labor force                                  |
| short_term_rate  | decimal     | Reasonable rate range (e.g., -10 to 100%)    | Yes                | Nullable; when present, annualized percent rate                                    |
| long_term_rate   | decimal     | Reasonable rate range (e.g., -10 to 100%)    | Yes                | Nullable; when present, annualized percent rate                                    |
