# 📊 AmazonBI — Product Analytics ELT Pipeline

AmazonBI is a complete **ELT** pipeline for e-commerce analytics, built in **Python + PostgreSQL**, using a combination of data sources, warehouse logic, a star schema, and a Streamlit dashboard for reporting.

---

## 📁 Project Structure

```
AmazonBI/
├── config/                      # DB settings
│   └── settings.py
│
├── elt/                         # ELT logic
│   ├── full_load_warehouse.py
│   ├── incremental_load_warehouse.py
│   ├── full_load_star.py
│   ├── incremental_load_star.py
│   ├── pull_exchange_rates.py
│   └── run_incremental_elt.ps1      # Daily-scheduled runner
│
├── models/                      # Source data loaders and mock test tools
│   ├── amazon_products.csv
│   ├── amazon_products_cleaned.csv
│   ├── csvClean.py
│   ├── database.py
│   └── mock.py
│
├── reports/
│   └── dashboard.py             # Streamlit-based reporting app
```

---

## 🛠️ Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/AmazonBI.git
   cd AmazonBI
   ```

2. Configure your PostgreSQL connection in `config/settings.py`.

3. Create a virtual environment:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # or source .venv/bin/activate on Unix
   pip install -r requirements.txt
   ```

---

## 📥 Data Sources

### ✅ Source 1: Relational (CSV → `public.*`)

- CSV cleaned using `csvClean.py`
- Loaded into:
  - `public.products`
  - `public.categories`
  - `public.users`
  - `public.reviews`
  - `public.locations`

### 🌐 Source 2: API Exchange Rates (`staging.*`)

- Script: `pull_exchange_rates.py`
- Inserts data into `staging.exchange_rates_raw`

---

## 🔄 ELT Pipeline

### 🧱 Full Loads

| Stage              | Script                   |
| ------------------ | ------------------------ |
| Source → Warehouse | `full_load_warehouse.py` |
| Warehouse → Star   | `full_load_star.py`      |

### ♻️ Incremental Loads

- Implemented using **SCD Type 2** logic (start/end dates, insert/update IDs)

| Stage              | Script                          |
| ------------------ | ------------------------------- |
| Source → Warehouse | `incremental_load_warehouse.py` |
| Warehouse → Star   | `incremental_load_star.py`      |

---

## 🗓️ Daily Scheduling

### 🔁 Script:

```ps1
# run_incremental_elt.ps1
python .\etl\incremental_load_warehouse.py
python .\etl\incremental_load_star.py
```

### 📅 Schedule Daily at 1 AM:

```powershell
schtasks /Create `
  /TN "AmazonBI Incremental ELT" `
  /TR "powershell -NoProfile -ExecutionPolicy Bypass -File \"C:\Path\To\run_incremental_elt.ps1\"" `
  /SC DAILY /ST 01:00 /RL HIGHEST
```

### ✅ Check Schedule:

```bash
schtasks /Query /TN "AmazonBI Incremental ELT" /V /FO LIST
```

---

## 🧪 Mock Data for Testing

Script: `models/mock.py`

- Inserts mock products, reviews, users, and locations
- Updates currency (e.g. USD → EUR)
- Deletes selected products and cascades deletes to related rows
- Validates:
  - SCD2 logic
  - Star schema updates
  - Dashboard refresh behavior

---

## 📊 Streamlit Dashboard

### Launch:

```bash
python -m streamlit run reports/dashboard.py
```

### 🔍 Features:

- Filters by date, category, and product
- KPI cards for:
  - Average price
  - Discount %
  - Rate to base currency
- Charts:
  - Bar chart: price by category
  - Line chart: product price over time
- Interactive data table

---

## ✅ Summary Flow

```
CSV File ─────▶ public schema
                      │
Exchange Rate API ───▶ staging schema
                      │
    [Full/Incremental Loads]
                      ↓
              warehouse schema
                      ↓
              star schema (facts & dims)
                      ↓
               Streamlit Dashboard
```

Public schema ERD:

![image](https://github.com/user-attachments/assets/5114216e-821a-4d66-83b6-3d6b90726e84)


Staging schema ERD:

![image](https://github.com/user-attachments/assets/7baba7bd-3c30-44ae-8d36-5974c3fae10e)


Warehouse schema ERD:

![image](https://github.com/user-attachments/assets/9feebe3f-4033-48d2-bae2-106d0f9666de)



Star schema ERD:

![image](https://github.com/user-attachments/assets/f304bcbc-5764-4893-95f5-c65567da346a)








