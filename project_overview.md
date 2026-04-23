# RetailVelocity

> High-Performance E-Commerce Sales Analytics & Forecasting

**🎯 Business Value:**
In the competitive e-commerce landscape, inventory mismanagement and generic marketing are silent revenue killers. This project transforms raw transaction logs into a strategic asset. By leveraging high-performance analytics, the goal is to reduce stockout rates (lost revenue) by 15–20% and optimize marketing spend by identifying high-value customer segments, ultimately boosting net revenue and operational efficiency.

**🚀 The "Polars" Advantage:**
Unlike traditional Pandas-based projects, this project utilizes **Polars** to demonstrate the ability to handle **Big Data** constraints.

- **Speed & Scale:** E-commerce transaction logs often span millions of rows. Polars' multi-threaded engine allows for data ingestion and aggregation that is 10–20x faster than Pandas. This means analyzing 3 years of historical data in seconds rather than minutes.
- **Memory Efficiency:** Using Polars' **Lazy API**, the project scans data without loading it entirely into memory, enabling analysis on datasets larger than the machine's RAM.
- **Modern Data Stack:** Showcases familiarity with the next generation of data tools, signaling to recruiters that you are future-proof and performance-oriented.

**📈 Core Tasks & Methodology:**

1.  **Descriptive Analytics:** Rapidly aggregate massive transaction datasets to visualize sales velocity and seasonality.
2.  **Customer Segmentation (RFM):** Efficiently group millions of transaction records by user to calculate Recency, Frequency, and Monetary value without crashing the kernel.
3.  **Demand Forecasting:** Build time-series models to predict future SKU-level demand, feeding directly into supply chain planning.
4.  **Cohort Analysis:** Track customer retention over time to identify churn points.

**🛠️ Tech Stack:**

- **Data Manipulation:** **Polars** (for high-performance dataframes), Python
- **Modeling:** Scikit-Learn, Prophet/Statsmodels (for time series)
- **Database:** PostgreSQL (handling structured transaction data)
- **Visualization/BI:** Streamlit (for executive dashboards), Matplotlib/Seaborn (for deep-dive notebooks)
- **VCS:** Git & GitHub

**📁 Data Sources:**

- **Primary:** Large-scale transaction logs (simulated or via Kaggle "E-commerce Data" sets expanded to 1M+ rows for testing performance).
- **Secondary:** Product metadata tables, Customer demographics.

---

### 🔍 Deep Dive: Analysis Dimensions

#### 1. 🔍 Descriptive: "The Health Pulse"

**Objective:** Understand the baseline performance of the business across time and geography.
**Process with Polars:**

- Utilize Polars' powerful `group_by` and `aggregation` expressions to compute daily/monthly sales in milliseconds.
- Use `rolling` window functions to smooth out noisy daily data and identify underlying trends.
  **Visualization Ideas:**
- **Interactive Line Charts:** Sales vs. Profit Margin over time, filtered dynamically by Region or Product Category.
- **Heatmaps:** Sales intensity by "Day of Week" vs. "Hour of Day" to optimize email blast timing.

#### 2. 🔬 Diagnostic: "The Search for Gold"

**Objective:** separate the best customers from the one-time buyers and identify underperforming inventory.
**Process with Polars (RFM Modeling):**

- **Recency:** Calculate the days since the last purchase for every unique customer using date arithmetic.
- **Frequency:** Count the number of unique invoices per customer efficiently using `value_counts`.
- **Monetary:** Sum the total spend per customer.
- **Segmentation:** Use quantile-based `when().then().otherwise()` expressions in Polars to bucket users into tiers (Platinum, Gold, Silver, Bronze) instantly.
  **Visualization Ideas:**
- **Scatter Plots:** RFM Distribution (Frequency vs. Monetary) colored by Recency tier.
- **Bar Charts:** "Dead Stock" report—items with zero sales in the last 6 months but high inventory value.

#### 3. 🔮 Predictive: "Looking Ahead"

**Objective:** Move from "What happened?" to "What will happen?" to prevent stockouts.
**Process:**

- Feed the preprocessed Polars dataframes (converted to NumPy arrays or Arrow tables) into forecasting models.
- **Multi-series Forecasting:** Train models for the top 50 SKUs individually.
- **Evaluation:** Use MAPE (Mean Absolute Percentage Error) to validate accuracy against a hold-out set.
  **Visualization Ideas:**
- **Forecast Curves:** Historical sales lines extending into the future with shaded **confidence intervals** (80% and 95%) to show risk tolerance to the supply chain team.

#### 4. 🎯 Prescriptive: "Actionable Intelligence"

**Objective:** Turn data into a to-do list for the marketing and logistics teams.
**Process:**

- Calculate "Reorder Points" based on lead times and forecast error rates.
- Identify "At-Risk" high-value customers (High Monetary but low Recency).
  **Visualization Ideas:**
- **Interactive Dashboards:** A "Supply Chain Command Center" showing current inventory vs. forecasted demand (Red/Yellow/Green indicators).
- **Recommendation Lists:** "Top 10 products to discount today" based on overstock predictions.

---

### 💡 Interview Highlights (The Polars Narrative)

**1. Performance Engineering:**

> "In this project, I replaced the standard Pandas workflow with **Polars**. I was able to perform a full RFM segmentation on **2 million transaction records in under 3 seconds**—a process that took over 45 seconds with Pandas. This speed allowed me to run the segmentation daily rather than monthly, making our marketing triggers significantly more relevant."

**2. Handling "Big Data" on Small Machines:**

> "I utilized Polars' **LazyFrame** API to scan and filter a 20GB dataset without loading it into RAM. This proved that I can perform analysis on large-scale production data using standard consumer hardware, reducing infrastructure costs."

**3. Bridging Analytics to Finance:**

> "My forecasting model didn't just predict sales; it quantified the financial risk of stockouts. By identifying a consistent 15% forecast error in 'Seasonal Items,' I proposed a safety stock buffer that reduced emergency shipping costs by an estimated $50k annually."

### 📂 Project Deliverables (Your Output)

1.  **Jupyter Notebook:** A step-by-step analysis using Polars, documenting the logic behind every cleaning step and calculation.
2.  **Interactive Streamlit Dashboard:** A live app showing KPIs, trends, and the SKU forecast view.
3.  **Technical Write-up:** A blog post or README section comparing the syntax and speed of Polars vs. Pandas within the context of this project.
