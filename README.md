# End-to-End Customer Segmentation Pipeline using MySQL  

## 📌 Executive Summary  
This project demonstrates the development of a **fully automated customer segmentation pipeline** using **MySQL 8** on the Olist e-commerce dataset.  

### Business Problem  
Without segmentation, marketing campaigns are generic and inefficient. The objective was to create a **data-driven framework** that empowers personalized marketing, improves customer retention, and increases lifetime value.  

### Solution  
I designed and deployed a **complete ETL and customer segmentation system** entirely in SQL. The pipeline ingests raw transactional data, applies an **RFM (Recency, Frequency, Monetary) model**, and outputs actionable customer segments for downstream BI and marketing activation.  

---

## ⚙️ Tech Stack  
- **Database:** MySQL 8  
- **Language:** SQL (CTEs, Window Functions, Stored Procedures, Events)  
- **Tools:** MySQL Workbench / CLI  

---

## 🔄 Project Workflow  

### **Phase 1: Data Ingestion & Schema Design**  
- Loaded 8 raw CSV files (customers, orders, payments, products, etc.) into a relational schema.  
- Designed staging tables to ensure data integrity during ingestion.  

### **Phase 2: Data Transformation (ETL)**  
- Created a canonical `order_values` table by cleaning & aggregating raw transactional data.  
- Engineered a customer-level **RFM data model** with metrics:  
  - **Recency:** Days since last purchase  
  - **Frequency:** Number of distinct orders  
  - **Monetary:** Total lifetime spend  

### **Phase 3: RFM Scoring & Segmentation**  
- Applied **NTILE() window functions** to assign scores (1–5) for R, F, and M.  
- Classified customers into **6 actionable segments** using CASE logic:  
  - Champions, Loyal Customers, Potential Loyalists, New Customers, At-Risk, Lost Customers  

### **Phase 4: Productionalization & Automation**  
- Encapsulated the entire pipeline into a **Stored Procedure** (`CALL refresh_customer_segments();`).  
- Scheduled monthly execution using **MySQL Events** for hands-free updates.  
- Created **BI-ready Views** (`vw_customer_segments`, `vw_customer_rfm`) for Tableau/Power BI integration.  

---

## 📊 Key Outcomes  
- **Automated segmentation pipeline** with monthly refreshes.  
- Final **customer_segments table** covering 90,000+ customers.  
- Business deliverables: Strategic recommendations + sample personalized campaigns.  

---

## 🚀 Business Impact  
This pipeline allows businesses to:  
- Run **data-driven, personalized marketing campaigns**.  
- Track **customer lifecycle health** (retention, churn, growth).  
- Integrate directly with BI dashboards for **real-time decision-making**.  

---

## 📂 Repository Structure  
```plaintext
.
├── CSA.sql                      # The complete end-to-end SQL pipeline script.
├── outputs/                     # Folder containing all exported CSV results from the analysis.
│   ├── final_customer_segments.csv  # Main deliverable: The final segment assigned to each customer.
│   ├── segment_summary_analysis.csv # High-level business summary of each segment's value and size.
│   ├── customer_rfm_scores.csv      # The underlying RFM (Recency, Frequency, Monetary) scores.
│   ├── intermediate_order_values.csv  # The cleaned transaction data (ETL output) used for the model.
│   └── bi_view_customer_segments.csv  # A clean data export ready for BI tools like Tableau or Power BI.
└── README.md                    # Project overview, setup instructions, and key findings.
