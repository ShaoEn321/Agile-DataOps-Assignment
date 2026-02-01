# Agile DataOps (Assignment 2) — Contoso Cloud Migration + Trusted Reporting Layer

This repository contains our end-to-end **DataOps pipeline** that migrates the **Contoso retail dataset** into a cloud data warehouse, builds a **trusted reporting layer**, and delivers **role-based dashboards** for business stakeholders.

> Tech stack: Snowflake, SQL (ELT), GitHub (version control + CI/CD workflow), Power BI (DirectQuery dashboards), Snowflake CLI (bonus tool)

---

## Project Summary

**Objective:** Modernize an aging on-premises analytics setup by migrating Contoso’s retail data into a scalable cloud platform, unifying siloed datasets into a **single source of truth**, and enabling near real-time reporting for omni-channel decision-making. :contentReference[oaicite:2]{index=2}

**What we built:**
- A reproducible ELT pipeline in **Snowflake** with clear run order and validation scripts  
- A star-schema style **data warehouse / trusted layer** for analytics performance  
- **Power BI dashboards** connected via **DirectQuery** for live (un-cached) reporting  
- A GitHub-based workflow for traceability and controlled deployments (Sandbox → Main) :contentReference[oaicite:3]{index=3}

---

## Key Results / Insights

From the trusted reporting layer + dashboards, we surfaced actionable business insights, including:
- Identification of strong profitability drivers (e.g., ~57% profit margin patterns) and category-level opportunities :contentReference[oaicite:4]{index=4}  
- Discovery of “dead stock” candidates (legacy MP3/DVD items) for liquidation to improve inventory efficiency :contentReference[oaicite:5]{index=5}  
- A reporting experience designed for fast executive visibility and drill-down exploration (slicers + deep-dive pages) :contentReference[oaicite:6]{index=6}  

---


