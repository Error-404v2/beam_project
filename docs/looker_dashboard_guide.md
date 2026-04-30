# Looker Studio Dashboard — Construction Guide

## Your 5 BigQuery Tables

| Table | What It Contains | Rows |
|---|---|---|
| `summary_stats` | Hospital-wide KPIs (1 row) | 1 |
| `top_diagnoses` | Top 10 ICD codes by frequency | 10 |
| `patient_dashboard_summary` | Per-patient summary card | 1 per patient |
| `patient_admissions_timeline` | Each admission event | 1 per admission |
| `patient_lab_history` | Lab results with values + flags | 1 per lab result |

---

## Dashboard Layout (3 Pages)

### Page 1: Hospital Overview (uses `summary_stats` + `top_diagnoses`)

```
┌─────────────────────────────────────────────────────────────────┐
│                    🏥 Hospital Overview                         │
├──────────┬──────────┬──────────┬──────────┬─────────────────────┤
│ Total    │ Total    │ Avg Age  │ Mortality│ Alive / Deceased    │
│ Patients │ Diagnoses│          │ Rate %   │                     │
│ [KPI]    │ [KPI]    │ [KPI]    │ [KPI]    │ [Donut Chart]       │
├──────────┴──────────┴──────────┴──────────┴─────────────────────┤
│                                                                 │
│  Gender Split          │  Top 10 Diagnoses                      │
│  [Pie Chart]           │  [Horizontal Bar Chart]                │
│  M vs F                │  icd_code + description → count        │
│                        │                                        │
├────────────────────────┴────────────────────────────────────────┤
│  Age Range: min_age — max_age    │  Avg Diagnoses/Patient       │
│  [Scorecard]                     │  [Scorecard]                 │
└──────────────────────────────────┴──────────────────────────────┘
```

### Page 2: Patient Explorer (uses `patient_dashboard_summary` + `patient_admissions_timeline`)

```
┌─────────────────────────────────────────────────────────────────┐
│                    👤 Patient Explorer                          │
├─────────────────────────────────────────────────────────────────┤
│ [Filter Bar] — Gender dropdown │ Alive/Deceased │ Age slider    │
├──────────┬──────────┬──────────┬────────────────────────────────┤
│ Total    │ Avg      │ Avg Meds │ Avg Procedures                 │
│ Patients │ Admiss.  │ per Pt   │ per Patient                    │
│ [KPI]    │ [KPI]    │ [KPI]    │ [KPI]                          │
├──────────┴──────────┴──────────┴────────────────────────────────┤
│                                                                 │
│  Admissions by Year              │  Patient Detail Table        │
│  [Bar Chart / Line Chart]        │  ┌────────────────────────┐  │
│  from admission_year             │  │ ID │ Age │ Adm │ Proc │  │
│                                  │  │    │     │ Cnt │ Cnt  │  │
│                                  │  │    │     │ Med │ Diag │  │
│                                  │  └────────────────────────┘  │
│                                  │  [Sortable Table w/ search]  │
├──────────────────────────────────┴──────────────────────────────┤
│  Admissions Distribution         │  Procedures vs Meds Scatter  │
│  [Histogram]                     │  [Scatter Plot]              │
│  admission_count buckets         │  x=procedure_count           │
│                                  │  y=unique_med_count           │
└──────────────────────────────────┴──────────────────────────────┘
```

### Page 3: Lab Results (uses `patient_lab_history`)

```
┌─────────────────────────────────────────────────────────────────┐
│                    🔬 Lab Results Explorer                      │
├─────────────────────────────────────────────────────────────────┤
│ [Filter Bar] — Lab Category │ Lab Name │ Patient ID │ Abnormal  │
├──────────┬──────────┬──────────┬────────────────────────────────┤
│ Total    │ Abnormal │ Abnormal │ Unique Labs                    │
│ Lab Tests│ Count    │ Rate %   │ Tested                         │
│ [KPI]    │ [KPI]    │ [KPI]    │ [KPI]                          │
├──────────┴──────────┴──────────┴────────────────────────────────┤
│                                                                 │
│  Abnormal Rate by Lab Category   │  Lab Results Detail Table    │
│  [Stacked Bar Chart]             │  ┌────────────────────────┐  │
│  Normal vs Abnormal per category │  │ Patient │ Lab  │ Value │  │
│                                  │  │ ID      │ Name │ Unit  │  │
│                                  │  │ hadm_id │ Flag │ Time  │  │
│                                  │  └────────────────────────┘  │
├──────────────────────────────────┴──────────────────────────────┤
│  Lab Value Distribution (selected lab)                          │
│  [Histogram / Box Plot]                                         │
│  valuenum distribution for filtered lab_label                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Step-by-Step Construction in Looker Studio

### Step 1: Connect BigQuery Data Sources

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com)
2. Click **Create → Report**
3. Click **Add data** → **BigQuery**
4. Select project: `cloudypedia-intern` → dataset: `hospital_profiling`
5. Add **all 5 tables** one by one:
   - `summary_stats`
   - `top_diagnoses`
   - `patient_dashboard_summary`
   - `patient_admissions_timeline`
   - `patient_lab_history`

### Step 2: Create a Blend (Join) for Patient + Admissions

> [!IMPORTANT]
> To show admission details alongside patient info, you need a **data blend**.

1. Click **Resource → Manage blends → Add a blend**
2. **Left table:** `patient_dashboard_summary`
3. **Right table:** `patient_admissions_timeline`
4. **Join condition:** `subject_id = subject_id`
5. **Join type:** LEFT JOIN
6. Name it: `Patient + Admissions`

### Step 3: Build Page 1 — Hospital Overview

| Component | Chart Type | Data Source | Configuration |
|---|---|---|---|
| Total Patients | Scorecard | `patient_dashboard_summary` | Metric: `Record Count` |
| Total Diagnoses | Scorecard | `summary_stats` | Metric: `stat_value` (MAX), Filter: `stat_name = "total_diagnoses"` |
| Unique Codes | Scorecard | `summary_stats` | Metric: `stat_value` (MAX), Filter: `stat_name = "unique_codes"` |
| Avg Age | Scorecard | `patient_dashboard_summary` | Metric: `age` (AVG) |
| Mortality Rate | Scorecard | `patient_dashboard_summary` | Metric: `Mortality Rate` (Calculated field below) |
| Gender Donut | Pie Chart | `patient_dashboard_summary` | Dimension: `gender`, Metric: `Record Count` |
| Top 10 Diagnoses | Bar Chart | `top_diagnoses` | Dimension: `description`, Metric: `count`, Sort: `count` DESC |
| Age Range | Scorecard | `patient_dashboard_summary` | Create two scorecards using `age` (MIN) and `age` (MAX). |

> [!TIP]
> **Important Note:** Most of the hospital-wide statistics (like Total Patients, Age, and Gender) are now dynamically calculated in Looker Studio directly from the `patient_dashboard_summary` table! The `summary_stats` table is now strictly reserved for metrics that cannot be calculated from the patient grain, specifically `total_diagnoses` and `unique_codes`.

### Step 4: Build Page 2 — Patient Explorer

| Component | Chart Type | Data Source | Configuration |
|---|---|---|---|
| Patient Table | Table | `patient_dashboard_summary` | Dimensions: `subject_id`, `gender`, `age`, `is_alive`. Metrics: `admission_count`, `procedure_count`, `unique_med_count`, `unique_diagnosis_count` |
| Admissions by Year | Bar Chart | `patient_admissions_timeline` | Dimension: `admission_year`, Metric: `Record Count` |
| Filter: Gender | Drop-down | `patient_dashboard_summary` | Field: `gender` |
| Filter: Alive/Dead | Drop-down | `patient_dashboard_summary` | Field: `is_alive` |
| Scatter Plot | Scatter | `patient_dashboard_summary` | X: `procedure_count`, Y: `unique_med_count`, Dimension: `subject_id` |

### Step 5: Build Page 3 — Lab Explorer

| Component | Chart Type | Data Source | Configuration |
|---|---|---|---|
| Total Labs | Scorecard | `patient_lab_history` | Metric: `Record Count` |
| Abnormal Count | Scorecard | `patient_lab_history` | Metric: `Record Count`, Filter: `is_abnormal = true` |
| Lab Detail Table | Table | `patient_lab_history` | Dimensions: `subject_id`, `hadm_id`, `lab_label`, `charttime`, `value`, `valueuom`, `flag` |
| Abnormal by Category | Stacked Bar | `patient_lab_history` | Dimension: `lab_category`, Breakdown: `is_abnormal`, Metric: `Record Count` |
| Filter: Lab Category | Drop-down | `patient_lab_history` | Field: `lab_category` |
| Filter: Patient ID | Input box | `patient_lab_history` | Field: `subject_id` |

### Step 6: Create Calculated Fields

Add these in **Resource → Manage added data sources → Edit**:

```sql
```sql
-- Mortality Rate (%) — in patient_dashboard_summary
Mortality_Rate = COUNTIF(is_alive = false) / COUNT(subject_id)

-- Abnormal Rate (%) — in patient_lab_history
Abnormal_Rate = COUNTIF(is_abnormal = true) / COUNT(is_abnormal) * 100

-- Admission Bucket — in patient_dashboard_summary  
Admission_Bucket = CASE 
  WHEN admission_count = 1 THEN "1 visit"
  WHEN admission_count BETWEEN 2 AND 3 THEN "2-3 visits"
  WHEN admission_count BETWEEN 4 AND 6 THEN "4-6 visits"
  ELSE "7+ visits"
END

-- Age Group — in patient_dashboard_summary
Age_Group = CASE
  WHEN age < 30 THEN "Under 30"
  WHEN age BETWEEN 30 AND 49 THEN "30-49"
  WHEN age BETWEEN 50 AND 69 THEN "50-69"
  ELSE "70+"
END
```

---

## Styling Tips

- **Theme:** Use a dark medical theme (dark navy `#1a1a2e` background, white text, accent color `#00d4aa`)
- **KPI Cards:** Large font numbers with subtle background cards
- **Charts:** Use consistent color palette across all pages
- **Filters:** Place at the top of each page in a horizontal filter bar
- **Navigation:** Add page navigation tabs at the top

---

## What You CAN Answer with Current Tables

| Question | ✅/❌ | How |
|---|---|---|
| How many times was Patient X admitted? | ✅ | `patient_dashboard_summary.admission_count` |
| When were Patient X's admissions? | ✅ | `patient_admissions_timeline` filtered by `subject_id` |
| How many procedures did Patient X have? | ✅ | `patient_dashboard_summary.procedure_count` |
| How many medications did Patient X take? | ✅ | `patient_dashboard_summary.unique_med_count` |
| What are Patient X's lab results? | ✅ | `patient_lab_history` filtered by `subject_id` |
| Were any labs abnormal? | ✅ | `patient_lab_history.is_abnormal` / `flag` |
| Lab results for a specific admission? | ✅ | Filter `patient_lab_history` by `hadm_id` |
| *Which* specific procedures? | ❌ | Only count available, not detail |
| *Which* specific medications? | ❌ | Only count available, not detail |
| *Which* diagnoses per admission? | ❌ | Not linked to `hadm_id` |

> [!NOTE]
> You can build a **solid, useful dashboard** with your current 5 tables. The counts for procedures/meds give you a good overview. If you later want drill-down into specific procedure or medication names, you'll need the detail tables added to the pipeline.
