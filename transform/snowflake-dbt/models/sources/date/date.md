## Columns

{% docs date_date_day %}
Calendar date, e.g. '2019-02-02'
{% enddocs %}

{% docs date_date_actual %}
Matches `date_day`, duplicated for ease of use
{% enddocs %}

{% docs date_day_name %}
Abbreviated name of the day of the week, e.g. 'Sat' for 2019-02-02
{% enddocs %}

{% docs date_month_actual %}
Number for the calendar month of the year, e.g. '2' for 2019-02-02
{% enddocs %}

{% docs date_year_actual %}
Calendar year, e.g. '2019' for 2019-02-02
{% enddocs %}

{% docs date_quarter_actual %}
Calendar quarter, e.g. '1' for 2019-02-02
{% enddocs %}

{% docs date_day_of_week %}
Number of the day of the week, with Sunday = 1 and Saturday = 7
{% enddocs %}

{% docs date_first_day_of_week %}
Calendar date of the first Sunday of that week, e.g. '2019-01-27' for 2019-02-02
{% enddocs %}

{% docs date_week_of_year %}
Calendar week of year, e.g. '5' for 2019-02-02
{% enddocs %}

{% docs date_day_of_month %}
Day Number of the month, e.g. '2' for 2019-02-02
{% enddocs %}

{% docs date_day_of_quarter %}
Day Number from the start of the calendar quarter, e.g. '33' for 2019-02-02
{% enddocs %}

{% docs date_day_of_year %}
Day Number from the start of the calendar year, e.g. '33' for 2019-02-02
{% enddocs %}

{% docs date_fiscal_year %}
Fiscal year for the date, e.g. '2020' for 2019-02-02
{% enddocs %}

{% docs date_fiscal_quarter %}
Fiscal quarter for the date, e.g. '1' for 2019-02-02
{% enddocs %}

{% docs date_day_of_fiscal_quarter %}
Day Number from the start of the fiscal quarter, e.g. '2' for 2019-02-02
{% enddocs %}

{% docs date_day_of_fiscal_year %}
Day Number from the start of the fiscal year, e.g. '2' for 2019-02-02
{% enddocs %}

{% docs date_month_name %}
The full month name for any calendar month, e.g. 'February' for 2019-02-02
{% enddocs %}

{% docs date_first_day_of_month %}
The first day of a calendar month, e.g. '2019-02-01' for 2019-02-02
{% enddocs %}

{% docs date_last_day_of_month %}
The last day of a calendar month, e.g. '2019-02-28' for 2019-02-02
{% enddocs %}

{% docs date_first_day_of_year %}
The first day of a calendar year, e.g. '2019-01-01' for 2019-02-02
{% enddocs %}

{% docs date_last_day_of_year %}
The last day of a calendar year, e.g. '2019-12-31' for 2019-02-02
{% enddocs %}

{% docs date_first_day_of_quarter %}
The first day of a calendar quarter, e.g. '2019-01-01' for 2019-02-02
{% enddocs %}

{% docs date_last_day_of_quarter %}
The last day of a calendar quarter, e.g. '2019-03-31' for 2019-02-02
{% enddocs %}

{% docs date_first_day_of_fiscal_quarter %}
The first day of the fiscal quarter, e.g. '2019-02-01' for 2019-02-02
{% enddocs %}

{% docs date_last_day_of_fiscal_quarter %}
The last day of the fiscal quarter, e.g. '2019-04-30' for 2019-02-02
{% enddocs %}

{% docs date_first_day_of_fiscal_year %}
The first day of the fiscal year, e.g. '2019-02-01' for 2019-02-02
{% enddocs %}

{% docs date_last_day_of_fiscal_year %}
The last day of the fiscal year, e.g. '2020-01-31' for 2019-02-02
{% enddocs %}

{% docs date_week_of_fiscal_year %}
The week number for the fiscal year, e.g. '1' for 2019-02-02
{% enddocs %}

{% docs date_month_of_fiscal_year %}
The month number for the fiscal year, e.g. '1' for 2019-02-02
{% enddocs %}

{% docs date_last_day_of_week %}
The Saturday of the week, e.g. '2019-02-02' for 2019-02-02
{% enddocs %}

{% docs date_quarter_name %}
The name of the calendar quarter, e.g. '2019-Q1' for 2019-02-02
{% enddocs %}

{% docs date_fiscal_quarter_name %}
The name of the fiscal quarter, e.g '2020-Q1' for 2019-02-02
{% enddocs %}

{% docs date_fiscal_quarter_name_fy %}
The name of the fiscal quarter, e.g 'FY20-Q1' for 2019-02-02
{% enddocs %}

{% docs date_fiscal_quarter_number_absolute %}
Monotonically increasing integer for each fiscal quarter. This allows for comparing the relative order of fiscal quarters.
{% enddocs %}

{% docs date_fiscal_month_name %}
The name of the fiscal month, e.g '2020-Feb' for 2019-02-02
{% enddocs %}

{% docs date_fiscal_month_name_fy %}
The name of the fiscal month, e.g 'FY20-Feb' for 2019-02-02
{% enddocs %}

{% docs date_holiday_desc %}
The name of the holiday, if applicable
{% enddocs %}

{% docs date_is_holiday %}
Whether or not it is a holiday
{% enddocs %}

{% docs date_last_month_of_fiscal_quarter %}
Date indicating last month of fiscal quarter e.g '2020-04-01' for 2020-02-02
{% enddocs %}

{% docs date_is_first_day_of_last_month_of_fiscal_quarter %}
Flag indicating date that is the first day of last month of fiscal quarter. E.g TRUE for '2020-04-01'
{% enddocs %}
  
{% docs date_last_month_of_fiscal_year %}
Date indicating last month of fiscal year e.g '2021-01-01' for 2020-02-02
{% enddocs %}

{% docs date_is_first_day_of_last_month_of_fiscal_year %}
Flag indicating date that is the first day of last month of fiscal year. E.g TRUE for '2021-01-01'
{% enddocs %}

{% docs date_snapshot_date_fpa %}
8th calendar day of a month used for FP&A snapshots 
{% enddocs %}

{% docs date_snapshot_date_billings %}
45 calendar day after a month begins used for Billings snapshots 
{% enddocs %}

{% docs date_days_in_month_count %}
Number of calendar days in the given month.
{% enddocs %}
