WITH netsuite_date AS (

    {% set tables = ['accounting_books' , 'accounting_periods','accounts','classes','currencies','customers','departments','entity','subsidiaries','transaction_lines','vendors' ] %}	
                                                          
    {% for table in tables %} 
    SELECT '{{table}}'                                                            AS table_name,
        MAX(
            {% if table == 'transaction_lines' %}
                convert_timezone('Etc/GMT','Etc/UTC',date_last_modified_gmt::TIMESTAMP)  -- As the column is in GMT, need to convert it to UTC
            {% else %}
                date_last_modified
            {% endif %}
            )                                                   AS max_date
    FROM {{source('netsuite', table)}}  
  
  
    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %} 
  
)


  SELECT *
  FROM netsuite_date 
  