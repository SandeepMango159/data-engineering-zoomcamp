{#
    This macro returns the description of the payment_type using a case statement
    Safe cast is DBT built in macro that chooses the right cast depening on the data warehouse you're connected to
    api column translate type returns the platform specific type
    So this code is safer and more platform independent
#}

{% macro get_payment_type_description(payment_type) -%}

    case {{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }}  
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'EMPTY'
    end

{%- endmacro %}