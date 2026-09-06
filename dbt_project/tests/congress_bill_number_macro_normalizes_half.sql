with normalized as (
    select {{ normalize_congress_bill_number_text("'123½'") }} as bill_number
)
select bill_number
from normalized
where bill_number <> '123.5'
