select
  ...

from raw.jaffle_shop.orders

left join raw.jaffle_shop.customers using (customer_id)