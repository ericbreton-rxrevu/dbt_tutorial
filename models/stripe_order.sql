with orders as (

select * from {{ref('stg_orders')}}
)
select 
orderid order_id,
customer_id,
(sum(amount)/100) amount
from  {{ source('stripe', 'payment') }}
left join orders on orders.order_id=raw.STRIPE.PAYMENT.orderid
where raw.STRIPE.PAYMENT.status='success'
group by 
  orderid,
  customer_id


