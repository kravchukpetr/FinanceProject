select amount_usd, company_amount_usd, *
from payments.transactions
where id IN (53462858);


select *
from payments.payments
where id IN (53462858);
AND account_id NOT IN (
(SELECT DISTINCT p.account_id FROM payments.payments p WHERE p.account_id IN (23995873,429368,25171688) and p.code = 'AG')


SELECT * FROM facts.operations.payments
WHERE transaction_id = 53462858

USE [KPIv2]


