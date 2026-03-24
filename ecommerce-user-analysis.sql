/* =========================================================
   TASK 1 — SQL (PostgreSQL)
   Дані: public.users, public.orders, public.events
   ========================================================= */

------------------------------------------------------------
-- 1.1. Базова агрегація
-- Знайдіть ТОП-10 країн за кількістю зареєстрованих користувачів у 2024 році.
------------------------------------------------------------
--Обираємо країну та підраховуємо кількість унікальних користувачів
SELECT
    country,
    COUNT(DISTINCT user_id) AS registered_users
FROM public.users
-- Фільтруємо користувачів, які зареєструвалися у 2024 році
WHERE registration_date >= DATE '2024-01-01'
  AND registration_date <  DATE '2025-01-01'
  -- Виключаємо записи без зазначеної країни
  AND country IS NOT NULL
-- Групуємо користувачів за країною
GROUP BY country
-- Сортуємо країни за кількістю реєстрацій у спадаючому порядку
ORDER BY registered_users DESC
-- Обмежуємо результат ТОП-10 країнами
LIMIT 10;
------------------------------------------------------------
-- 1.2. Конверсія
-- Порахуйте конверсію з реєстрації в перше замовлення (%) по місяцях.
------------------------------------------------------------
--1) Знаходимо дату першого замовлення для кожного користувача
WITH first_orders AS (
    SELECT
        user_id,
        MIN(order_date) AS first_order_date
    FROM public.orders
    GROUP BY user_id
),
-- 2) Додаємо до користувачів інформацію про перше замовлення (якщо воно було)
users_with_orders AS (
    SELECT
        u.user_id,
        u.registration_date,
        fo.first_order_date
    FROM public.users u
    LEFT JOIN first_orders fo
        ON u.user_id = fo.user_id
)
-- 3) Рахуємо конверсію по місяцю реєстрації
SELECT
    -- Робимо короткий формат місяця: YYYY-MM
    TO_CHAR(DATE_TRUNC('month', registration_date), 'YYYY-MM') AS registration_month,
    COUNT(DISTINCT user_id) AS registered_users,
    COUNT(DISTINCT CASE WHEN first_order_date IS NOT NULL THEN user_id END) AS users_with_first_order,
    -- Конверсія у відсотках (0–100), округлення до 2 знаків
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN first_order_date IS NOT NULL THEN user_id END)
        / NULLIF(COUNT(DISTINCT user_id), 0),
        2
    ) AS conversion_to_first_order_pct
FROM users_with_orders
GROUP BY 1
ORDER BY 1;
------------------------------------------------------------
-- 1.3. Фільтрація зі складною умовою
-- Знайдіть користувачів, які зробили більше 3 транзакцій,
-- але жодна з них не була успішною (status ≠ 'completed').
------------------------------------------------------------
-- Обʼєднуємо транзакції по користувачах і фільтруємо за умовою:
-- 1) більше 3 транзакцій
-- 2) немає жодної транзакції зі статусом 'completed'
SELECT
    user_id,
    COUNT(*) AS total_transactions
FROM public.orders
GROUP BY user_id
HAVING COUNT(*) > 3
   -- Умова "жодна не була успішною" означає: кількість completed = 0
   AND COUNT(*) FILTER (WHERE status = 'completed') = 0
ORDER BY total_transactions DESC;
------------------------------------------------------------
--  1.4. Когортний аналіз (Retention)
-- Порахуйте Retention Day 1, Day 7, Day 30 для когорт по місяцю реєстрації.
-- Припущення:
-- Активність = будь-яка подія в public.events у відповідний день.
-- Exact-day retention: активність рівно на D+1 / D+7 / D+30.-- Припущення:
------------------------------------------------------------
-- 1) Формуємо когорти користувачів за місяцем реєстрації
WITH cohorts AS (
    SELECT
        u.user_id,
        u.registration_date,
        DATE_TRUNC('month', u.registration_date)::date AS cohort_month
    FROM public.users u
),
-- 2) Беремо активність користувачів по днях (без дублів)
daily_activity AS (
    SELECT DISTINCT
        e.user_id,
        e.event_date::date AS activity_date
    FROM public.events e
),
-- 3) Обчислюємо різницю в днях між активністю та реєстрацією
activity_with_diff AS (
    SELECT
        c.user_id,
        c.cohort_month,
        (da.activity_date - c.registration_date) AS day_diff
    FROM cohorts c
    JOIN daily_activity da
        ON c.user_id = da.user_id
    WHERE da.activity_date >= c.registration_date
)
-- 4) Рахуємо Retention для кожної когорти
SELECT
    TO_CHAR(c.cohort_month, 'YYYY-MM') AS cohort_month,
    COUNT(DISTINCT c.user_id) AS cohort_size,
    -- Retention Day 1
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN a.day_diff = 1 THEN a.user_id END)
        / NULLIF(COUNT(DISTINCT c.user_id), 0),
        2
    ) AS retention_d1_pct,
    -- Retention Day 7
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN a.day_diff = 7 THEN a.user_id END)
        / NULLIF(COUNT(DISTINCT c.user_id), 0),
        2
    ) AS retention_d7_pct,
    -- Retention Day 30
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN a.day_diff = 30 THEN a.user_id END)
        / NULLIF(COUNT(DISTINCT c.user_id), 0),
        2
    ) AS retention_d30_pct
FROM cohorts c
LEFT JOIN activity_with_diff a
    ON c.user_id = a.user_id
   AND c.cohort_month = a.cohort_month
GROUP BY c.cohort_month
ORDER BY c.cohort_month;
------------------------------------------------------------
-- 1.5. Віконні функції
-- Динаміка середнього чека (AOV) по тижнях з % зміни до попереднього тижня (WoW).
-- AOV = AVG(amount). Далі використовуємо LAG() для попереднього тижня.
-- За потреби можна рахувати лише успішні замовлення: WHERE status = 'completed'
------------------------------------------------------------
-- 1) Рахуємо AOV по тижнях (можна додати фільтр status = 'completed', якщо потрібно)
WITH weekly_aov AS (
    SELECT
        DATE_TRUNC('week', order_date)::date AS week_start,
        AVG(amount) AS aov
    FROM public.orders
    -- За потреби можна рахувати лише успішні замовлення:
    -- WHERE status = 'completed'
    GROUP BY 1
)
-- 2) Додаємо AOV попереднього тижня і рахуємо % зміни WoW
SELECT
    week_start,
    ROUND(aov::numeric, 2) AS aov,
    ROUND(LAG(aov) OVER (ORDER BY week_start)::numeric, 2) AS prev_week_aov,
    ROUND(
        100.0 * (aov - LAG(aov) OVER (ORDER BY week_start))
        / NULLIF(LAG(aov) OVER (ORDER BY week_start), 0),
        2
    ) AS wow_change_pct
FROM weekly_aov
ORDER BY week_start;
