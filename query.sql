WITH state_category_max_amount AS (
    SELECT
        us_state,
        cat_id,
        max(amount) as max_amount
    FROM transactions
    GROUP BY us_state, cat_id
),
ranked_categories AS (
    SELECT
        us_state,
        cat_id,
        max_amount,
        row_number() OVER (PARTITION BY us_state ORDER BY max_amount DESC) as rn
    FROM state_category_max_amount
)

SELECT
    us_state AS state,
    cat_id AS category,
    max_amount AS highest_transaction
FROM ranked_categories
WHERE rn = 1
ORDER BY us_state;