-- ============================================================
-- TẦNG GOLD (postgres-warehouse) — bảng KPI sẵn sàng cho dashboard
-- Chạy sau create_silver.sql
-- Quy ước: doanh thu chỉ tính đơn 'completed'.
--          cancel_rate (%) = cancelled / (completed + cancelled), không tính đơn đang pending.
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.daily_sales (
    sales_date DATE PRIMARY KEY,            -- ngày theo giờ Việt Nam (UTC+7)
    total_orders INT NOT NULL,
    completed_orders INT NOT NULL,
    cancelled_orders INT NOT NULL,
    pending_orders INT NOT NULL,
    revenue NUMERIC(14, 2) NOT NULL,
    avg_order_value NUMERIC(12, 2),
    cancel_rate NUMERIC(5, 2),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.product_sales (
    product_name TEXT PRIMARY KEY,
    orders_completed INT NOT NULL,
    units_sold INT NOT NULL,
    revenue NUMERIC(14, 2) NOT NULL,
    revenue_rank INT NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Theo phút (không theo giờ) vì dữ liệu giả lập chỉ trải trên vài chục phút,
-- gom theo giờ thì biểu đồ chỉ có 1-2 điểm. Giờ Việt Nam (UTC+7).
CREATE TABLE IF NOT EXISTS gold.minute_sales (
    sales_minute TIMESTAMP PRIMARY KEY,
    total_orders INT NOT NULL,
    completed_orders INT NOT NULL,
    cancelled_orders INT NOT NULL,
    revenue NUMERIC(14, 2) NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
