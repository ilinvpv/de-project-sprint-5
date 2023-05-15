CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
     id SERIAL PRIMARY KEY,
     courier_id VARCHAR NOT NULL,
     courier_name VARCHAR(255) NOT NULL,
     settlement_year INT NOT NULL,
     settlement_month INT NOT NULL,
     orders_count INT NOT NULL,
     orders_total_sum NUMERIC(10, 2) NOT NULL,
     rate_avg DECIMAL(4, 2) NOT NULL,
     order_processing_fee DECIMAL(10, 2) NOT NULL,
     courier_order_sum DECIMAL(10, 2) NOT NULL,
     courier_tips_sum DECIMAL(10, 2) NOT NULL,
     courier_reward_sum DECIMAL(10, 2) NOT NULL,
     CONSTRAINT fk_courier FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers (courier_id)
);

CREATE INDEX IF NOT EXISTS idx_settlement_year_month ON cdm.dm_courier_ledger (settlement_year, settlement_month);
CREATE INDEX IF NOT EXISTS idx_courier_id ON cdm.dm_courier_ledger (courier_id);
