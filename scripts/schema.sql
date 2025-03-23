CREATE TABLE vehicle_events (
    vehicle_id UUID NOT NULL,
    vehicle_type TEXT CHECK (vehicle_type IN ('CAR', 'TRUCK')) NOT NULL,
    lane TEXT CHECK (lane IN ('UP', 'DOWN')) NOT NULL,
    speed_kmh NUMERIC(6,2) NOT NULL,
    five_min_bin INTEGER NOT NULL,
    video_id UUID NOT NULL  -- Partition Key
);

-- Distribute the table across nodes by 'video_id'
SELECT create_distributed_table('vehicle_events', 'video_id');
