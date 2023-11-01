CREATE TABLE table_gc7 (
    id SERIAL PRIMARY KEY,
    Rk INT,
    Pk INT,
    Tm VARCHAR(3),
    Player VARCHAR(255),
    College VARCHAR(255),
    Yrs VARCHAR(255),
    G VARCHAR(255),
    TOTMP VARCHAR(255),
    TOTPTS VARCHAR(255),
    TOTTRB VARCHAR(255),
    TOTAST VARCHAR(255),
    "FG%" VARCHAR(255),
    "3P%" VARCHAR(255),
    "FT%" VARCHAR(255),
    WS VARCHAR(255),
    "WS/48" VARCHAR(255),
    BPM VARCHAR(255),
    VORP VARCHAR(255),
    DraftYr INT,
    MPG VARCHAR(255),
    PPG VARCHAR(255),
    RPG VARCHAR(255),
    APG VARCHAR(255),
    playerurl VARCHAR(255),
    DraftYear INT
);


COPY table_gc7 FROM 'C:\\Users\\farha\\Hacktiv8\\Phase 2\\GradedChallange\\p2-ftds023-rmt-g7-FarhanSatrio\\P2G7_farhan_satrio_data_raw.csv' DELIMITER ',' CSV HEADER;


SELECT * FROM table_gc7