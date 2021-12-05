CREATE SCHEMA core;

-- HUB people

CREATE TABLE IF NOT EXISTS core.hub_people
(
 person_hub_id VARCHAR(50) NOT NULL PRIMARY KEY,
 load_dttm TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 source_entity VARCHAR(50) NOT NULL,
 source_person_id VARCHAR(1000) NOT NULL,
 source_region_id VARCHAR(1000) NOT NULL
);

INSERT
    INTO
    core.hub_people (
        person_hub_id
        , source_entity
        , source_person_id
        , source_region_id
    )
SELECT
    DISTINCT 
    MD5(UPPER(TRIM(person)) || ';' || UPPER(TRIM(region)) || ';' || 'PUBLIC.PEOPLE') AS person_hub_id
    , 'PUBLIC.PEOPLE' AS source_entity
    , UPPER(TRIM(person)) AS source_person_id
    , UPPER(TRIM(region)) AS source_region_id
FROM
    public.people;

COMMIT;

--SELECT
--    *
--FROM
--    core.hub_people;


-- HUB orders

CREATE TABLE IF NOT EXISTS core.hub_orders
(
    order_hub_id varchar(50) NOT NULL PRIMARY KEY
    , load_dttm timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
    , source_entity varchar(50) NOT NULL
    , source_order_id varchar(50) NOT NULL
    , source_row_id BIGINT NOT NULL
);

INSERT
    INTO
    core.hub_orders (
        order_hub_id
        , source_entity
        , source_order_id
        , source_row_id
    )
SELECT
    DISTINCT 
    MD5(UPPER(TRIM(order_id)) || ';' || row_id::TEXT || ';' || 'PUBLIC.ORDERS') AS order_hub_id
    , 'PUBLIC.ORDERS' AS source_entity
    , UPPER(TRIM(order_id)) AS source_id
    , row_id
FROM
    public.orders;

COMMIT;

--SELECT
--    *
--FROM
--    core.hub_orders;

CREATE TABLE IF NOT EXISTS core.sat_orders
(
 order_hub_id     varchar(50) NOT NULL,
 valid_end_dttm   timestamp NOT NULL,
 load_dttm        timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
 source_entity    varchar(50) NOT NULL,
 valid_start_dttm timestamp NOT NULL,
 order_date       date NOT NULL,
 ship_date        date NOT NULL,
 sale_amount      numeric NOT NULL,
 quantity         int NOT NULL,
 discount         numeric NOT NULL,
 profit           numeric NOT NULL,
 CONSTRAINT pk_sat_orders PRIMARY KEY ( order_hub_id, valid_end_dttm ),
 CONSTRAINT fk_sat_orders FOREIGN KEY ( order_hub_id ) REFERENCES core.hub_orders ( order_hub_id )
);

CREATE INDEX idx_sat_orders ON core.sat_orders
(
 order_hub_id
);

INSERT
    INTO
    core.sat_orders (
        order_hub_id
        , valid_end_dttm
        , source_entity
        , valid_start_dttm
        , order_date
        , ship_date
        , sale_amount
        , quantity
        , discount
        , profit
    )
SELECT DISTINCT 
    MD5(UPPER(TRIM(order_id)) || ';' || row_id::TEXT || ';' || 'PUBLIC.ORDERS') AS order_hub_id
    , '9999-12-31'::TIMESTAMP AS valid_end_dttm
    , 'PUBLIC.ORDERS' AS source_entity
    , order_date AS valid_start_dttm
    , order_date
    , ship_date
    , sales
    , quantity
    , discount
    , profit
FROM
    public.orders;
   
   
-- lnk orders to people
   
DROP TABLE core.lnk_people_to_orders;
   
CREATE TABLE core.lnk_people_to_orders
(
 people_to_orders_lnk_id varchar(50) NOT NULL PRIMARY KEY,
 load_dttm               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
 source_entity           varchar(50) NOT NULL,
 person_hub_id           varchar(50) NOT NULL,
 order_hub_id            varchar(50) NOT NULL,
 CONSTRAINT fk_lnk_people_to_orders_ppl FOREIGN KEY ( person_hub_id ) REFERENCES core.hub_people ( person_hub_id ),
 CONSTRAINT fk_lnk_people_to_orders_ord FOREIGN KEY ( order_hub_id ) REFERENCES core.hub_orders ( order_hub_id )
);

CREATE INDEX idx_lnk_people_to_orders_ppl ON core.lnk_people_to_orders
(
 person_hub_id
);

CREATE INDEX idx_lnk_people_to_orders_ord ON core.lnk_people_to_orders
(
 order_hub_id
);

INSERT
	INTO
	core.lnk_people_to_orders (
		people_to_orders_lnk_id
		, source_entity
		, person_hub_id
		, order_hub_id
	)
SELECT DISTINCT 
	MD5(UPPER(TRIM(p.person)) || ';' || UPPER(TRIM(p.region)) || ';' || 'PUBLIC.PEOPLE' || ';' 
	    || UPPER(TRIM(o.order_id)) || ';' || o.row_id::TEXT || ';' || 'PUBLIC.ORDERS' ) AS people_to_orders_lnk_id
	, 'PUBLIC.ORDERS' AS source_entity
	, MD5(UPPER(TRIM(p.person)) || ';' || UPPER(TRIM(p.region)) || ';' || 'PUBLIC.PEOPLE') AS person_hub_id
	, MD5(UPPER(TRIM(o.order_id)) || ';' || o.row_id::TEXT || ';' || 'PUBLIC.ORDERS') AS order_hub_id
FROM
	public.orders o
JOIN public.people p ON
	o.region = p.region;


-- check
SELECT
	SUM(so.sale_amount) AS total_sales
	, SUM(so.profit) AS total_profit
	, SUM(so.discount) AS total_discount
FROM
	core.lnk_people_to_orders lpo
JOIN core.hub_people p ON
	lpo.person_hub_id = p.person_hub_id
JOIN core.hub_orders o ON
	lpo.order_hub_id = o.order_hub_id
JOIN core.sat_orders so ON
	o.order_hub_id = so.order_hub_id
UNION ALL
SELECT
	SUM(o2.sales) AS total_sales
	, SUM(o2.profit) AS total_profit
	, SUM(o2.discount) AS total_discount
FROM
	public.orders o2;


-- HUB ship modes

CREATE TABLE IF NOT EXISTS core.hub_ship_modes
(
 ship_mode_hub_id VARCHAR(50) NOT NULL PRIMARY KEY,
 load_dttm TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 source_entity VARCHAR(50) NOT NULL,
 source_id VARCHAR(1000) NOT NULL
);

INSERT
    INTO
    core.hub_ship_modes (
        ship_mode_hub_id
        , source_entity
        , source_id
    )
SELECT
    DISTINCT 
    MD5(UPPER(TRIM(ship_mode)) || ';' || 'PUBLIC.ORDERS') AS ship_mode_hub_id
    , 'PUBLIC.ORDERS' AS source_entity
    , UPPER(TRIM(ship_mode)) AS source_id
FROM
    public.orders;

COMMIT;

--SELECT
--    *
--FROM
--    core.hub_ship_modes;


-- LNK ship modes to orders


CREATE TABLE IF NOT EXISTS core.lnk_ship_modes_to_orders
(
 ship_modes_to_orders_lnk_id varchar(50) NOT NULL PRIMARY KEY,
 load_dttm                   timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
 source_entity           	 varchar(50) NOT NULL, 
 ship_mode_hub_id            varchar(50) NOT NULL,
 order_hub_id                varchar(50) NOT NULL,
 CONSTRAINT fk_lnk_ship_modes_to_orders_sm FOREIGN KEY ( ship_mode_hub_id ) REFERENCES core.hub_ship_modes ( ship_mode_hub_id ),
 CONSTRAINT fk_lnk_ship_modes_to_orders_ord FOREIGN KEY ( order_hub_id ) REFERENCES core.hub_orders ( order_hub_id )
);

CREATE INDEX idx_lnk_ship_modes_to_orders_sm ON core.lnk_ship_modes_to_orders
(
 ship_mode_hub_id
);

CREATE INDEX idx_lnk_ship_modes_to_orders_ord ON core.lnk_ship_modes_to_orders
(
 order_hub_id
);

INSERT
	INTO
	core.lnk_ship_modes_to_orders (
		ship_modes_to_orders_lnk_id
		, source_entity
		, ship_mode_hub_id
		, order_hub_id
	)
SELECT DISTINCT 
	MD5(UPPER(TRIM(o.ship_mode)) || ';' || 'PUBLIC.ORDERS' || ';' 
	    || UPPER(TRIM(o.order_id)) || ';' || o.row_id::TEXT || ';' || 'PUBLIC.ORDERS' ) AS people_to_orders_lnk_id
	, 'PUBLIC.ORDERS' AS source_entity
	, MD5(UPPER(TRIM(ship_mode)) || ';' || 'PUBLIC.ORDERS') AS ship_mode_hub_id
	, MD5(UPPER(TRIM(o.order_id)) || ';' || o.row_id::TEXT || ';' || 'PUBLIC.ORDERS') AS order_hub_id
FROM
	public.orders o;


-- hub customers
-- sat customers

-- lnk customers to orders

-- hub consumer segments

-- lnk segments to customers

-- hub addresses
-- sat addresses

-- lnk orders to addresses

-- hub products
-- sat product ids

-- lnk products to orders

-- hub categories

-- hub subcategories

-- lnk subcategories to products

-- lnk categories to subcategories

select * from public.orders o LIMIT 10;