--
-- Zabbix auto-partitioning DDL code for PostgreSQL.
-- 2015 (c) Pablo Ruiz -- <pablo.ruiz _at_ gmail.com>
--

BEGIN;

CREATE SCHEMA partitions
--  AUTHORIZATION zabbix
;

CREATE OR REPLACE FUNCTION create_partition(
	parent name,
	schema text,
	partition text,
	min integer,
	max integer 
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
	lock bigint := hashtext(format('%I.%I', schema, partition));
BEGIN
	
	PERFORM pg_advisory_xact_lock(lock);

	IF NOT EXISTS (
		SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace 
		WHERE n.nspname = schema AND c.relname=partition) 
	THEN
		RAISE NOTICE 'A new partition is set to be created %', partition;

		EXECUTE format (
			'CREATE TABLE IF NOT EXISTS %1$I.%2$I ('
				'CHECK ((clock >= %4$s AND clock < %5$s))'
			') INHERITS (%3$I);',
			schema, partition, parent, min, max
		);

		EXECUTE format ('CREATE INDEX %1$s_1 ON %2$I.%1$I (itemid, clock);', partition, schema);
		--EXECUTE 'GRANT ALL ON TABLE ' || partition || ' TO postgres;';
 
	END IF;

END;
$$;

CREATE OR REPLACE FUNCTION partitioned_insert_handler()
	RETURNS trigger
	LANGUAGE plpgsql VOLATILE
	COST 100
AS $$
DECLARE
	selector text := TG_ARGV[0];
	_interval INTERVAL := '1 ' || TG_ARGV[0];
	schema text := 'partitions';
	timeformat text;
	partition text;
	partition_number text;
	min integer;
	max integer;
BEGIN
	IF selector = 'day' THEN
		timeformat := 'YYYYMMDD';
	ELSIF selector = 'week' THEN
		timeformat := 'YYYY"w"WW';
	ELSIF selector = 'month' THEN
		timeformat := 'YYYYMM';
	ELSE
		RAISE EXCEPTION 'Invalid selector: %', selector;
	END IF;

	partition_number := TO_CHAR(TO_TIMESTAMP(NEW.clock), timeformat);
	partition := TG_TABLE_NAME || '_' || partition_number;

	EXECUTE format('INSERT INTO %I.%I VALUES ($1.*)', schema, partition) USING NEW;
	RETURN NULL;

	EXCEPTION
		WHEN undefined_table THEN
			min := EXTRACT(epoch FROM date_trunc(selector, TO_TIMESTAMP(NEW.clock)));
			max := EXTRACT(epoch FROM date_trunc(selector, TO_TIMESTAMP(NEW.clock) + _interval ));
			PERFORM create_partition(TG_TABLE_NAME, schema, partition, min, max);
			EXECUTE format('INSERT INTO %I.%I VALUES ($1.*)', schema, partition) USING NEW;
			RETURN NULL;

END;
$$;

CREATE OR REPLACE FUNCTION delete_partitions(intervaltodelete INTERVAL, tabletype text)
	RETURNS text
	LANGUAGE plpgsql VOLATILE
	COST 100
AS $$
DECLARE
	result RECORD ;
	schema text := 'partitions';
	table_timestamp TIMESTAMP;
	delete_before_date DATE;
	regex text;
	sregex text := '_([0-9]*)$';
	dformat text;
BEGIN
	IF tabletype = 'day' THEN
		regex := '%_[0-9]{8}';
		dformat := 'YYYYMMDD';
	ELSIF tabletype = 'week' THEN
		regex := '%_[0-9]{4}w[0-9]{2}';
		sregex := '_([0-9w]*)$';
		dformat := 'YYYY"w"WW';
	ELSIF tabletype = 'month' THEN
		regex := '%_[0-9]{6}';
		dformat := 'YYYYMM';
	ELSE
		RAISE EXCEPTION 'Please specify "month", "week" or "day" instead of %', tabletype;
	END IF;

	FOR result IN (
		SELECT * FROM pg_tables WHERE schemaname = 'partitions' AND tablename SIMILAR TO regex
	) LOOP
 
		table_timestamp := TO_TIMESTAMP(substring(result.tablename FROM sregex), dformat);
		delete_before_date := date_trunc('day', NOW() - intervalToDelete);
 
		IF table_timestamp <= delete_before_date THEN
			RAISE NOTICE 'Deleting table %', quote_ident(result.tablename);
			EXECUTE format ('DROP TABLE %I.%I;', schema, result.tablename);
		END IF;
	END LOOP;

	RETURN 'OK';
END;
$$;

--CREATE TABLE test (
--        itemid                   bigint                                    NOT NULL,
--        clock                    integer         DEFAULT '0'               NOT NULL,
--        value                    numeric(16,4)   DEFAULT '0.0000'          NOT NULL,
--        ns                       integer         DEFAULT '0'               NOT NULL
--);
--CREATE INDEX test_1 ON test (itemid,clock);
--CREATE TRIGGER partitioning_trigger BEFORE INSERT ON test FOR EACH ROW EXECUTE PROCEDURE partitioned_insert_handler('week');
--INSERT INTO test (itemid, clock) VALUES (1,extract(epoch from now()));
--INSERT INTO test (itemid, clock) VALUES (2,extract(epoch from (now() - '1 week'::interval)));
--SELECT delete_partitions('1 weeks', 'week');

CREATE TRIGGER partitioning_trigger BEFORE INSERT ON history           FOR EACH ROW EXECUTE PROCEDURE partitioned_insert_handler('week');
CREATE TRIGGER partitioning_trigger BEFORE INSERT ON history_sync      FOR EACH ROW EXECUTE PROCEDURE partitioned_insert_handler('week');
CREATE TRIGGER partitioning_trigger BEFORE INSERT ON history_uint      FOR EACH ROW EXECUTE PROCEDURE partitioned_insert_handler('week');
CREATE TRIGGER partitioning_trigger BEFORE INSERT ON history_str       FOR EACH ROW EXECUTE PROCEDURE partitioned_insert_handler('week');
CREATE TRIGGER partitioning_trigger BEFORE INSERT ON history_str_sync  FOR EACH ROW EXECUTE PROCEDURE partitioned_insert_handler('week');
CREATE TRIGGER partitioning_trigger BEFORE INSERT ON history_text      FOR EACH ROW EXECUTE PROCEDURE partitioned_insert_handler('week');
CREATE TRIGGER partitioning_trigger BEFORE INSERT ON history_log       FOR EACH ROW EXECUTE PROCEDURE partitioned_insert_handler('week');
CREATE TRIGGER partitioning_trigger BEFORE INSERT ON trends            FOR EACH ROW EXECUTE PROCEDURE partitioned_insert_handler('month');
CREATE TRIGGER partitioning_trigger BEFORE INSERT ON trends_uint       FOR EACH ROW EXECUTE PROCEDURE partitioned_insert_handler('month');

COMMIT;
