/**************************************************************************
* Derive entire hierarchies from NCI Metathesaurus MRHIER Table
* Authors: Meera Patel
* Date: 2021-10-27
* https://lucid.app/lucidchart/f68b0a0a-104c-4550-bc38-455efda9ee25/edit?viewport_loc=-776%2C-93%2C2719%2C1620%2C0_0&invitationId=inv_e6984473-d151-4be5-8302-86fa64f4865c
* 
* Note: SNOMEDCT_US is excluded from processing due to its immense size while 
* also being duplicative of the UMLS Metathesaurus MRHIER processing.
*
* | MRHIER | --> | NCI_MRHIER |
* ptr_id is added to the source table. ptr_id is the source NCI_MRHIER's row number.
* It is added as an identifier for each unique AUI-RELA-PTR (ptr: Path To Root).
* Note that unlike the identifiers provided
* by the NCI, this one cannot be used across different Metathesaurus
* versions.
*
* | MRHIER | --> | NCI_MRHIER_STR | + | NCI_MRHIER_STR_EXCL |
* NCI_MRHIER is then processed to replace the decimal-separated `ptr` string into
* individual atoms (`aui`) and mapped to the atom's `str` value. Any missing
* `ptr` values in `NCI_MRHIER_STR` are accounted for in the `NCI_MRHIER_STR_EXCL` table.
*
* To Do:
* [ ] Add the `sab` field back to final NCI_MRHIER_STR table by doing a join 
*     to the MRCONSO table at this stage
* [ ] Log entries from `ext_` to `pivot_` do not have `sab` value
* [ ] Change sort order of final tables in NCI Class
* [ ] Add indexes to final NCI Class tables
**************************************************************************/


/**************************************************************************
LOG TABLES
Process log table logs the processing.
Setup log table logs the final `NCI_MRHIER`, `NCI_MRHIER_STR`, and `NCI_MRHIER_STR_EXCL` tables.
Both are setup if it does not already exist.
**************************************************************************/


CREATE TABLE IF NOT EXISTS public.process_nci_mrhier_log (
    process_start_datetime timestamp without time zone,
    process_stop_datetime timestamp without time zone,
    mth_version character varying(255),
    mth_release_dt character varying(255),
    sab character varying(255),
    target_schema character varying(255),
    source_table character varying(255),
    target_table character varying(255),
    source_row_ct numeric,
    target_row_ct numeric
);


CREATE TABLE IF NOT EXISTS public.setup_nci_class_log (
    snc_datetime timestamp without time zone,
    nci_mth_version character varying(255),
    nci_mth_release_dt character varying(255),
    target_schema character varying(255),
    nci_mrhier bigint,
    nci_mrhier_str bigint,
    nci_mrhier_str_excl bigint, 
    nci_mrhier_code bigint
);

/**************************************************************************
Logging Functions
**************************************************************************/

create or replace function get_log_timestamp()
returns timestamp without time zone
language plpgsql
as
$$
declare
  log_timestamp timestamp without time zone;
begin
  SELECT date_trunc('second', timeofday()::timestamp)
  INTO log_timestamp;

  RETURN log_timestamp;
END;
$$;

create or replace function get_nci_mth_version()
returns varchar
language plpgsql
as
$$
declare
	nci_mth_version varchar;
begin
	WITH ncim AS (
	  SELECT * 
	  FROM public.setup_nci_log 
	  WHERE nci_type = 'Metathesaurus'
	)
	
	SELECT nci_version
	INTO nci_mth_version
	FROM ncim
	WHERE sn_datetime IN (SELECT MAX(sn_datetime) FROM ncim);

  	RETURN nci_mth_version;
END;
$$;

create or replace function get_nci_mth_dt()
returns varchar
language plpgsql
as
$$
declare
	nci_mth_dt varchar;
begin
	SELECT sm_release_date
	INTO nci_mth_dt
	FROM public.setup_mth_log
	WHERE sm_datetime IN (SELECT MAX(sm_datetime) FROM public.setup_mth_log);

  	RETURN nci_mth_dt;
END;
$$;


create or replace function get_row_count(_tbl varchar)
returns bigint
language plpgsql
AS
$$
DECLARE
  row_count bigint;
BEGIN
  EXECUTE
    format('
	  SELECT COUNT(*)
	  FROM %s;
	 ',
	 _tbl)
  INTO row_count;

  RETURN row_count;
END;
$$;


create or replace function check_if_nci_requires_processing(nci_mth_version varchar, source_table varchar, target_table varchar)
returns boolean
language plpgsql
as
$$
declare
    row_count integer;
	requires_processing boolean;
begin
	EXECUTE
	  format(
	    '
		SELECT COUNT(*)
		FROM public.process_nci_mrhier_log l
		WHERE
		  l.mth_version = ''%s'' AND
		  l.source_table = ''%s'' AND
		  l.target_table = ''%s'' AND
		  l.process_stop_datetime IS NOT NULL
		  ;
	    ',
	    nci_mth_version,
	    source_table,
	    target_table
	  )
	  INTO row_count;


	IF row_count = 0
	  THEN requires_processing := TRUE;
	  ELSE requires_processing := FALSE;
	END IF;

  	RETURN requires_processing;
END;
$$;


create or replace function notify_iteration(iteration int, total_iterations int, objectname varchar)
returns void
language plpgsql
as
$$
declare
  notice_timestamp timestamp;
begin
  SELECT get_log_timestamp()
  INTO notice_timestamp
  ;

  RAISE NOTICE '[%] %/% %', notice_timestamp, iteration, total_iterations, objectname;
END;
$$;

create or replace function notify_start(report varchar)
returns void
language plpgsql
as
$$
declare
  notice_timestamp timestamp;
begin
  SELECT get_log_timestamp()
  INTO notice_timestamp
  ;

  RAISE NOTICE '[%] Started %', notice_timestamp, report;
END;
$$;

create or replace function notify_completion(report varchar)
returns void
language plpgsql
as
$$
declare
  notice_timestamp timestamp;
begin
  SELECT get_log_timestamp()
  INTO notice_timestamp
  ;

  RAISE NOTICE '[%] Completed %', notice_timestamp, report;
END;
$$;


create or replace function notify_timediff(report varchar, start_timestamp timestamp, stop_timestamp timestamp)
returns void
language plpgsql
as
$$
begin
	RAISE NOTICE '% required %s to complete.', report, stop_timestamp - start_timestamp;
end;
$$
;

create or replace function sab_to_tablename(sab varchar)
returns varchar
language plpgsql
as
$$
declare
  tablename varchar;
begin
	SELECT REGEXP_REPLACE(sab, '[[:punct:]]', '_', 'g') INTO tablename;

	RETURN tablename;
end;
$$
;




/**************************************************************************
/ I. Transfer MRHIER to `nci_mrhier` Schema 
/ -------------------------------------------------------------------------
/ If the current NCI Metathesaurus version is not logged for
/ the transfer of the MRHIER table, the `nci_mrhier` schema is dropped.
/ The unique AUI-RELA-PTR from the NCI_MRHIER table in the `nci` schema 
/ is then copied to the along with the AUI's CODE, SAB and STR in the MRCONSO 
/ table. A `ptr_id` to serve as a unique identifier each row number, which 
/ represents a unique classification for the given AUI.
**************************************************************************/
DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	source_rows bigint;
	target_rows bigint;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;

	SELECT check_if_nci_requires_processing(mth_version, 'MRHIER', 'NCI_MRHIER')
	INTO requires_processing;

  	IF requires_processing THEN

  		DROP SCHEMA IF EXISTS nci_mrhier CASCADE;
		CREATE SCHEMA nci_mrhier;

  		SELECT get_log_timestamp()
  		INTO start_timestamp
  		;

  		PERFORM notify_start('processing MRHIER');


		DROP TABLE IF EXISTS nci_mrhier.tmp_mrhier;
		CREATE TABLE nci_mrhier.tmp_mrhier AS (
			SELECT DISTINCT
			  m.AUI,
			  c.CODE,
			  c.SAB,
			  c.STR,
			  m.RELA,
			  m.PTR
			 FROM nci.mrhier m
			 INNER JOIN nci.mrconso c
			 ON c.aui = m.aui
		);


		DROP TABLE IF EXISTS nci_mrhier.nci_mrhier;
		CREATE TABLE nci_mrhier.nci_mrhier AS (
		   SELECT ROW_NUMBER() OVER() AS ptr_id, m.*
		   FROM nci_mrhier.tmp_mrhier m
		)
		;

		ALTER TABLE nci_mrhier.nci_mrhier
		ADD CONSTRAINT xpk_nci_mrhier
		PRIMARY KEY (ptr_id);

		CREATE INDEX x_nci_mrhier_sab ON nci_mrhier.nci_mrhier(sab);
		CREATE INDEX x_nci_mrhier_aui ON nci_mrhier.nci_mrhier(aui);
		CREATE INDEX x_nci_mrhier_code ON nci_mrhier.nci_mrhier(code);

		DROP TABLE nci_mrhier.tmp_mrhier;

		PERFORM notify_completion('processing MRHIER');

		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_version()
		INTO mth_version
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		SELECT get_row_count('nci.mrhier')
		INTO source_rows
		;

		SELECT get_row_count('nci_mrhier.nci_mrhier')
		INTO target_rows
		;

		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''MRHIER'',
			  ''NCI_MRHIER'',
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);
			  
		PERFORM notify_timediff('processing MRHIER', start_timestamp, stop_timestamp);

	END IF;
end;
$$
;

/**************************************************************************
/ II. CREATE LOOKUP TABLES `LOOKUP_ENG` and `LOOKUP_PARSE`
/ -------------------------------------------------------------------------
/ `LOOKUP_ENG` is a single field of all SAB that have a LAT value of 'ENG' 
/ in the MRCONSO table. 
**************************************************************************/
DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	source_rows bigint;
	target_rows bigint;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;

	SELECT check_if_nci_requires_processing(mth_version, 'MRCONSO', 'LOOKUP_ENG')
	INTO requires_processing;

  	IF requires_processing THEN

  		SELECT get_log_timestamp()
  		INTO start_timestamp
  		;

  		PERFORM notify_start('processing LOOKUP_ENG');

		DROP TABLE IF EXISTS nci_mrhier.lookup_eng;
		CREATE TABLE nci_mrhier.lookup_eng (
		    sab character varying(40)
		);

		INSERT INTO nci_mrhier.lookup_eng
		SELECT DISTINCT sab
		FROM nci.mrconso
		WHERE lat = 'ENG' AND sab <> 'SNOMEDCT_US'
		ORDER BY sab;

		PERFORM notify_completion('processing LOOKUP_ENG');

		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_version()
		INTO mth_version
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		SELECT get_row_count('nci.mrconso')
		INTO source_rows
		;

		SELECT get_row_count('nci_mrhier.lookup_eng')
		INTO target_rows
		;

		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''MRCONSO'',
			  ''LOOKUP_ENG'',
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);
			  
		COMMIT;
			  
		
		PERFORM notify_timediff('processing LOOKUP_ENG', start_timestamp, stop_timestamp);



	END IF;
end;
$$
;


/**************************************************************************
/ II. CREATE LOOKUP TABLES `LOOKUP_ENG` and `LOOKUP_PARSE`
/ -------------------------------------------------------------------------
/ `LOOKUP_PARSE` maps each SAB to its corresponding tablename because 
/ some SABs contain punctuation that is forbidden in Postgres tablenames.  
**************************************************************************/

DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	source_rows bigint;
	target_rows bigint;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;

	SELECT check_if_nci_requires_processing(mth_version, 'NCI_MRHIER', 'LOOKUP_PARSE')
	INTO requires_processing;

  	IF requires_processing THEN
  		SELECT get_log_timestamp()
  		INTO start_timestamp
  		;

  		PERFORM notify_start('processing LOOKUP_PARSE');


		DROP TABLE IF EXISTS nci_mrhier.lookup_parse;
		CREATE TABLE nci_mrhier.lookup_parse (
		    hierarchy_sab character varying(40),
		    hierarchy_table text,
		    count bigint
		);

		WITH df as (
		      SELECT
			    h.sab AS hierarchy_sab,
			    sab_to_tablename(h.sab) AS hierarchy_table,
			    COUNT(*)
			  FROM nci_mrhier.nci_mrhier h
			  INNER JOIN nci_mrhier.lookup_eng eng
			  ON eng.sab = h.sab
			  GROUP BY h.sab
			  HAVING COUNT(*) > 1
			  ORDER BY COUNT(*)
		)

		INSERT INTO nci_mrhier.lookup_parse
		SELECT *
		FROM df
		ORDER BY count -- ordered so that when writing tables later on, can see that the script is working fine over multiple small tables at first
		;
		

		PERFORM notify_completion('processing LOOKUP_PARSE');
		
		COMMIT;

		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_version()
		INTO mth_version
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		SELECT get_row_count('nci_mrhier.nci_mrhier')
		INTO source_rows
		;

		SELECT get_row_count('nci_mrhier.lookup_parse')
		INTO target_rows
		;

		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''NCI_MRHIER'',
			  ''LOOKUP_PARSE'',
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);
			  
		COMMIT;
			  
		
		PERFORM notify_timediff('processing LOOKUP_PARSE', start_timestamp, stop_timestamp);





	END IF;
end;
$$
;


/**************************************************************************
/ III. PARSE PTR BY SAB ('parse')
/ -------------------------------------------------------------------------
/ For each unique SAB in the NCI_MRHIER table,
/ the decimal-separated PTR string is parsed along with its
/ ordinality as PTR_LEVEL. The parsed individual PTR_AUI
/ is joined to MRCONSO to add the PTR_CODE and PTR. Each SAB is written to 
/ its own table referenced in the `lookup_parse` table. 
**************************************************************************/


DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	source_rows bigint;
	target_rows bigint;
    f record;
    target_table varchar(255);
    source_sab varchar(255);
	iteration int;
    total_iterations int;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;

	SELECT COUNT(*) INTO total_iterations FROM nci_mrhier.lookup_parse;
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM nci_mrhier.lookup_parse l
    LOOP
		iteration := f.iteration;
		target_table := f.hierarchy_table;
		source_sab := f.hierarchy_sab;

		SELECT check_if_nci_requires_processing(mth_version, 'NCI_MRHIER', target_table)
		INTO requires_processing;

  		IF requires_processing THEN

   			PERFORM notify_start(CONCAT('processing', ' ', source_sab, ' into table ', target_table));
  			SELECT get_log_timestamp()
  			INTO start_timestamp
  			;

  			EXECUTE
			format(
				'
				SELECT COUNT(*)
				FROM nci_mrhier.nci_mrhier
				WHERE sab = ''%s'';
				',
					source_sab
			)
			INTO source_rows;

  	  		PERFORM notify_iteration(iteration, total_iterations, source_sab || ' (' || source_rows || ' source rows)');

			EXECUTE
			format(
			  '
			  DROP TABLE IF EXISTS nci_mrhier.%s;
			  CREATE TABLE  nci_mrhier.%s (
			    ptr_id INTEGER NOT NULL,
			    ptr text NOT NULL,
			  	aui varchar(12),
			  	code varchar(100),
			  	str text,
			  	rela varchar(100),
			  	ptr_level INTEGER NOT NULL,
			  	ptr_aui varchar(12) NOT NULL,
			  	ptr_code varchar(100),
			  	ptr_str text
			  );

			  WITH relatives0 AS (
				SELECT DISTINCT m.ptr_id, s1.aui, s1.code, s1.str, m.rela, m.ptr
				FROM nci_mrhier.nci_mrhier m
				INNER JOIN nci.mrconso s1
				ON s1.aui = m.aui
				WHERE m.sab = ''%s''
			  ),
			  relatives1 AS (
			  	SELECT ptr_id, ptr, aui, code, str, rela, unnest(string_to_array(ptr, ''.'')) AS ptr_aui
			  	FROM relatives0 r0
			  	ORDER BY ptr_id
			  ),
			  relatives2 AS (
			  	SELECT r1.*, ROW_NUMBER() OVER (PARTITION BY ptr_id) AS ptr_level
			  	FROM relatives1 r1
			  ),
			  relatives3 AS (
			  	SELECT r2.*, m.code AS ptr_code, m.str AS ptr_str
			  	FROM relatives2 r2
			  	LEFT JOIN nci.mrconso m
			  	ON m.aui = r2.ptr_aui
			  )

			  INSERT INTO nci_mrhier.%s
			  SELECT DISTINCT
			    ptr_id,
			    ptr,
			  	aui,
			  	code,
			  	str,
			  	rela,
			  	ptr_level,
			  	ptr_aui,
			  	ptr_code,
			  	ptr_str
			  FROM relatives3
			  ORDER BY ptr_id, ptr_level
			  ;

			  CREATE UNIQUE INDEX idx_%s_ptr
			  ON nci_mrhier.%s (ptr_id, ptr_level);
			  CLUSTER nci_mrhier.%s USING idx_%s_ptr;

			  CREATE INDEX x_%s_aui ON nci_mrhier.%s(aui);
			  CREATE INDEX x_%s_code ON nci_mrhier.%s(code);
			  CREATE INDEX x_%s_ptr_aui ON nci_mrhier.%s(ptr_aui);
			  CREATE INDEX x_%s_ptr_code ON nci_mrhier.%s(ptr_code);
			  CREATE INDEX x_%s_ptr_level ON nci_mrhier.%s(ptr_level);
			  ',
			  	target_table,
			  	target_table,
			  	source_sab,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table
			  	);
			  	
		COMMIT;
			  


  		PERFORM notify_completion(CONCAT('processing', ' ', source_sab, ' into table ', target_table));


  		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_version()
		INTO mth_version
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;


		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', target_table)
		INTO target_rows
		;

		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''nci_mrhier'',
			  ''NCI_MRHIER'',
			  ''%s'',
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_sab,
			  target_table,
			  source_rows,
			  target_rows);

		
		PERFORM notify_timediff(CONCAT('processing', ' ', source_sab, ' into table ', target_table), start_timestamp, stop_timestamp);
	END IF;
	END LOOP;
end;
$$
;



/**************************************************************************
/ V. EXTEND PATH TO ROOT WITH LEAF ('ext')
/ -------------------------------------------------------------------------
/ The leaf of the hierarchy is represented by the AUI, CODE, and STR. 
/ These leafs are added at the end of the path to root to get a complete 
/ representation of the classification.  
/ Note that the RxClass subset are derived from this step.
**************************************************************************/
DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	source_rows bigint;
	target_rows bigint;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;

	SELECT check_if_nci_requires_processing(mth_version, 'LOOKUP_PARSE', 'LOOKUP_EXT')
	INTO requires_processing;

  	IF requires_processing THEN

  		SELECT get_log_timestamp()
  		INTO start_timestamp
  		;

  		PERFORM notify_start('processing LOOKUP_EXT');
  		
		DROP TABLE IF EXISTS nci_mrhier.tmp_lookup_ext;
		CREATE TABLE nci_mrhier.tmp_lookup_ext AS (
			SELECT
			  *
			FROM nci_mrhier.lookup_parse lu
		)
		;
		DROP TABLE IF EXISTS nci_mrhier.lookup_ext;
		CREATE TABLE nci_mrhier.lookup_ext AS (
		  SELECT
		  	*,
		  	SUBSTRING(CONCAT('ext_', hierarchy_table), 1, 60) AS extended_table
		  FROM nci_mrhier.tmp_lookup_ext
		);
		DROP TABLE nci_mrhier.tmp_lookup_ext;
		
		COMMIT;
		
		
		
		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_version()
		INTO mth_version
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		SELECT get_row_count('nci_mrhier.lookup_parse')
		INTO source_rows
		;

		SELECT get_row_count('nci_mrhier.lookup_ext')
		INTO target_rows
		;

		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''LOOKUP_PARSE'',
			  ''LOOKUP_EXT'',
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);


		PERFORM notify_completion('processing LOOKUP_EXT');

		
		PERFORM notify_timediff('processing LOOKUP_EXT', start_timestamp, stop_timestamp);

	END IF;


END;
$$
;


DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	source_rows bigint;
	target_rows bigint;
    f record;
    sab varchar(100);
    source_table varchar(255);
    target_table varchar(255);
	iteration int;
    total_iterations int;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;

	SELECT COUNT(*) INTO total_iterations FROM nci_mrhier.lookup_ext;
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM nci_mrhier.lookup_ext l
    LOOP
		iteration    := f.iteration;
		source_table := f.hierarchy_table;
		target_table := f.extended_table;
		source_rows  := f.count;
		sab          := f.hierarchy_sab;

		SELECT check_if_nci_requires_processing(mth_version, source_table, target_table)
		INTO requires_processing;

  		IF requires_processing THEN


		PERFORM notify_start(CONCAT('processing table ', source_table, ' into table ', target_table));

  			SELECT get_log_timestamp()
  			INTO start_timestamp
  			;

  			PERFORM notify_iteration(iteration, total_iterations, source_table || ' (' || source_rows || ' rows)');

			EXECUTE
			format(
			  '
			  DROP TABLE IF EXISTS nci_mrhier.%s;
			  CREATE TABLE  nci_mrhier.%s (
			    ptr_id INTEGER NOT NULL,
			    ptr text NOT NULL,
			  	aui varchar(12),
			  	code varchar(100),
			  	str text,
			  	rela varchar(100),
			  	ptr_level INTEGER NOT NULL,
			  	ptr_aui varchar(12) NOT NULL,
			  	ptr_code varchar(100),
			  	ptr_str text
			  );

			  WITH leafs AS (
				SELECT
				  ptr_id,
				  ptr,
				  aui,
				  code,
				  str,
				  rela,
				  max(ptr_level)+1 AS ptr_level,
				  aui AS ptr_aui,
				  code AS ptr_code,
				  str AS ptr_str
				FROM nci_mrhier.%s
				GROUP BY ptr_id, ptr, aui, code, str, rela
			  ),
			  with_leafs AS (
			  	SELECT *
			  	FROM leafs
			  	UNION
			  	SELECT *
			  	FROM nci_mrhier.%s
			  )

			  INSERT INTO nci_mrhier.%s
			  SELECT *
			  FROM with_leafs
			  ORDER BY ptr_id, ptr_level
			  ;
			  ',
			  	target_table,
			  	target_table,
			  	source_table,
			  	source_table,
			  	target_table);
			  	
			COMMIT;

			PERFORM notify_completion(CONCAT('processing table ', source_table, ' into table ', target_table));


			SELECT get_log_timestamp()
			INTO stop_timestamp
			;

			SELECT get_nci_mth_version()
			INTO mth_version
			;

			SELECT get_nci_mth_dt()
			INTO mth_date
			;


			EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', target_table)
			INTO target_rows
			;

			EXECUTE
			  format(
			    '
				INSERT INTO public.process_nci_mrhier_log
				VALUES (
				  ''%s'',
				  ''%s'',
				  ''%s'',
				  ''%s'',
				  ''%s'',
				  ''nci_mrhier'',
				  ''%s'',
				  ''%s'',
				  ''%s'',
				  ''%s'');
				',
				  start_timestamp,
				  stop_timestamp,
				  mth_version,
				  mth_date,
				  sab,
				  source_table,
				  target_table,
				  source_rows,
				  target_rows);

			
		COMMIT;
		
		PERFORM notify_timediff(CONCAT('processing table ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);


    end if;
    end loop;
end;
$$
;


/**************************************************************************
/ VI. PIVOT CLASSIFICATIONS ('pivot')
/ -------------------------------------------------------------------------
/ Each table is pivoted on ptr_id to compile classifications in at 
/ the row level.
**************************************************************************/

DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	source_rows bigint;
	target_rows bigint;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;

	SELECT check_if_nci_requires_processing(mth_version, 'LOOKUP_EXT', 'LOOKUP_PIVOT_TABLES')
	INTO requires_processing;

  	IF requires_processing THEN

  		SELECT get_log_timestamp()
  		INTO start_timestamp
  		;

  		PERFORM notify_start('processing LOOKUP_PIVOT_TABLES');

  		
		DROP TABLE IF EXISTS nci_mrhier.lookup_pivot_tables;
		CREATE TABLE nci_mrhier.lookup_pivot_tables AS (
		  SELECT
		  	*,
		  	SUBSTRING(CONCAT('tmp_pivot_', hierarchy_table), 1, 60) AS tmp_pivot_table,
		  	SUBSTRING(CONCAT('pivot_', hierarchy_table), 1, 60) AS pivot_table
		  FROM nci_mrhier.lookup_ext
		);
		COMMIT;
		
		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_version()
		INTO mth_version
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		SELECT get_row_count('nci_mrhier.lookup_ext')
		INTO source_rows
		;

		SELECT get_row_count('nci_mrhier.lookup_pivot_tables')
		INTO target_rows
		;

		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''LOOKUP_EXT'',
			  ''LOOKUP_PIVOT_TABLES'',
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);


		PERFORM notify_completion('processing LOOKUP_PIVOT_TABLES');
		
		COMMIT;
		
		PERFORM notify_timediff('processing LOOKUP_PIVOT_TABLES', start_timestamp, stop_timestamp);

	END IF;
END;
$$
;


-- A second pivot lookup is made to construct the crosstab 
-- function call
-- A crosstab function call is created to pivot each table
-- based on the maximum `ptr_level` in that table. This is
-- required to pass the subsequent column names as the
-- argument to the crosstab function.
DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	max_level int;
	source_rows bigint;
	target_rows bigint;
    f record;
    sab varchar(100);
    source_table varchar(255);
    target_table varchar(255);
    pivot_table varchar(255);
	iteration int;
    total_iterations int;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;
	
	SELECT check_if_nci_requires_processing(mth_version, 'LOOKUP_PIVOT_TABLES', 'LOOKUP_PIVOT_CROSSTAB')
	INTO requires_processing; 
	
	IF requires_processing THEN 
		SELECT get_log_timestamp()
		INTO start_timestamp
		;
		
		DROP TABLE IF EXISTS nci_mrhier.lookup_pivot_crosstab;
		CREATE TABLE  nci_mrhier.lookup_pivot_crosstab (
		  extended_table varchar(255),
		  tmp_pivot_table varchar(255),
		  pivot_table varchar(255),
		  sql_statement text
		)
		;
		
		COMMIT;
		
		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_version()
		INTO mth_version
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		SELECT get_row_count('nci_mrhier.lookup_pivot_tables')
		INTO source_rows
		;

		SELECT get_row_count('nci_mrhier.lookup_pivot_crosstab')
		INTO target_rows
		;

		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''LOOKUP_PIVOT_TABLES'',
			  ''LOOKUP_PIVOT_CROSSTAB'',
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);
			  
		COMMIT;
		
		
		
	END IF;

	SELECT COUNT(*) INTO total_iterations FROM nci_mrhier.lookup_pivot_tables WHERE hierarchy_sab <> 'SRC';
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM nci_mrhier.lookup_pivot_tables l WHERE l.hierarchy_sab <> 'SRC'
    LOOP
		iteration    := f.iteration;
		source_table := f.extended_table;
		target_table := f.tmp_pivot_table;
		pivot_table  := f.pivot_table;
		source_rows  := f.count;
		sab          := f.hierarchy_sab;

		SELECT check_if_nci_requires_processing(mth_version, source_table, 'LOOKUP_PIVOT_CROSSTAB')
		INTO requires_processing;

  		IF requires_processing THEN


		PERFORM notify_start(CONCAT('processing sql statement for ', source_table, ' into table ', target_table));

		SELECT get_log_timestamp()
		INTO start_timestamp
		;

		PERFORM notify_iteration(iteration, total_iterations, source_table || ' (' || source_rows || ' rows)');

		EXECUTE format('SELECT MAX(ptr_level) FROM nci_mrhier.%s', source_table)
		INTO max_level;


	  EXECUTE
	    format(
	      '
	      WITH seq1 AS (SELECT generate_series(1,%s) AS series),
	      seq2 AS (
	      	SELECT
	      		''%s'' AS extended_table,
	      		''%s'' AS tmp_pivot_table,
	      		''%s'' AS pivot_table,
	      		STRING_AGG(CONCAT(''level_'', series, '' text''), '', '') AS crosstab_ddl
	      	FROM seq1
	      	GROUP BY extended_table, tmp_pivot_table),
	      seq3 AS (
	      	SELECT
	      		extended_table,
	      		tmp_pivot_table,
	      		pivot_table,
	      		CONCAT(''ptr_id BIGINT, '', crosstab_ddl) AS crosstab_ddl
	      	FROM seq2
	      ),
	      seq4 AS (
	        SELECT
	          extended_table,
	          tmp_pivot_table,
	          pivot_table,
	          '''''''' || CONCAT(''SELECT ptr_id, ptr_level, ptr_str FROM nci_mrhier.'', extended_table, '' ORDER BY 1,2'') || '''''''' AS crosstab_arg1,
	          '''''''' || CONCAT(''SELECT DISTINCT ptr_level FROM nci_mrhier.'', extended_table, '' ORDER BY 1'') || '''''''' AS crosstab_arg2,
	          crosstab_ddl
	         FROM seq3
	      ),
	      seq5 AS (
	      	SELECT
	      	  extended_table,
	      	  tmp_pivot_table,
	      	  pivot_table,
	      	  ''DROP TABLE IF EXISTS nci_mrhier.'' || tmp_pivot_table || '';'' || '' CREATE TABLE nci_mrhier.'' || tmp_pivot_table || '' AS (SELECT * FROM CROSSTAB('' || crosstab_arg1 || '','' || crosstab_arg2 || '') AS ('' || crosstab_ddl || ''));'' AS sql_statement
	      	  FROM seq4

	      )

	      INSERT INTO nci_mrhier.lookup_pivot_crosstab
	      SELECT * FROM seq5
	      ;
	      ',
	      max_level,
	      source_table,
	      target_table,
	      pivot_table);

	      COMMIT;

		PERFORM notify_completion(CONCAT('processing sql statement for ', source_table, ' into table ', target_table));


		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_version()
		INTO mth_version
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;


		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''nci_mrhier'',
			  ''%s'',
			  ''LOOKUP_PIVOT_CROSSTAB'',
			  ''%s'',
			   NULL);
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  sab,
			  source_table,
			  source_rows);
			  
		COMMIT;
		
		PERFORM notify_timediff(CONCAT('processing sql statement for ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);



    end if;
    end loop;
end;
$$
;

-- The sql statements are executed from 
-- 'LOOKUP_PIVOT_CROSSTAB'
DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	source_rows bigint;
	target_rows bigint;
    f record;
    sab varchar(100);
    source_table varchar(255);
    tmp_table varchar(255);
    target_table varchar(255);
	iteration int;
    total_iterations int;
    sql_statement text;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;

	SELECT COUNT(*) INTO total_iterations FROM nci_mrhier.lookup_pivot_crosstab;
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM nci_mrhier.lookup_pivot_crosstab l
    LOOP
		iteration    := f.iteration;
		source_table := f.extended_table;
		tmp_table    := f.tmp_pivot_table;
		target_table := f.pivot_table;
		
		PERFORM notify_iteration(iteration, total_iterations, source_table || ' --> ' || target_table);

		SELECT check_if_nci_requires_processing(mth_version, source_table, target_table)
		INTO requires_processing;

  		IF requires_processing THEN

  		SELECT get_log_timestamp()
		INTO start_timestamp
		;


		PERFORM notify_start(CONCAT('processing ', source_table, ' into table ', target_table));

		    sql_statement := f.sql_statement;
		    EXECUTE sql_statement;


	    EXECUTE
	      format(
	      	'
	      	DROP TABLE IF EXISTS nci_mrhier.%s;
	      	CREATE TABLE nci_mrhier.%s AS (
		      	SELECT DISTINCT
		      	  h.aui,
		      	  h.code,
		      	  h.str,
		      	  t.*
		      	FROM nci_mrhier.%s h
		      	LEFT JOIN nci_mrhier.%s t
		      	ON t.ptr_id = h.ptr_id
	      	);
	      	DROP TABLE nci_mrhier.%s;
	      	',
	      		target_table,
	      		target_table,
	      		source_table,
	      		tmp_table,
	      		tmp_table);
	      		
	    COMMIT;

		PERFORM notify_completion(CONCAT('processing ', source_table, ' into table ', target_table));


		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_version()
		INTO mth_version
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', target_table)
		INTO target_rows;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', source_table)
		INTO source_rows;


		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			   ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_table,
			  target_table,
			  source_rows,
			  target_rows);
			  
			  
		COMMIT;
		
		
		PERFORM notify_timediff(CONCAT('processing ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);



    end if;
    end loop;
end;
$$
;


/**************************************************************************
/ VIb. PIVOT CLASSIFICATIONS BY CODE ('pivot')
/ -------------------------------------------------------------------------
/ Each table is pivoted on ptr_id to compile classifications in at 
/ the row level.
**************************************************************************/

DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	source_rows bigint;
	target_rows bigint;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;

	SELECT check_if_nci_requires_processing(mth_version, 'LOOKUP_EXT', 'LOOKUP_PIVOT_TABLES_CODE')
	INTO requires_processing;

  	IF requires_processing THEN

  		SELECT get_log_timestamp()
  		INTO start_timestamp
  		;

  		PERFORM notify_start('processing LOOKUP_PIVOT_TABLES_CODE');

  		
		DROP TABLE IF EXISTS nci_mrhier.lookup_pivot_tables_code;
		CREATE TABLE nci_mrhier.lookup_pivot_tables_code AS (
		  SELECT
		  	*,
		  	SUBSTRING(CONCAT('tmp_pivot_code_', hierarchy_table), 1, 60) AS tmp_pivot_code_table,
		  	SUBSTRING(CONCAT('pivot_code_', hierarchy_table), 1, 60) AS pivot_code_table
		  FROM nci_mrhier.lookup_ext
		);
		COMMIT;
		
		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_version()
		INTO mth_version
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		SELECT get_row_count('nci_mrhier.lookup_ext')
		INTO source_rows
		;

		SELECT get_row_count('nci_mrhier.lookup_pivot_tables_code')
		INTO target_rows
		;

		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''LOOKUP_EXT'',
			  ''LOOKUP_PIVOT_TABLES_CODE'',
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);


		PERFORM notify_completion('processing LOOKUP_PIVOT_TABLES_CODE');
		
		PERFORM notify_timediff('processing LOOKUP_PIVOT_TABLES_CODE', start_timestamp, stop_timestamp);

	END IF;
END;
$$
;


-- A second pivot lookup is made to construct the crosstab 
-- function call
-- A crosstab function call is created to pivot each table
-- based on the maximum `ptr_level` in that table. This is
-- required to pass the subsequent column names as the
-- argument to the crosstab function.
DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	max_level int;
	source_rows bigint;
	target_rows bigint;
    f record;
    sab varchar(100);
    source_table varchar(255);
    target_table varchar(255);
    pivot_table varchar(255);
	iteration int;
    total_iterations int;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;
	
	SELECT check_if_nci_requires_processing(mth_version, 'LOOKUP_PIVOT_TABLES_CODE', 'LOOKUP_PIVOT_CROSSTAB_CODE')
	INTO requires_processing; 
	
	IF requires_processing THEN 
		SELECT get_log_timestamp()
		INTO start_timestamp
		;
		
		DROP TABLE IF EXISTS nci_mrhier.lookup_pivot_crosstab_code;
		CREATE TABLE  nci_mrhier.lookup_pivot_crosstab_code (
		  extended_table varchar(255),
		  tmp_pivot_code_table varchar(255),
		  pivot_code_table varchar(255),
		  sql_statement text
		)
		;
		
		COMMIT;
		
		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_version()
		INTO mth_version
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		SELECT get_row_count('nci_mrhier.lookup_pivot_tables_code')
		INTO source_rows
		;

		SELECT get_row_count('nci_mrhier.lookup_pivot_crosstab_code')
		INTO target_rows
		;

		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''LOOKUP_PIVOT_TABLES_CODE'',
			  ''LOOKUP_PIVOT_CROSSTAB_CODE'',
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);
			  
		COMMIT;
		
		
		
	END IF;

	SELECT COUNT(*) INTO total_iterations FROM nci_mrhier.lookup_pivot_tables_code WHERE hierarchy_sab <> 'SRC';
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM nci_mrhier.lookup_pivot_tables_code l WHERE l.hierarchy_sab <> 'SRC'
    LOOP
		iteration    := f.iteration;
		source_table := f.extended_table;
		target_table := f.tmp_pivot_code_table;
		pivot_table  := f.pivot_code_table;
		source_rows  := f.count;
		sab          := f.hierarchy_sab;

		SELECT check_if_nci_requires_processing(mth_version, source_table, 'LOOKUP_PIVOT_CROSSTAB_CODE')
		INTO requires_processing;

  		IF requires_processing THEN


		PERFORM notify_start(CONCAT('processing sql statement for ', source_table, ' into table ', target_table));

		SELECT get_log_timestamp()
		INTO start_timestamp
		;

		PERFORM notify_iteration(iteration, total_iterations, source_table || ' (' || source_rows || ' rows)');

		EXECUTE format('SELECT MAX(ptr_level) FROM nci_mrhier.%s', source_table)
		INTO max_level;


	  EXECUTE
	    format(
	      '
	      WITH seq1 AS (SELECT generate_series(1,%s) AS series),
	      seq2 AS (
	      	SELECT
	      		''%s'' AS extended_table,
	      		''%s'' AS tmp_pivot_code_table,
	      		''%s'' AS pivot_code_table,
	      		STRING_AGG(CONCAT(''level_'', series, ''_code text''), '', '') AS crosstab_ddl
	      	FROM seq1
	      	GROUP BY extended_table, tmp_pivot_code_table),
	      seq3 AS (
	      	SELECT
	      		extended_table,
	      		tmp_pivot_code_table,
	      		pivot_code_table,
	      		CONCAT(''ptr_id BIGINT, '', crosstab_ddl) AS crosstab_ddl
	      	FROM seq2
	      ),
	      seq4 AS (
	        SELECT
	          extended_table,
	          tmp_pivot_code_table,
	          pivot_code_table,
	          '''''''' || CONCAT(''SELECT ptr_id, ptr_level, ptr_code FROM nci_mrhier.'', extended_table, '' ORDER BY 1,2'') || '''''''' AS crosstab_arg1,
	          '''''''' || CONCAT(''SELECT DISTINCT ptr_level FROM nci_mrhier.'', extended_table, '' ORDER BY 1'') || '''''''' AS crosstab_arg2,
	          crosstab_ddl
	         FROM seq3
	      ),
	      seq5 AS (
	      	SELECT
	      	  extended_table,
	      	  tmp_pivot_code_table,
	      	  pivot_code_table,
	      	  ''DROP TABLE IF EXISTS nci_mrhier.'' || tmp_pivot_code_table || '';'' || '' CREATE TABLE nci_mrhier.'' || tmp_pivot_code_table || '' AS (SELECT * FROM CROSSTAB('' || crosstab_arg1 || '','' || crosstab_arg2 || '') AS ('' || crosstab_ddl || ''));'' AS sql_statement
	      	  FROM seq4

	      )

	      INSERT INTO nci_mrhier.lookup_pivot_crosstab_code
	      SELECT * FROM seq5
	      ;
	      ',
	      max_level,
	      source_table,
	      target_table,
	      pivot_table);

	      COMMIT;

		PERFORM notify_completion(CONCAT('processing sql statement for ', source_table, ' into table ', target_table));


		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_version()
		INTO mth_version
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;


		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''nci_mrhier'',
			  ''%s'',
			  ''LOOKUP_PIVOT_CROSSTAB_CODE'',
			  ''%s'',
			   NULL);
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  sab,
			  source_table,
			  source_rows);
			  
		COMMIT;
		
		PERFORM notify_timediff(CONCAT('processing sql statement for ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);



    end if;
    end loop;
end;
$$
;


-- The sql statements are executed from 
-- 'LOOKUP_PIVOT_CROSSTAB_CODE'
DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	source_rows bigint;
	target_rows bigint;
    f record;
    sab varchar(100);
    source_table varchar(255);
    tmp_table varchar(255);
    target_table varchar(255);
	iteration int;
    total_iterations int;
    sql_statement text;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;

	SELECT COUNT(*) INTO total_iterations FROM nci_mrhier.lookup_pivot_crosstab_code;
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM nci_mrhier.lookup_pivot_crosstab_code l
    LOOP
		iteration    := f.iteration;
		source_table := f.extended_table;
		tmp_table    := f.tmp_pivot_code_table;
		target_table := f.pivot_code_table;
		
		PERFORM notify_iteration(iteration, total_iterations, source_table || ' --> ' || target_table);

		SELECT check_if_nci_requires_processing(mth_version, source_table, target_table)
		INTO requires_processing;

  		IF requires_processing THEN

  		SELECT get_log_timestamp()
		INTO start_timestamp
		;


		PERFORM notify_start(CONCAT('processing ', source_table, ' into table ', target_table));

	    sql_statement := f.sql_statement;
	    EXECUTE sql_statement;
	    
	    COMMIT;


	    EXECUTE
	      format(
	      	'
	      	DROP TABLE IF EXISTS nci_mrhier.%s;
	      	CREATE TABLE nci_mrhier.%s AS (
		      	SELECT DISTINCT
		      	  h.aui,
		      	  h.code,
		      	  h.str,
		      	  t.*
		      	FROM nci_mrhier.%s h
		      	LEFT JOIN nci_mrhier.%s t
		      	ON t.ptr_id = h.ptr_id
	      	);
	      	DROP TABLE nci_mrhier.%s;
	      	',
	      		target_table,
	      		target_table,
	      		source_table,
	      		tmp_table,
	      		tmp_table);
	      		
	    COMMIT;

		PERFORM notify_completion(CONCAT('processing ', source_table, ' into table ', target_table));


		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_version()
		INTO mth_version
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', target_table)
		INTO target_rows;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', source_table)
		INTO source_rows;


		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			   ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_table,
			  target_table,
			  source_rows,
			  target_rows);
			  
			  
		COMMIT;
		
		
		PERFORM notify_timediff(CONCAT('processing ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);



    end if;
    end loop;
end;
$$
;






/**************************************************************************
/ VII. NCI_MRHIER_STR: UNION PIVOTED TABLES
/ -------------------------------------------------------------------------
/ A NCI_MRHIER_STR table is written that is a union of all the pivoted 
/ tables.
/ The absolute maximum ptr level across the entire NCI_MRHIER
/ is derived to generate the DDL for the column names of the
/ final NCI_MRHIER_STR table.
**************************************************************************/
DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	max_level int;
	source_rows bigint;
	target_rows bigint;
    f record;
    sab varchar(100);
    source_table varchar(255);
    target_table varchar(255) := 'LOOKUP_MRHIER_ABS_MAX';
    pivot_table varchar(255);
	iteration int;
    total_iterations int;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;
	
	SELECT check_if_nci_requires_processing(mth_version, 'LOOKUP_EXT', 'LOOKUP_MRHIER_ABS_MAX')
	INTO requires_processing; 
	
	IF requires_processing THEN 
	
		SELECT get_log_timestamp()
		INTO start_timestamp
		;
		
		DROP TABLE IF EXISTS nci_mrhier.lookup_mrhier_abs_max;
		CREATE TABLE nci_mrhier.lookup_mrhier_abs_max (
		  extended_table varchar(255),
		  max_ptr_level int
		);	
		
		COMMIT;
		
		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'lookup_ext')
		INTO target_rows;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'lookup_mrhier_abs_max')
		INTO source_rows;


		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			   ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  'LOOKUP_EXT',
			  'LOOKUP_MRHIER_ABS_MAX',
			  source_rows,
			  target_rows);
			  
			  
		COMMIT;
	
	END IF;
	
	SELECT COUNT(*) INTO total_iterations FROM nci_mrhier.lookup_ext;
  	for f in select ROW_NUMBER() OVER() AS iteration, l.* from nci_mrhier.lookup_ext l
 	LOOP
 		iteration    := f.iteration;
		source_table := f.extended_table;
		
		PERFORM notify_iteration(iteration, total_iterations, source_table || ' --> ' || target_table);

		SELECT check_if_nci_requires_processing(mth_version, source_table, target_table)
		INTO requires_processing;

  		IF requires_processing THEN
  		
  			PERFORM notify_start(CONCAT('processing ', source_table, ' into table ', target_table));

	  		SELECT get_log_timestamp()
			INTO start_timestamp
			;
			
			EXECUTE
		      format(
		      	'
		      	INSERT INTO nci_mrhier.lookup_mrhier_abs_max
		      	SELECT
		      	 ''%s'' AS extended_table,
		      	 MAX(ptr_level) AS max_ptr_level
		      	 FROM nci_mrhier.%s
		      	 ;
		      	',
		      		source_table,
		      		source_table);
		    	    COMMIT;

			PERFORM notify_completion(CONCAT('processing ', source_table, ' into table ', target_table));


			SELECT get_log_timestamp()
			INTO stop_timestamp
			;
	
			SELECT get_nci_mth_version()
			INTO mth_version
			;
	
			SELECT get_nci_mth_dt()
			INTO mth_date
			;
	
			EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', target_table)
			INTO target_rows;
	
			EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', source_table)
			INTO source_rows;
	
	
			EXECUTE
			  format(
			    '
				INSERT INTO public.process_nci_mrhier_log
				VALUES (
				  ''%s'',
				  ''%s'',
				  ''%s'',
				  ''%s'',
				  NULL,
				  ''nci_mrhier'',
				  ''%s'',
				  ''%s'',
				  ''%s'',
				   ''%s'');
				',
				  start_timestamp,
				  stop_timestamp,
				  mth_version,
				  mth_date,
				  source_table,
				  target_table,
				  source_rows,
				  target_rows);
				  
				  
			COMMIT;
			
			
			PERFORM notify_timediff(CONCAT('processing ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);

	END IF;
	END LOOP;

END;
$$
;



-- Write NCI_MRHIER_STR Table 
DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	max_level int;
	source_rows bigint;
	target_rows bigint;
    f record;
    sab varchar(100);
    source_table varchar(255);
    target_table varchar(255);
    pivot_table varchar(255);
	iteration int;
    total_iterations int;
    processed_mrhier_ddl text;
    abs_max_ptr_level int;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;
	
	SELECT check_if_nci_requires_processing(mth_version, 'LOOKUP_MRHIER_ABS_MAX', 'LOOKUP_MRHIER_DDL')
	INTO requires_processing; 
	
	IF requires_processing THEN 
	
		SELECT get_log_timestamp()
		INTO start_timestamp
		;
		
		SELECT MAX(max_ptr_level) INTO abs_max_ptr_level FROM nci_mrhier.lookup_mrhier_abs_max; 
		
		EXECUTE
		format(
		'
		DROP TABLE IF EXISTS nci_mrhier.lookup_mrhier_ddl; 
		CREATE TABLE nci_mrhier.lookup_mrhier_ddl (
			ddl text
		);

		
		  WITH seq1 AS (SELECT generate_series(1, %s) AS series),
		  seq2 AS (
		    SELECT
		      STRING_AGG(CONCAT(''level_'', series, ''_str text''), '', '') AS ddl
		      FROM seq1
		  )
		
		  INSERT INTO nci_mrhier.lookup_mrhier_ddl
		  SELECT ddl
		  FROM seq2
		  ;',
		  abs_max_ptr_level);
		  
		  
		  COMMIT;
		  
		  	SELECT get_log_timestamp()
			INTO stop_timestamp
			;
	
			SELECT get_nci_mth_version()
			INTO mth_version
			;
	
			SELECT get_nci_mth_dt()
			INTO mth_date
			;
	
			EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'LOOKUP_MRHIER_DDL')
			INTO target_rows;
	
			EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'LOOKUP_MRHIER_ABS_MAX')
			INTO source_rows;
	
	
			EXECUTE
			  format(
			    '
				INSERT INTO public.process_nci_mrhier_log
				VALUES (
				  ''%s'',
				  ''%s'',
				  ''%s'',
				  ''%s'',
				  NULL,
				  ''nci_mrhier'',
				  ''%s'',
				  ''%s'',
				  ''%s'',
				   ''%s'');
				',
				  start_timestamp,
				  stop_timestamp,
				  mth_version,
				  mth_date,
				  'LOOKUP_MRHIER_ABS_MAX',
				  'LOOKUP_MRHIER_DDL',
				  source_rows,
				  target_rows);
		  
	END IF;
	
	SELECT check_if_nci_requires_processing(mth_version, 'LOOKUP_MRHIER_DDL', 'NCI_MRHIER_STR')
	INTO requires_processing; 
	
	IF requires_processing THEN 
	
		SELECT get_log_timestamp()
		INTO start_timestamp
		;
	
	  SELECT ddl
	  INTO processed_mrhier_ddl
	  FROM nci_mrhier.lookup_mrhier_ddl;
	
	  EXECUTE
	    format(
	    '
	    DROP TABLE IF EXISTS nci_mrhier.nci_mrhier_str;
	    CREATE TABLE nci_mrhier.nci_mrhier_str (
	      aui varchar(12),
	      code text,
	      str text,
	      ptr_id bigint,
	      %s
	    );
	    ',
	    processed_mrhier_ddl
	    );
	    
		
		COMMIT;
		
		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'NCI_MRHIER_STR')
		INTO target_rows;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'LOOKUP_MRHIER_DDL')
		INTO source_rows;


		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			   ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  'LOOKUP_MRHIER_DDL',
			  'NCI_MRHIER_STR',
			  source_rows,
			  target_rows);
			  
			  
		COMMIT;
	
	END IF;
	
  SELECT COUNT(*) INTO total_iterations FROM nci_mrhier.lookup_pivot_crosstab;
  for f in select ROW_NUMBER() OVER() AS iteration, pl.* from nci_mrhier.lookup_pivot_crosstab pl
  loop
    iteration := f.iteration;
    pivot_table := f.pivot_table;
    source_table := f.pivot_table;
    target_table := 'NCI_MRHIER_STR';
    
	PERFORM notify_iteration(iteration, total_iterations, source_table || ' --> ' || target_table);

	SELECT check_if_nci_requires_processing(mth_version, source_table, target_table)
	INTO requires_processing;

  	IF requires_processing THEN
    
	    SELECT get_log_timestamp()
		INTO start_timestamp
		;
		
		PERFORM notify_start(CONCAT('Adding ' || source_table || ' to ' || target_table));
		
	    EXECUTE
	      format('
	      INSERT INTO nci_mrhier.nci_mrhier_str
	      SELECT * FROM nci_mrhier.%s
	      ',
	      pivot_table
	      );
	      
	      		COMMIT;
		
		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'NCI_MRHIER_STR')
		INTO target_rows;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', source_table)
		INTO source_rows;


		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			   ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_table,
			  'NCI_MRHIER_STR',
			  source_rows,
			  target_rows);
			  
			COMMIT;
			
			
			PERFORM notify_timediff(CONCAT('processing ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);
			
   END IF;
   END loop;

	
	
END;
$$
; 


/**************************************************************************
/ VII. NCI_MRHIER_CODE: UNION PIVOTED TABLES
/ -------------------------------------------------------------------------
/ A NCI_MRHIER_CODE table is written that is a union of all the pivoted 
/ tables.
/ The absolute maximum ptr level across the entire NCI_MRHIER
/ is derived to generate the DDL for the column names of the
/ final NCI_MRHIER_CODE table.
**************************************************************************/
DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	max_level int;
	source_rows bigint;
	target_rows bigint;
    f record;
    sab varchar(100);
    source_table varchar(255);
    target_table varchar(255) := 'LOOKUP_MRHIER_ABS_MAX';
    pivot_table varchar(255);
	iteration int;
    total_iterations int;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;
	
	SELECT check_if_nci_requires_processing(mth_version, 'LOOKUP_EXT', 'LOOKUP_MRHIER_ABS_MAX')
	INTO requires_processing; 
	
	IF requires_processing THEN 
	
		SELECT get_log_timestamp()
		INTO start_timestamp
		;
		
		DROP TABLE IF EXISTS nci_mrhier.lookup_mrhier_abs_max;
		CREATE TABLE nci_mrhier.lookup_mrhier_abs_max (
		  extended_table varchar(255),
		  max_ptr_level int
		);	
		
		COMMIT;
		
		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'lookup_ext')
		INTO target_rows;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'lookup_mrhier_abs_max')
		INTO source_rows;


		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			   ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  'LOOKUP_EXT',
			  'LOOKUP_MRHIER_ABS_MAX',
			  source_rows,
			  target_rows);

	
	END IF;
	
	SELECT COUNT(*) INTO total_iterations FROM nci_mrhier.lookup_ext;
  	for f in select ROW_NUMBER() OVER() AS iteration, l.* from nci_mrhier.lookup_ext l
 	LOOP
 		iteration    := f.iteration;
		source_table := f.extended_table;
		
		PERFORM notify_iteration(iteration, total_iterations, source_table || ' --> ' || target_table);

		SELECT check_if_nci_requires_processing(mth_version, source_table, target_table)
		INTO requires_processing;

  		IF requires_processing THEN
  		
  			PERFORM notify_start(CONCAT('processing ', source_table, ' into table ', target_table));

	  		SELECT get_log_timestamp()
			INTO start_timestamp
			;
			
			EXECUTE
		      format(
		      	'
		      	INSERT INTO nci_mrhier.lookup_mrhier_abs_max
		      	SELECT
		      	 ''%s'' AS extended_table,
		      	 MAX(ptr_level) AS max_ptr_level
		      	 FROM nci_mrhier.%s
		      	 ;
		      	',
		      		source_table,
		      		source_table);
		    	    COMMIT;

			PERFORM notify_completion(CONCAT('processing ', source_table, ' into table ', target_table));


			SELECT get_log_timestamp()
			INTO stop_timestamp
			;
	
			SELECT get_nci_mth_version()
			INTO mth_version
			;
	
			SELECT get_nci_mth_dt()
			INTO mth_date
			;
	
			EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', target_table)
			INTO target_rows;
	
			EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', source_table)
			INTO source_rows;
	
	
			EXECUTE
			  format(
			    '
				INSERT INTO public.process_nci_mrhier_log
				VALUES (
				  ''%s'',
				  ''%s'',
				  ''%s'',
				  ''%s'',
				  NULL,
				  ''nci_mrhier'',
				  ''%s'',
				  ''%s'',
				  ''%s'',
				   ''%s'');
				',
				  start_timestamp,
				  stop_timestamp,
				  mth_version,
				  mth_date,
				  source_table,
				  target_table,
				  source_rows,
				  target_rows);
				  
				  
			COMMIT;
			
			
			PERFORM notify_timediff(CONCAT('processing ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);

	END IF;
	END LOOP;

END;
$$
;



-- Write NCI_MRHIER_CODE Table 
DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	max_level int;
	source_rows bigint;
	target_rows bigint;
    f record;
    sab varchar(100);
    source_table varchar(255);
    target_table varchar(255);
    pivot_table varchar(255);
	iteration int;
    total_iterations int;
    processed_mrhier_ddl text;
    abs_max_ptr_level int;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;
	
	SELECT check_if_nci_requires_processing(mth_version, 'LOOKUP_MRHIER_ABS_MAX', 'LOOKUP_MRHIER_DDL_CODE')
	INTO requires_processing; 
	
	IF requires_processing THEN 
	
		SELECT get_log_timestamp()
		INTO start_timestamp
		;
		
		SELECT MAX(max_ptr_level) INTO abs_max_ptr_level FROM nci_mrhier.lookup_mrhier_abs_max; 
		
		EXECUTE
		format(
		'
		DROP TABLE IF EXISTS nci_mrhier.lookup_mrhier_ddl_code; 
		CREATE TABLE nci_mrhier.lookup_mrhier_ddl_code (
			ddl text
		);

		
		  WITH seq1 AS (SELECT generate_series(1, %s) AS series),
		  seq2 AS (
		    SELECT
		      STRING_AGG(CONCAT(''level_'', series, ''_code text''), '', '') AS ddl
		      FROM seq1
		  )
		
		  INSERT INTO nci_mrhier.lookup_mrhier_ddl_code
		  SELECT ddl
		  FROM seq2
		  ;',
		  abs_max_ptr_level);
		  
		  
		  COMMIT;
		  
		  	SELECT get_log_timestamp()
			INTO stop_timestamp
			;
	
			SELECT get_nci_mth_version()
			INTO mth_version
			;
	
			SELECT get_nci_mth_dt()
			INTO mth_date
			;
	
			EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'LOOKUP_MRHIER_DDL_CODE')
			INTO target_rows;
	
			EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'LOOKUP_MRHIER_ABS_MAX')
			INTO source_rows;
	
	
			EXECUTE
			  format(
			    '
				INSERT INTO public.process_nci_mrhier_log
				VALUES (
				  ''%s'',
				  ''%s'',
				  ''%s'',
				  ''%s'',
				  NULL,
				  ''nci_mrhier'',
				  ''%s'',
				  ''%s'',
				  ''%s'',
				   ''%s'');
				',
				  start_timestamp,
				  stop_timestamp,
				  mth_version,
				  mth_date,
				  'LOOKUP_MRHIER_ABS_MAX',
				  'LOOKUP_MRHIER_DDL_CODE',
				  source_rows,
				  target_rows);
		  
	END IF;
	
	SELECT check_if_nci_requires_processing(mth_version, 'LOOKUP_MRHIER_DDL_CODE', 'NCI_MRHIER_CODE')
	INTO requires_processing; 
	
	IF requires_processing THEN 
	
		SELECT get_log_timestamp()
		INTO start_timestamp
		;
	
	  SELECT ddl
	  INTO processed_mrhier_ddl
	  FROM nci_mrhier.lookup_mrhier_ddl_code;
	
	  EXECUTE
	    format(
	    '
	    DROP TABLE IF EXISTS nci_mrhier.nci_mrhier_code;
	    CREATE TABLE nci_mrhier.nci_mrhier_code (
	      aui varchar(12),
	      code text,
	      str text,
	      ptr_id bigint,
	      %s
	    );
	    ',
	    processed_mrhier_ddl
	    );
	    
		
		COMMIT;
		
		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'NCI_MRHIER_CODE')
		INTO target_rows;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'LOOKUP_MRHIER_DDL_CODE')
		INTO source_rows;


		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			   ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  'LOOKUP_MRHIER_DDL_CODE',
			  'NCI_MRHIER_CODE',
			  source_rows,
			  target_rows);
			  
			  
		COMMIT;
	
	END IF;
	
  SELECT COUNT(*) INTO total_iterations FROM nci_mrhier.lookup_pivot_crosstab_code;
  for f in select ROW_NUMBER() OVER() AS iteration, pl.* from nci_mrhier.lookup_pivot_crosstab_code pl
  loop
    iteration := f.iteration;
    pivot_table := f.pivot_code_table;
    source_table := f.pivot_code_table;
    target_table := 'NCI_MRHIER_CODE';
    
	PERFORM notify_iteration(iteration, total_iterations, source_table || ' --> ' || target_table);

	SELECT check_if_nci_requires_processing(mth_version, source_table, target_table)
	INTO requires_processing;

  	IF requires_processing THEN
    
	    SELECT get_log_timestamp()
		INTO start_timestamp
		;
		
		PERFORM notify_start(CONCAT('Adding ' || source_table || ' to ' || target_table));
		
	    EXECUTE
	      format('
	      INSERT INTO nci_mrhier.nci_mrhier_code
	      SELECT * FROM nci_mrhier.%s
	      ',
	      pivot_table
	      );
	      
	      		COMMIT;
		
		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', 'NCI_MRHIER_CODE')
		INTO target_rows;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', source_table)
		INTO source_rows;


		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			   ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_table,
			  'NCI_MRHIER_CODE',
			  source_rows,
			  target_rows);
			  
			COMMIT;
			
			
			PERFORM notify_timediff(CONCAT('processing ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);
			
   END IF;
   END loop;

	
	
END;
$$
; 


/**************************************************************************
/ VIII. NCI_MRHIER_STR_EXCL: EXCLUDED PATH TO ROOT VALUES
/ -------------------------------------------------------------------------
/ Table that includes any source NCI_MRHIER `ptr` that did not make it
/ to the `NCI_MRHIER_STR` table. Only vocabularies where `LAT = 'ENG'` and not 'SRC' 
/ in the MRCONSO table are included.  
**************************************************************************/
DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	max_level int;
	source_rows bigint;
	target_rows bigint;
    f record;
    sab varchar(100);
    source_table varchar(255) := 'NCI_MRHIER';
    target_table varchar(255) := 'NCI_MRHIER_STR_EXCL';
    pivot_table varchar(255);
	iteration int;
    total_iterations int;
    processed_mrhier_ddl text;
    abs_max_ptr_level int;
BEGIN
	SELECT get_nci_mth_version()
	INTO mth_version;
	
	SELECT check_if_nci_requires_processing(mth_version, source_table, target_table)
	INTO requires_processing; 
	
	IF requires_processing THEN 
	
	  	SELECT get_log_timestamp()
		INTO start_timestamp
		;
		
		PERFORM notify_start('Writing NCI_MRHIER_STR_EXCL Table');
		
		
		DROP TABLE IF EXISTS nci_mrhier.nci_mrhier_str_excl;
		CREATE TABLE nci_mrhier.nci_mrhier_str_excl AS (
			SELECT m1.*
			FROM nci_mrhier.nci_mrhier m1
			LEFT JOIN nci_mrhier.nci_mrhier_str m2
			ON m1.ptr_id = m2.ptr_id
			WHERE
			  m2.ptr_id IS NULL AND
			  m1.ptr IS NOT NULL AND
			  m1.sab IN (SELECT l.sab FROM nci_mrhier.lookup_eng l) AND
			  m1.sab <> 'SRC'
			ORDER BY m1.sab DESC -- Arbitrarily in descending order to include SNOMEDCT_US first
			)
		;
		
		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', target_table)
		INTO target_rows;

		EXECUTE format('SELECT COUNT(*) FROM nci_mrhier.%s;', source_table)
		INTO source_rows;


		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_mrhier'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			   ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_table,
			  target_table,
			  source_rows,
			  target_rows);
			  
			COMMIT;
			
			
			PERFORM notify_timediff('Writing NCI_MRHIER_STR_EXCL table', start_timestamp, stop_timestamp);
	
	END IF; 
	
END;
$$
;


/**************************************************************************
/ IX. NCI_CLASS Schema
/ -------------------------------------------------------------------------
/ A fresh NCI_CLASS schema is created with copies of the NCI_MRHIER, NCI_MRHIER_STR, 
/ and NCI_MRHIER_STR_EXCL tables to section them off from the processing tables in the 
/ NCI_MRHIER schema with a corresponding SETUP_NCI_CLASS_LOG log table in 
/ the public schema. 
**************************************************************************/
DO
$$
DECLARE
	log_timestamp timestamp;
	mth_version varchar;
	mth_release_dt varchar;
	target_schema varchar := 'nci_class';
	source_table varchar := ''; 
	target_table varchar := 'NCI_CLASS Tables';
	mrhier_rows bigint;
	mrhier_str_rows bigint;
	mrhier_code_rows bigint;
	mrhier_str_excl_rows bigint;
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_date varchar;
BEGIN

	SELECT get_nci_mth_version()
	INTO mth_version;

	SELECT check_if_nci_requires_processing(mth_version, source_table, target_table)
	INTO requires_processing; 
	
	IF requires_processing THEN 
	
	   	SELECT get_log_timestamp()
		INTO start_timestamp
		;
		
		SELECT get_nci_mth_dt()
		INTO mth_release_dt;
		
		PERFORM notify_start('writing nci_class schema');
		
		DROP TABLE IF EXISTS public.tmp_setup_nci_class_log;
		CREATE TABLE IF NOT EXISTS public.tmp_setup_nci_class_log (
		    nci_mth_version character varying(255),
		    nci_mth_release_dt character varying(255),
		    target_schema character varying(255),
		    nci_mrhier bigint,
		    nci_mrhier_str bigint,
		    nci_mrhier_str_excl bigint,
		    nci_mrhier_code bigint
		);

		EXECUTE
		 format('
		 INSERT INTO public.tmp_setup_nci_class_log(nci_mth_version, nci_mth_release_dt, target_schema)
		 VALUES (''%s'', ''%s'', ''%s'')
		 ;
		 ',
		 	mth_version,
		 	mth_release_dt,
		 	target_schema
		 	);
		 	
		DROP SCHEMA nci_class CASCADE; 
		CREATE SCHEMA nci_class; 
		
		PERFORM notify_start('copying NCI_MRHIER table'); 
		
		DROP TABLE IF EXISTS nci_class.nci_mrhier;
		CREATE TABLE nci_class.nci_mrhier AS (
		SELECT *
		FROM nci_mrhier.nci_mrhier
		)
		;
		COMMIT;
		
		SELECT COUNT(*) 
		INTO mrhier_rows 
		FROM nci_class.nci_mrhier;
		
		PERFORM notify_completion('copying NCI_MRHIER table');
		
		  EXECUTE
		    format(
		    '
		    UPDATE public.tmp_setup_nci_class_log
		    SET nci_mrhier = %s
		    WHERE nci_mth_version = ''%s'';
		    ',
		    mrhier_rows,
		    mth_version
		    )
		  ;
		  COMMIT;
		  
		PERFORM notify_start('copying NCI_MRHIER_STR table'); 
		
		DROP TABLE IF EXISTS nci_class.nci_mrhier_str;
		CREATE TABLE nci_class.nci_mrhier_str AS (
		SELECT *
		FROM nci_mrhier.nci_mrhier_str
		)
		;
		COMMIT;
		
		SELECT COUNT(*) 
		INTO mrhier_str_rows 
		FROM nci_class.nci_mrhier_str;
		
		PERFORM notify_completion('copying NCI_MRHIER_STR table');
		
		EXECUTE
		    format(
		    '
		    UPDATE public.tmp_setup_nci_class_log
		    SET nci_mrhier_str = %s
		    WHERE nci_mth_version = ''%s'';
		    ',
		    mrhier_str_rows,
		    mth_version
		    )
		 ;

		PERFORM notify_start('copying NCI_MRHIER_CODE table'); 
		
		DROP TABLE IF EXISTS nci_class.nci_mrhier_code;
		CREATE TABLE nci_class.nci_mrhier_code AS (
		SELECT *
		FROM nci_mrhier.nci_mrhier_code
		)
		;
		COMMIT;
		
		SELECT COUNT(*) 
		INTO mrhier_code_rows 
		FROM nci_class.nci_mrhier_code;
		
		PERFORM notify_completion('copying NCI_MRHIER_CODE table');
		
		EXECUTE
		    format(
		    '
		    UPDATE public.tmp_setup_nci_class_log
		    SET nci_mrhier_code = %s
		    WHERE nci_mth_version = ''%s'';
		    ',
		    mrhier_code_rows,
		    mth_version
		    )
		 ;



		 
		PERFORM notify_start('copying NCI_MRHIER_STR_EXCL table');
		
	 	DROP TABLE IF EXISTS nci_class.nci_mrhier_str_excl;
		CREATE TABLE nci_class.nci_mrhier_str_excl AS (
		SELECT *
		FROM nci_mrhier.nci_mrhier_str_excl
		)
		;
		COMMIT;
		
		SELECT COUNT(*) 
		INTO mrhier_str_excl_rows 
		FROM nci_class.nci_mrhier_str_excl;
		
		PERFORM notify_completion('copying NCI_MRHIER_STR_EXCL table');
		
		EXECUTE
		    format(
		    '
		    UPDATE public.tmp_setup_nci_class_log
		    SET nci_mrhier_str = %s
		    WHERE nci_mth_version = ''%s'';
		    ',
		    mrhier_str_excl_rows,
		    mth_version
		    )
		 ;
		
		PERFORM notify_start('adding constraints');
		ALTER TABLE nci_class.nci_mrhier_str
		ADD CONSTRAINT xpk_nci_mrhier_str
		PRIMARY KEY (ptr_id);
		
		CREATE INDEX x_nci_mrhier_str_aui ON nci_class.nci_mrhier_str(aui);
		CREATE INDEX x_nci_mrhier_str_code ON nci_class.nci_mrhier_str(code);
		
		ALTER TABLE nci_class.nci_mrhier_code
		ADD CONSTRAINT xpk_nci_mrhier_code
		PRIMARY KEY (ptr_id);
		
		CREATE INDEX x_nci_mrhier_code_aui ON nci_class.nci_mrhier_code(aui);
		CREATE INDEX x_nci_mrhier_code_code ON nci_class.nci_mrhier_code(code);
		
		
		ALTER TABLE nci_class.nci_mrhier_str_excl
		ADD CONSTRAINT xpk_nci_mrhier_str_excl
		PRIMARY KEY (ptr_id);
		
		CREATE INDEX x_nci_mrhier_str_excl_aui ON nci_class.nci_mrhier_str_excl(aui);
		CREATE INDEX x_nci_mrhier_str_excl_code ON nci_class.nci_mrhier_str_excl(code);
		CREATE INDEX x_nci_mrhier_str_excl_sab ON nci_class.nci_mrhier_str_excl(sab);
		 
		PERFORM notify_completion('adding constraints');
		
		INSERT INTO public.setup_nci_class_log 
		SELECT 
		   TIMEOFDAY()::timestamp AS snc_datetime, 
		   log.nci_mth_version, 
		   log.nci_mth_release_dt, 
		   log.target_schema, 
		   log.nci_mrhier, 
		   log.nci_mrhier_str, 
		   log.nci_mrhier_str_excl, 
		   log.nci_mrhier_code
		FROM public.tmp_setup_nci_class_log log
		; 
		
		DROP TABLE public.tmp_setup_nci_class_log;
		COMMIT;
		
	    PERFORM notify_completion('writing nci_class schema');
		 
		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_nci_mth_dt()
		INTO mth_date
		;

		EXECUTE
		  format(
		    '
			INSERT INTO public.process_nci_mrhier_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL,
			  ''nci_class'',
			  ''%s'',
			  ''%s'',
			   NULL,
			   NULL);
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_table,
			  target_table);
			  
			COMMIT;
	 	
	end if; 	
END;
$$
;
