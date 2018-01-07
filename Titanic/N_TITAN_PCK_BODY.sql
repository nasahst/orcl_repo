create or replace PACKAGE BODY N_TITAN_PCK AS

FUNCTION clean_data (v_session IN NUMBER) 
RETURN BOOLEAN 
IS

v_result BOOLEAN := TRUE;
v_record VARCHAR2(200);
v_error_count  NUMBER;
v_error_message VARCHAR2(200);
v_error_index number;

TYPE t_out_titanic IS TABLE OF N_TITANIC_TARGET_FORVAR%ROWTYPE INDEX BY BINARY_INTEGER;
v_out_titianic t_out_titanic;


ex_dml_errors EXCEPTION;
PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);

CURSOR titanic_mid_cur 
IS
    SELECT
    NULL,
    t_name,
    trim(SUBSTR(tit.t_name,instr(t_name,' ',1,2))),
    SUBSTR(tit.t_name,1,instr(tit.t_name,',')-1), 
    SUBSTR(tit.t_class,1,1),
    CASE WHEN tit.t_age = 'NA' THEN NULL 
    ELSE to_number(tit.t_age) end as age,
    SUBSTR(tit.t_sex,1,1),
    SUBSTR(tit.t_survived,1,1),
    SUBSTR(tit.t_sexcode,1,1)
    FROM n_titanic_target_mid tit; 


BEGIN

DELETE FROM n_titanic_target_mid
  WHERE t_name = 'Name';

OPEN titanic_mid_cur;

LOOP
  FETCH titanic_mid_cur
  BULK COLLECT INTO v_out_titianic
  LIMIT 100;
BEGIN
 FORALL i IN v_out_titianic.first .. v_out_titianic.last  SAVE EXCEPTIONS
      INSERT INTO N_TITANIC_TARGET (
      T_NAME_LONG,
      T_FIRST_NAME,
      T_LAST_NAME,
      T_CLASS,
      T_AGE,
      T_SEX,
      T_SURVIVED,
      T_SEXCODE
      )
      VALUES (
      v_out_titianic(i).T_NAME_LONG,
      v_out_titianic(i).T_FIRST_NAME,
      v_out_titianic(i).T_LAST_NAME,
      v_out_titianic(i).T_CLASS,
      v_out_titianic(i).T_AGE,
      v_out_titianic(i).T_SEX,
      v_out_titianic(i).T_SURVIVED,
      v_out_titianic(i).T_SEXCODE
      );
      

  EXCEPTION
    WHEN ex_dml_errors THEN        
     v_result := FALSE;
      v_error_count := SQL%BULK_EXCEPTIONS.count;
      DBMS_OUTPUT.put_line('Number of failures: ' || v_error_count);
      FOR i IN 1 .. v_error_count LOOP
       v_error_message:='Error: ' || i || 
          ' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||
          ' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE);
        DBMS_OUTPUT.put_line(v_error_message);
   
        v_error_index := SQL%BULK_EXCEPTIONS(i).error_index;
        v_record := v_out_titianic(v_error_index).T_NAME_LONG || ' # with index:' || SQL%BULK_EXCEPTIONS(i).error_index;  
        INSERT INTO N_ERROR_LOG (
        T_RECORD,
        T_SESSION,
        T_ERROR_MESSAGE
        )
        VALUES(
        v_record,
        v_session,
        v_error_message
        );
       
      
     
      END LOOP;
END;
EXIT WHEN v_out_titianic.COUNT = 0;      
END LOOP;   
CLOSE titanic_mid_cur;

COMMIT;


RETURN v_result;

END;

PROCEDURE create_cust_sales (v_spread NUMBER) IS
 
TYPE t_sales IS TABLE OF N_ART_SALES%ROWTYPE INDEX BY BINARY_INTEGER;
    v_sales t_sales;
    v_sale N_ART_SALES%ROWTYPE;
    v_amount number;
    v_cust_number number;
    v_custs_processed number;
    v_previous_date number;
    
    CURSOR c_clients IS
      SELECT DISTINCT 
      NULL,
      ROWNUM,
      ntt.t_first_name,
      ntt.t_last_name,
      SYSDATE,
      NULL,
      NULL,
      NULL
      FROM n_titanic_target ntt
      ORDER BY ROWNUM;
    
    
    BEGIN
    -- clean target table -- temporary
    EXECUTE IMMEDIATE 'TRUNCATE TABLE N_ART_SALES';   
    -- find number of customers processing
    SELECT COUNT(1) INTO v_custs_processed FROM n_titanic_target;
    
    
    OPEN c_clients;
    
    LOOP
    FETCH c_clients INTO v_sale;
    
    -- add random purchase amount
    v_amount := ROUND(dbms_random.value(79,10000),2);
   -- v_cust_number := ROUND(dbms_random.value(1,v_custs_processed),2);
    
    INSERT INTO n_art_sales (
    T_ID_CUST,
    T_FIRST_NAM,
    T_LAST_NAME,
    T_SALE_DATE,
    T_CITY_SALES,
    T_TOTAL_AMOUNT
    )
    VALUES (
    v_sale.t_id_cust,
    v_sale.t_first_nam,
    v_sale.t_last_name,
    v_sale.t_sale_date,
    NULL,
    v_amount
    );

    EXIT WHEN c_clients%NOTFOUND;
    
    END LOOP;
    
    CLOSE c_clients;
    
   COMMIT;
    
    FOR cntr IN 1..v_spread
    LOOP
    v_cust_number := ROUND(dbms_random.value(1,v_custs_processed),0);
    v_amount := ROUND(dbms_random.value(79,10000),2);
    v_previous_date := ROUND(dbms_random.value(1,365),0);
      SELECT 
          NULL,
          ntt.t_id_cust,
          ntt.t_first_nam,
          ntt.t_last_name,
          SYSDATE - v_previous_date,
          NULL,
          NULL,
          NULL
          INTO v_sale
          FROM n_art_sales ntt
          WHERE ntt.t_id_cust = v_cust_number
          FETCH FIRST 1 ROWS ONLY;
    
    INSERT INTO n_art_sales (
    T_ID_CUST,
    T_FIRST_NAM,
    T_LAST_NAME,
    T_SALE_DATE,
    T_CITY_SALES,
    T_TOTAL_AMOUNT
    )
    VALUES (
    v_sale.t_id_cust,
    v_sale.t_first_nam,
    v_sale.t_last_name,
    v_sale.t_sale_date,
    NULL,
    v_amount
    );
    
    
    END LOOP;
 
    END;
    
    PROCEDURE update_special_trans IS
    
    l_task     VARCHAR2(30);
    l_sql_stmt VARCHAR2(32767);
    l_try      NUMBER;
    l_status   NUMBER;
    
BEGIN
  
  l_task := 'Parallel_task' || to_char(parallel_seq.nextval);

  DBMS_PARALLEL_EXECUTE.create_task (task_name => l_task);

  DBMS_PARALLEL_EXECUTE.create_chunks_by_rowid(task_name   => l_task,
                                               table_owner => 'NASA',
                                               table_name  => 'N_ART_SALES',
                                               by_row      => TRUE,
                                               chunk_size  => 100000);

  l_sql_stmt := 'UPDATE N_ART_SALES t 
                 SET    t.t_special = ''S''
                 WHERE t.t_total_amount > 4000
                 AND rowid BETWEEN :start_id AND :end_id';

  DBMS_PARALLEL_EXECUTE.run_task(task_name      => l_task,
                                 sql_stmt       => l_sql_stmt,
                                 language_flag  => DBMS_SQL.NATIVE,
                                 parallel_level => 10);

  
  l_try := 0;
  l_status := DBMS_PARALLEL_EXECUTE.task_status(l_task);
  WHILE(l_try < 2 and l_status != DBMS_PARALLEL_EXECUTE.FINISHED) 
  Loop
    l_try := l_try + 1;
    DBMS_PARALLEL_EXECUTE.resume_task(l_task);
    l_status := DBMS_PARALLEL_EXECUTE.task_status(l_task);
  END LOOP;

  DBMS_PARALLEL_EXECUTE.drop_task(l_task);
    
    END;


END N_TITAN_PCK;