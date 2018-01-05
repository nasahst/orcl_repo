create or replace PACKAGE BODY N_TITAN_PCK AS

FUNCTION clean_data (v_session IN NUMBER) 
RETURN BOOLEAN 
IS

v_result BOOLEAN := TRUE;
v_record VARCHAR2(300);
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
    NULL,
 --   SUBSTR(SUBSTR(tit.t_name,1,instr(tit.t_name,',')-1),1,8), 
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

END N_TITAN_PCK;
