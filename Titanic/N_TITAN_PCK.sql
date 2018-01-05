create or replace package n_titan_pck is

FUNCTION clean_data (v_session IN NUMBER)  RETURN BOOLEAN;

end n_titan_pck;
