create or replace package n_titan_pck is

FUNCTION clean_data (v_session IN NUMBER)  RETURN BOOLEAN;
PROCEDURE create_cust_sales (v_spread NUMBER);
PROCEDURE update_special_trans ;

end n_titan_pck;