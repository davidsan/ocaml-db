let _ =
  let open Db in
  let db = new db [ "name" ; "id" ] in
  (* fill the db *)
  ignore (db # insert [ "name", "Bob" ; "id", "12" ]) ;
  ignore (db # insert [ "name", "Bernard" ; "id", "14" ]) ;
  ignore (db # insert [ "name", "Marcel" ; "id", "233" ]) ;
  ignore (db # insert [ "name", "Melchior" ; "id", "1" ]) ;
  ignore (db # insert [ "name", "Racl" ; "id", "27" ]) ;
  ignore (db # insert [ "name", "Jean" ; "id", "13" ]) ;
  ignore (db # insert [ "name", "Luc" ; "id", "111" ]) ;
  (* add column for female versions *)
  db # add_column "female" (concat (field "name") (const "ette")) ;
  write_csv "res0.txt" db ;
(* must be (lines can be in another order) :
"name","id","female",
"Luc","111","Lucette",
"Jean","13","Jeanette",
"Racl","27","Raclette",
"Melchior","1","Melchiorette",
"Marcel","233","Marcelette",
"Bernard","14","Bernardette",
"Bob","12","Bobette",
*)
  (* try deletion *)
  db # delete (db # select_one (equals (field "id") (const "14"))) ;
  (* must raise Not_found *)
  (try db # select_one (equals (field "id") (const "7")) ; assert false with Not_found -> ()) ;
  (* add column with generated value *)
  let unique_string_expr =
    (* this expression generates a number, here we don't use the parameters so we use fake expressions *)
    let unique = ref 0 in
    string_op (fun _ _ -> incr unique ; string_of_int !unique) (const "fake") (const "fake")
  in
  db # add_column "number" unique_string_expr ;
  write_csv "res1.txt" db ;
(* must be (lines can be in another order) :
"name","id","female","number",
"Luc","111","Lucette","1",
"Jean","13","Jeanette","2",
"Racl","27","Raclette","3",
"Melchior","1","Melchiorette","4",
"Marcel","233","Marcelette","5",
"Bob","12","Bobette","6",
*)
  (* add column which concatenates id and number with a specific format (id,number) *)
  let field_pair_expr f1 f2 =
    string_op (fun vf1 vf2 -> "(" ^ vf1 ^ "," ^ vf2 ^ ")") (field f1) (field f2)
  in
  db # add_column "correspondance" (field_pair_expr "id" "number") ;
  (* select and update *)
  (db # select_one (equals (field "name") (const "Marcel"))) # update "female" "Marcelle" ;
  write_csv "res2.txt" db
(* must be (lines can be in another order) :
"name","id","female","number","correspondance",
"Luc","111","Lucette","1","(111,1)",
"Jean","13","Jeanette","2","(13,2)",
"Racl","27","Raclette","3","(27,3)",
"Melchior","1","Melchiorette","4","(1,4)",
"Marcel","233","Marcelle","5","(233,5)",
"Bob","12","Bobette","6","(12,6)",
*)
