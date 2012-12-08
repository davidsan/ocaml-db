(* File: db.ml
   David San
   email: david.san@etu.upmc.fr

   Str library must be linked at compilation
   $ ocamlc str.cma db.ml
*)

(** exception raised when an unknown field name is used. *)
exception Unbound_field
(** exception raised when an unknown or already destroyed row is used. *)
exception Unbound_row
(** exception raised when a file is empty. *)
exception Empty_file

(** The type of rows in the database.
    Objects of this type are not to be created by the user.
    They have to be retrieved by a selection or insertion in the database. *)
class type row = object
  (** Returns the value of a given field.
      May raise Unbound_field. *)
  method access : string -> string
  (** Sets the value of a given field.
      May raise Unbound_field. *)
  method update : string -> string -> unit
  (** Removes this row from its database.
      Any use of the object after a call to this method will raise Unbound_row. *)
  method destroy : unit -> unit
end

(** The (abstract) type of selection expressions. Expressions are
    evaluated internally for a given row to produce a string result
    (by a select operation or a column creation). *)
type expr = row -> string

(** Gives the value of a field from the row. *)
let field str =
  let f row =
    row#access str
  in f
(** Returns "true" if the results of the two expressions are the same
    or "false" otherwise. *)
let equals e1 e2 =
  let f row =
    if (e1 row) = (e2 row) then "true" else	"false"
  in f
(** Produces a constant string. *)
let const str =
  let f row =
    str
  in f
(** Concatenates the results of the two expressions. *)
let concat e1 e2 =
  let f row =
    (e1 row) ^ (e2 row)
  in f
(** checks if the result of the first expr contains the result of the second expr,
    * when applied, returns "true" or "false" *)
let contains e1 e2 =
  let f row =
    let re = Str.regexp_string (e2 row)
    in
    try ignore (Str.search_forward re (e1 row) 0); "true"
    with Not_found -> "false"
  in f
(** applies a standard OCaml operator (&&), (||) etc. to the results of the two expr,
    * assuming "false" is false and anything else is true,
    * when applied, return "true" or "false" *)
let bool_op bool_fun e1 e2 =
  let f row =
    let str2bool str =
      match str with
      | "false" -> false
      | _ -> true
    in
    let b1 = str2bool (e1 row) and b2 = str2bool (e2 row)
    in
    if bool_fun b1 b2 then "true" else "false"
  in f
(** applies an OCaml binary string function. *)
let string_op string_fun e1 e2 =
  let f row =
    string_fun (e1 row) (e2 row)
  in f

(** The implementation of rows in the database. *)
class db_row : row = object
  (* Mark is set to true when the object is scheduled for garbage collection. *)
  val mutable mark = false
  (* Average number of fields in a database table. *)
  val mutable tbl = Hashtbl.create 15
  (** Returns the value of a given field.
      May raise Unbound_field. *)
  method access field =
    try
      if mark then raise Unbound_row else
	Hashtbl.find tbl field
    with Not_found -> raise Unbound_field
  (** Sets the value of a given field.
      May raise Unbound_field. *)
  method update field value =
    if mark then raise Unbound_row else
      Hashtbl.replace tbl field value
  (** Removes this row from its database.
      Any use of the object after a call to this method will raise Unbound_row. *)
  method destroy () = mark<-true
end

(** The type of databases.
    Instanciating this classe creates an empty database,
    using the field names given as arguments. *)
class db fields_init =
object (self)
  (** List of fields. *)
  val mutable fields = fields_init
  (** List of rows. *)
  val mutable rows = []
  (** Retrives all the rows as objects of the type row. *)
  method all =
    rows
  (** Retrives rows for which an application of the given expr returns anything but "false". *)
  method select e =
    let rec f l accu =
      match l with
      | [] -> accu
      | hd::tl ->
	match (e hd) with
	| "false" -> f tl accu
	| _ -> f tl (hd::accu)
    in f rows []
  (** Same as select, but returns only one matching row.
      Can raise Not_found. *)
  method select_one e =
    let rec f l =
      match l with
      | [] -> raise Not_found
      | hd::tl ->
	match (e hd) with
	| "false" -> f tl
	| _ -> hd
    in f rows
  (** Removes a row from the database.
      Any use of the object after a call to this method will raise Unbound_row. *)
  method delete r =
    rows <- List.filter (fun x -> x<>r) rows;
    r#destroy()
  (** Returns the list of field names. *)
  method describe =
    fields
  (** Insert a new row from a list associating field names to values.
      If a field is not given, its value is "".
      If an unknown field is given, Unbound_field is raised. *)
  method insert data =
    let r = new db_row in
    let rec f l =
      match l with
      | [] -> List.iter
      	(fun x ->
          if not (List.mem_assoc x data)
          then r#update x "")
      	self#describe;
      	rows <- r::rows;
      	r
      | (field, value)::tl ->
      	if List.mem field self#describe then
          begin
      	    r#update field value;
      	    f tl
	  end else raise Unbound_field
    in f data
  (** Adds a field. If it already exists, this method does nothing.
      A new value is computed for each row using an expression
      (which can use the values of other fields of the row). *)
  method add_column field e =
    if not (List.mem field self#describe) then
    begin
      fields <- fields@[field];
      List.iter (fun r -> r#update field (e r)) rows
    end
  (** Removes a field and the associated values in all rows.
      If an unknown field is given, Unbound_field is raised. *)
  method remove_column field =
    if List.mem field fields then begin
      List.iter (fun r -> r#update field "") rows;
      fields <- List.filter (fun x -> x<>field) fields
    end else
      raise Unbound_field
end
(** Remove the last element of a list. *)
let remove_last l =
  List.rev (List.tl (List.rev l))
(** Strip the double quotes that surround a string. *)
let strip_quotes str =
  let offset = 1 in
  String.sub str offset (String.length str - offset - 1);;
(** Imports a database from a CSV file.
    The first line must contains the field names.
    Fields are separated by commas, all fields have to be surrounded by
    double quotes (""). *)
(* FIXME: This assumes that the file supplied is in the CSV format. *)
let read_csv filename =
  let chan = open_in filename in
  let new_db = new db [] in
  try
    let fields = input_line chan in
    let ifs = Str.regexp "," in
    List.iter
      (fun x ->
        new_db#add_column
          (strip_quotes x)
          (const ""))
      (remove_last (Str.split_delim ifs fields));
    while true; do
      let buffer = input_line chan in
      let assocs =
        List.map2
          (fun field value ->
            (field, (strip_quotes value)))
          new_db#describe
          (remove_last (Str.split_delim ifs buffer))
      in
      ignore (new_db#insert assocs)
    done; raise Empty_file
  with End_of_file ->
    close_in chan;
    new_db
(** Print a database on the specified output channel. *)
let print_db chan db =
  List.iter
    (fun field ->
      Printf.fprintf chan "\"%s\"," field)
    db#describe;
  Printf.fprintf chan "\n";
  List.iter
    (fun r ->
      List.iter
        (fun x -> Printf.fprintf chan "\"%s\"," (r#access x))
        db#describe;
      Printf.fprintf chan "\n")
    db#all
(** Exports a database to a CSV file.
    Same format as read_csv. *)
let write_csv filename db =
  let chan = open_out filename in
  print_db chan db;
  close_out chan
