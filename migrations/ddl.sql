CREATE TABLE master (
    ID SERIAL,
    FECHA varchar NOT NULL,
    TIPO_DOC varchar,
    NUMERO_DOC varchar,
    CUENTA varchar,
    NOMBRE varchar,
    CONCEPTO varchar,
    IDENTIDAD varchar,
    DV varchar,
    TELEFONO varchar,
    CIUDAD varchar,
    CENTRO_COSTO varchar,
    DEBITO varchar,
    CREDITO varchar,
    PRIMARY KEY(ID)
);

CREATE TABLE master_process_1 (
    ID varchar(32) NOT NULL,
    PRIMARY KEY(ID)
);

CREATE TABLE master_state (
    ID varchar(32) NOT NULL,
    id_thor varchar(32) NOT NULL,
    file_origin varchar(256),
    state smallint,
    message varchar(4096),
    update_date date,
    creation_date date,
    PRIMARY KEY(ID)
);

--truncate table master;
drop table menores;
select left(cuenta, 4)        as cuenta,
       22222222               as identidad,
       '0'                    as dv,
       '0'                    as telefono,
       'bogota'               as ciudad,
       'na'                   as direccion,
       ''                     as primer_apellido,
       ''                     as segundo_apellido,
       ''                     as primer_nombre,
       ''                     as segundo_nombre,
       'CUANTIAS MENORES'     as empresa,
       sum(debito ::integer)  as debito,
       sum(credito ::integer) as credito
into menores
from master
group by left(cuenta, 4), empresa
having sum(debito::integer) < 100000
order by left(cuenta, 4), empresa asc;

select *
from menores;

select left(cuenta, 4)         as cuenta,
       max(identidad)::text    as identidad,
       max(dv)::text           as dv,
       max(telefono)::text     as telefono,
       max(ciudad)::text       as ciudad,
       max(centro_costo)::text as direccion,
       primer_apellido,
       segundo_apellido,
       primer_nombre,
       segundo_nombre,
       empresa,
       sum(debito ::integer)   as debito,
       sum(credito ::integer)  as credito
from master
group by left(cuenta, 4),
         primer_apellido,
         segundo_apellido,
         primer_nombre,
         segundo_nombre,
         empresa
having sum(debito::integer) >= 100000
union all
select cuenta,
       max(identidad)::text   as identidad,
       max(dv)::text          as dv,
       max(telefono)::text    as telefono,
       max(ciudad)::text      as ciudad,
       max(direccion)::text   as direccion,
       primer_apellido,
       segundo_apellido,
       primer_nombre,
       segundo_nombre,
       empresa,
       sum(debito ::integer)  as debito,
       sum(credito ::integer) as credito
from menores
group by cuenta,
         primer_apellido,
         segundo_apellido,
         primer_nombre,
         segundo_nombre,
         empresa
order by 1, primer_apellido asc;

