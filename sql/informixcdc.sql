begin work;

{
    informixcdc_opntxns saves the open transactions for a informixcdc instance
}
drop table if exists informixcdc_opntxns;
create table informixcdc_opntxns (
    id              smallint not null,
    transaction_id  integer not null,
    seq_number      bigint not null
) extent size 64 next size 64 lock mode row;

create unique index informixcdc_opntxns_pk on informixcdc_opntxns(id, transaction_id);
alter table informixcdc_opntxns add constraint primary key (id, transaction_id)
    constraint informixcdc_opntxns_pk;
alter table informixcdc_opntxns add constraint check (id > 0)
    constraint informixcdc_opntxns_ck1;
alter table informixcdc_opntxns add constraint check (transaction_id >= 0)
    constraint informixcdc_opntxns_ck2;
alter table informixcdc_opntxns add constraint check (seq_number >= 0)
    constraint informixcdc_opntxns_ck3;

{
    informixcdc_lsttxn saves the highest sequence number we have committed
}
drop table if exists informixcdc_lsttxn;
create table informixcdc_lsttxn (
    id              smallint not null,
    seq_number      bigint not null
) extent size 16 next size 16 lock mode row;

create unique index informixcdc_lsttxn_pk on informixcdc_opntxns(id);
alter table informixcdc_lsttxn add constraint primary key (id)
    constraint informixcdc_lsttxn_pk;
alter table informixcdc_lsttxn add constraint check (id > 0)
    constraint informixcdc_lsttxn_ck1;
alter table informixcdc_lsttxn add constraint check (seq_number >= 0)
    constraint informixcdc_lsttxn_ck2;

commit work;
