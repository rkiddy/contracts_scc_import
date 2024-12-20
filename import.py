
import csv

from dotenv import dotenv_values
from sqlalchemy import create_engine

cfg = dotenv_values('.env')

engine = create_engine(f"mysql+pymysql://{cfg['USR']}:{cfg['PWD']}@{cfg['HOST']}/{cfg['DB']}")
conn = engine.connect()

def db_exec(eng, this_sql):
    # print(f"sql: {sql}")
    if this_sql.strip().startswith('select'):
        return [dict(row) for row in eng.execute(this_sql).fetchall()]
    else:
        # print(f"sql: {this_sql}")
        return eng.execute(this_sql)


def fix_str(val):
    if val == '':
        return 'NULL'
    val = val.replace("'", "''").replace('%', '%%')
    return f"'{val}'"


def fix_str_nq(val):
    if val == '':
        return 'NULL'
    val = val.replace("'", "''").replace('%', '%%')
    return val


def fix_int(val):
    if val is None:
        return 'NULL'
    return str(val)


def fix_date(val):
    if val is None or val == '':
        return 'NULL'
    parts = val.split('/')
    return f"'{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}'"


def fix_money(val):

    if val is None or val == '':
        return 'NULL'

    # the usual case...
    #
    if val.startswith('$'):
        val = val[1:].replace(' ', '').replace(',', '').replace('.', '')
        if val == '-':
            return '0'
        else:
            return val

    # new formats found in SA/BC file...
    #
    if '.' in val:
        return val.replace(',', '').replace('.', '')

    if '.' not in val and ',' not in val:
        return str(int(val)*100)

    raise Exception(f"UNEXPECTED MONEY VALUE: '{val}'")


def find_vendor(name):
    vendor_pk = None
    curr = name
    sql = f"select * from vendors where name like '{fix_str_nq(curr)}%%' order by pk"
    rows = db_exec(engine, sql)
    if len(rows) >= 1:
        vendor_pk = rows[0]['pk']
        return {'pk': vendor_pk}
    else:
        next_pk = db_exec(engine, "select max(pk) as pk from vendors")[0]['pk'] + 1
        sql = f"insert into vendors values ({fix_int(next_pk)}, {fix_str(name)})"
        return {'pk': next_pk, 'sql': sql}


def find_budget_units(name, num=None):
    if num:
        # the SA-BC report gives a single budget unit and, seperately, a number.

        # check names first.
        sql = f"select * from budget_unit_names where unit_name = {fix_str(name)}"
        rows = db_exec(engine, sql)
        if len(rows) == 1:
            return [{'pk': rows[0]['unit_pk']}]

        # now chec budget units themselves.
        sql = f"""select * from budget_units
            where unit_num = {fix_int(num)} and unit_name like '{fix_str_nq(name)}%%' order by pk"""
        rows = db_exec(engine, sql)
        if len(rows) >= 1:
            return [{'pk': rows[0]['pk']}]
        else:
            next_pk = db_exec(engine, "select max(pk) as pk from budget_units")[0]['pk'] + 1
            sql = f"insert into budget_units values ({fix_int(next_pk)}, {fix_int(num)}, {fix_str(name)})"
            return [{'pk': next_pk, 'sql': sql}]
    else:
       # the Contract report gives us a list like 'Unit1 - Num\rUnit 2 - Num'.

       results = list()
       next_pk = None
       units = name.split('\r')
       for unit in units:

           # check names first.
           sql = f"select * from budget_unit_names where unit_name = {fix_str(unit)}"
           rows = db_exec(engine, sql)
           if len(rows) == 1:
               results.append({'pk': rows[0]['unit_pk']})
               continue

           # now check budget units themselves.
           parts = unit.split(' - ')
           if parts[-1].isnumeric():
               u_num = int(parts[-1])
               u_name = ' - '.join(parts[:-1])
               sql = f"""select * from budget_units
                   where unit_num = {fix_int(u_num)} and
                       unit_name like '{fix_str_nq(u_name)}%%' order by pk"""
               rows = db_exec(engine, sql)
               if len(rows) >= 1:
                   results.append({'pk': rows[0]['pk']})
               else:
                   if not next_pk:
                       next_pk = db_exec(engine, "select max(pk) as pk from budget_units")[0]['pk'] + 1
                   else:
                       next_pk += 1
                   sql = f"""insert into budget_units values
                       ({fix_int(next_pk)}, {fix_int(u_num)}, {fix_str(u_name)})"""
                   results.append({'pk': next_pk, 'sql': sql})
           else:
               sql = f"select * from budget_units where unit_name like '{fix_str_nq(unit)}%%' order by pk"
               rows = db_exec(engine, sql)
               if len(rows) >= 1:
                    results.append({'pk': rows[0]['pk']})
               else:
                   # we will only get here if we have a BU we have not seen that does not have a num.
                   # which does not make sense...
                   if not next_pk:
                       next_pk = db_exec(engine, "select max(pk) as pk from budget_units")[0]['pk'] + 1
                   else:
                       next_pk += 1
                   sql = f"""insert into budget_units values
                       ({fix_int(next_pk)}, NULL, {fix_str(unit)})"""
                   results.append({'pk': next_pk, 'sql': sql})

       return results


if __name__ == '__main__':

    contract_pk = db_exec(engine, "select max(pk) as pk from contracts")[0]['pk'] + 1
    # print(f"next contract_pk: {contract_pk}")

    files = ['contracts-report-for-month-october-2024.tsv', 'sa-bc-report-for-month-of-october_2024.tsv']

    found_users = list()

    for file in files:

        with open(file, newline='', encoding='latin1') as f:
            rdr = csv.DictReader(f, delimiter='\t')

            line = 0

            for row in rdr:
                # print(f"\nrow: {row}")
                line += 1

                sqls = list()
                bu_sqls = None

                if file.startswith('contracts'):

                    if row['Owner Name'] == 'Owner Name':
                        continue

                    keys = list()
                    vals = list()

                    keys.append('pk')
                    vals.append(fix_int(contract_pk))

                    keys.append('owner_name')
                    vals.append(fix_str(row['Owner Name']))

                    keys.append('ariba_id')
                    vals.append(fix_str(row['Contract ID Ariba']))

                    keys.append('sap_id')
                    vals.append(fix_str(row['Contract ID SAP']))

                    keys.append('vendor_name')
                    vals.append(fix_str(row['Vendor Name']))

                    keys.append('effective_date')
                    vals.append(fix_date(row['Effective\rDate']))

                    keys.append('expir_date')
                    vals.append(fix_date(row['Expiration\rDate']))

                    keys.append('contract_value')
                    vals.append(fix_money(row['Contract Value']))

                    keys.append('commodity_desc')
                    vals.append(fix_str(row['Commodity Description']))

                    bu_sqls = find_budget_units(row['Authorized Users'])

                if file.startswith('sa-bc'):

                    if row['Report Month'] == 'Report Month':
                        continue

                    keys = list()
                    vals = list()

                    keys.append('pk')
                    vals.append(fix_int(contract_pk))

                    keys.append('contract_type')
                    vals.append(fix_str(row['Document Type']))

                    keys.append('contract_id')
                    vals.append(fix_str(row['Contract ID\r(PO ID)']))

                    keys.append('vendor_name')
                    vals.append(fix_str(row['Vendor Name']))

                    keys.append('effective_date')
                    vals.append(fix_date(row['Effective Date']))

                    keys.append('expir_date')
                    vals.append(fix_date(row['Expiration Date']))

                    keys.append('contract_value')
                    vals.append(fix_money(row['Contract Value\r(PO Value)']))

                    keys.append('commodity_desc')
                    vals.append(fix_str(row['Commodity Description']))

                    bu_sqls = find_budget_units(row['Budget Unit Name'], row['Budget Unit'])

                keys.append('month_pk')
                vals.append(fix_int(56))

                keys.append('source_pk')
                vals.append(fix_int(117))

                keys.append('line_num')
                vals.append(fix_int(line))

                for bu_sql in bu_sqls:
                    if 'sql' in bu_sql:
                        sqls.append(bu_sql['sql'])
                    sqls.append(f"""insert into budget_unit_joins (contract_pk, unit_pk) values
                        ({contract_pk}, {bu_sql['pk']})""")

                found = find_vendor(row['Vendor Name'])

                if 'sql' in found:
                    sqls.append(found['sql'])

                keys.append('vendor_pk')
                vals.append(fix_int(found['pk']))

                sqls.append(f"insert into contracts ({', '.join(keys)}) values ({', '.join(vals)})")

                # print(f"\nsqls: {'\n'.join(sqls)}")

                for sql in sqls:
                    db_exec(engine, sql)

                contract_pk += 1
                print('.', end='')

    print("")

