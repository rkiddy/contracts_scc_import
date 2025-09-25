
import argparse
import csv
import os
import subprocess
import sys

from dotenv import dotenv_values
from sqlalchemy import create_engine

cfg = dotenv_values('.env')

engine = create_engine(f"mysql+pymysql://{cfg['USR']}:{cfg['PWD']}@{cfg['HOST']}/{cfg['DB']}")
conn = engine.connect()

def arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--month_pk', '-m', default='-1', help="Month pk to use.")
    return parser.parse_args()


def db_exec(eng, this_sql):
    # print(f"sql: {sql}")
    if this_sql.strip().startswith('select'):
        return [dict(row) for row in eng.execute(this_sql).fetchall()]
    else:
        # print(f"sql: {this_sql}")
        return eng.execute(this_sql)


def get_max_pk(table_name):
    pk_rows = db_exec(conn, f"select max(pk) as max from {table_name}")
    if len(pk_rows) == 0 or pk_rows[0]['max'] is None:
        return 0
    else:
        return int(pk_rows[0]['max'])


def files_with_ending(s):
    return [r for r in os.listdir() if r.endswith(f".{s}")]


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
        val = val[1:].replace(' ', '').replace(',', '').replace('.', '').replace('\r', '')
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


def is_contracts_file(f):
    if f is None or f == '':
        raise Exception(f"Not a valid file, should be \"contract...\" or \"sa-bc...\"")
    return f.lower().startswith('contract')


def is_sabc_file(f):
    if f is None or f == '':
        raise Exception(f"Not a valid file, should be \"contract...\" or \"sa-bc...\"")
    return f.lower().replace('-', '').startswith('sabc')


def contracts_source_pk(m_pk):
    found = list()
    for row in db_exec(engine, f"select * from sources where month_pk = {m_pk}"):
        if '/contracts' in row['source_url'].replace(' ', '').replace('-','').lower():
            found.append(row['pk'])
    return max(found)


def sa_bc_source_pk(m_pk):
    found = list()
    for row in db_exec(engine, f"select * from sources where month_pk = {m_pk}"):
        if '/sabc' in row['source_url'].replace(' ', '').replace('-','').lower():
            found.append(row['pk'])
    return max(found)


def process_pdf_files():
    cmd = "java -jar /home/ray/Projects/tabula/tabula-1.0.4-SNAPSHOT-jar-with-dependencies.jar --batch " \
          "/home/ray/Projects/contracts_scc_import/ --lattice --pages all --format TSV"
    subprocess.run(cmd, shell=True, check=True)


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
        sql = f"insert into vendors (pk, name) values ({fix_int(next_pk)}, {fix_str(name)})"
        return {'pk': next_pk, 'sql': sql}


def find_budget_units(name, num=None):
    if num:
        # the SA-BC report gives a single budget unit and, seperately, a number.

        # check names first.
        sql = f"select * from budget_unit_names where name = {fix_str(name)}"
        rows = db_exec(engine, sql)
        if len(rows) == 1:
            return [{'pk': rows[0]['unit_pk']}]

        # now chec budget units themselves.
        sql = f"""select * from budget_units
            where unit_num = {fix_int(num)} and name like '{fix_str_nq(name)}%%' order by pk"""
        rows = db_exec(engine, sql)
        if len(rows) >= 1:
            return [{'pk': rows[0]['pk']}]
        else:
            next_pk = db_exec(engine, "select max(pk) as pk from budget_units")[0]['pk'] + 1
            sql = f"""insert into budget_units (pk, unit_num, name)
                      values ({fix_int(next_pk)}, {fix_int(num)}, {fix_str(name)})"""
            return [{'pk': next_pk, 'sql': sql}]
    else:
       # the Contract report gives us a list like 'Unit1 - Num\rUnit 2 - Num'.

       results = list()
       next_pk = None
       units = name.split('\r')
       for unit in units:

           # check names first.
           sql = f"select * from budget_unit_names where name = {fix_str(unit)}"
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
                       name like '{fix_str_nq(u_name)}%%' order by pk"""
               rows = db_exec(engine, sql)
               if len(rows) >= 1:
                   results.append({'pk': rows[0]['pk']})
               else:
                   if not next_pk:
                       next_pk = db_exec(engine, "select max(pk) as pk from budget_units")[0]['pk'] + 1
                   else:
                       next_pk += 1
                   sql = f"""insert into budget_units (pk, unit_num, name) values
                       ({fix_int(next_pk)}, {fix_int(u_num)}, {fix_str(u_name)})"""
                   results.append({'pk': next_pk, 'sql': sql})
           else:
               sql = f"select * from budget_units where name like '{fix_str_nq(unit)}%%' order by pk"
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
                   sql = f"""insert into budget_units (pk, unit_num, name) values
                       ({fix_int(next_pk)}, NULL, {fix_str(unit)})"""
                   results.append({'pk': next_pk, 'sql': sql})

       return results


if __name__ == '__main__':

    args = arguments()

    if int(args.month_pk) < 0:
        m_pk = get_max_pk('months')
    else:
        m_pk = int(args.month_pk)

    con_pk = contracts_source_pk(m_pk)
    sabc_pk = sa_bc_source_pk(m_pk)
    print(f"m_pk: {m_pk}, con_pk: {con_pk}, sabc_pk: {sabc_pk}")

    contract_pk = db_exec(engine, "select max(pk) as pk from contracts")[0]['pk'] + 1
    print(f"next contract_pk: {contract_pk}")

    # files = ['contracts-report-for-month-october-2024.tsv', 'sa-bc-report-for-month-of-october_2024.tsv']
    pdf_files = files_with_ending('pdf')
    tsv_files = files_with_ending('tsv')
    print(f"pdf_files: {pdf_files}")
    print(f"tsv_files: {tsv_files}")

    if len(pdf_files) != len(tsv_files):
        print("processing pdf files...")
        process_pdf_files()
    else:
        print("found existing pdf files")

    existing = db_exec(engine, f"select count(0) as count from contracts where month_pk = {m_pk}")[0]['count']

    if existing > 0:
        print(f"found contracts for month # {existing}. Continue?")
        sys.stdin.read()

    found_users = list()

    for file in tsv_files:

        with open(file, newline='', encoding='latin1') as f:
            print(f"processing file: {file}...")

            rdr = csv.DictReader(f, delimiter='\t')

            line = 0

            for row in rdr:
                # print(f"\nrow: {row}")
                line += 1

                sqls = list()
                bu_sqls = list()

                if is_contracts_file(file):

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

                    keys.append('source_pk')
                    vals.append(fix_int(131))

                    bu_sqls = find_budget_units(row['Authorized Users'])

                elif is_sabc_file(file):

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

                    keys.append('source_pk')
                    vals.append(fix_int(132))

                    bu_sqls = find_budget_units(row['Budget Unit Name'], row['Budget Unit'])

                else:
                    raise Exception(f"file cannot be identified: {file}")

                keys.append('month_pk')
                vals.append(str(m_pk))

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

                # TODO include column names in this SQL statement? Should I? -rrk 20250924
                sqls.append(f"insert into contracts ({', '.join(keys)}) values ({', '.join(vals)})")

                # print(f"\nsqls: {'\n'.join(sqls)}")

                for sql in sqls:
                    db_exec(engine, sql)

                contract_pk += 1
                print('.', end='')

    print("")

