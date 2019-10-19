from src.BaseDataTable import BaseDataTable
import pymysql

import logging
logger = logging.getLogger()


def get_connection(connect_info):
    """

    :param connect_info: A dictionary containing the information necessary to make a PyMySQL connection.
    :return: The connection. May raise an Exception/Error.
    """

    cnx = pymysql.connect(**connect_info)
    return cnx


def run_q(sql, args=None, fetch=True, cur=None, conn=None, commit=True):
    '''
    Helper function to run an SQL statement.

    This is a modification that better supports HW1. An RDBDataTable MUST have a connection specified by
    the connection information. This means that this implementation of run_q MUST NOT try to obtain
    a defailt connection.

    :param sql: SQL template with placeholders for parameters. Canno be NULL.
    :param args: Values to pass with statement. May be null.
    :param fetch: Execute a fetch and return data if TRUE.
    :param conn: The database connection to use. This cannot be NULL, unless a cursor is passed.
        DO NOT PASS CURSORS for HW1.
    :param cur: The cursor to use. This is wizard stuff. Do not worry about it for now.
        DO NOT PASS CURSORS for HW1.
    :param commit: This is wizard stuff. Do not worry about it.

    :return: A pair of the form (execute response, fetched data). There will only be fetched data if
        the fetch parameter is True. 'execute response' is the return from the connection.execute, which
        is typically the number of rows effected.
    '''

    cursor_created = False
    connection_created = False

    try:

        if conn is None:
            raise ValueError("In this implementation, conn cannot be None.")

        if cur is None:
            cursor_created = True
            cur = conn.cursor()

        if args is not None:
            log_message = cur.mogrify(sql, args)
        else:
            log_message = sql

        logger.debug("Executing SQL = " + log_message)

        res = cur.execute(sql, args)

        if fetch:
            data = cur.fetchall()
        else:
            data = None

        # Do not ask.
        if commit == True:
            conn.commit()

    except Exception as e:
        raise(e)

    return (res, data)


def template_to_tuple(template):
    return ", ".join(["%s" for i in range(len(template))])


def template_to_equalities(template):
    """

    :param template: One of those weird templates
    :return: WHERE clause corresponding to the template.
    """

    if template is None or template == {}:
        result = (None, None)
    else:
        args = []
        terms = []

        for k, v in template.items():
            terms.append("{} = %s".format(k))
            args.append(v)

        clause = " AND ".join(terms)

        result = (clause, args)

    return result


def create_fields(fields):

    if fields is None:
        return " * "
    else:
        return ", ".join(fields)


def create_select(table_name, template, fields, order_by=None, limit=None, offset=None):
    """
    Produce a select statement: sql string and args.

    :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
    :param fields: Columns to select (an array of column name)
    :param template: One of Don Ferguson's weird JSON/python dictionary templates.
    :param order_by: Ignore for now.
    :param limit: Ignore for now.
    :param offset: Ignore for now.
    :return: A tuple of the form (sql string, args), where the sql string is a template.
    """
    w_clause, w_args = template_to_equalities(template)
    sql = "SELECT {} FROM {} WHERE {}".format(create_fields(fields), table_name, w_clause)
    return sql, w_args


def create_delete(table_name, template):
    w_clause, w_args = template_to_equalities(template)
    sql = "DELETE FROM {} WHERE {}".format(table_name, w_clause)
    return sql, w_args


def create_update(table_name, template, new_values):
    s_clause, s_args = template_to_equalities(new_values)
    w_clause, w_args = template_to_equalities(template)
    sql = "UPDATE {} SET {} WHERE {}".format(table_name, s_clause, w_clause)
    return sql, s_args + w_args


def create_insert(table_name, template):
    column_tup = ", ".join(template.keys())
    sql = "INSERT INTO {} ({}) VALUES ({})".format(table_name, column_tup, template_to_tuple(template))
    return sql, list(template.values())
