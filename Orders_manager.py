# Author: Ka Kit Leng
# Date: 3 Apr 2022
# Description: A program the manages customer's orders using mysql and northwind.sql


import mysql.connector as dbconn
import datetime

db = dbconn.connect(host="localhost", user="cs4430", passwd="cs4430", db="northwind")

cursor = db.cursor(buffered=True)
sql_transaction = "SET SESSION transaction_isolation='SERIALIZABLE'"
cursor.execute(sql_transaction)


def choices(option):
    if option == 1:
        add_customer()
    elif option == 2:
        add_order()
    elif option == 3:
        remove_order()
    elif option == 4:
        ship_order()
    elif option == 5:
        print_pending()
    elif option == 6:
        print("Thank you, have a nice day!")


# add a customer (1)
def add_customer():
    cid = get_cid()
    company_name = input("Company's Name: ")
    last_name = input("Last name: ")
    first_name = input("First name: ")
    email = input("Email: ")
    job_title = input("Job title: ")
    business_phone = input("Business phone: ")
    sql = "INSERT INTO Customers(id, company, lastname, firstname, email, jobtitle, businessphone) VALUES " \
          "(%s, %s, %s, %s, %s, %s, %s)"
    info = (cid, company_name, last_name, first_name, email, job_title, business_phone)
    cursor.execute(sql, info)
    db.commit()
    print("\nSuccessfully added customers!\n")
    main()


# add an order (2)
def add_order():
    print("ADD ORDER")
    oid = get_oid()
    add_order_table(oid)
    add_order_details_table(oid)


def add_order_details_table(oid):
    productid = get_pid()
    sql = "SELECT discontinued, listprice FROM Products WHERE id = %s"
    val = (productid, )
    cursor.execute(sql, val)
    for (discontinued, listprice) in cursor:
        if discontinued == 1:
            print("Sorry, this product has been discontinued!")
            get_add_prompt(oid)
        else:
            detail_id = get_detail_id()
            sql = "INSERT INTO Order_Details (id, orderid, productid, unitprice) VALUES (%s, %s, %s, %s)"
            val = (detail_id, oid, productid, listprice)
            cursor.execute(sql, val)
            db.commit()
            get_add_prompt(oid)


def add_order_table(oid):
    print("\nPlease add order details.\n")
    cid = check_cid()
    eid = check_eid()
    sid = check_sid()
    orderdate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    shipaddress = input("Shipping Address: ")
    sql = "INSERT INTO Orders (orderid, employeeid, customerid, shipperid, orderdate, shipaddress) VALUES " \
          "(%s, %s, %s, %s, %s, %s)"
    val = (oid, eid, cid, sid, orderdate, shipaddress)
    cursor.execute(sql, val)
    db.commit()


# remove an order
def remove_order():
    print("Removing an order\n")
    sql1 = "DELETE FROM Orders WHERE orderid = %s"
    sql2 = "DELETE FROM Order_Details WHERE orderid = %s"
    orderid = check_oid()
    val = (orderid,)
    cursor.execute(sql2, val)
    cursor.execute(sql1, val)
    db.commit()
    print("\nOrder has been deleted successfully!")
    main()


# ship an order
def ship_order():
    can_be_ship = True
    run_view = True
    sql = "SELECT productid, quantity FROM Order_Details WHERE orderid = %s"
    oid = check_oid()
    if check_shipped_date(oid) is True:
        val = (oid, )
        cursor.execute(sql, val)
        ordered_list = []
        for (productid, quantity) in cursor:
            ordered_list.append([productid, quantity])
        for productid, quantity in ordered_list:
            if run_view is True:
                sql_view = "CREATE VIEW inventory AS SELECT productid, " \
                           "SUM(case when transactiontype = 1 then quantity else 0 end) - " \
                           "SUM(case when transactiontype in (2,3) then quantity else 0 end) AS " \
                           "Unit_in_Stock FROM Inventory_Transactions GROUP BY productid"
                cursor.execute(sql_view)
                run_view = False
            else:
                sql = "SELECT * FROM inventory WHERE productid = %s"
                val = (productid, )
                cursor.execute(sql, val)
                for (productid, Unit_in_stock) in cursor:
                    if can_be_ship is True:
                        if quantity <= Unit_in_stock:
                            can_be_ship = True
                            start_shipping_process(oid)
                            update_unit_in_stock(productid, quantity)
                            print("\nOrder shipped successfully!")
                        else:
                            can_be_ship = False
                            print("There isn't enough inventory to be ship!")
                            break
        sql_remove_view = "DROP VIEW inventory"
        cursor.execute(sql_remove_view)
    else:
        print("Order has already been shipped!")
    main()


def update_unit_in_stock(pid, order_quantity):
    new_unit_in_stock = 0
    sql = "SELECT * FROM inventory WHERE productid = %s"
    val = (pid, )
    cursor.execute(sql, val)
    for (productid, Unit_in_stock) in cursor:
        new_unit_in_stock = Unit_in_stock - order_quantity
    sql = "UPDATE inventory SET Unit_in_stock = %s WHERE productid = %s"
    val = (new_unit_in_stock, pid)
    cursor.execute(sql, val)
    db.commit()


def start_shipping_process(oid):
    # fill in shipdate, shipper id, shipping fee
    # set ship date as current date
    ship_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sid = check_sid()
    shipping_fee = "{:.2f}".format(float(input("Shipping fee: ")))
    sql = "UPDATE Orders SET shippeddate = %s, shipperid = %s, shippingfee = %s WHERE orderid = %s"
    val = (ship_date, sid, shipping_fee, oid)
    cursor.execute(sql, val)
    db.commit()


def check_shipped_date(oid):
    sql = "SELECT shippeddate, orderid FROM Orders WHERE orderid = %s"
    val = (oid, )
    cursor.execute(sql, val)
    for (shippeddate, orderid) in cursor:
        if shippeddate is None:
            return True
        else:
            return False


# print pending
def print_pending():
    print("YOUR PENDING ORDERS\n")
    # print in order of order date
    sql = "SELECT orderid, customerid, orderdate FROM Orders WHERE shippeddate is NULL ORDER BY orderdate"
    f_str1 = "{:20s} | {:10s} | {:10s} | {:20s} | {:30s} | {:15s} | {:s}"
    f_str2 = "{:20s} | {:^10d} | {:10d} | {:20s} | {:30s} | {:15s} | {:s}"
    print(f_str1.format("OrderDate", "OrderID", "CustomerID", "Firstname", "Lastname", "Company name", "Business Phone"))
    cursor.execute(sql)
    order_list = []
    for (orderid, customerid, orderdate) in cursor:
        order_list.append([orderid, customerid, orderdate])

    for orderid, customerid, orderdate in order_list:
        cus_sql = "SELECT lastname, firstname, company, businessphone FROM Customers where id = %s"
        val = (customerid,)
        cursor.execute(cus_sql, val)
        for (lastname, firstname, compname, businessphone) in cursor:
            firstname = firstname[:20]
            lastname = lastname[:30]
            compname = compname[:15]
            print(f_str2.format(str(orderdate), orderid, customerid, firstname, lastname, compname, businessphone))
    main()

def check_oid():
    oid = input("Order ID: ")
    sql = "SELECT orderid FROM Orders"
    cursor.execute(sql)
    oid_list = []
    for (orderid, ) in cursor:
        oid_list.append(orderid)
    while oid.isdigit() is False or int(oid) not in oid_list:
        print("Order ID does not exist\n")
        oid = input("Please enter a valid order ID: ")
    return int(oid)


def check_cid():
    cid = input("CustomerID: ")
    sql_cid = "SELECT id, lastname FROM Customers"
    cursor.execute(sql_cid)
    cid_list = []
    for (id, lastname) in cursor:
        cid_list.append(id)
    while cid.isdigit() is False or int(cid) not in cid_list:
        print("CustomerID does not exist, please enter valid CID")
        cid = input("CustomerID: ")
    return int(cid)


def check_eid():
    eid = input("EmployeeID: ")
    sql_eid = "SELECT id, lastname FROM Employees"
    cursor.execute(sql_eid)
    eid_list = []
    for (id, lastname) in cursor:
        eid_list.append(id)
    while eid.isdigit() is False or int(eid) not in eid_list:
        print("EmployeeID does not exist, please enter valid EID")
        eid = input("EmployeeID: ")
    return int(eid)


def check_sid():
    sid = input("ShipperID: ")
    sql_sid = "SELECT id, lastname FROM Shippers"
    cursor.execute(sql_sid)
    sid_list = []
    for (id, lastname) in cursor:
        sid_list.append(id)
    while sid.isdigit() is False or int(sid) not in sid_list:
        print("ShipperID does not exist, please enter valid EID")
        sid = input("ShipperID: ")
    return int(sid)


def get_cid():
    cid = "SELECT LAST_INSERT_ID() + 1"
    cursor.execute(cid)
    return cid


def get_oid():
    oid = "SELECT LAST_INSERT_ID() + 1"
    cursor.execute(oid)
    return oid


def get_pid():
    pid = "SELECT LAST_INSERT_ID() + 1"
    cursor.execute(pid)
    return pid


def get_detail_id():
    detail_id = "SELECT LAST_INSERT_ID() + 1"
    cursor.execute(detail_id)
    return detail_id


def get_add_prompt(oid):
    add_prompt = input("Would you like to add another product? (y/n): ")
    if add_prompt[0].upper() == 'Y':
        add_order_details_table(oid)
    else:
        print("Order successfully added! OrderID:", oid)
        main()


def start_program():
    print('''
        --------------------Main Menu-----------------------
        1. Add a customer
        2. Add an order
        3. Remove an order
        4. Ship an order
        5. Print pending orders
        6. Exit
        ''')
    option = input("Please choose an option showed above by tying in a number: ")
    while option.isdigit() is False or int(option) < 1 or int(option) > 6:
        print("Please enter valid option!")
        option = input("\nPlease choose an option showed above by tying in a number: ")
    print()
    return int(option)


def main():
    option = start_program()
    choices(option)

main()
cursor.close()
db.close()
