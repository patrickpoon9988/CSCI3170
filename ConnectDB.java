import java.io.File;
import java.sql.*;
import java.util.Scanner;
import java.util.StringTokenizer;

public class ConnectDB {

    static String dbAddress = "jdbc:mysql://projgw.cse.cuhk.edu.hk:2633/db29";
    static String dbUsername = "Group29";
    static String dbpassword = "CSCI3170";

    static Connection conn = null;
    static Statement stmt = null;
    static ResultSet rs = null;

    public static void main(String[] args) {

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(dbAddress, dbUsername, dbpassword);
            System.out.println("\nWelcome to Car Renting System!");
            Start();
        } catch (ClassNotFoundException e) {
            System.out.println("[Error]: Java MYSQL DB Driver not found");
            System.exit(0);
        } catch (SQLException e){
            System.out.println(e);
        }
    }


    public static void Start(){

        System.out.println(" ");
        System.out.println("------Main menu-----");
        System.out.println("What kinds of operations would you like to perform?");
        System.out.println("1. Operations for Adminstrator");
        System.out.println("2. Operations for User");
        System.out.println("3. Operations for Manager");
        System.out.println("4. Exit this program");
        System.out.print("Enter your choice: ");

        Scanner scanner = new Scanner(System.in);
        int choice = scanner.nextInt();

        System.out.println(" ");

        switch (choice) {
            case 1:
                openAdminMenu();
                break;

            case 2:
                openUserMenu();
                break;

            case 3:
                openManagerMenu();
                break;

            case 4:
                System.out.println("Exiting program");
                return;

            default:
                System.out.println("Input invalid");
                Start();
        }
    }

    public static void openAdminMenu(){
        System.out.println("----- Operations for adminstrator menu -----");
        System.out.println("What kinds of operation would you like to perform?");
        System.out.println("1. Create all tables");
        System.out.println("2. Delete all tables");
        System.out.println("3. Load from datafile");
        System.out.println("4. Show number of records in each table");
        System.out.println("5. Return to the main menu");
        System.out.print("Enter your choice: ");

        Scanner scanner = new Scanner(System.in);
        int choice = scanner.nextInt();

        switch (choice) {
            case 1:
                CreateTable();
                break;

            case 2:
                DeleteTable();
                break;

            case 3:
                LoadDataFile();
                break;

            case 4:
                ShowRecord();
                break;

            case 5:
                BackMenu();
                break;

            default:
                System.out.println("Input invalid");
                Start();
        }
    }

    public static void CreateTable(){

        try {
            String sql1 =
                    "CREATE TABLE user_category("+
                            " ucid  INTEGER,"+
                            " max   INTEGER NOT NULL,"+
                            " period INTEGER NOT NULL,"+
                            " PRIMARY KEY (ucid),"+
                            " CHECK (ucid>0 AND ucid<10 AND max<10 AND period<100) );" ;
            String sql2 =
                    "CREATE TABLE user("+
                            " uid VARCHAR(12),"+
                            " name VARCHAR(25) NOT NULL,"+
                            " age INTEGER NOT NULL,"+
                            " occupation VARCHAR(20) NOT NULL,"+
                            " ucid INTEGER NOT NULL,"+
                            " PRIMARY KEY (uid),"+
                            " CHECK (age<100 AND ucid>0 AND ucid<10) );" ;
            String sql3 =
                    "CREATE TABLE car_category("+
                            " ccid INTEGER,"+
                            " ccname VARCHAR(20) NOT NULL,"+
                            " PRIMARY KEY (ccid),"+
                            " CHECK (ccid<10 AND ccid>0) );" ;
            String sql4 =
                    "CREATE TABLE car("+
                            " callnum VARCHAR(8),"+
                            " name VARCHAR(10) NOT NULL,"+
                            " manufacture DATE NOT NULL,"+
                            " time_rent INTEGER NOT NULL,"+
                            " ccid INTEGER NOT NULL,"+
                            " PRIMARY KEY (callnum),"+
                            " CHECK(time_rent<100 AND ccid<10 AND ccid>0) );" ;
            String sql5 =
                    "CREATE TABLE copy("+
                            " callnum VARCHAR(8),"+
                            " copynum INTEGER,"+
                            " PRIMARY KEY(callnum, copynum),"+
                            " CHECK(copynum>0 AND copynum<10) );" ;
            String sql6 =
                    "CREATE TABLE rent("+
                            " uid VARCHAR(12),"+
                            " callnum VARCHAR(8),"+
                            " copynum INTEGER,"+
                            " checkout DATE,"+
                            " return_date DATE,"+
                            " PRIMARY KEY(uid, callnum, copynum, checkout),"+
                            " CHECK(copynum>0 AND copynum<10) );" ;
            String sql7 =
                    "CREATE TABLE produce("+
                            " cname VARCHAR(25),"+
                            " callnum VARCHAR(8),"+
                            " PRIMARY KEY(cname, callnum) );";

            System.out.print("Processing...");
            stmt = conn.createStatement();
            stmt.addBatch(sql1);
            stmt.addBatch(sql2);
            stmt.addBatch(sql3);
            stmt.addBatch(sql4);
            stmt.addBatch(sql5);
            stmt.addBatch(sql6);
            stmt.addBatch(sql7);

            stmt.executeBatch();
            System.out.println("Done. Database is initialized");
        }
        catch (SQLException ex){
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        openAdminMenu();
    }

    public static void DeleteTable(){
        try {
            String sql1 ="DROP TABLE IF EXISTS user_category;" ;
            String sql2 ="DROP TABLE IF EXISTS user;" ;
            String sql3 ="DROP TABLE IF EXISTS car_category;" ;
            String sql4 ="DROP TABLE IF EXISTS car;" ;
            String sql5 ="DROP TABLE IF EXISTS copy;" ;
            String sql6 ="DROP TABLE IF EXISTS rent;" ;
            String sql7 ="DROP TABLE IF EXISTS produce;" ;

            System.out.print("Processing...");
            stmt = conn.createStatement();
            stmt.addBatch(sql1);
            stmt.addBatch(sql2);
            stmt.addBatch(sql3);
            stmt.addBatch(sql4);
            stmt.addBatch(sql5);
            stmt.addBatch(sql6);
            stmt.addBatch(sql7);

            stmt.executeBatch();
            System.out.println("Done. Database is removed");

        }
        catch (SQLException ex){
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        openAdminMenu();
    }

    public static void LoadDataFile(){
        System.out.println("Type in the Source Data Folder Path: ");
        Scanner path = new Scanner(System.in);
        String input = path.nextLine();
        try{
            System.out.print("Processing...");
            stmt = conn.createStatement();
            File folderPath = new File(input);
            String folderAbsolute = folderPath.getAbsolutePath();
            for (File datafile : folderPath.listFiles()){
                String fileName = datafile.getName();
                if (fileName.equals("user_category.txt")){
                    String sql =
                            "LOAD DATA LOCAL INFILE '%s' INTO TABLE user_category " +
                                    "FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' " +
                                    "LINES TERMINATED BY '\\n' (ucid, max, period);";
                    sql = String.format(sql, folderAbsolute.replace('\\','/') + "/" + fileName);
                    stmt.addBatch(sql);
                }
                else if (fileName.equals("car_category.txt")){
                    String sql =
                            "LOAD DATA LOCAL INFILE '%s' INTO TABLE car_category " +
                                    "FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' " +
                                    "LINES TERMINATED BY '\\n' (ccid, ccname);";
                    sql = String.format(sql, folderAbsolute.replace('\\','/') + "/" + fileName);
                    stmt.addBatch(sql);
                }
                else if (fileName.equals("car.txt")){
                    String sql1 =
                            "LOAD DATA LOCAL INFILE '%s' INTO TABLE car " +
                                    "FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' " +
                                    "LINES TERMINATED BY '\\n' " +
                                    "(callnum, @dummy, name, @dummy, @manufacture, time_rent, ccid)";
                    String sql1_cont =
                            "SET manufacture = STR_TO_DATE(@manufacture, '%Y-%m-%d');";
                    sql1 = String.format(sql1, folderAbsolute.replace('\\','/') + "/" + fileName);
                    sql1 += sql1_cont;
                    stmt.addBatch(sql1);

                    // load data into produce table
                    String sql2 =
                            "LOAD DATA LOCAL INFILE '%s' INTO TABLE produce " +
                                    "FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' " +
                                    "LINES TERMINATED BY '\\n' " +
                                    "(callnum, @dummy, @dummy, cname, @dummy, @dummy, @dummy);";
                    sql2 = String.format(sql2, folderAbsolute.replace('\\','/') + "/" + fileName);
                    stmt.addBatch(sql2);

                    // load data into copy table
                    File file_path = new File(folderAbsolute.replace('\\','/') + "/" + fileName);
                    Scanner scan_car = new Scanner(file_path);
                    while (scan_car.hasNextLine()){
                        String line = scan_car.nextLine();
                        int position = 0;
                        String callnum = null;
                        int numCopies = -1;
                        StringTokenizer tok = new StringTokenizer(line);
                        while(tok.hasMoreTokens()){
                            String field = tok.nextToken();
                            if (position == 0){
                                callnum = field;
                            }
                            else if (position == 1){
                                numCopies = Integer.parseInt(field);
                            }
                            else
                                break;
                            position++;
                        }
                        for (int i=1; i<=numCopies; i++){
                            String sqltmp =
                                    "INSERT INTO copy(callnum, copynum) VALUES ('%s', %d);";
                            sqltmp = String.format(sqltmp, callnum, i);
                            stmt.addBatch(sqltmp);
                        }
                    }

                }
                else if (fileName.equals("user.txt")){
                    String sql =
                            "LOAD DATA LOCAL INFILE '%s' INTO TABLE user " +
                                    "FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' " +
                                    "LINES TERMINATED BY '\\n' " +
                                    "(uid, name, age, occupation, ucid);";

                    sql = String.format(sql, folderAbsolute.replace('\\','/') + "/" + fileName);
                    stmt.addBatch(sql);
                }
                else if (fileName.equals("rent.txt")){

                    String sql =
                            "LOAD DATA LOCAL INFILE '%s' INTO TABLE rent " +
                                    "FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' " +
                                    "LINES TERMINATED BY '\\n' " +
                                    "(callnum, copynum, uid, @checkout, @return_date) ";
                    String sql_cont =
                            "SET checkout = STR_TO_DATE(@checkout, '%Y-%m-%d')," +
                                    "return_date = IF(STRCMP(@return_date,'NULL') = 0," +
                                    "NULL, STR_TO_DATE(@return_date, '%Y-%m-%d'));";

                    sql = String.format(sql, folderAbsolute.replace('\\','/') + "/" + fileName);
                    sql += sql_cont;

                    stmt.addBatch(sql);
                }
            }
            stmt.executeBatch();
            System.out.println("Done. Data is inputted to the database");

        } catch (NullPointerException e){
            System.out.println("[Error]: Unknown path.");
        }
        catch (Exception e){
            System.out.println("[Error]: " + e);
        }
        openAdminMenu();
    }

    public static void ShowRecord(){
        int count = 0;
        try {
            String[] query = {
                    "select count(*) from user_category;",
                    "select count(*) from user;",
                    "select count(*) from car_category;",
                    "select count(*) from car;",
                    "select count(*) from copy;",
                    "select count(*) from rent;",
                    "select count(*) from produce;"   } ;
            String[] tables = {"user_category", "user", "car_category", "car", "copy", "rent", "produce"};

            System.out.println("Number of records in each table: ");
            for (int i = 0; i < query.length ; i++){
                stmt = conn.createStatement();
                rs = stmt.executeQuery(query[i]);
                rs.next();
                System.out.println(tables[i] + ":" + rs.getInt(1));
            }

        }
        catch (SQLException ex){
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        openAdminMenu();
    }

    public static void BackMenu(){
        Start();
    }

    public static void openUserMenu(){
        System.out.println("----- Operations for user menu -----");
        System.out.println("What kind of operation would you like to perform?");
        System.out.println("1. Search for Cars");
        System.out.println("2. Show loan record of a user");
        System.out.println("3. Return to the main menu");
        System.out.print("Enter your choice: ");

        Scanner scanner = new Scanner(System.in);
        int choice = scanner.nextInt();

        switch (choice) {
            case 1:
                SearchCar();
                break;

            case 2:
                ShowLoanRecord();
                break;

            case 3:
                BackMenu();
                break;

            default:
                System.out.println("Input invalid");
                openUserMenu();
        }
    }

    public static void SearchCar(){
        System.out.println("Choose the search criterion: ");
        System.out.println("1. call number");
        System.out.println("2. name");
        System.out.println("3. company");
        System.out.print("Choose the search criterion: ");

        Scanner scanner = new Scanner(System.in);
        int choice = scanner.nextInt();

        // SQL---------------

        String sql =                                                                                    // count available copy by (number of all copy - unreturned number of copy)
                "SELECT DISTINCT C.callnum, C.name, CC.ccname, P.cname, result.fnal "+
                        "FROM "+
                        "car C, car_category CC, produce P, "+

                        "( SELECT CPN.callnum, ABS(CPN.ct - RCNN.cont) AS fnal "+
                        "FROM "+
                        "(SELECT callnum, IF(cnt IS NULL, 0, cnt) AS cont "+
                        "FROM "+
                        "(SELECT CP.callnum, RC.cnt "+
                        "FROM copy CP "+
                        "LEFT JOIN "+
                        "(SELECT callnum, COUNT(*) AS cnt "+
                        "FROM rent "+
                        "WHERE return_date IS NULL "+
                        "GROUP BY callnum)  AS RC "+
                        "ON CP.callnum = RC.callnum) AS tbl "+
                        ")  AS RCNN, "+
                        "(SELECT callnum, COUNT(*) AS ct "+
                        "FROM copy "+
                        "GROUP BY callnum)  AS CPN "+
                        "WHERE RCNN.callnum = CPN.callnum "+
                        ") AS result "+
                        "WHERE P.callnum = C.callnum AND C.ccid = CC.ccid AND result.callnum = P.callnum ";


        switch (choice) {
            case 1:
                sql = sql.concat("AND C.callnum = '%s' ");
                break;

            case 2:
                sql = sql.concat("AND C.name LIKE '%s%%' ");
                break;

            case 3:
                sql = sql.concat("AND P.cname LIKE '%s%%' ");
                break;

            default:
                System.out.println("Input invalid");
                SearchCar();
        }
        sql = sql.concat("ORDER BY C.callnum ;");

        System.out.print("Type in the Search Keyword: ");
        Scanner scanner2 = new Scanner(System.in);
        String input = scanner2.nextLine();

        sql = String.format(sql, input);
        //System.out.println("\n\n"+sql+"\n\n");        //debug use only

        try{
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
        }catch(SQLException e) {
            System.out.println("[Error]:" + e);
        }

        try {
            // output-------------------------
            if (!rs.isBeforeFirst()){
                System.out.println("no matching cars found");
            }
            else{
                System.out.println("|Callnum|Name|Car Category|Available no. of copy|");
                while (rs.next()){
                    System.out.println("|"+rs.getString("callnum")+"|"+rs.getString("name")+"|"+rs.getString("ccname")+"|"+rs.getString("cname")+"|"+rs.getInt(5)+"|");
                }
                System.out.println("End of Query");
            }
        }catch (SQLException e){
            System.out.println("[Error]:"+ e);
        }
        openUserMenu();
    }

    public static void ShowLoanRecord(){
        System.out.print("Enter The user ID: ");

        Scanner scanner = new Scanner(System.in);
        String uid = scanner.nextLine();

        // SQL--------------------
        String sql =
                "SELECT R.callnum, R.copynum, C.name, P.cname, R.checkout, IF(ISNULL(R.return_date),'No', 'Yes') AS Returned "+
                        "FROM rent R, produce P, car C "+
                        "WHERE C.callnum = P.callnum AND P.callnum = R.callnum AND "+
                        "R.uid = '%s' "+
                        "ORDER BY R.checkout DESC ;";

        sql = String.format(sql, uid);

        try{
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);

            if (!rs.isBeforeFirst()){
                System.out.println("No record found");
            }else{
                //------------output------------
                System.out.println("Loan Record:");
                System.out.println("|Callnum|Copynum|Name|Company|Check-out|Returned?|");
                while (rs.next()){
                    System.out.println("|"+rs.getString("callnum")+"|"+rs.getInt("copynum")+"|"+rs.getString("name")+"|"+rs.getString("cname")+"|"+rs.getDate("checkout")+"|"+rs.getString("Returned")+"|");
                }
                System.out.println("End of Query");
            }

        }catch (SQLException e){
            System.out.println("[Error]:" + e);
        }

        openUserMenu();
    }

    public static void openManagerMenu(){
        System.out.println("----- Operations for manager menu -----");
        System.out.println("What kind of operation would you like to perform?");
        System.out.println("1. Car Renting");
        System.out.println("2. Car Returning");
        System.out.println("3. List all un-returned car copies which are checked-out within a period");
        System.out.println("4. Return to the main menu");
        System.out.print("Enter your choice: ");

        Scanner scanner = new Scanner(System.in);
        int choice = scanner.nextInt();

        switch (choice) {
            case 1:
                CarRent();
                break;

            case 2:
                CarReturn();
                break;

            case 3:
                ListCar();
                break;

            case 4:
                BackMenu();
                break;

            default:
                System.out.println("Input invalid");
                openManagerMenu();
        }
    }

    public static void CarRent(){

        System.out.print("Enter The User ID: ");
        Scanner scanner = new Scanner(System.in);
        String uid = scanner.nextLine();
        System.out.print("Enter The Call Number: ");
        String callno = scanner.nextLine();
        System.out.print("Enter The Copy Number: ");
        int cpno = scanner.nextInt();

        long td = System.currentTimeMillis();
        java.sql.Date today = new java.sql.Date(td);
        int n = -1;
        // SQL here------------------------------
        // check availablity
        String sql =
                "SELECT COUNT(*) "+
                        "FROM rent R "+
                        "WHERE R.callnum = '%s' AND copynum = %d AND R.return_date IS NULL;";
        sql = String.format(sql, callno, cpno);

        try{
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
        }catch(SQLException e) {
            System.out.println("[Error]:" + e);
        }
        try {
            rs.next();
            n = rs.getInt(1);
        }catch (SQLException e){
            System.out.println("[Error]: "+ e);
        }


        // insert rent record if avaliable
        if (n==0){
            // check if user and car exists
            sql =
                    "SELECT * "+
                            "FROM user U, copy CP "+
                            "WHERE U.uid = '%s' AND CP.callnum = '%s' AND CP.copynum = %d ;";
            sql = String.format(sql, uid, callno, cpno);
            try{
                stmt = conn.createStatement();
                rs = stmt.executeQuery(sql);
                if (!rs.isBeforeFirst()){
                    System.out.println("No matching user or car copy found");
                    openManagerMenu();
                    return;
                }
            }catch(SQLException e){
                System.out.println("[Error]: " + e);
            }

            sql =
                    "INSERT "+
                            "INTO rent (uid, callnum, copynum, checkout, return_date) "+
                            "Values ('%s', '%s', %d, ?, NULL);";
            sql = String.format(sql, uid, callno, cpno);

            String sql2 = "UPDATE car C SET C.time_rent = C.time_rent + 1 WHERE C.callnum = '%s' ;";
            sql2 = String.format(sql2, callno);

            try{
                PreparedStatement s = conn.prepareStatement(sql);
                s.setDate(1, today);
                s.execute();
                stmt = conn.createStatement();
                stmt.execute(sql2);
                System.out.println("Car rented successfully");
            }catch(SQLException e) {
                System.out.println("[Error]:" + e);
            }
        }
        else if (n>0){
            System.out.println("Selected car has not been returned");
        }
        else{
            System.out.println("[Error] n not assigned");
        }
        openManagerMenu();
    }

    public static void CarReturn(){

        System.out.print("Enter The User ID: ");
        Scanner scanner = new Scanner(System.in);
        String uid = scanner.nextLine();
        System.out.print("Enter The Call Number: ");
        String callno = scanner.nextLine();
        System.out.print("Enter The Copy Number: ");
        int cpno = scanner.nextInt();

        long td = System.currentTimeMillis();
        java.sql.Date today = new java.sql.Date(td);
        // SQL --------------------
        // check if record exist
        String sql =
                "SELECT * "+
                        "FROM rent "+
                        "WHERE uid = '%s' AND callnum = '%s' AND copynum = %d AND return_date IS NULL;";
        sql = String.format(sql, uid, callno, cpno);
        try{
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            if (!rs.isBeforeFirst()){
                System.out.println("No unreturned record found");
                openManagerMenu();
                return;
            }
        }catch(SQLException e){
            System.out.println("[Error]: " + e);
        }

        sql =
                "UPDATE rent R "+
                        "SET R.return_date = ? "+
                        "WHERE uid = '%s' AND callnum = '%s' AND copynum = %d AND return_date IS NULL;";
        sql = String.format(sql, uid, callno, cpno);


        try{
            PreparedStatement s = conn.prepareStatement(sql);
            s.setDate(1,today);
            s.execute();
            System.out.println("Car returned successfully");
        }catch(SQLException e){
            System.out.println("[Error]:" + e);
        }

        openManagerMenu();
    }

    public static void ListCar(){

        System.out.print("Type in the starting date [dd/mm/yyyy]: ");
        Scanner scanner = new Scanner(System.in);
        String Sdate = scanner.nextLine();
        System.out.print("Type in the ending date [dd/mm/yyyy]: ");
        String Edate = scanner.nextLine();
        // reformat string to Date
        String d1[] = Sdate.split("/");
        String d2[] = Edate.split("/");
        String sd = d1[2]+"-"+d1[1]+"-"+d1[0];
        String ed = d2[2]+"-"+d2[1]+"-"+d2[0];
        java.sql.Date FromD = Date.valueOf(sd);
        java.sql.Date ToD = Date.valueOf(ed);


        // SQL here -------------------------------

        String sql =
                "SELECT * "+
                        "FROM rent R "+
                        "WHERE R.checkout >= ? AND R.checkout <= ? AND R.return_date IS NULL;";


        try{
            PreparedStatement s = conn.prepareStatement(sql);
            s.setDate(1, FromD);
            s.setDate(2, ToD);
            rs = s.executeQuery();
            if (rs.isBeforeFirst()){
                System.out.println("List of Unreturned Cars: ");
                // output-----------------------------------
                System.out.println("|UID|Callnum|Copynum|Checkout|");
                while (rs.next()){
                    System.out.println("|"+rs.getString("uid")+"|"+rs.getString("callnum")+"|"+rs.getInt("copynum")+"|"+rs.getDate("checkout")+"|");
                }
                System.out.println("End of Query");
            }else{
                System.out.println("No record found");
            }

        }catch(SQLException e){
            System.out.println("[Error]:" + e);
        }

        openManagerMenu();
    }
}