package com.abm.pers_finance;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

public class TestCliQueryManager {

    private String username = "postgres";
    private String password= "docker";


    @Before
    public void initialize(){
        Connection conn = null;
        conn = DBConnectionManager.getDBConnection(username, password);

        try {
            Statement s = conn.createStatement();
            String s1 = "delete from  "+
                    "backup_acct_code_to_acct_description; ";
            String s2 = "delete from  "+
                    "backup_descrip_to_acct_code; ";
            String s3= "insert into backup_acct_code_to_acct_description "+
                    "select * from acct_code_to_acct_description ";
            String s4= "insert into backup_descrip_to_acct_code "+
                    "select * from  descrip_to_acct_code";
            String s5 = "delete from  "+
                    "acct_code_to_acct_description; ";
            String s6 = "delete from  "+
                    "descrip_to_acct_code; ";
            s.addBatch(s1);
            s.addBatch(s2);
            s.addBatch(s3);
            s.addBatch(s4);
            s.addBatch(s5);
            s.addBatch(s6);
            s.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    @After
    public void tidyup(){
        Connection conn = null;
        conn = DBConnectionManager.getDBConnection(username, password);

        try {
            Statement s = conn.createStatement();
            String s1 = "delete from  "+
                    "acct_code_to_acct_description; ";
            String s2 = "delete from  "+
                    "descrip_to_acct_code; ";
            String s3= "insert into acct_code_to_acct_description "+
                    "select * from backup_acct_code_to_acct_description ";
            String s4= "insert into descrip_to_acct_code "+
                    "select * from  backup_descrip_to_acct_code";
            s.addBatch(s1);
            s.addBatch(s2);
            s.addBatch(s3);
            s.addBatch(s4);
            s.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
    @Test
    public void testAddAcctLabel() {

        boolean acctExistsBefore= CliQueryManager.doesAccCodeExist(username, password, "hi");
        assertFalse(acctExistsBefore);
        CliQueryManager.createAcctCode(username,
                password, "hi",
                "Health insurance.");
        boolean acctExistsAfter= CliQueryManager.doesAccCodeExist(username, password, "hi");
        assertTrue(acctExistsAfter);



    }

    @Test
    public void testAddDescripLabel() {
        boolean descriptionExistsBefore= CliQueryManager.doesTranDescripExist(username,
                password, "random transaction description");
        assertFalse(descriptionExistsBefore);
        CliQueryManager.mapDescription(username, password, "random transaction description", "hi");
        boolean descriptionExistsAfter= CliQueryManager.doesTranDescripExist(username,
                password, "random transaction description");
        assertFalse(descriptionExistsAfter);
        CliQueryManager.createAcctCode(username,
                password, "hi",
                "Health insurance.");
        CliQueryManager.mapDescription(username, password, "random transaction description", "hi");
        boolean descriptionExistsAfter2= CliQueryManager.doesTranDescripExist(username,
                password, "random transaction description");
        assertTrue(descriptionExistsAfter2);
    }
}
