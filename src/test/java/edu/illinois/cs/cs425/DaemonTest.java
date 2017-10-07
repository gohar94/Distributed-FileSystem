package edu.illinois.cs.cs425;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for MP2.
 */
public class DaemonTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public DaemonTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(DaemonTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() {
        assertTrue(true);
    }
}
