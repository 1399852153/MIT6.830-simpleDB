package simpledb;

import junit.framework.JUnit4TestAdapter;
import org.junit.Before;
import org.junit.Test;
import simpledb.TestUtil.SkeletonFile;
import simpledb.model.*;
import simpledb.model.dbfile.HeapFileEncoder;
import simpledb.model.field.IntField;
import simpledb.model.page.HeapPage;
import simpledb.model.pageid.HeapPageId;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;
import simpledb.util.BufferPool;
import simpledb.util.Utility;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.*;

public class HeapPageReadTest extends SimpleDbTestBase {
    private HeapPageId pid;

    public static final int[][] EXAMPLE_VALUES = new int[][] {
        { 31933, 862 },
        { 29402, 56883 },
        { 1468, 5825 },
        { 17876, 52278 },
        { 6350, 36090 },
        { 34784, 43771 },
        { 28617, 56874 },
        { 19209, 23253 },
        { 56462, 24979 },
        { 51440, 56685 },
        { 3596, 62307 },
        { 45569, 2719 },
        { 22064, 43575 },
        { 42812, 44947 },
        { 22189, 19724 },
        { 33549, 36554 },
        { 9086, 53184 },
        { 42878, 33394 },
        { 62778, 21122 },
        { 17197, 16388 }
    };

    public static final byte[] EXAMPLE_DATA;
    static {
        // Build the input table
        ArrayList<ArrayList<Integer>> table = new ArrayList<ArrayList<Integer>>();
        for (int[] tuple : EXAMPLE_VALUES) {
            ArrayList<Integer> listTuple = new ArrayList<Integer>();
            for (int value : tuple) {
                listTuple.add(value);
            }
            table.add(listTuple);
        }

        // Convert it to a HeapFile and read in the bytes
        try {
            File temp = File.createTempFile("table", ".dat");
            temp.deleteOnExit();
            HeapFileEncoder.convert(table, temp, BufferPool.getPageSize(), 2);
            EXAMPLE_DATA = TestUtil.readFileBytes(temp.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Set up initial resources for each unit test.
     */
    @Before public void addTable() throws Exception {
        this.pid = new HeapPageId(-1, -1);
        Database.getCatalog().addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), SystemTestUtil.getUUID());
    }

    /**
     * Unit test for HeapPage.getId()
     */
    @Test public void getId() throws Exception {
        HeapPage page = new HeapPage(pid, EXAMPLE_DATA);
        assertEquals(pid, page.getId());
    }

    /**
     * Unit test for HeapPage.iterator()
     */
    @Test public void testIterator() throws Exception {
        HeapPage page = new HeapPage(pid, EXAMPLE_DATA);
        Iterator<Tuple> it = page.iterator();

        int row = 0;
        while (it.hasNext()) {
            Tuple tup = it.next();
            IntField f0 = (IntField) tup.getField(0);
            IntField f1 = (IntField) tup.getField(1);

            assertEquals(EXAMPLE_VALUES[row][0], f0.getValue());
            assertEquals(EXAMPLE_VALUES[row][1], f1.getValue());
            row++;
        }
    }

    /**
     * Unit test for HeapPage.getNumEmptySlots()
     */
    @Test public void getNumEmptySlots() throws Exception {
        HeapPage page = new HeapPage(pid, EXAMPLE_DATA);
        assertEquals(484, page.getNumEmptySlots());
    }

    /**
     * Unit test for HeapPage.isSlotUsed()
     */
    @Test public void getSlot() throws Exception {
        HeapPage page = new HeapPage(pid, EXAMPLE_DATA);

        for (int i = 0; i < 20; ++i)
            assertTrue(page.isSlotUsed(i));

        for (int i = 20; i < 504; ++i)
            assertFalse(page.isSlotUsed(i));
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapPageReadTest.class);
    }
}
