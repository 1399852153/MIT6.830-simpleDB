package simpledb.model.dbfile;

import simpledb.exception.DbException;
import simpledb.exception.TransactionAbortedException;
import simpledb.model.*;
import simpledb.model.iterator.dbfile.DbFileIterator;
import simpledb.model.page.HeapPage;
import simpledb.model.page.Page;
import simpledb.model.pageid.HeapPageId;
import simpledb.model.pageid.PageId;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File dbFile;
    private final TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.dbFile = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return dbFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // generate unique tableid
        return dbFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    @Override
    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableid = pid.getTableId();
        int pgNo = pid.pageNumber();
        final int pageSize = Database.getBufferPool().getPageSize();
        byte[] rawPgData = HeapPage.createEmptyPageData();

        // random access read from disk
        FileInputStream in = null;
        try {
            in = new FileInputStream(dbFile);
            try {
                in.skip(pgNo * pageSize);
                in.read(rawPgData);
                return new HeapPage(new HeapPageId(tableid, pgNo), rawPgData);
            } catch (IOException e) {
                throw new IllegalArgumentException("HeapFile: readPage:");
            } finally {
                in.close();
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("HeapFile: readPage: file not found");
        }
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        int tableid = pid.getTableId();
        int pgNo = pid.pageNumber();

        final int pageSize = Database.getBufferPool().getPageSize();
        byte[] pgData = page.getPageData();

        RandomAccessFile dbfile = new RandomAccessFile(dbFile, "rws");
        dbfile.skipBytes(pgNo * pageSize);
        dbfile.write(pgData);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int fileSizeinByte = (int) dbFile.length();
        return fileSizeinByte / Database.getBufferPool().getPageSize();
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> affected = new ArrayList<>(1);
        int numPages = numPages();

        for (int pgNo = 0; pgNo <= numPages; pgNo++) {
            HeapPageId pid = new HeapPageId(getId(), pgNo);
            HeapPage pg;
            if (pgNo < numPages) {
                // 从已存在的页中尝试着找到一个空闲插槽
                pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            } else {
                // pgNo = numpages -> we need add new page

                // 已经遍历了所有已存在的页，没有找到空闲的插槽可用，必须创建一个新的页来承载新插入的Tuple
                pg = new HeapPage(pid, HeapPage.createEmptyPageData());
            }

            // 依然存在空插槽
            if (pg.getNumEmptySlots() > 0) {
                // insert will update tuple when inserted
                pg.insertTuple(t);
                // writePage(pg);
                if (pgNo < numPages) {
                    // 被影响的页
                    pg.markDirty(true, tid);
                    affected.add(pg);
                } else {
                    // should append the dbfile
                    // 当前HeapFile写入一个新的页
                    writePage(pg);
                }
                return affected;
            }

            // 换下一页继续查询空插槽
        }
        // otherwise create new page and insert
        throw new DbException("HeapFile: InsertTuple: Tuple can not be added");
    }

    // see DbFile.java for javadocs
    @Override
    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> affected = new ArrayList<>(1);
        RecordId rid = t.getRecordId();
        HeapPageId pid = (HeapPageId) rid.getPageId();
        if (pid.getTableId() == getId()) {
            // int pgNo = pid.pageNumber();
            HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            pg.deleteTuple(t);
            // writePage(pg);
            pg.markDirty(true, tid);
            affected.add(pg);
            return affected;
        }
        throw new DbException("HeapFile: deleteTuple: tuple.tableid != getId");
    }

    private class HeapFileIterator implements DbFileIterator {

        private Integer pgCursor;
        private Iterator<Tuple> tupleIter;
        private final TransactionId transactionId;
        private final int tableId;
        private final int numPages;

        public HeapFileIterator(TransactionId tid) {
            this.pgCursor = null;
            this.tupleIter = null;
            this.transactionId = tid;
            this.tableId = getId();
            this.numPages = numPages();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pgCursor = 0;
            tupleIter = getTupleIter(pgCursor);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // < numpage - 1
            if (pgCursor != null) {
                while (pgCursor < numPages - 1) {
                    if (tupleIter.hasNext()) {
                        return true;
                    } else {
                        pgCursor += 1;
                        tupleIter = getTupleIter(pgCursor);
                    }
                }
                return tupleIter.hasNext();
            } else {
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext())  {
                return tupleIter.next();
            }
            throw new NoSuchElementException("HeapFileIterator: error: next: no more elemens");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            pgCursor = null;
            tupleIter = null;
        }

        private Iterator<Tuple> getTupleIter(int pgNo)
                throws TransactionAbortedException, DbException {
            PageId pid = new HeapPageId(tableId, pgNo);
            return ((HeapPage)
                    Database
                            .getBufferPool()
                            .getPage(transactionId, pid, Permissions.READ_ONLY))
                    .iterator();
        }
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

