package simpledb.model.page;

import simpledb.model.*;
import simpledb.model.dbfile.HeapFile;
import simpledb.model.field.Field;
import simpledb.model.pageid.HeapPageId;
import simpledb.util.BufferPool;
import simpledb.exception.DbException;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++) {
            header[i] = dis.readByte();
        }
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++){
                tuples[i] = readNextTuple(dis,i);
            }
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     * @return the number of tuples on this page
     */
    private int getNumTuples() {        
        // BufferPool.getPageSize() * 8 => 每个HeapPage的字节数 * 8 => 每个HeapPage的字节数bit数（1Byte字节=8bit）
        // td.getSize() * 8 + 1 => 每一个tuple的字节数 * 8 + 1(每个tuple占HeapPage header位图的1bit位)
        // return返回值：HeapPage一页能容纳的tuple最大数量（slot插槽数）
        return (BufferPool.getPageSize() * 8) / (td.getSize() * 8 + 1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // page对应的slot插槽数决定了需要为header位图分配的内存大小
        // header位图是以Byte为单位分配的，因此需要分配的Byte数为插槽数/8，向上取整
        // 例如插槽数是9，那么必须要9/8=1.xxxx(向上取整)=2Byte共16bit的空间（1*8 < 9 < 2*8）来对应这9个插槽
        return (int)Math.ceil(getNumTuples()*1.0/8);
    }
    
    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    @Override
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    @Override
    public void setBeforeImage() {
        synchronized(oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    @Override
    public HeapPageId getId() {
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    @Override
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        assert t != null;
        RecordId recToDelete = t.getRecordId();
        if (recToDelete != null && pid.equals(recToDelete.pageId)) {
            for (int i = 0; i < numSlots; i++) {
                // 顺序扫描当前页的每一个插槽，进行recordId的匹配
                if (isSlotUsed(i) && t.getRecordId().equals(tuples[i].getRecordId())) {
                    // 匹配到了，将插槽标记为未使用
                    markSlotUsed(i, false);

                    // t.setRecordId(null);

                    // 对应的tuple设置为null
                    tuples[i] = null;
                    return;
                }
            }
            throw new DbException("deleteTuple: Error: tuple slot is empty");
        }
        throw new DbException("deleteTuple: Error: tuple is not on this page");
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        assert t != null;
        if (td.equals(t.getTupleDesc())) {
            for (int i = 0; i < numSlots; i++) {
                // 遍历当前HeapPage的每一个插槽
                if (!isSlotUsed(i)) {
                    // 发现一个未使用的空插槽
                    // 对应插槽标记为已使用
                    markSlotUsed(i, true);
                    // 设置唯一的recordID （RecordID由pageID + page内插槽号组成）
                    t.setRecordId(new RecordId(pid, i));
                    tuples[i] = t;
                    return;
                }
            }
            throw new DbException("insertTuple: ERROR: no tuple is inserted");
        }
        throw new DbException("insertTuple: no empty slots or tupledesc is mismatch");
    }

    private boolean dirty;
    private TransactionId dirtyby;
    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    @Override
    public void markDirty(boolean dirty, TransactionId tid) {
        if (dirty) {
            // 标记为脏页
            this.dirty = dirty;
            this.dirtyby = tid;
        } else {
            // 标记为非脏页
            this.dirty = false;
            this.dirtyby = null;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    @Override
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        return dirtyby;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int numEmptSlot = 0;
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                numEmptSlot += 1;
            }
        }
        return numEmptSlot;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        // big endian most significant in lower address
        if (i < numSlots) {
            int hdNo = i / 8;
            int offset = i % 8;
            return (header[hdNo] & (0x1 << offset)) != 0;
        }
        return false;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        if (i < numSlots) {
            // 计算字节数组下标
            int hdNo = i / 8;
            // 计算对应字节中的bit位偏移
            int offset = i % 8;

            byte mask = (byte) (0x1 << offset);
            if (value) {
                // 标记为已使用
                header[hdNo] |= mask;
            } else {
                // 标记为未使用
                header[hdNo] &= ~mask;
            }
        }
    }

    protected class HeapPageTupleIterator implements Iterator<Tuple> {
        private final Iterator<Tuple> iter;
        public HeapPageTupleIterator() {
            ArrayList<Tuple> tupleArrayList = new ArrayList<>(numSlots);
            for (int i = 0; i < numSlots; i++) {
                if (isSlotUsed(i)) {
                    tupleArrayList.add(i, tuples[i]);
                }
            }
            iter = tupleArrayList.iterator();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("TupleIterator: remove not supported");
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Tuple next() {
            return iter.next();
        }
    }
    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new HeapPageTupleIterator();
    }

}

