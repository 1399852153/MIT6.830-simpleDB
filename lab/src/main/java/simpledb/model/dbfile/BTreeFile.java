package simpledb.model.dbfile;

import simpledb.btree.BTreeEntry;
import simpledb.enums.Predicate;
import simpledb.exception.DbException;
import simpledb.exception.TransactionAbortedException;
import simpledb.model.*;
import simpledb.model.field.Field;
import simpledb.model.iterator.dbfile.AbstractDbFileIterator;
import simpledb.model.iterator.dbfile.DbFileIterator;
import simpledb.model.page.*;
import simpledb.log.Debug;
import simpledb.model.pageid.BTreePageId;
import simpledb.model.pageid.PageId;
import simpledb.util.BufferPool;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * BTreeFile is an implementation of a DbFile that stores a B+ tree.
 * Specifically, it stores a pointer to a root page,
 * a set of internal pages, and a set of leaf pages, which contain a collection of tuples
 * in sorted order. BTreeFile works closely with BTreeLeafPage, BTreeInternalPage,
 * and BTreeRootPtrPage. The format of these pages is described in their constructors.
 * 
 * @see BTreeLeafPage#BTreeLeafPage
 * @see BTreeInternalPage#BTreeInternalPage
 * @see BTreeHeaderPage#BTreeHeaderPage
 * @see BTreeRootPtrPage#BTreeRootPtrPage
 * @author Becca Taft
 */
public class BTreeFile implements DbFile {

	private final File f;
	private final TupleDesc td;
	private final int tableid ;
	private int keyField;

	/**
	 * Constructs a B+ tree file backed by the specified file.
	 * 
	 * @param f - the file that stores the on-disk backing store for this B+ tree
	 *            file.
	 * @param key - the field which index is keyed on
	 * @param td - the tuple descriptor of tuples in the file
	 */
	public BTreeFile(File f, int key, TupleDesc td) {
		this.f = f;
		this.tableid = f.getAbsoluteFile().hashCode();
		this.keyField = key;
		this.td = td;
	}

	/**
	 * Returns the File backing this BTreeFile on disk.
	 */
	public File getFile() {
		return f;
	}

	/**
	 * Returns an ID uniquely identifying this BTreeFile. Implementation note:
	 * you will need to generate this tableid somewhere and ensure that each
	 * BTreeFile has a "unique id," and that you always return the same value for
	 * a particular BTreeFile. We suggest hashing the absolute file name of the
	 * file underlying the BTreeFile, i.e. f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this BTreeFile.
	 */
	public int getId() {
		return tableid;
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return td;
	}

	/**
	 * Read a page from the file on disk. This should not be called directly
	 * but should be called from the BufferPool via getPage()
	 * 
	 * @param pid - the id of the page to read from disk
	 * @return the page constructed from the contents on disk
	 */
	public Page readPage(PageId pid) {
		BTreePageId id = (BTreePageId) pid;
		BufferedInputStream bis = null;

		try {
			bis = new BufferedInputStream(new FileInputStream(f));
			if(id.pgcateg() == BTreePageId.ROOT_PTR) {
				// 根节点
				byte pageBuf[] = new byte[BTreeRootPtrPage.getPageSize()];
				// 从文件的头部开始读(偏移为0)
				int retval = bis.read(pageBuf, 0, BTreeRootPtrPage.getPageSize());
				if (retval == -1) {
					throw new IllegalArgumentException("Read past end of table");
				}
				if (retval < BTreeRootPtrPage.getPageSize()) {
					throw new IllegalArgumentException("Unable to read "
							+ BTreeRootPtrPage.getPageSize() + " bytes from BTreeFile");
				}
				Debug.log(1, "BTreeFile.readPage: read page %d", id.pageNumber());
				BTreeRootPtrPage p = new BTreeRootPtrPage(id, pageBuf);
				return p;
			}
			else {
				// 非根节点

				byte pageBuf[] = new byte[BufferPool.getPageSize()];
				// 根据缓冲区页大小 * 页号 计算偏移
				if (bis.skip(BTreeRootPtrPage.getPageSize() + (long) (id.pageNumber() - 1) * BufferPool.getPageSize()) !=
						BTreeRootPtrPage.getPageSize() + (long) (id.pageNumber() - 1) * BufferPool.getPageSize()) {
					throw new IllegalArgumentException(
							"Unable to seek to correct place in BTreeFile");
				}
				int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
				if (retval == -1) {
					throw new IllegalArgumentException("Read past end of table");
				}
				if (retval < BufferPool.getPageSize()) {
					throw new IllegalArgumentException("Unable to read "
							+ BufferPool.getPageSize() + " bytes from BTreeFile");
				}
				Debug.log(1, "BTreeFile.readPage: read page %d", id.pageNumber());
				if(id.pgcateg() == BTreePageId.INTERNAL) {
					// 内部节点
					BTreeInternalPage p = new BTreeInternalPage(id, pageBuf, keyField);
					return p;
				}
				else if(id.pgcateg() == BTreePageId.LEAF) {
					// 叶子节点
					BTreeLeafPage p = new BTreeLeafPage(id, pageBuf, keyField);
					return p;
				}
				else { // id.pgcateg() == BTreePageId.HEADER
					// 头节点
					BTreeHeaderPage p = new BTreeHeaderPage(id, pageBuf);
					return p;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			// Close the file on success or error
			try {
				if (bis != null)
					bis.close();
			} catch (IOException ioe) {
				// Ignore failures closing the file
			}
		}
	}

	/**
	 * Write a page to disk.  This should not be called directly but should 
	 * be called from the BufferPool when pages are flushed to disk
	 * 
	 * @param page - the page to write to disk
	 */
	public void writePage(Page page) throws IOException {
		BTreePageId id = (BTreePageId) page.getId();
		
		byte[] data = page.getPageData();
		RandomAccessFile rf = new RandomAccessFile(f, "rw");
		if(id.pgcateg() == BTreePageId.ROOT_PTR) {
			rf.write(data);
			rf.close();
		}
		else {
			// 页下标是从1开始的 需要seek跳转到BTreeRootPtrPage之后的第N页
			rf.seek(BTreeRootPtrPage.getPageSize() + (long) (page.getId().pageNumber() - 1) * BufferPool.getPageSize());
			rf.write(data);
			rf.close();
		}
	}
	
	/**
	 * Returns the number of pages in this BTreeFile.
	 */
	public int numPages() {
		// we only ever write full pages
		return (int) ((f.length() - BTreeRootPtrPage.getPageSize())/ BufferPool.getPageSize());
	}

	/**
	 * Returns the index of the field that this B+ tree is keyed on
	 */
	public int keyField() {
		return keyField;
	}

	/**
	 * Recursive function which finds and locks the leaf page in the B+ tree corresponding to
	 * the left-most page possibly containing the key field f. It locks all internal
	 * nodes along the path to the leaf node with READ_ONLY permission, and locks the 
	 * leaf node with permission perm.
	 * 
	 * If f is null, it finds the left-most leaf page -- used for the iterator
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - the current page being searched
	 * @param perm - the permissions with which to lock the leaf page
	 * @param f - the field to search for
	 * @return the left-most leaf page possibly containing the key field f
	 * 
	 */
	private BTreeLeafPage findLeafPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePageId pid, Permissions perm,
			Field f) throws DbException, TransactionAbortedException {
		// some code goes here
        if (pid.pgcateg() == BTreePageId.LEAF) {
        	// 查找到了最终的叶子节点类型的页
            return (BTreeLeafPage) this.getPage(tid, dirtypages, pid, perm);
        } else {
            // internal and page can not be empty

			// B+树位于中间的内部页
            BTreePageId nextSearchId;
            BTreeInternalPage searchPg = (BTreeInternalPage) this.getPage(tid, dirtypages, pid, Permissions.READ_ONLY);

            BTreeEntry entry;
            Iterator<BTreeEntry> it = searchPg.iterator();
            if (it.hasNext()) {
                entry = it.next();
            } else {
                throw new DbException("findLeafPage: InternalPage must contain at least one data");
            }

            if (f == null) {
            	// If f is null, it finds the left-most leaf page -- used for the iterator
                nextSearchId = entry.getLeftChild();
            } else {
            	// f不为null，是有针对性的查找对应的leafPage
                while (f.compare(Predicate.Op.GREATER_THAN, entry.getKey()) && it.hasNext()) {
                	// 在当前页中迭代，尝试查找到一个比f更大的项
                    entry = it.next();
                }
                // 跳出循环，说明找到了一个比f更大的项或者到了最右边

                if (f.compare(Predicate.Op.LESS_THAN_OR_EQ, entry.getKey())) {
                	// f<=找到的entry，去左孩子节点页寻找
                    nextSearchId = entry.getLeftChild();
                } else {
                	// greater than the last one
					// f比当前页中最后的一项更大，去右孩子节点寻找
                    nextSearchId = entry.getRightChild();
                }
            }

            // 递归查找B+树更下一层的页(nextSearchId)
            return findLeafPage(tid, dirtypages, nextSearchId, perm, f);
        }
	}
	
	/**
	 * Convenience method to find a leaf page when there is no dirtypages HashMap.
	 * Used by the BTreeFile iterator.
	 * @see #findLeafPage(TransactionId, HashMap, BTreePageId, Permissions, Field)
	 * 
	 * @param tid - the transaction id
	 * @param pid - the current page being searched
	 * @param perm - the permissions with which to lock the leaf page
	 * @param f - the field to search for
	 * @return the left-most leaf page possibly containing the key field f
	 * 
	 */
	BTreeLeafPage findLeafPage(TransactionId tid, BTreePageId pid, Permissions perm, Field f)
			throws DbException, TransactionAbortedException {
		return findLeafPage(tid, new HashMap<>(), pid, perm, f);
	}

	/**
	 * Split a leaf page to make room for new tuples and recursively split the parent node
	 * as needed to accommodate a new entry. The new entry should have a key matching the key field
	 * of the first tuple in the right-hand page (the key is "copied up"), and child pointers 
	 * pointing to the two leaf pages resulting from the split.  Update sibling pointers and parent 
	 * pointers as needed.  
	 * 
	 * Return the leaf page into which a new tuple with key field "field" should be inserted.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the leaf page to split
	 * @param field - the key field of the tuple to be inserted after the split is complete. Necessary to know
	 * which of the two pages to return.
	 * @see #getParentWithEmptySlots(TransactionId, HashMap, BTreePageId, Field)
	 * 
	 * @return the leaf page into which the new tuple should be inserted
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public BTreeLeafPage splitLeafPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeLeafPage page, Field field)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
        // YOUR CODE HERE:
        // Split the leaf page by adding a new page on the right of the existing
		// page and moving half of the tuples to the new page.  Copy the middle key up
		// into the parent page, and recursively split the parent as needed to accommodate
		// the new entry.  getParentWithEmtpySlots() will be useful here.  Don't forget to update
		// the sibling pointers of all the affected leaf pages.  Return the page into which a 
		// tuple with the given key field should be inserted.

        BTreeLeafPage newRightSib = (BTreeLeafPage) getEmptyPage(tid, dirtypages, BTreePageId.LEAF);
        // copy half tuples
        Iterator<Tuple> it = page.reverseIterator();
        Tuple[] tupleToMove = new Tuple[(page.getNumTuples()+1) / 2];
        int moveCnt = tupleToMove.length - 1;
        while (moveCnt >= 0 && it.hasNext()) {
            tupleToMove[moveCnt--] = it.next();
        }

        for (int i = tupleToMove.length-1; i >= 0; --i) {
            // t.getRecordId().getPageId().pageNumber();
            page.deleteTuple(tupleToMove[i]);
            newRightSib.insertTuple(tupleToMove[i]);
        }

        // assert newRightSib.getNumTuples() >= 1;
        Field midkey = tupleToMove[0].getField(keyField);
        BTreeInternalPage parent = getParentWithEmptySlots(tid, dirtypages, page.getParentId(), midkey);

        BTreePageId oldRightSibId = page.getRightSiblingId();

        newRightSib.setRightSiblingId(oldRightSibId);
        newRightSib.setLeftSiblingId(page.getId());
        page.setRightSiblingId(newRightSib.getId());

        // set up parent
        newRightSib.setParentId(parent.getId());
        page.setParentId(parent.getId());

        // Leaf -> internal, copy to parent
        BTreeEntry newParentEntry = new BTreeEntry(midkey, page.getId(), newRightSib.getId());
        parent.insertEntry(newParentEntry);
        // set dirtypages and old sibs
        if (oldRightSibId != null) {
            BTreeLeafPage oldRightSib = (BTreeLeafPage) getPage(tid, dirtypages, page.getRightSiblingId(), Permissions.READ_WRITE);
            oldRightSib.setLeftSiblingId(newRightSib.getId());
            dirtypages.put(oldRightSib.getId(), oldRightSib);
        }

        dirtypages.put(parent.getId(), parent);
        dirtypages.put(page.getId(), page);
        dirtypages.put(newRightSib.getId(), newRightSib);

        if (field.compare(Predicate.Op.GREATER_THAN, midkey)) {
            return newRightSib;
        } else {
            return page;
        }
/*

        final int numEntries = page.getNumTuples();
        final int midKeyNum = numEntries / 2;
        BTreeLeafPage rightSib = (BTreeLeafPage) this.getEmptyPage(tid, dirtypages, BTreePageId.LEAF);

        int copyCnt = numEntries - midKeyNum;
        Iterator<Tuple> it = page.reverseIterator();
        Tuple[] toMove = new Tuple[copyCnt];
        Field midKey = null;

        while (copyCnt > 0 && it.hasNext()) {
            toMove[--copyCnt] = it.next();
        }
        midKey = toMove[0].getField(keyField);
        // delete and insert
        for (int i = toMove.length - 1; i >= 0; i--) {
            page.deleteTuple(toMove[i]);
            rightSib.insertTuple(toMove[i]);
        }

        // copy up midkey
        BTreeInternalPage parent = getParentWithEmptySlots(tid, dirtypages, page.getParentId(), midKey);
        parent.insertEntry(new BTreeEntry(midKey, page.getId(), rightSib.getId()));
        // rightSib.setParentId(page.getParentId());

        // updatea parent ptr and sib ptrs
        BTreePageId origRightSibId = page.getRightSiblingId();

        page.setParentId(parent.getParentId());
        rightSib.setParentId(parent.getParentId());

        rightSib.setLeftSiblingId(page.getId());
        rightSib.setRightSiblingId(origRightSibId);
        page.setRightSiblingId(rightSib.getId());

        if (origRightSibId != null) {
            BTreeLeafPage origRightsib =(BTreeLeafPage) getPage(tid, dirtypages, origRightSibId, Permissions.READ_WRITE);
            origRightsib.setLeftSiblingId(rightSib.getId());
            dirtypages.put(origRightSibId, origRightsib);
        }
        dirtypages.put(page.getId(), page);
        dirtypages.put(rightSib.getId(), rightSib);
        dirtypages.put(parent.getId(), parent);

        if (field.compare(Op.GREATER_THAN, midKey)) {
            return rightSib;
        } else {
            return page;
        }
*/
	}
	
	/**
	 * Split an internal page to make room for new entries and recursively split its parent page
	 * as needed to accommodate a new entry. The new entry for the parent should have a key matching 
	 * the middle key in the original internal page being split (this key is "pushed up" to the parent). 
	 * The child pointers of the new parent entry should point to the two internal pages resulting 
	 * from the split. Update parent pointers as needed.
	 * 
	 * Return the internal page into which an entry with key field "field" should be inserted
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page to split
	 * @param field - the key field of the entry to be inserted after the split is complete. Necessary to know
	 * which of the two pages to return.
	 * @see #getParentWithEmptySlots(TransactionId, HashMap, BTreePageId, Field)
	 * @see #updateParentPointers(TransactionId, HashMap, BTreeInternalPage)
	 * 
	 * @return the internal page into which the new entry should be inserted
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public BTreeInternalPage splitInternalPage(TransactionId tid, HashMap<PageId, Page> dirtypages,
											   BTreeInternalPage page, Field field)
					throws DbException, IOException, TransactionAbortedException {
		// some code goes here
        // YOUR CODE HERE:
        // Split the internal page by adding a new page on the right of the existing
		// page and moving half of the entries to the new page.  Push the middle key up
		// into the parent page, and recursively split the parent as needed to accommodate
		// the new entry.  getParentWithEmtpySlots() will be useful here.  Don't forget to update
		// the parent pointers of all the children moving to the new page.  updateParentPointers()
		// will be useful here.  Return the page into which an entry with the given key field
		// should be inserted.

        BTreeInternalPage newInternalPg = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);

        Iterator<BTreeEntry> it = page.reverseIterator();
        BTreeEntry[] entryToMove = new BTreeEntry[(page.getNumEntries()+1) / 2];
        int moveCnt = entryToMove.length - 1;
        BTreeEntry midKey = null;

        while (moveCnt >= 0 && it.hasNext()) {
            entryToMove[moveCnt--] = it.next();
        }

        for (int i = entryToMove.length-1; i >= 0; --i) {
            if (i == 0) {
                page.deleteKeyAndRightChild(entryToMove[i]);
                midKey = entryToMove[0];
            } else {
                // order matters
                page.deleteKeyAndRightChild(entryToMove[i]);
                newInternalPg.insertEntry(entryToMove[i]);
            }
            updateParentPointer(tid, dirtypages, newInternalPg.getId(), entryToMove[i].getRightChild());
        }
        // update child
        midKey.setLeftChild(page.getId());
        midKey.setRightChild(newInternalPg.getId());

        BTreeInternalPage parent = getParentWithEmptySlots(tid, dirtypages, page.getParentId(), midKey.getKey());
        parent.insertEntry(midKey);

        // update midkey
        page.setParentId(parent.getId());
        newInternalPg.setParentId(parent.getId());

        dirtypages.put(page.getId(), page);
        dirtypages.put(newInternalPg.getId(), newInternalPg);
        dirtypages.put(parent.getId(), parent);

        if (field.compare(Predicate.Op.GREATER_THAN, midKey.getKey())) {
            return newInternalPg;
        } else {
            return page;
        }


/*
        final int numEntries = page.getNumEntries();
        final int midKeyNum = numEntries / 2;
        BTreeInternalPage rightSib = (BTreeInternalPage) this.getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);
        // push up mid key
        int copyCnt = numEntries - midKeyNum - 1;

        Iterator<BTreeEntry> it = page.reverseIterator();
        BTreeEntry[] entryToMove = new BTreeEntry[copyCnt];

        while (copyCnt > 0 && it.hasNext()) {
            entryToMove[--copyCnt] = it.next();
        }
        BTreeEntry midKey;
        if (it.hasNext()) {
            midKey = it.next();
        } else {
            throw new DbException("splitInternalPage: middle key should not be empty");
        }
        for (int i = entryToMove.length - 1; i >= 0; i--) {
            page.deleteKeyAndRightChild(entryToMove[i]);
            rightSib.insertEntry(entryToMove[i]);
        }
        page.deleteKeyAndRightChild(midKey);

        // push up midkey
        BTreeInternalPage parent = getParentWithEmptySlots(tid, dirtypages, page.getParentId(), midKey.getKey());
        parent.insertEntry(new BTreeEntry(midKey.getKey(), page.getId(), rightSib.getId()));

        page.setParentId(parent.getId());
        rightSib.setParentId(parent.getId());
        updateParentPointers(tid, dirtypages, rightSib);

        dirtypages.put(page.getId(), page);
        dirtypages.put(rightSib.getId(), rightSib);
        dirtypages.put(parent.getId(), parent);

        if (field.compare(Op.GREATER_THAN, midKey.getKey())) {
            return rightSib;
        } else {
            return page;
        }

*/
	}
	
	/**
	 * Method to encapsulate the process of getting a parent page ready to accept new entries.
	 * This may mean creating a page to become the new root of the tree, splitting the existing 
	 * parent page if there are no empty slots, or simply locking and returning the existing parent page.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param parentId - the id of the parent. May be an internal page or the RootPtr page
	 * @param field - the key of the entry which will be inserted. Needed in case the parent must be split
	 * to accommodate the new entry
	 * @return the parent page, guaranteed to have at least one empty slot
	 * @see #splitInternalPage(TransactionId, HashMap, BTreeInternalPage, Field)
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private BTreeInternalPage getParentWithEmptySlots(TransactionId tid, HashMap<PageId, Page> dirtypages, 
			BTreePageId parentId, Field field) throws DbException, IOException, TransactionAbortedException {
		
		BTreeInternalPage parent;
		
		// create a parent node if necessary
		// this will be the new root of the tree
		if(parentId.pgcateg() == BTreePageId.ROOT_PTR) {
			parent = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);
			
			// update the root pointer
			BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, 
					BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
			rootPtr.setRootId(parent.getId());		
		}
		else { 
			// lock the parent page
			parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId, 
					Permissions.READ_WRITE);
		}

		// split the parent if needed
		if(parent.getNumEmptySlots() == 0) {
			parent = splitInternalPage(tid, dirtypages, parent, field);
		}

		return parent;

	}

	/**
	 * Helper function to update the parent pointer of a node.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - id of the parent node
	 * @param child - id of the child node to be updated with the parent pointer
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void updateParentPointer(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePageId pid, BTreePageId child)
			throws DbException, IOException, TransactionAbortedException {

		BTreePage p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_ONLY);

		if(!p.getParentId().equals(pid)) {
			p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_WRITE);
			p.setParentId(pid);
		}

	}
	
	/**
	 * Update the parent pointer of every child of the given page so that it correctly points to
	 * the parent
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the parent page
	 * @see #updateParentPointer(TransactionId, HashMap, BTreePageId, BTreePageId)
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void updateParentPointers(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeInternalPage page) 
			throws DbException, IOException, TransactionAbortedException{
		Iterator<BTreeEntry> it = page.iterator();
		BTreePageId pid = page.getId();
		BTreeEntry e = null;
		while(it.hasNext()) {
			e = it.next();
			updateParentPointer(tid, dirtypages, pid, e.getLeftChild());
		}
		if(e != null) {
			updateParentPointer(tid, dirtypages, pid, e.getRightChild());
		}
	}
	
	/**
	 * Method to encapsulate the process of locking/fetching a page.  First the method checks the local 
	 * cache ("dirtypages"), and if it can't find the requested page there, it fetches it from the buffer pool.  
	 * It also adds pages to the dirtypages cache if they are fetched with read-write permission, since 
	 * presumably they will soon be dirtied by this transaction.
	 * 
	 * This method is needed to ensure that page updates are not lost if the same pages are
	 * accessed multiple times.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - the id of the requested page
	 * @param perm - the requested permissions on the page
	 * @return the requested page
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private Page getPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePageId pid, Permissions perm) 
			throws DbException, TransactionAbortedException {
		if(dirtypages.containsKey(pid)) {
			// 尝试在脏页中找到了对应的page页
			return dirtypages.get(pid);
		}
		else {
			// 从bufferPool中查找
			Page p = Database.getBufferPool().getPage(tid, pid, perm);
			if(perm == Permissions.READ_WRITE) {
				// 如果权限是可写的话,说明很有可能在后面的事务中会被修改(提前加入脏页缓存)
				// 索引的修改可能会连锁的修改叶节点向上的各个祖先节点(对应的页都将被修改)
				dirtypages.put(pid, p);
			}
			return p;
		}
	}

	/**
	 * Insert a tuple into this BTreeFile, keeping the tuples in sorted order. 
	 * May cause pages to split if the page where tuple t belongs is full.
	 * 
	 * @param tid - the transaction id
	 * @param t - the tuple to insert
	 * @return a list of all pages that were dirtied by this operation. Could include
	 * many pages since parent pointers will need to be updated when an internal node splits.
	 * @see #splitLeafPage(TransactionId, HashMap, BTreeLeafPage, Field)
	 */
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		HashMap<PageId, Page> dirtypages = new HashMap<PageId, Page>();

		// get a read lock on the root pointer page and use it to locate the root page
		BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
		BTreePageId rootId = rootPtr.getRootId();

		if(rootId == null) { // the root has just been created, so set the root pointer to point to it		
			rootId = new BTreePageId(tableid, numPages(), BTreePageId.LEAF);
			rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
			rootPtr.setRootId(rootId);
		}

		// find and lock the left-most leaf page corresponding to the key field,
		// and split the leaf page if there are no more slots available
		BTreeLeafPage leafPage = findLeafPage(tid, dirtypages, rootId, Permissions.READ_WRITE, t.getField(keyField));
		if(leafPage.getNumEmptySlots() == 0) {
			leafPage = splitLeafPage(tid, dirtypages, leafPage, t.getField(keyField));	
		}

		// insert the tuple into the leaf page
		leafPage.insertTuple(t);

		ArrayList<Page> dirtyPagesArr = new ArrayList<Page>();
		dirtyPagesArr.addAll(dirtypages.values());
		return dirtyPagesArr;
	}
	
	/**
	 * Handle the case when a B+ tree page becomes less than half full due to deletions.
	 * If one of its siblings has extra tuples/entries, redistribute those tuples/entries.
	 * Otherwise merge with one of the siblings. Update pointers as needed.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the page which is less than half full
	 * @see #handleMinOccupancyLeafPage(TransactionId, HashMap, BTreeLeafPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
	 * @see #handleMinOccupancyInternalPage(TransactionId, HashMap, BTreeInternalPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public void handleMinOccupancyPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePage page)
			throws DbException, TransactionAbortedException, IOException {
		BTreePageId parentId = page.getParentId();
		BTreeEntry leftEntry = null;
		BTreeEntry rightEntry = null;
		BTreeInternalPage parent = null;

		// find the left and right siblings through the parent so we make sure they have
		// the same parent as the page. Find the entries in the parent corresponding to 
		// the page and siblings
		if(parentId.pgcateg() != BTreePageId.ROOT_PTR) {
			parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId, Permissions.READ_WRITE);
			Iterator<BTreeEntry> ite = parent.iterator();
			while(ite.hasNext()) {
				BTreeEntry e = ite.next();
				if(e.getLeftChild().equals(page.getId())) {
					rightEntry = e;
					break;
				}
				else if(e.getRightChild().equals(page.getId())) {
					leftEntry = e;
				}
			}
		}
		
		if(page.getId().pgcateg() == BTreePageId.LEAF) {
			handleMinOccupancyLeafPage(tid, dirtypages, (BTreeLeafPage) page, parent, leftEntry, rightEntry);
		}
		else { // BTreePageId.INTERNAL
			handleMinOccupancyInternalPage(tid, dirtypages, (BTreeInternalPage) page, parent, leftEntry, rightEntry);
		}
	}
	
	/**
	 * Handle the case when a leaf page becomes less than half full due to deletions.
	 * If one of its siblings has extra tuples, redistribute those tuples.
	 * Otherwise merge with one of the siblings. Update pointers as needed.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the leaf page which is less than half full
	 * @param parent - the parent of the leaf page
	 * @param leftEntry - the entry in the parent pointing to the given page and its left-sibling
	 * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
	 * @see #mergeLeafPages(TransactionId, HashMap, BTreeLeafPage, BTreeLeafPage, BTreeInternalPage, BTreeEntry)
	 * @see #stealFromLeafPage(BTreeLeafPage, BTreeLeafPage, BTreeInternalPage, BTreeEntry, boolean)
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void handleMinOccupancyLeafPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeLeafPage page, 
			BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
			throws DbException, TransactionAbortedException, IOException {
		BTreePageId leftSiblingId = null;
		BTreePageId rightSiblingId = null;
		if(leftEntry != null) leftSiblingId = leftEntry.getLeftChild();
		if(rightEntry != null) rightSiblingId = rightEntry.getRightChild();
		
		int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples()/2; // ceiling
		if(leftSiblingId != null) {
			BTreeLeafPage leftSibling = (BTreeLeafPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
			// if the left sibling is at minimum occupancy, merge with it. Otherwise
			// steal some tuples from it
			if(leftSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeLeafPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
			}
			else {
				stealFromLeafPage(page, leftSibling, parent, leftEntry, false);				
			}
		}
		else if(rightSiblingId != null) {	
			BTreeLeafPage rightSibling = (BTreeLeafPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
			// if the right sibling is at minimum occupancy, merge with it. Otherwise
			// steal some tuples from it
			if(rightSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeLeafPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
			}
			else {
				stealFromLeafPage(page, rightSibling, parent, rightEntry, true);				
			}
		}
	}
	
	/**
	 * Steal tuples from a sibling and copy them to the given page so that both pages are at least
	 * half full.  Update the parent's entry so that the key matches the key field of the first
	 * tuple in the right-hand page.
	 * 
	 * @param page - the leaf page which is less than half full
	 * @param sibling - the sibling which has tuples to spare
	 * @param parent - the parent of the two leaf pages
	 * @param entry - the entry in the parent pointing to the two leaf pages
	 * @param isRightSibling - whether the sibling is a right-sibling
	 * 
	 * @throws DbException
	 */
	public void stealFromLeafPage(BTreeLeafPage page, BTreeLeafPage sibling,
			BTreeInternalPage parent, BTreeEntry entry, boolean isRightSibling) throws DbException {
		// some code goes here
        // YOUR CODE HERE:
        // Move some of the tuples from the sibling to the page so
		// that the tuples are evenly distributed. Be sure to update
		// the corresponding parent entry.

        BTreeLeafPage rhs;
        int numTupleToMove = (sibling.getNumTuples() - page.getNumTuples()) / 2;
        // assert sibling.getNumTuples() - numTupleToMove >= (sibling.getMaxTuples() / 2);
        // assert page.getNumTuples() + numTupleToMove >= (page.getMaxTuples() / 2);
        Tuple[] tpToMove = new Tuple[numTupleToMove];
        int cntdown = tpToMove.length - 1;
        Iterator<Tuple> it;
        if (isRightSibling) {
            rhs = sibling;
            it = sibling.iterator();
        } else {
            // left sib
            rhs = page;
            it = sibling.reverseIterator();
        }
        while (cntdown >= 0 && it.hasNext()) {
            tpToMove[cntdown--] = it.next();
        }

        // keys are effectively "rotated" through the parent
        for (Tuple t : tpToMove) {
            // entry.setKey(t.getField(keyField));
            sibling.deleteTuple(t);
            page.insertTuple(t);
            // parent.updateEntry(entry);
        }

        // assert rhs.getTuple(0) != null;
        if (rhs.getNumTuples() > 0) {
            entry.setKey(rhs.iterator().next().getField(keyField));
            parent.updateEntry(entry);
        }

    }

	/**
	 * Handle the case when an internal page becomes less than half full due to deletions.
	 * If one of its siblings has extra entries, redistribute those entries.
	 * Otherwise merge with one of the siblings. Update pointers as needed.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page which is less than half full
	 * @param parent - the parent of the internal page
	 * @param leftEntry - the entry in the parent pointing to the given page and its left-sibling
	 * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
	 * @see #mergeInternalPages(TransactionId, HashMap, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
	 * @see #stealFromLeftInternalPage(TransactionId, HashMap, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
	 * @see #stealFromRightInternalPage(TransactionId, HashMap, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void handleMinOccupancyInternalPage(TransactionId tid, HashMap<PageId, Page> dirtypages, 
			BTreeInternalPage page, BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry) 
					throws DbException, IOException, TransactionAbortedException {
		BTreePageId leftSiblingId = null;
		BTreePageId rightSiblingId = null;
		if(leftEntry != null) leftSiblingId = leftEntry.getLeftChild();
		if(rightEntry != null) rightSiblingId = rightEntry.getRightChild();
		
		int maxEmptySlots = page.getMaxEntries() - page.getMaxEntries()/2; // ceiling
		if(leftSiblingId != null) {
			BTreeInternalPage leftSibling = (BTreeInternalPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
			// if the left sibling is at minimum occupancy, merge with it. Otherwise
			// steal some entries from it
			if(leftSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeInternalPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
			}
			else {
				stealFromLeftInternalPage(tid, dirtypages, page, leftSibling, parent, leftEntry);
			}
		}
		else if(rightSiblingId != null) {
			BTreeInternalPage rightSibling = (BTreeInternalPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
			// if the right sibling is at minimum occupancy, merge with it. Otherwise
			// steal some entries from it
			if(rightSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeInternalPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
			}
			else {
				stealFromRightInternalPage(tid, dirtypages, page, rightSibling, parent, rightEntry);
			}
		}
	}
	
	/**
	 * Steal entries from the left sibling and copy them to the given page so that both pages are at least
	 * half full. Keys can be thought of as rotating through the parent entry, so the original key in the 
	 * parent is "pulled down" to the right-hand page, and the last key in the left-hand page is "pushed up"
	 * to the parent.  Update parent pointers as needed.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page which is less than half full
	 * @param leftSibling - the left sibling which has entries to spare
	 * @param parent - the parent of the two internal pages
	 * @param leftEntry - the entry in the parent pointing to the two internal pages
	 * @see #updateParentPointers(TransactionId, HashMap, BTreeInternalPage)
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public void stealFromLeftInternalPage(TransactionId tid, HashMap<PageId, Page> dirtypages,
										  BTreeInternalPage page, BTreeInternalPage leftSibling, BTreeInternalPage parent,
										  BTreeEntry leftEntry) throws DbException, IOException, TransactionAbortedException {
		// some code goes here
        // YOUR CODE HERE:
        // Move some of the entries from the left sibling to the page so
		// that the entries are evenly distributed. Be sure to update
		// the corresponding parent entry. Be sure to update the parent
		// pointers of all children in the entries that were moved.
        int numToMove = (leftSibling.getNumEntries() - page.getNumEntries()) / 2;
        BTreeEntry[] entryToMove = new BTreeEntry[numToMove];

        Iterator<BTreeEntry> it = leftSibling.reverseIterator();
        int cntdown = entryToMove.length - 1;

        while (cntdown >= 0 && it.hasNext()) {
            entryToMove[cntdown--] = it.next();
        }

        for (int i=entryToMove.length-1; i >= 0; --i) {
            BTreeEntry entry = entryToMove[i];
            // rotated through the parent
            leftSibling.deleteKeyAndRightChild(entry);
            updateParentPointer(tid, dirtypages, page.getId(), entry.getRightChild());
            // entry from leftsib must be inserted into first place in right page
            BTreePageId pid = entry.getRightChild();
            entry.setLeftChild(leftEntry.getLeftChild());
            entry.setRightChild(leftEntry.getRightChild());
            entry.setRecordId(leftEntry.getRecordId());

            parent.updateEntry(entry);

            leftEntry.setLeftChild(pid);
            assert page.iterator().hasNext();
            leftEntry.setRightChild(page.iterator().next().getLeftChild());

            // entry.setLeftChild(entry.getRightChild());
            // entry.setRightChild(page.getChildId(0));
            page.insertEntry(leftEntry);
            leftEntry = entry;
        }
        dirtypages.put(parent.getId(), parent);
        dirtypages.put(leftSibling.getId(), leftSibling);
        dirtypages.put(page.getId(), page);
	}
	
	/**
	 * Steal entries from the right sibling and copy them to the given page so that both pages are at least
	 * half full. Keys can be thought of as rotating through the parent entry, so the original key in the 
	 * parent is "pulled down" to the left-hand page, and the last key in the right-hand page is "pushed up"
	 * to the parent.  Update parent pointers as needed.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page which is less than half full
	 * @param rightSibling - the right sibling which has entries to spare
	 * @param parent - the parent of the two internal pages
	 * @param rightEntry - the entry in the parent pointing to the two internal pages
	 * @see #updateParentPointers(TransactionId, HashMap, BTreeInternalPage)
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public void stealFromRightInternalPage(TransactionId tid, HashMap<PageId, Page> dirtypages,
										   BTreeInternalPage page, BTreeInternalPage rightSibling, BTreeInternalPage parent,
										   BTreeEntry rightEntry) throws DbException, IOException, TransactionAbortedException {
		// some code goes here
        // YOUR CODE HERE:
        // Move some of the entries from the right sibling to the page so
		// that the entries are evenly distributed. Be sure to update
		// the corresponding parent entry. Be sure to update the parent
		// pointers of all children in the entries that were moved.
        int numToMove = (rightSibling.getNumEntries() - page.getNumEntries()) / 2;
        BTreeEntry[] btentries = new BTreeEntry[numToMove];

        Iterator<BTreeEntry> it = rightSibling.iterator();
        int cntdown = 0;

        // make copy avoid copy while modify in lll
        while (cntdown < btentries.length && it.hasNext()) {
            btentries[cntdown++] = it.next();
        }

        for (int i=0; i < btentries.length; ++i) {
            BTreeEntry entry = btentries[i];
            // rotated through the parent
            rightSibling.deleteKeyAndLeftChild(entry);
            updateParentPointer(tid, dirtypages, page.getId(), entry.getLeftChild());

            BTreePageId pid = entry.getLeftChild();
            entry.setLeftChild(rightEntry.getLeftChild());
            entry.setRightChild(rightEntry.getRightChild());
            entry.setRecordId(rightEntry.getRecordId());

            parent.updateEntry(entry);

            rightEntry.setRightChild(pid);
            assert page.reverseIterator().hasNext();
            rightEntry.setLeftChild(page.reverseIterator().next().getRightChild());

            // entry.setLeftChild(entry.getRightChild());
            // entry.setRightChild(page.getChildId(0));
            page.insertEntry(rightEntry);
            rightEntry = entry;

        }
        dirtypages.put(parent.getId(), parent);
        dirtypages.put(page.getId(), page);
        dirtypages.put(rightSibling.getId(), rightSibling);
    }
	
	/**
	 * Merge two leaf pages by moving all tuples from the right page to the left page. 
	 * Delete the corresponding key and right child pointer from the parent, and recursively 
	 * handle the case when the parent gets below minimum occupancy.
	 * Update sibling pointers as needed, and make the right page available for reuse.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param leftPage - the left leaf page
	 * @param rightPage - the right leaf page
	 * @param parent - the parent of the two pages
	 * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
	 * @see #deleteParentEntry(TransactionId, HashMap, BTreePage, BTreeInternalPage, BTreeEntry)
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public void mergeLeafPages(TransactionId tid, HashMap<PageId, Page> dirtypages,
			BTreeLeafPage leftPage, BTreeLeafPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
			throws DbException, TransactionAbortedException, IOException {

		// some code goes here
        // YOUR CODE HERE: 
		// Move all the tuples from the right page to the left page, update
		// the sibling pointers, and make the right page available for reuse.
		// Delete the entry in the parent corresponding to the two pages that are merging -
		// deleteParentEntry() will be useful here
        Tuple[] tpToDelete = new Tuple[rightPage.getNumTuples()];
        int deleteCnt = tpToDelete.length - 1;
        Iterator<Tuple> it = rightPage.iterator();
        while (deleteCnt >= 0 && it.hasNext()) {
            tpToDelete[deleteCnt--] = it.next();
        }

        for (Tuple t : tpToDelete) {
            rightPage.deleteTuple(t);
            leftPage.insertTuple(t);
        }
        //
        BTreePageId rightSibId = rightPage.getRightSiblingId();
        leftPage.setRightSiblingId(rightSibId);
        if (rightSibId != null) {
            BTreeLeafPage rightSib = (BTreeLeafPage) getPage(tid, dirtypages, rightSibId, Permissions.READ_WRITE);
            rightSib.setLeftSiblingId(leftPage.getId());
            dirtypages.put(rightSibId, rightSib);
        }
        dirtypages.put(leftPage.getId(), leftPage);
        setEmptyPage(tid, dirtypages, rightPage.getId().pageNumber());

        deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);
    }

	/**
	 * Merge two internal pages by moving all entries from the right page to the left page 
	 * and "pulling down" the corresponding key from the parent entry. 
	 * Delete the corresponding key and right child pointer from the parent, and recursively 
	 * handle the case when the parent gets below minimum occupancy.
	 * Update parent pointers as needed, and make the right page available for reuse.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param leftPage - the left internal page
	 * @param rightPage - the right internal page
	 * @param parent - the parent of the two pages
	 * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
	 * @see #deleteParentEntry(TransactionId, HashMap, BTreePage, BTreeInternalPage, BTreeEntry)
	 * @see #updateParentPointers(TransactionId, HashMap, BTreeInternalPage)
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public void mergeInternalPages(TransactionId tid, HashMap<PageId, Page> dirtypages,
								   BTreeInternalPage leftPage, BTreeInternalPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
					throws DbException, IOException, TransactionAbortedException {
		
		// some code goes here
        // YOUR CODE HERE:
        // Move all the entries from the right page to the left page, update
		// the parent pointers of the children in the entries that were moved, 
		// and make the right page available for reuse
		// Delete the entry in the parent corresponding to the two pages that are merging -
		// deleteParentEntry() will be useful here
        deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);

        parentEntry.setLeftChild(leftPage.reverseIterator().next().getRightChild());
        parentEntry.setRightChild(rightPage.iterator().next().getLeftChild());

        leftPage.insertEntry(parentEntry);

        while (rightPage.iterator().hasNext()) {
            BTreeEntry entry = rightPage.iterator().next();
            rightPage.deleteKeyAndLeftChild(entry);
            updateParentPointer(tid, dirtypages, leftPage.getId(), entry.getLeftChild());
            updateParentPointer(tid, dirtypages, leftPage.getId(), entry.getRightChild());
            leftPage.insertEntry(entry);
        }
        dirtypages.put(leftPage.getId(), leftPage);
        setEmptyPage(tid, dirtypages, rightPage.getId().pageNumber());
	}
	
	/**
	 * Method to encapsulate the process of deleting an entry (specifically the key and right child) 
	 * from a parent node.  If the parent becomes empty (no keys remaining), that indicates that it 
	 * was the root node and should be replaced by its one remaining child.  Otherwise, if it gets 
	 * below minimum occupancy for non-root internal nodes, it should steal from one of its siblings or 
	 * merge with a sibling.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param leftPage - the child remaining after the key and right child are deleted
	 * @param parent - the parent containing the entry to be deleted
	 * @param parentEntry - the entry to be deleted
	 * @see #handleMinOccupancyPage(TransactionId, HashMap, BTreePage)
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void deleteParentEntry(TransactionId tid, HashMap<PageId, Page> dirtypages, 
			BTreePage leftPage, BTreeInternalPage parent, BTreeEntry parentEntry) 
					throws DbException, IOException, TransactionAbortedException {		
		
		// delete the entry in the parent.  If
		// the parent is below minimum occupancy, get some tuples from its siblings
		// or merge with one of the siblings
		parent.deleteKeyAndRightChild(parentEntry);
		int maxEmptySlots = parent.getMaxEntries() - parent.getMaxEntries()/2; // ceiling
		if(parent.getNumEmptySlots() == parent.getMaxEntries()) {
			// This was the last entry in the parent.
			// In this case, the parent (root node) should be deleted, and the merged 
			// page will become the new root
			BTreePageId rootPtrId = parent.getParentId();
			if(rootPtrId.pgcateg() != BTreePageId.ROOT_PTR) {
				throw new DbException("attempting to delete a non-root node");
			}
			BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, rootPtrId, Permissions.READ_WRITE);
			leftPage.setParentId(rootPtrId);
			rootPtr.setRootId(leftPage.getId());

			// release the parent page for reuse
			setEmptyPage(tid, dirtypages, parent.getId().pageNumber());
		}
		else if(parent.getNumEmptySlots() > maxEmptySlots) { 
			handleMinOccupancyPage(tid, dirtypages, parent);
		}
	}

	/**
	 * Delete a tuple from this BTreeFile. 
	 * May cause pages to merge or redistribute entries/tuples if the pages 
	 * become less than half full.
	 * 
	 * @param tid - the transaction id
	 * @param t - the tuple to delete
	 * @return a list of all pages that were dirtied by this operation. Could include
	 * many pages since parent pointers will need to be updated when an internal node merges.
	 * @see #handleMinOccupancyPage(TransactionId, HashMap, BTreePage)
	 */
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		HashMap<PageId, Page> dirtypages = new HashMap<>();

		BTreePageId pageId = new BTreePageId(tableid, t.getRecordId().getPageId().pageNumber(), 
				BTreePageId.LEAF);
		BTreeLeafPage page = (BTreeLeafPage) getPage(tid, dirtypages, pageId, Permissions.READ_WRITE);
		page.deleteTuple(t);

		// if the page is below minimum occupancy, get some tuples from its siblings
		// or merge with one of the siblings
		int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples()/2; // ceiling
		if(page.getNumEmptySlots() > maxEmptySlots) {
			try {
				handleMinOccupancyPage(tid, dirtypages, page);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		ArrayList<Page> dirtyPagesArr = new ArrayList<Page>();
		dirtyPagesArr.addAll(dirtypages.values());
		return dirtyPagesArr;
	}

	/**
	 * Get a read lock on the root pointer page. Create the root pointer page and root page
	 * if necessary.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages 
	 * @return the root pointer page
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private BTreeRootPtrPage getRootPtrPage(TransactionId tid, HashMap<PageId, Page> dirtypages) throws DbException, IOException, TransactionAbortedException {
		synchronized(this) {
			if(f.length() == 0) {
				// create the root pointer page and the root page
				BufferedOutputStream bw = new BufferedOutputStream(
						new FileOutputStream(f, true));
				byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
				byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
				bw.write(emptyRootPtrData);
				bw.write(emptyLeafData);
				bw.close();
			}
		}

		// get a read lock on the root pointer page
		return (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_ONLY);
	}

	/**
	 * Get the page number of the first empty page in this BTreeFile.
	 * Creates a new page if none of the existing pages are empty.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @return the page number of the first empty page
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public int getEmptyPageNo(TransactionId tid, HashMap<PageId, Page> dirtypages)
			throws DbException, IOException, TransactionAbortedException {
		// get a read lock on the root pointer page and use it to locate the first header page
		BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
		BTreePageId headerId = rootPtr.getHeaderId();
		int emptyPageNo = 0;

		if(headerId != null) {
			BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
			int headerPageCount = 0;
			// try to find a header page with an empty slot
			while(headerPage != null && headerPage.getEmptySlot() == -1) {
				headerId = headerPage.getNextPageId();
				if(headerId != null) {
					headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
					headerPageCount++;
				}
				else {
					headerPage = null;
				}
			}

			// if headerPage is not null, it must have an empty slot
			if(headerPage != null) {
				headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
				int emptySlot = headerPage.getEmptySlot();
				headerPage.markSlotUsed(emptySlot, true);
				emptyPageNo = headerPageCount * BTreeHeaderPage.getNumSlots() + emptySlot;
			}
		}

		// at this point if headerId is null, either there are no header pages 
		// or there are no free slots
		if(headerId == null) {		
			synchronized(this) {
				// create the new page
				BufferedOutputStream bw = new BufferedOutputStream(
						new FileOutputStream(f, true));
				byte[] emptyData = BTreeInternalPage.createEmptyPageData();
				bw.write(emptyData);
				bw.close();
				emptyPageNo = numPages();
			}
		}

		return emptyPageNo; 
	}
	
	/**
	 * Method to encapsulate the process of creating a new page.  It reuses old pages if possible,
	 * and creates a new page if none are available.  It wipes the page on disk and in the cache and 
	 * returns a clean copy locked with read-write permission
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pgcateg - the BTreePageId category of the new page.  Either LEAF, INTERNAL, or HEADER
	 * @return the new empty page
	 * @see #getEmptyPageNo(TransactionId, HashMap)
	 * @see #setEmptyPage(TransactionId, HashMap, int)
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private Page getEmptyPage(TransactionId tid, HashMap<PageId, Page> dirtypages, int pgcateg)
			throws DbException, IOException, TransactionAbortedException {
		// create the new page
		int emptyPageNo = getEmptyPageNo(tid, dirtypages);
		BTreePageId newPageId = new BTreePageId(tableid, emptyPageNo, pgcateg);
		
		// write empty page to disk
		RandomAccessFile rf = new RandomAccessFile(f, "rw");
		rf.seek(BTreeRootPtrPage.getPageSize() + (emptyPageNo-1) * BufferPool.getPageSize());
		rf.write(BTreePage.createEmptyPageData());
		rf.close();
		
		// make sure the page is not in the buffer pool	or in the local cache		
		Database.getBufferPool().discardPage(newPageId);
		dirtypages.remove(newPageId);
		
		return getPage(tid, dirtypages, newPageId, Permissions.READ_WRITE);
	}

	/**
	 * Mark a page in this BTreeFile as empty. Find the corresponding header page 
	 * (create it if needed), and mark the corresponding slot in the header page as empty.
	 * 
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param emptyPageNo - the page number of the empty page
	 * @see #getEmptyPage(TransactionId, HashMap, int)
	 * 
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public void setEmptyPage(TransactionId tid, HashMap<PageId, Page> dirtypages, int emptyPageNo)
			throws DbException, IOException, TransactionAbortedException {

		// if this is the last page in the file (and not the only page), just 
		// truncate the file
		// @TODO: Commented out because we should probably do this somewhere else in case the transaction aborts....
//		synchronized(this) {
//			if(emptyPageNo == numPages()) {
//				if(emptyPageNo <= 1) {
//					// if this is the only page in the file, just return.
//					// It just means we have an empty root page
//					return;
//				}
//				long newSize = f.length() - BufferPool.getPageSize();
//				FileOutputStream fos = new FileOutputStream(f, true);
//				FileChannel fc = fos.getChannel();
//				fc.truncate(newSize);
//				fc.close();
//				fos.close();
//				return;
//			}
//		}

		// otherwise, get a read lock on the root pointer page and use it to locate 
		// the first header page
		BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
		BTreePageId headerId = rootPtr.getHeaderId();
		BTreePageId prevId = null;
		int headerPageCount = 0;

		// if there are no header pages, create the first header page and update
		// the header pointer in the BTreeRootPtrPage
		if(headerId == null) {
			rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
			
			BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
			headerId = headerPage.getId();
			headerPage.init();
			rootPtr.setHeaderId(headerId);
		}

		// iterate through all the existing header pages to find the one containing the slot
		// corresponding to emptyPageNo
		while(headerId != null && (headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
			BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
			prevId = headerId;
			headerId = headerPage.getNextPageId();
			headerPageCount++;
		}

		// at this point headerId should either be null or set with 
		// the headerPage containing the slot corresponding to emptyPageNo.
		// Add header pages until we have one with a slot corresponding to emptyPageNo
		while((headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
			BTreeHeaderPage prevPage = (BTreeHeaderPage) getPage(tid, dirtypages, prevId, Permissions.READ_WRITE);
			
			BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
			headerId = headerPage.getId();
			headerPage.init();
			headerPage.setPrevPageId(prevId);
			prevPage.setNextPageId(headerId);
			
			headerPageCount++;
			prevId = headerId;
		}

		// now headerId should be set with the headerPage containing the slot corresponding to 
		// emptyPageNo
		BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
		int emptySlot = emptyPageNo - headerPageCount * BTreeHeaderPage.getNumSlots();
		headerPage.markSlotUsed(emptySlot, false);
	}

	/**
	 * get the specified tuples from the file based on its IndexPredicate value on
	 * behalf of the specified transaction. This method will acquire a read lock on
	 * the affected pages of the file, and may block until the lock can be
	 * acquired.
	 * 
	 * @param tid - the transaction id
	 * @param ipred - the index predicate value to filter on
	 * @return an iterator for the filtered tuples
	 */
	public DbFileIterator indexIterator(TransactionId tid, IndexPredicate ipred) {
		return new BTreeSearchIterator(this, tid, ipred);
	}

	/**
	 * Get an iterator for all tuples in this B+ tree file in sorted order. This method 
	 * will acquire a read lock on the affected pages of the file, and may block until 
	 * the lock can be acquired.
	 * 
	 * @param tid - the transaction id
	 * @return an iterator for all the tuples in this file
	 */
	public DbFileIterator iterator(TransactionId tid) {
		return new BTreeFileIterator(this, tid);
	}

}

/**
 * Helper class that implements the Java Iterator for tuples on a BTreeFile
 */
class BTreeFileIterator extends AbstractDbFileIterator {

	Iterator<Tuple> it = null;
	BTreeLeafPage curp = null;

	TransactionId tid;
	BTreeFile f;

	/**
	 * Constructor for this iterator
	 * @param f - the BTreeFile containing the tuples
	 * @param tid - the transaction id
	 */
	public BTreeFileIterator(BTreeFile f, TransactionId tid) {
		this.f = f;
		this.tid = tid;
	}

	/**
	 * Open this iterator by getting an iterator on the first leaf page
	 */
	public void open() throws DbException, TransactionAbortedException {
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
		BTreePageId root = rootPtr.getRootId();
		curp = f.findLeafPage(tid, root, Permissions.READ_ONLY, null);
		it = curp.iterator();
	}

	/**
	 * Read the next tuple either from the current page if it has more tuples or
	 * from the next page by following the right sibling pointer.
	 * 
	 * @return the next tuple, or null if none exists
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		if (it != null && !it.hasNext())
			it = null;

		while (it == null && curp != null) {
			BTreePageId nextp = curp.getRightSiblingId();
			if(nextp == null) {
				curp = null;
			}
			else {
				curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
						nextp, Permissions.READ_ONLY);
				it = curp.iterator();
				if (!it.hasNext())
					it = null;
			}
		}

		if (it == null)
			return null;
		return it.next();
	}

	/**
	 * rewind this iterator back to the beginning of the tuples
	 */
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	/**
	 * close the iterator
	 */
	public void close() {
		super.close();
		it = null;
		curp = null;
	}
}

/**
 * Helper class that implements the DbFileIterator for search tuples on a
 * B+ Tree File
 */
class BTreeSearchIterator extends AbstractDbFileIterator {

	Iterator<Tuple> it = null;
	BTreeLeafPage curp = null;

	TransactionId tid;
	BTreeFile f;
	IndexPredicate ipred;

	/**
	 * Constructor for this iterator
	 * @param f - the BTreeFile containing the tuples
	 * @param tid - the transaction id
	 * @param ipred - the predicate to filter on
	 */
	public BTreeSearchIterator(BTreeFile f, TransactionId tid, IndexPredicate ipred) {
		this.f = f;
		this.tid = tid;
		this.ipred = ipred;
	}

	/**
	 * Open this iterator by getting an iterator on the first leaf page applicable
	 * for the given predicate operation
	 */
	public void open() throws DbException, TransactionAbortedException {
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
		BTreePageId root = rootPtr.getRootId();
		if(ipred.getOp() == Predicate.Op.EQUALS || ipred.getOp() == Predicate.Op.GREATER_THAN
				|| ipred.getOp() == Predicate.Op.GREATER_THAN_OR_EQ) {
			curp = f.findLeafPage(tid, root, Permissions.READ_ONLY, ipred.getField());
		}
		else {
			curp = f.findLeafPage(tid, root, Permissions.READ_ONLY, null);
		}
		it = curp.iterator();
	}

	/**
	 * Read the next tuple either from the current page if it has more tuples matching
	 * the predicate or from the next page by following the right sibling pointer.
	 * 
	 * @return the next tuple matching the predicate, or null if none exists
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException, NoSuchElementException {
		while (it != null) {
			while (it.hasNext()) {
				Tuple t = it.next();
				if (t.getField(f.keyField()).compare(ipred.getOp(), ipred.getField())) {
					return t;
				}
				else if(ipred.getOp() == Predicate.Op.LESS_THAN || ipred.getOp() == Predicate.Op.LESS_THAN_OR_EQ) {
					// if the predicate was not satisfied and the operation is less than, we have
					// hit the end
					return null;
				}
				else if(ipred.getOp() == Predicate.Op.EQUALS &&
						t.getField(f.keyField()).compare(Predicate.Op.GREATER_THAN, ipred.getField())) {
					// if the tuple is now greater than the field passed in and the operation
					// is equals, we have reached the end
					return null;
				}
			}

			BTreePageId nextp = curp.getRightSiblingId();
			// if there are no more pages to the right, end the iteration
			if(nextp == null) {
				return null;
			}
			else {
				curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
						nextp, Permissions.READ_ONLY);
				it = curp.iterator();
			}
		}

		return null;
	}

	/**
	 * rewind this iterator back to the beginning of the tuples
	 */
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	/**
	 * close the iterator
	 */
	public void close() {
		super.close();
		it = null;
	}
}
