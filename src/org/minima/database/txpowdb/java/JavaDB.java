package org.minima.database.txpowdb.java;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import org.minima.database.txpowdb.TxPOWDBRow;
import org.minima.database.txpowdb.TxPowDB;
import org.minima.objects.TxPoW;
import org.minima.objects.base.MiniData;
import org.minima.objects.base.MiniNumber;

public class JavaDB implements TxPowDB{

	private Hashtable<String , JavaDBRow> mRowTable;
	private Hashtable<String , JavaDBRow> mDeletedRowTable;
	
	private ArrayList<JavaDBRow> mRows;
	private ArrayList<JavaDBRow> mDeletedRows;
	
	public JavaDB() {
		mRowTable = new Hashtable<>();
		mDeletedRowTable = new Hashtable<>();
		
		mRows = new ArrayList<>();
		mDeletedRows = new ArrayList<>();
	}

	@Override
	public TxPOWDBRow addTxPOWDBRow(TxPoW zTxPOW) {
		//Only add it once!
		TxPOWDBRow prev = findTxPOWDBRow(zTxPOW.getTxPowID());
		if(prev!=null) {
			return prev;
		} 
		
		//Create a new row
		JavaDBRow row = new JavaDBRow(zTxPOW);
		
		//Add it
//		mRows.add(row);
		mRowTable.put(zTxPOW.getTxPowID().to0xString(), row);
		
		return row;
	}

	/**
	 * Searched the Deleted Rows TOO!
	 */
	@Override
	public TxPOWDBRow findTxPOWDBRow(MiniData zTxPOWID) {
		String search = zTxPOWID.to0xString();
		
		JavaDBRow row = mRowTable.get(search);
		if(row != null) {
			return row;
		}
		
		row = mDeletedRowTable.get(search);
		if(row != null) {
			return row;
		}
		
		return null;
		
		
//		for(JavaDBRow row : mRows) {
//			if(row.getTxPOW().getTxPowID().isEqual(zTxPOWID)) {
//				return row;
//			}
//		}
//		
//		for(JavaDBRow row : mDeletedRows) {
//			if(row.getTxPOW().getTxPowID().isEqual(zTxPOWID)) {
//				return row;
//			}
//		}
//		
//		return null;
	}

	@Override
	public ArrayList<TxPOWDBRow> removeTxPOWInBlockLessThan(MiniNumber zBlockNumber) {
		ArrayList<TxPOWDBRow> removed = new ArrayList<>();
		
		Hashtable<String , JavaDBRow> newRowTable = new Hashtable<>();
//		ArrayList<JavaDBRow> newRows = new ArrayList<>();
		
		//The minimum block before its too late - TODO!
		MiniNumber minblock = zBlockNumber.add(MiniNumber.TEN);
		
		Enumeration<JavaDBRow> allrows = mRowTable.elements();
		while(allrows.hasMoreElements()) {
			JavaDBRow row = allrows.nextElement();
			
			if(row.isOnChainBlock()) {
				newRowTable.put(row.getTxPOW().getTxPowID().to0xString(), row);
				
				//Other wise the proofs are too old..
			}else if(!row.isInBlock() && row.getTxPOW().getBlockNumber().isMore(minblock)) {
				newRowTable.put(row.getTxPOW().getTxPowID().to0xString(), row);
				
				//It's in the chain
			}else if(row.isInBlock() && row.getInBlockNumber().isMoreEqual(zBlockNumber)) {
				newRowTable.put(row.getTxPOW().getTxPowID().to0xString(), row);
				
			}else {
				//Remove it..
				removed.add(row);
				
				//Add to the deleted rows
				deleteRow(row);
			}
		}
			
		
//		for(JavaDBRow row : mRows) {
//			
//			if(row.isOnChainBlock()) {
//				newRows.add(row);
//				
//				//Other wise the proofs are too old..
//			}else if(!row.isInBlock() && row.getTxPOW().getBlockNumber().isMore(minblock)) {
//				newRows.add(row);
//			
//				//It's in the chain
//			}else if(row.isInBlock() && row.getInBlockNumber().isMoreEqual(zBlockNumber)) {
//				newRows.add(row);
//			
//			}else {
//				//Remove it..
//				removed.add(row);
//				
//				//Add to the deleted rows
//				deleteRow(row);
//			}
//		}
		
		//re-assign
		mRowTable = newRowTable;
//		mRows = newRows;
		
		//Remove the deleted.. called periodically
		removeDeleted();
		
		//Return the removed..
		return removed;
	}
	
	private void deleteRow(JavaDBRow zRow) {
		zRow.deleteRow();
//		mDeletedRows.add(zRow);
		mDeletedRowTable.put(zRow.getTxPOW().getTxPowID().to0xString(), zRow);
	}

	private ArrayList<TxPOWDBRow> removeDeleted() {
		ArrayList<TxPOWDBRow> removed = new ArrayList<>();
		
		Hashtable<String , JavaDBRow> newDeletedRowTable = new Hashtable<>();
//		ArrayList<JavaDBRow> newDeletedRows = new ArrayList<>();

		//Keep for 1 HR in the past
		long timedelete = System.currentTimeMillis() - 1000*60*60;
		
		Enumeration<JavaDBRow> allrows = mDeletedRowTable.elements();
		while(allrows.hasMoreElements()) {
			JavaDBRow row = allrows.nextElement();
			if(row.getDeleteTime() == 0) {
//				newDeletedRows.add(row);
				newDeletedRowTable.put(row.getTxPOW().getTxPowID().to0xString(), row);
			}else if(row.getDeleteTime() > timedelete) {
				newDeletedRowTable.put(row.getTxPOW().getTxPowID().to0xString(), row);
//				newDeletedRows.add(row);
			}else {
				removed.add(row);
			}
		}
		
		//Reset
		mDeletedRowTable = newDeletedRowTable;
//		mDeletedRows = newDeletedRows;
		
		return removed;
	}

	@Override
	public ArrayList<TxPOWDBRow> getAllUnusedTxPOW() {
		ArrayList<TxPOWDBRow> ret = new ArrayList<>();
		
		
		Enumeration<JavaDBRow> allrows = mRowTable.elements();
		while(allrows.hasMoreElements()) {
			JavaDBRow row = allrows.nextElement();
		
			if(!row.isInBlock()) {
				ret.add(row);
			}
		}
		
		return ret;
	}

	@Override
	public int getCompleteSize() {
		return mRowTable.size()+ mDeletedRowTable.size();
	}

	@Override
	public void removeTxPOW(MiniData zTxPOWID) {
		mRowTable.remove(zTxPOWID.to0xString());
		
		ArrayList<JavaDBRow> newRows = new ArrayList<>();
		
		boolean found = false;
		for(JavaDBRow row : mRows) {
			if( !found && row.getTxPOW().getTxPowID().isEqual(zTxPOWID) ) {
				//There can be only one as the TxPoWID is unique
				found = true;
				
				//Add to the deleted rows..
				deleteRow(row);
				
			}else{
				//Keep it..
				newRows.add(row);
			}
		}
		
		//re-assign
		mRows = newRows;
	}

	@Override
	public ArrayList<TxPOWDBRow> getChildBlocksTxPOW(MiniData zParent) {
		ArrayList<TxPOWDBRow> ret = new ArrayList<>();
		
		for(JavaDBRow row : mRows) {
			if(row.getTxPOW().isBlock() && row.getTxPOW().getParentID().isEqual(zParent)) {
				ret.add(row);
			}
		}
		
		return ret;
	}

	@Override
	public ArrayList<TxPOWDBRow> getAllTxPOWDBRow() {
		ArrayList<TxPOWDBRow> copy = new ArrayList<>();
		for(TxPOWDBRow row : mRows) {
			copy.add(row);
		}
		return copy;
	}

	@Override
	public void resetAllInBlocks() {
		for(TxPOWDBRow row : mRows) {
			row.setIsInBlock(false);
			row.setOnChainBlock(false);
		}
	}

	@Override
	public void resetBlocksFromOnwards(MiniNumber zFromBlock) {
		for(TxPOWDBRow row : mRows) {
			if(row.isInBlock()) {
				if(row.getInBlockNumber().isMoreEqual(zFromBlock)) {
					row.setIsInBlock(false);
					row.setOnChainBlock(false);
				}
			}
		}
	}
	
	@Override
	public ArrayList<TxPOWDBRow> getAllBlocksMissingTransactions() {
		ArrayList<TxPOWDBRow> ret = new ArrayList<>();
		
		for(JavaDBRow row : mRows) {
			if( row.getTxPOW().isBlock() && row.getBlockState() == TxPOWDBRow.TXPOWDBROW_STATE_BASIC ) {
				ret.add(row);
			}
		}
		
		return ret;
	}

	@Override
	public void ClearDB() {
		mRows = new ArrayList<>();
		mDeletedRows = new ArrayList<>();
	}

	
	
}
